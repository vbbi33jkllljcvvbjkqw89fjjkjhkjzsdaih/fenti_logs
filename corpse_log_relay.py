import errno
import io
import json
import os
import queue
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


BOT_TOKEN = os.environ.get("DISCORD_BOT_TOKEN", "").strip()
CHANNEL_ID = os.environ.get("DISCORD_CHANNEL_ID", "").strip()
SHARED_SECRET = os.environ.get("RELAY_SHARED_SECRET", "").strip()
# Render, Railway, and similar hosts set PORT; local dev can use RELAY_PORT.
PORT = int(os.environ.get("PORT", os.environ.get("RELAY_PORT", "8787")))
# Log full JSON body on each POST (noisy; never enable on a shared relay).
RELAY_LOG_FULL_BODY = os.environ.get("RELAY_LOG_FULL_BODY", "").strip() in ("1", "true", "yes")
# Default True: return 202 immediately and post to Discord in a background thread (stops Roblox/Render
# timeouts and BrokenPipeError when CF 1015 forces multi-minute backoff). Set RELAY_ASYNC_DISCORD=0 to wait for Discord in-request.
def _relay_discord_async() -> bool:
    v = os.environ.get("RELAY_ASYNC_DISCORD", "").strip().lower()
    if v in ("0", "false", "no", "off", "sync"):
        return False
    return True


RELAY_ASYNC_DISCORD = _relay_discord_async()
# Bounded queue: one worker drains to Discord so parallel POSTs do not stack concurrent 1015 retry loops.
try:
    RELAY_DISCORD_QUEUE_MAX = max(1, int(os.environ.get("RELAY_DISCORD_QUEUE_MAX", "80")))
except ValueError:
    RELAY_DISCORD_QUEUE_MAX = 80
_discord_job_queue: queue.Queue = queue.Queue(maxsize=RELAY_DISCORD_QUEUE_MAX)
_discord_worker_lock = threading.Lock()
_discord_worker_started = False


def _log_incoming_corpse_payload(incoming):
    """Safe one-line summary so Render logs show shape without echoing secrets."""
    if not isinstance(incoming, dict):
        print(f"[relay] corpse-log incoming type={type(incoming).__name__}", flush=True)
        return
    keys = sorted(incoming.keys())
    emb = incoming.get("embeds")
    n = len(emb) if isinstance(emb, list) else -1
    hint = ""
    if isinstance(emb, list) and emb and isinstance(emb[0], dict):
        t = emb[0].get("title")
        if isinstance(t, str) and t:
            hint = f" first_title_len={len(t)}"
    print(f"[relay] corpse-log JSON keys={keys} embed_count={n}{hint}", flush=True)
    if RELAY_LOG_FULL_BODY:
        try:
            print(f"[relay] corpse-log FULL_BODY={json.dumps(incoming)[:8000]}", flush=True)
        except Exception as exc:
            print(f"[relay] corpse-log FULL_BODY encode error {exc!r}", flush=True)


def discord_headers():
    return {
        "Authorization": f"Bot {BOT_TOKEN}",
        "Content-Type": "application/json",
        "User-Agent": "fenti-corpse-relay/1.0",
    }


def discord_post_message(payload):
    if not BOT_TOKEN:
        raise RuntimeError("DISCORD_BOT_TOKEN is not set")
    if not CHANNEL_ID:
        raise RuntimeError("DISCORD_CHANNEL_ID is not set")

    url = f"https://discord.com/api/v10/channels/{CHANNEL_ID}/messages"
    body_bytes = json.dumps(payload).encode("utf-8")
    hdrs = discord_headers()

    # 429 + Cloudflare 1015 often hits shared host egress (e.g. Render); needs longer waits than API JSON retry_after.
    max_attempts = 7
    for attempt in range(1, max_attempts + 1):
        req = Request(url, data=body_bytes, headers=hdrs, method="POST")
        try:
            with urlopen(req, timeout=25) as resp:
                out = resp.read().decode("utf-8", errors="replace")
                sc = getattr(resp, "status", None) or getattr(resp, "code", None) or "?"
                print(f"[relay] Discord POST ok http_status={sc} response_len={len(out)}", flush=True)
                return out
        except HTTPError as exc:
            err_body = exc.read().decode("utf-8", errors="replace")
            print(f"[relay] Discord HTTPError {exc.code} attempt={attempt} body={err_body[:400]}", flush=True)
            is_cf1015 = "1015" in err_body or (
                exc.code == 429 and "cloudflare" in err_body.lower()
            )
            if is_cf1015 and attempt == 1:
                print(
                    "[relay] Discord returned Cloudflare 1015 / edge throttle — not your JSON. "
                    "Common on shared cloud egress; space out messages or run the relay on a VPS with a calmer IP.",
                    flush=True,
                )
            if exc.code == 429 and attempt < max_attempts:
                wait_sec = 2.0
                try:
                    j = json.loads(err_body)
                    if isinstance(j, dict) and isinstance(j.get("retry_after"), (int, float)):
                        wait_sec = float(j["retry_after"]) + 0.6
                except Exception:
                    pass
                try:
                    if exc.headers:
                        ra = exc.headers.get("Retry-After") or exc.headers.get("retry-after")
                        if ra:
                            wait_sec = max(wait_sec, float(ra) + 0.35)
                except Exception:
                    pass
                if is_cf1015:
                    # Do not cap at 60s — CF 1015 often needs longer; still bound so one job cannot hang forever.
                    wait_sec = max(wait_sec, 20.0 + float(attempt) * 22.0)
                    wait_sec = min(max(wait_sec, 5.0), 240.0)
                else:
                    wait_sec = min(max(wait_sec, 1.0), 60.0)
                print(f"[relay] 429/rate-limit backoff sleeping {wait_sec:.2f}s", flush=True)
                time.sleep(wait_sec)
                continue
            raise HTTPError(
                exc.url,
                exc.code,
                exc.msg,
                exc.headers,
                io.BytesIO(err_body.encode("utf-8")),
            ) from exc
    raise RuntimeError("Discord POST failed after retries")


def _deliver_discord_background(outgoing):
    try:
        discord_post_message(outgoing)
        print("[relay] async: Discord delivery finished OK", flush=True)
    except HTTPError as exc:
        try:
            detail = exc.read().decode("utf-8", errors="replace")
        except Exception:
            detail = ""
        print(f"[relay] async: Discord HTTPError {exc.code} detail={detail[:900]}", flush=True)
    except URLError as exc:
        print(f"[relay] async: Discord URLError {exc}", flush=True)
    except Exception as exc:
        print(f"[relay] async: delivery error {exc!r}", flush=True)


def _discord_queue_worker_loop():
    while True:
        job = _discord_job_queue.get()
        try:
            _deliver_discord_background(job)
        finally:
            _discord_job_queue.task_done()


def _ensure_discord_queue_worker():
    global _discord_worker_started
    with _discord_worker_lock:
        if _discord_worker_started:
            return
        _discord_worker_started = True
        threading.Thread(
            target=_discord_queue_worker_loop,
            daemon=True,
            name="discord-queue-worker",
        ).start()
        print("[relay] started single Discord queue worker (serializes outbound posts)", flush=True)


def _enqueue_discord_outgoing(outgoing) -> tuple[bool, int]:
    """Returns (ok, depth_after_put)."""
    _ensure_discord_queue_worker()
    depth_before = _discord_job_queue.qsize()
    try:
        _discord_job_queue.put_nowait(outgoing)
    except queue.Full:
        return False, depth_before
    depth_after = _discord_job_queue.qsize()
    if depth_before > 0 or depth_after > 1:
        print(
            f"[relay] Discord job queued (was_waiting={depth_before} now_queued≈{depth_after}) — "
            "single worker avoids parallel Cloudflare 1015 retry storms",
            flush=True,
        )
    return True, depth_after


class RelayHandler(BaseHTTPRequestHandler):
    server_version = "FentiCorpseRelay/1.0"

    def _log_access(self, note: str) -> None:
        print(f"[relay] {note}", flush=True)

    def _reply(self, status_code, body):
        raw = json.dumps(body).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        try:
            self.wfile.write(raw)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError) as exc:
            print(f"[relay] client closed socket before response finished ({type(exc).__name__})", flush=True)
        except OSError as exc:
            # WinError 10053 / EPIPE / ECONNRESET when client already disconnected (e.g. long Discord retries).
            if exc.errno in (errno.EPIPE, errno.ECONNRESET, 10053):
                print(f"[relay] client gone during response write (errno={exc.errno})", flush=True)
            else:
                raise

    def do_GET(self):
        self._log_access(f"GET {self.path} from {self.client_address[0]}")
        if self.path in ("/", "/health"):
            self._reply(
                200,
                {
                    "ok": True,
                    "service": "fenti-corpse-relay",
                    "bot_token_set": bool(BOT_TOKEN),
                    "channel_id_set": bool(CHANNEL_ID),
                    "relay_auth_required": bool(SHARED_SECRET),
                    "post_path": "/api/corpse-log",
                    "discord_delivery": "async" if RELAY_ASYNC_DISCORD else "sync",
                    "discord_queue_max": RELAY_DISCORD_QUEUE_MAX if RELAY_ASYNC_DISCORD else None,
                    "discord_queue_depth_approx": _discord_job_queue.qsize() if RELAY_ASYNC_DISCORD else None,
                    "note": "This service uses a bot token + channel id (DISCORD_BOT_TOKEN, DISCORD_CHANNEL_ID), not a webhook URL. Bots that only POST messages may stay offline in the member list; that is normal.",
                },
            )
            return
        # Browsers open this URL with GET — only POST is valid for logging.
        if self.path == "/api/corpse-log" or self.path.startswith("/api/corpse-log?"):
            self._reply(
                200,
                {
                    "ok": False,
                    "error": "method_not_allowed",
                    "hint": "Open this in a browser as GET / or GET /health to verify the service. Logging requires HTTP POST with JSON body + Authorization: Bearer <RELAY_SHARED_SECRET> (from Roblox executor or curl), not a browser visit.",
                    "try_get": ["/", "/health"],
                },
            )
            return
        self._reply(404, {"ok": False, "error": "not_found"})

    def do_POST(self):
        self._log_access(f"POST {self.path} from {self.client_address[0]}")
        if self.path != "/api/corpse-log":
            self._reply(404, {"ok": False, "error": "not_found"})
            return

        if SHARED_SECRET:
            # Header names are case-insensitive; strip CRLF/spaces from sloppy clients.
            auth = (self.headers.get("Authorization") or "").strip()
            expected = f"Bearer {SHARED_SECRET}".strip()
            if auth != expected:
                got_secret_part = auth[7:].strip() if auth.lower().startswith("bearer ") else auth
                print(
                    f"[relay] 401 unauthorized auth_len={len(auth)} expected_len={len(expected)} "
                    f"has_bearer_prefix={auth.lower().startswith('bearer ')} bearer_token_len={len(got_secret_part)} "
                    f"env_secret_len={len(SHARED_SECRET)} — RELAY_SHARED_SECRET on Render must equal the token after 'Bearer '.",
                    flush=True,
                )
                self._reply(401, {"ok": False, "error": "unauthorized"})
                return

        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self._reply(400, {"ok": False, "error": "bad_content_length"})
            return

        raw = self.rfile.read(length)
        try:
            incoming = json.loads(raw.decode("utf-8"))
        except Exception:
            self._reply(400, {"ok": False, "error": "bad_json"})
            return

        if not isinstance(incoming, dict):
            self._reply(400, {"ok": False, "error": "json_object_required"})
            return

        _log_incoming_corpse_payload(incoming)

        outgoing = {
            "username": str(incoming.get("username") or "fenti corpse sniper")[:80],
            "avatar_url": incoming.get("avatar_url"),
            "embeds": incoming.get("embeds") if isinstance(incoming.get("embeds"), list) else [],
            "allowed_mentions": {"parse": []},
        }

        if not outgoing["embeds"]:
            print("[relay] 400 missing or empty embeds after normalize (executor must send embeds: [])", flush=True)
            self._reply(400, {"ok": False, "error": "missing_embeds"})
            return

        print(
            f"[relay] calling Discord channels/.../messages (embeds={len(outgoing['embeds'])}, "
            f"channel_id_suffix=…{CHANNEL_ID[-6:] if len(CHANNEL_ID) >= 6 else CHANNEL_ID})",
            flush=True,
        )
        if RELAY_ASYNC_DISCORD:
            ok_q, depth = _enqueue_discord_outgoing(outgoing)
            if not ok_q:
                print(
                    f"[relay] 503 discord queue full (max={RELAY_DISCORD_QUEUE_MAX}) — slow consumer / CF 1015; retry later",
                    flush=True,
                )
                self._reply(
                    503,
                    {
                        "ok": False,
                        "error": "discord_queue_full",
                        "max": RELAY_DISCORD_QUEUE_MAX,
                        "hint": "Too many pending Discord deliveries. Wait and retry, or host relay on calmer egress if CF 1015 persists.",
                    },
                )
                return
            print("[relay] POST accepted → Discord delivery queued (check logs for CF 1015 / result)", flush=True)
            self._reply(
                202,
                {
                    "ok": True,
                    "accepted": True,
                    "discord": "async",
                    "queue_depth_approx": depth,
                    "hint": "One background worker drains the queue so concurrent posts do not parallelize 1015 retries. Failures appear only in service logs.",
                },
            )
            return

        try:
            discord_post_message(outgoing)
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            print(f"[relay] Discord HTTPError code={exc.code} detail={detail[:900]}", flush=True)
            self._reply(
                502,
                {"ok": False, "error": "discord_http_error", "status": exc.code, "detail": detail},
            )
            return
        except URLError as exc:
            print(f"[relay] Discord URLError {exc}", flush=True)
            self._reply(502, {"ok": False, "error": "discord_network_error", "detail": str(exc)})
            return
        except Exception as exc:
            print(f"[relay] relay_error {exc!r}", flush=True)
            self._reply(500, {"ok": False, "error": "relay_error", "detail": str(exc)})
            return

        print("[relay] POST /api/corpse-log -> Discord ok", flush=True)
        self._reply(200, {"ok": True})

    def log_message(self, format, *args):
        # Default handler is silent on Render — print so “Logs” shows every hit.
        try:
            print("%s - %s" % (self.address_string(), format % args), flush=True)
        except Exception:
            pass


if __name__ == "__main__":
    print(
        f"[relay] boot PORT={PORT} bot_token={'set' if BOT_TOKEN else 'MISSING'} "
        f"channel_id={'set' if CHANNEL_ID else 'MISSING'} shared_secret={'set' if SHARED_SECRET else 'off'} "
        f"async_discord={'on' if RELAY_ASYNC_DISCORD else 'off (RELAY_ASYNC_DISCORD=0)'} "
        f"discord_queue_max={RELAY_DISCORD_QUEUE_MAX if RELAY_ASYNC_DISCORD else 'n/a'}"
    )
    if RELAY_ASYNC_DISCORD:
        _ensure_discord_queue_worker()
    server = ThreadingHTTPServer(("0.0.0.0", PORT), RelayHandler)
    print(f"[relay] listening 0.0.0.0:{PORT}")
    server.serve_forever()
