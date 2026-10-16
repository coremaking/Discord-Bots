from __future__ import annotations
import asyncio, contextlib, json, logging, os, random, signal
import sys, time, httpx, websockets
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional
from websockets.exceptions import ConnectionClosed
def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()

USER_TOKEN: str = _env("USER_TOKEN", "")

TARGET_GUILD_IDS: list[str] = [
    x for x in _env("TARGET_GUILD_IDS", "").split(",") if x.strip()
]

WATCH_GUILD_IDS: list[str] = [
    x for x in _env("WATCH_GUILD_IDS", "").split(",") if x.strip()
]

IGNORE_GUILD_IDS: set[str] = {
    x for x in _env("IGNORE_GUILD_IDS", "").split(",") if x.strip()
}

WHITELIST_CODES: set[str] = {
    x for x in _env("WHITELIST_CODES", "").split(",") if x.strip()
}

SUCCESS_WEBHOOK_URL: str = _env("SUCCESS_WEBHOOK_URL", "")
ERROR_WEBHOOK_URL: str = _env("ERROR_WEBHOOK_URL", "")
LOG_LEVEL: str = _env("LOG_LEVEL", "INFO")
LOG_FILE: Optional[str] = _env("LOG_FILE") or None
DEDUPE_WINDOW_S: float = 5.0
SNIPE_TIMEOUT_S: float = 8.0
MAX_RECONNECT_BACKOFF_S: float = 60.0
CLIENT_OS: str = "Windows"
CLIENT_BROWSER: str = "Chrome"
CLIENT_DEVICE: str = "PC"
API_BASE: str = "https://discord.com/api/v10"
GATEWAY_URL: str = "wss://gateway.discord.gg"

def setup_logging() -> logging.Logger:
    log = logging.getLogger("sniper")
    log.setLevel(LOG_LEVEL.upper())
    log.propagate = False
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%H:%M:%S",
    )
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    log.addHandler(sh)
    if LOG_FILE:
        fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
        fh.setFormatter(fmt)
        log.addHandler(fh)
    return log

LOG = setup_logging()

class Op:
    DISPATCH = 0
    HEARTBEAT = 1
    IDENTIFY = 2
    PRESENCE_UPDATE = 3
    RESUME = 6
    RECONNECT = 7
    INVALID_SESSION = 9
    HELLO = 10
    HEARTBEAT_ACK = 11

@dataclass
class GuildState:
    id: str
    vanity_url_code: Optional[str] = None
    last_update: float = field(default_factory=time.monotonic)

@dataclass
class SnipeResult:
    target_guild_id: str
    code: str
    ok: bool
    status: int
    latency_ms: float
    live_code: Optional[str] = None
    error: Any = None

async def post_webhook(client: httpx.AsyncClient, url: str, content: str) -> None:
    if not url:
        return
    try:
        await client.post(
            url,
            json={
                "content": content[:2000],
                "allowed_mentions": {"parse": ["everyone"]},
            },
        )
    except Exception as exc:  # noqa: BLE001
        LOG.debug("Webhook post failed: %s", exc)

class Sniper:
    def __init__(
        self,
        user_token: str,
        target_guild_ids: Iterable[str],
    ) -> None:
        self.token = user_token
        self.targets: list[str] = [str(x) for x in target_guild_ids if str(x).strip()]
        self.guilds: dict[str, GuildState] = {}
        self._recent_attempts: dict[str, float] = {}
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.session_id: Optional[str] = None
        self.resume_gateway_url: Optional[str] = None
        self.sequence: Optional[int] = None
        self._hb_task: Optional[asyncio.Task] = None
        self._closed = asyncio.Event()
        self.http = httpx.AsyncClient(
            http2=True,
            timeout=httpx.Timeout(10.0, connect=5.0),
            limits=httpx.Limits(max_connections=40, max_keepalive_connections=20),
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept": "*/*",
                "Accept-Language": "en-US,en;q=0.9",
            },
            transport=httpx.AsyncHTTPTransport(retries=2),
        )

    async def run(self) -> None:
        backoff = 1.0
        while not self._closed.is_set():
            try:
                await self._gateway_session()
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                LOG.exception("Gateway error: %s", exc)
            if self._closed.is_set():
                break
            sleep_for = min(MAX_RECONNECT_BACKOFF_S, backoff) * (0.5 + random.random())
            LOG.warning("Reconnecting in %.2fs…", sleep_for)
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._closed.wait(), timeout=sleep_for)
            backoff = min(MAX_RECONNECT_BACKOFF_S, backoff * 2)
        await self.http.aclose()

    async def shutdown(self) -> None:
        LOG.info("Shutting down…")
        self._closed.set()
        if self._hb_task and not self._hb_task.done():
            self._hb_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._hb_task
        if self.ws:
            with contextlib.suppress(Exception):
                await self.ws.close(code=1000, reason="shutdown")

    async def _gateway_session(self) -> None:
        base = self.resume_gateway_url or GATEWAY_URL
        url = f"{base}/?v=10&encoding=json"
        LOG.info("Connecting to %s", url)
        async with websockets.connect(
            url, max_size=2**24, ping_interval=None, close_timeout=5,
        ) as ws:
            self.ws = ws
            hello = json.loads(await ws.recv())
            if hello.get("op") != Op.HELLO:
                raise RuntimeError(f"Expected HELLO, got {hello!r}")
            interval_ms = int(hello["d"]["heartbeat_interval"])
            LOG.debug("Heartbeat interval = %sms", interval_ms)
            if self.session_id and self.resume_gateway_url:
                await self._send(
                    {"op": Op.RESUME, "d": {
                        "token": self.token,
                        "session_id": self.session_id,
                        "seq": self.sequence,
                    }}
                )
                LOG.info("→ RESUME (seq=%s)", self.sequence)
            else:
                await self._send({
                    "op": Op.IDENTIFY,
                    "d": {
                        "token": self.token,
                        "capabilities": 16381,
                        "compress": False,
                        "properties": {
                            "os": CLIENT_OS,
                            "browser": CLIENT_BROWSER,
                            "device": CLIENT_DEVICE,
                        },
                        "intents": 1 << 0,
                        "presence": {
                            "status": "online",
                            "since": 0,
                            "activities": [],
                            "afk": False,
                        },
                    },
                })
                LOG.info("→ IDENTIFY")
            self._hb_task = asyncio.create_task(
                self._heartbeat(interval_ms / 1000.0)
            )
            try:
                async for raw in ws:
                    await self._on_message(raw)
            finally:
                if self._hb_task and not self._hb_task.done():
                    self._hb_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await self._hb_task

    async def _send(self, obj: dict[str, Any]) -> None:
        if self.ws:
            await self.ws.send(json.dumps(obj))

    async def _heartbeat(self, interval: float) -> None:
        await asyncio.sleep(interval * random.random())
        while not self._closed.is_set():
            try:
                await self._send({"op": Op.HEARTBEAT, "d": self.sequence})
                LOG.debug("→ HEARTBEAT seq=%s", self.sequence)
            except ConnectionClosed:
                return
            except Exception as exc:  # noqa: BLE001
                LOG.debug("Heartbeat failed: %s", exc)
                return
            await asyncio.sleep(interval)

    async def _on_message(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            LOG.warning("Non-JSON message received.")
            return
        if msg.get("s") is not None:
            self.sequence = msg["s"]
        op = msg.get("op")
        if op == Op.DISPATCH:
            await self._on_dispatch(msg)
        elif op == Op.HEARTBEAT:
            await self._send({"op": Op.HEARTBEAT, "d": self.sequence})
        elif op == Op.HEARTBEAT_ACK:
            LOG.debug("← HEARTBEAT_ACK")
        elif op == Op.RECONNECT:
            LOG.warning("Server → RECONNECT")
            if self.ws:
                await self.ws.close(code=4000)
        elif op == Op.INVALID_SESSION:
            resumable = bool(msg.get("d"))
            LOG.warning("INVALID_SESSION (resumable=%s)", resumable)
            if not resumable:
                self.session_id = None
                self.resume_gateway_url = None
            if self.ws:
                await self.ws.close(code=4000)

    async def _on_dispatch(self, msg: dict[str, Any]) -> None:
        t = msg.get("t")
        d = msg.get("d") or {}
        if t == "READY":
            self.session_id = d.get("session_id")
            self.resume_gateway_url = d.get("resume_gateway_url")
            user = d.get("user") or {}
            LOG.info("READY as %s (session=%s)", user.get("username", "?"), self.session_id)
            self.guilds.clear()
            await post_webhook(
                self.http,
                SUCCESS_WEBHOOK_URL,
                f"Sniper online. Targets: {len(self.targets)} guild(s). "
                f"Watching for freed vanity URLs.",
            )

        elif t == "RESUMED":
            LOG.info("Session resumed.")

        elif t == "GUILD_CREATE":
            gid = str(d.get("id"))
            if gid in IGNORE_GUILD_IDS:
                return
            if WATCH_GUILD_IDS and gid not in WATCH_GUILD_IDS:
                return
            self.guilds[gid] = GuildState(id=gid, vanity_url_code=d.get("vanity_url_code"))
            LOG.debug("GUILD_CREATE %s vanity=%s", gid, d.get("vanity_url_code"))

        elif t == "GUILD_UPDATE":
            await self._on_guild_update(d)

        elif t == "GUILD_DELETE":
            gid = str(d.get("id"))
            state = self.guilds.pop(gid, None)
            if not state or not state.vanity_url_code:
                return
            LOG.info("GUILD_DELETE %s freed '%s'", gid, state.vanity_url_code)
            self._spawn_snipe(state.vanity_url_code, reason="guild_deleted")

    async def _on_guild_update(self, d: dict[str, Any]) -> None:
        gid = str(d.get("id"))
        if gid in IGNORE_GUILD_IDS:
            return
        if WATCH_GUILD_IDS and gid not in WATCH_GUILD_IDS:
            return
        new_vanity = d.get("vanity_url_code")
        state = self.guilds.get(gid)
        if state is None:
            self.guilds[gid] = GuildState(id=gid, vanity_url_code=new_vanity)
            return
        old_vanity = state.vanity_url_code
        state.vanity_url_code = new_vanity
        state.last_update = time.monotonic()
        if new_vanity or not old_vanity:
            return
        LOG.info("Freed '%s' on %s → sniping", old_vanity, gid)
        self._spawn_snipe(old_vanity, reason="vanity_cleared")

    def _spawn_snipe(self, code: str, reason: str) -> None:
        now = time.monotonic()
        if now - self._recent_attempts.get(code, 0.0) < DEDUPE_WINDOW_S:
            LOG.debug("Dedupe skip for '%s'", code)
            return
        self._recent_attempts[code] = now
        if WHITELIST_CODES and code not in WHITELIST_CODES:
            LOG.debug("Not whitelisted: '%s'", code)
            return
        if not self.targets:
            LOG.error("No target guilds configured; cannot snipe '%s'.", code)
            return
        asyncio.create_task(self._race_targets(code, reason), name=f"snipe:{code}")

    async def _race_targets(self, code: str, reason: str) -> None:
        LOG.info("Racing '%s' across %d target(s).", code, len(self.targets))
        t0 = time.perf_counter()
        tasks = {
            asyncio.create_task(
                self._patch_target(gid, code, reason),
                name=f"patch:{gid}:{code}",
            ): gid
            for gid in self.targets
        }
        winner: Optional[SnipeResult] = None
        try:
            for coro in asyncio.as_completed(tasks.keys(), timeout=SNIPE_TIMEOUT_S):
                result = await coro
                prefix = "✅" if result.ok else "❌"
                LOG.info(
                    "%s target %s → HTTP %s (%.0fms)",
                    prefix, result.target_guild_id, result.status, result.latency_ms,
                )
                if result.ok:
                    winner = result
                    break
        except asyncio.TimeoutError:
            LOG.warning("Race for '%s' timed out.", code)
        finally:
            for t in tasks:
                if not t.done():
                    t.cancel()
            with contextlib.suppress(Exception):
                await asyncio.gather(*tasks, return_exceptions=True)
        elapsed = (time.perf_counter() - t0) * 1000.0
        if winner:
            await post_webhook(
                self.http,
                SUCCESS_WEBHOOK_URL,
                f"**Sniped** `discord.gg/{winner.live_code or winner.code}` "
                f"on guild `{winner.target_guild_id}` "
                f"in `{winner.latency_ms:.0f}ms` "
                f"(race total `{elapsed:.0f}ms`). ||@everyone||",
            )
        else:
            await post_webhook(
                self.http,
                ERROR_WEBHOOK_URL,
                f"Race for `{code}` failed on all targets "
                f"({len(self.targets)} tried, `{elapsed:.0f}ms`).",
            )

    async def _patch_target(
        self, guild_id: str, code: str, reason: str,
    ) -> SnipeResult:
        url = f"{API_BASE}/guilds/{guild_id}/vanity-url"
        headers = {
            "Authorization": self.token,
            "Content-Type": "application/json",
            "X-Audit-Log-Reason": f"Sniper: {reason}"[:512],
        }
        t0 = time.perf_counter()
        try:
            r = await self.http.patch(url, headers=headers, json={"code": code})
        except Exception as exc:  # noqa: BLE001
            return SnipeResult(
                target_guild_id=guild_id, code=code, ok=False,
                status=0, latency_ms=(time.perf_counter() - t0) * 1000.0,
                error=str(exc),
            )
        latency = (time.perf_counter() - t0) * 1000.0
        if r.status_code in (200, 201):
            live = None
            with contextlib.suppress(Exception):
                live = (r.json() or {}).get("code")
            return SnipeResult(
                target_guild_id=guild_id, code=code, ok=True,
                status=r.status_code, latency_ms=latency, live_code=live,
            )
        try:
            err = r.json()
        except Exception:  # noqa: BLE001
            err = r.text
        return SnipeResult(
            target_guild_id=guild_id, code=code, ok=False,
            status=r.status_code, latency_ms=latency, error=err,
        )

async def amain() -> None:
    if not USER_TOKEN:
        LOG.error(
            "USER_TOKEN is empty. Set it in main.py or via the USER_TOKEN "
            "env var."
        )
        sys.exit(1)
    if not TARGET_GUILD_IDS:
        LOG.error(
            "TARGET_GUILD_IDS is empty. Put at least one guild ID you control "
            "in the CONFIG block or TARGET_GUILD_IDS env var."
        )
        sys.exit(1)

    LOG.info("Targets: %s", ", ".join(TARGET_GUILD_IDS))
    if WATCH_GUILD_IDS:
        LOG.info("Watch list: %s", ", ".join(WATCH_GUILD_IDS))
    if WHITELIST_CODES:
        LOG.info("Whitelist: %s", ", ".join(sorted(WHITELIST_CODES)))
    sniper = Sniper(USER_TOKEN, TARGET_GUILD_IDS)
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    def _stop(*_: Any) -> None:
        stop.set()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, _stop)
    run_task = asyncio.create_task(sniper.run(), name="sniper")
    stop_task = asyncio.create_task(stop.wait(), name="stop")
    done, pending = await asyncio.wait(
        {run_task, stop_task}, return_when=asyncio.FIRST_COMPLETED,
    )
    for t in pending:
        t.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await t
    await sniper.shutdown()

def main() -> None:
    try:
        asyncio.run(amain())
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
