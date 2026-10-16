from __future__ import annotations
import asyncio, contextlib, json, logging, websockets
import os, random, signal, sys, time, httpx
from dataclasses import dataclass, field
from typing import Any, Optional
from websockets.exceptions import ConnectionClosed

def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()

@dataclass(frozen=True)
class Config:
    sniper_guild_id: str = _env("SNIPER_GUILD_ID")
    sniper_self_token: str = _env("SNIPER_SELF_TOKEN")
    patch_token: str = _env("PATCH_TOKEN")
    success_webhook_url: str = _env("SUCCESS_WEBHOOK_URL")
    error_webhook_url: str = _env("ERROR_WEBHOOK_URL")
    log_level: str = _env("LOG_LEVEL", "INFO")
    log_file: Optional[str] = _env("LOG_FILE") or None
    gateway_url: str = "wss://gateway.discord.gg"
    api_base: str = "https://discord.com/api/v10"
    browser: str = "Chrome"
    os_name: str = "Windows"
    device: str = "PC"
    dedupe_window_s: float = 5.0

def setup_logging(cfg: Config) -> logging.Logger:
    logger = logging.getLogger("sniper")
    logger.setLevel(cfg.log_level.upper())
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    if cfg.log_file:
        fh = logging.FileHandler(cfg.log_file, encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    logger.propagate = False
    return logger

class Op:
    DISPATCH = 0
    HEARTBEAT = 1
    IDENTIFY = 2
    PRESENCE_UPDATE = 3
    VOICE_STATE_UPDATE = 4
    RESUME = 6
    RECONNECT = 7
    REQUEST_GUILD_MEMBERS = 8
    INVALID_SESSION = 9
    HELLO = 10
    HEARTBEAT_ACK = 11

RESUMABLE_CLOSE_CODES = {4000, 4001, 4002, 4003, 4005, 4007, 4008, 4009}

@dataclass
class GuildState:
    id: str
    vanity_url_code: Optional[str] = None
    last_seen: float = field(default_factory=time.monotonic)

class Sniper:
    def __init__(self, cfg: Config, logger: logging.Logger) -> None:
        self.cfg = cfg
        self.log = logger
        self.guilds: dict[str, GuildState] = {}
        self._recent_attempts: dict[str, float] = {}
        self.session_id: Optional[str] = None
        self.resume_gateway_url: Optional[str] = None
        self.sequence: Optional[int] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._closed = asyncio.Event()
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self.http = httpx.AsyncClient(
            http2=True,
            timeout=httpx.Timeout(10.0, connect=5.0),
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
            headers={"User-Agent": "VanitySniper/2.0 (+https://discord.com)"},
        )

    async def run(self) -> None:
        backoff = 1.0
        max_backoff = 60.0
        while not self._closed.is_set():
            try:
                await self._connect_and_run()
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self.log.exception("Gateway loop error: %s", exc)
            if self._closed.is_set():
                break
            sleep_for = min(max_backoff, backoff) * (0.5 + random.random())
            self.log.warning("Reconnecting in %.2fs...", sleep_for)
            try:
                await asyncio.wait_for(self._closed.wait(), timeout=sleep_for)
            except asyncio.TimeoutError:
                pass
            backoff = min(max_backoff, backoff * 2)
        await self.http.aclose()

    async def shutdown(self) -> None:
        self.log.info("Shutting down...")
        self._closed.set()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._heartbeat_task
        if self._ws:
            with contextlib.suppress(Exception):
                await self._ws.close(code=1000, reason="shutdown")

    async def _connect_and_run(self) -> None:
        base = self.resume_gateway_url or self.cfg.gateway_url
        url = f"{base}/?v=10&encoding=json"
        self.log.info("Connecting to gateway: %s", url)
        async with websockets.connect(url, max_size=2**24, ping_interval=None) as ws:
            self._ws = ws
            self.log.info("Gateway connected.")
            hello = json.loads(await ws.recv())
            if hello.get("op") != Op.HELLO:
                raise RuntimeError(f"Expected HELLO, got: {hello}")
            interval_ms = hello["d"]["heartbeat_interval"]
            self.log.debug("Heartbeat interval: %sms", interval_ms)
            if self.session_id and self.resume_gateway_url:
                await self._send_resume(ws)
            else:
                await self._send_identify(ws)
            self._heartbeat_task = asyncio.create_task(
                self._heartbeat_loop(ws, interval_ms / 1000.0)
            )
            try:
                async for raw in ws:
                    await self._handle_message(raw)
            finally:
                self._heartbeat_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._heartbeat_task

    async def _heartbeat_loop(
        self, ws: websockets.WebSocketClientProtocol, interval: float
    ) -> None:
        await asyncio.sleep(interval * random.random())
        while True:
            try:
                await ws.send(json.dumps({"op": Op.HEARTBEAT, "d": self.sequence}))
                self.log.debug("→ HEARTBEAT (seq=%s)", self.sequence)
            except ConnectionClosed:
                return
            except Exception as exc:  # noqa: BLE001
                self.log.warning("Heartbeat send failed: %s", exc)
                return
            await asyncio.sleep(interval)

    async def _send_identify(
        self, ws: websockets.WebSocketClientProtocol
    ) -> None:
        payload = {
            "op": Op.IDENTIFY,
            "d": {
                "token": self.cfg.sniper_self_token,
                "capabilities": 0,
                "properties": {
                    "os": self.cfg.os_name,
                    "browser": self.cfg.browser,
                    "device": self.cfg.device,
                },
                "intents": 1 << 0,
                "presence": {
                    "status": "online",
                    "since": 0,
                    "activities": [],
                    "afk": False,
                },
            },
        }
        await ws.send(json.dumps(payload))
        self.log.info("→ IDENTIFY sent.")

    async def _send_resume(
        self, ws: websockets.WebSocketClientProtocol
    ) -> None:
        payload = {
            "op": Op.RESUME,
            "d": {
                "token": self.cfg.sniper_self_token,
                "session_id": self.session_id,
                "seq": self.sequence,
            },
        }
        await ws.send(json.dumps(payload))
        self.log.info("→ RESUME sent (session=%s, seq=%s).", self.session_id, self.sequence)

    async def _handle_message(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            self.log.warning("Received non-JSON message.")
            return
        op = msg.get("op")
        if msg.get("s") is not None:
            self.sequence = msg["s"]
        if op == Op.DISPATCH:
            await self._handle_dispatch(msg)
        elif op == Op.HEARTBEAT:
            if self._ws:
                await self._ws.send(
                    json.dumps({"op": Op.HEARTBEAT, "d": self.sequence})
                )
        elif op == Op.HEARTBEAT_ACK:
            self.log.debug("← HEARTBEAT_ACK")
        elif op == Op.RECONNECT:
            self.log.warning("Server requested reconnect.")
            if self._ws:
                await self._ws.close(code=4000)
        elif op == Op.INVALID_SESSION:
            resumable = bool(msg.get("d"))
            self.log.warning("INVALID_SESSION (resumable=%s).", resumable)
            if not resumable:
                self.session_id = None
                self.resume_gateway_url = None
            if self._ws:
                await self._ws.close(code=4000)
        else:
            self.log.debug("Unhandled opcode: %s", op)

    async def _handle_dispatch(self, msg: dict[str, Any]) -> None:
        event = msg.get("t")
        data = msg.get("d") or {}
        if event == "READY":
            self.session_id = data.get("session_id")
            self.resume_gateway_url = data.get("resume_gateway_url")
            self.log.info(
                "READY as %s#%s (session=%s)",
                (data.get("user") or {}).get("username", "?"),
                (data.get("user") or {}).get("discriminator", "?"),
                self.session_id,
            )
            self.guilds.clear()
            await self._notify_success(
                f"Sniper online. Watching for vanity changes."
            )
        elif event == "RESUMED":
            self.log.info("Session resumed successfully.")
        elif event == "GUILD_CREATE":
            gid = str(data.get("id"))
            vanity = data.get("vanity_url_code")
            self.guilds[gid] = GuildState(id=gid, vanity_url_code=vanity)
            self.log.debug("GUILD_CREATE %s (vanity=%s)", gid, vanity)
            await self._maybe_report_watchlist()
        elif event == "GUILD_UPDATE":
            await self._handle_guild_update(data)
        elif event == "GUILD_DELETE":
            gid = str(data.get("id"))
            state = self.guilds.pop(gid, None)
            if not state or not state.vanity_url_code:
                return
            self.log.info(
                "GUILD_DELETE %s freed vanity '%s' — attempting snipe.",
                gid,
                state.vanity_url_code,
            )
            await self._snipe(state.vanity_url_code, reason="guild_deleted")

    async def _handle_guild_update(self, data: dict[str, Any]) -> None:
        gid = str(data.get("id"))
        new_vanity = data.get("vanity_url_code")
        state = self.guilds.get(gid)
        old_vanity = state.vanity_url_code if state else None
        if state is None:
            self.guilds[gid] = GuildState(id=gid, vanity_url_code=new_vanity)
            return
        state.vanity_url_code = new_vanity
        state.last_seen = time.monotonic()
        if new_vanity:
            return
        if not old_vanity:
            return
        self.log.info(
            "Vanity '%s' freed on guild %s — sniping immediately.",
            old_vanity,
            gid,
        )
        await self._snipe(old_vanity, reason="vanity_cleared")

    async def _snipe(self, code: str, reason: str) -> None:
        now = time.monotonic()
        last = self._recent_attempts.get(code, 0.0)
        if now - last < self.cfg.dedupe_window_s:
            self.log.debug("Skipping duplicate snipe for '%s'.", code)
            return
        self._recent_attempts[code] = now
        if not self.cfg.sniper_guild_id:
            self.log.error("SNIPER_GUILD_ID is not set; cannot snipe.")
            return
        if not self.cfg.sniper_self_token:
            self.log.error("SNIPER_SELF_TOKEN is not set; cannot snipe.")
            return
        url = f"{self.cfg.api_base}/guilds/{self.cfg.sniper_guild_id}/vanity-url"
        token = self.cfg.patch_token or self.cfg.sniper_self_token
        headers = {
            "Authorization": token,
            "Content-Type": "application/json",
            "X-Audit-Log-Reason": f"Vanity sniper: {reason}"[:512],
        }
        payload = {"code": code}
        t0 = time.perf_counter()
        try:
            r = await self.http.patch(url, headers=headers, json=payload)
        except Exception as exc:  # noqa: BLE001
            self.log.exception("PATCH failed: %s", exc)
            await self._notify_error(f"PATCH error for `{code}`: `{exc}`")
            return
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        if r.status_code in (200, 201):
            live = await self._fetch_current_vanity(headers)
            van = live or code
            self.log.info(
                "Sniped '%s' in %.1fms (live=%s).", code, elapsed_ms, van
            )
            await self._notify_success(
                f"**Sniped!** `https://discord.gg/{van}` "
                f"(requested `{code}`, took `{elapsed_ms:.0f}ms`) ||@everyone||"
            )
        else:
            body: Any
            try:
                body = r.json()
            except Exception:  # noqa: BLE001
                body = r.text
            self.log.warning(
                "Failed to snipe '%s' (HTTP %s): %s",
                code,
                r.status_code,
                body,
            )
            await self._notify_error(
                f"Failed to snipe `{code}` (HTTP {r.status_code})."
            )
            await self._notify_error(
                "```json\n" + json.dumps(body, indent=2)[:1800] + "\n```"
            )

    async def _fetch_current_vanity(self, headers: dict[str, str]) -> Optional[str]:
        try:
            r = await self.http.get(
                f"{self.cfg.api_base}/guilds/{self.cfg.sniper_guild_id}",
                headers=headers,
            )
            if r.status_code == 200:
                return r.json().get("vanity_url_code")
        except Exception as exc:  # noqa: BLE001
            self.log.debug("Post-snipe GET failed: %s", exc)
        return None

    async def _post_webhook(self, url: str, content: str) -> None:
        if not url:
            return
        try:
            await self.http.post(
                url,
                json={
                    "content": content[:2000],
                    "allowed_mentions": {"parse": ["everyone"]},
                },
            )
        except Exception as exc:  # noqa: BLE001
            self.log.debug("Webhook post failed: %s", exc)

    async def _notify_success(self, content: str) -> None:
        await self._post_webhook(self.cfg.success_webhook_url, content)

    async def _notify_error(self, content: str) -> None:
        await self._post_webhook(self.cfg.error_webhook_url, content)

    _reported_watchlist_size: int = -1

    async def _maybe_report_watchlist(self) -> None:
        n = len(self.guilds)
        if n == self._reported_watchlist_size:
            return
        self._reported_watchlist_size = n
        codes = [
            f"`{g.vanity_url_code}`"
            for g in self.guilds.values()
            if g.vanity_url_code
        ]
        if not codes:
            return
        preview = ", ".join(codes[:25])
        if len(codes) > 25:
            preview += f", … (+{len(codes) - 25} more)"
        await self._notify_success(
            f"Watching {n} guilds for vanity changes.\n{preview}"
        )

async def amain() -> None:
    cfg = Config()
    log = setup_logging(cfg)
    missing = [
        name
        for name, val in (
            ("SNIPER_GUILD_ID", cfg.sniper_guild_id),
            ("SNIPER_SELF_TOKEN", cfg.sniper_self_token),
        )
        if not val
    ]
    if missing:
        log.error("Missing required config: %s", ", ".join(missing))
        log.error(
            "Set them via environment variables, e.g.\n"
            "  export SNIPER_GUILD_ID=123\n"
            "  export SNIPER_SELF_TOKEN=abc.def.ghi"
        )
        sys.exit(1)
    sniper = Sniper(cfg, log)
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    def _request_stop(*_: Any) -> None:
        stop.set()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, _request_stop)
    run_task = asyncio.create_task(sniper.run(), name="sniper-run")
    stop_task = asyncio.create_task(stop.wait(), name="stop-wait")
    done, pending = await asyncio.wait(
        {run_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
    )
    for t in pending:
        t.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await t
    await sniper.shutdown()

def main() -> None:
    try:
        asyncio.run(amain())
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
