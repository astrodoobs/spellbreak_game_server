#!/usr/bin/env python3
"""
Game server lifecycle manager.

HTTP API consumed by udp_router:
  GET  /backend?mode=<char>   → {"host": "...", "port": 7777}
  POST /session/close         JSON: {"host": "...", "port": 7777}
  GET  /status                JSON: list of all tracked servers

On GET /backend:
  - Returns a running server in a joinable game state (WaitingForPlayers /
    WaitingToStart / Unknown) with available capacity (sessions < MAX_SESSIONS).
  - If all servers for the mode are in-progress or full, starts a stopped static
    server or creates a new dynamic container.
  - Blocks until the server is ready (up to SERVER_START_WAIT seconds).
  - Increments session count atomically so concurrent callers never double-assign.

On POST /session/close:
  - Decrements session count.
  - When sessions reach 0 and IDLE_TIMEOUT elapses with no new sessions, the server
    is stopped (static) or stopped+removed (dynamic).
"""

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field

import docker
from aiohttp import web

log = logging.getLogger('manager')

# ── Config ─────────────────────────────────────────────────────────────────────

MANAGER_HOST      = os.environ.get('MANAGER_HOST', '0.0.0.0')
MANAGER_PORT      = int(os.environ.get('MANAGER_PORT', '8888'))
DOCKER_NETWORK    = os.environ.get('DOCKER_NETWORK', 'spellbreak-dev')
MAX_SESSIONS      = int(os.environ.get('MAX_SESSIONS', '20'))
IDLE_TIMEOUT      = float(os.environ.get('IDLE_TIMEOUT', '1800'))    # 30 min
SERVER_START_WAIT = float(os.environ.get('SERVER_START_WAIT', '30')) # seconds after docker start

GAME_SERVER_IMAGE = os.environ.get('GAME_SERVER_IMAGE', 'spellbreak-game-server:latest')

_FORWARD_ENV = [
    'PATCH_ENV', 'REQUIRE_AUTH', 'PATCH_URL', 'PATCH_TEST_URL', 'MATCHTRACKFREQUENCY',
    'SESSION_TIMEOUT',
]

# Game states in which a server will accept new players.
_JOINABLE = {'WaitingForPlayers', 'WaitingToStart', 'WaitingForServer', 'Unknown'}

# ── Mode → server group ────────────────────────────────────────────────────────

@dataclass(frozen=True)
class GroupConfig:
    prefix:         str
    gamemode:       str
    broadcast_port: int

_GROUPS: dict[str, GroupConfig] = {
    's': GroupConfig('solo-dev',   'Solo',    int(os.environ.get('SOLO_BROADCAST_PORT',  '8777'))),
    'd': GroupConfig('solo-dev',   'Solo',    int(os.environ.get('SOLO_BROADCAST_PORT',  '8777'))),
    'q': GroupConfig('solo-dev',   'Solo',    int(os.environ.get('SOLO_BROADCAST_PORT',  '8777'))),
    'c': GroupConfig('arenas-dev', 'Capture', int(os.environ.get('ARENA_BROADCAST_PORT', '8777'))),
}

# ── Per-server state ───────────────────────────────────────────────────────────

@dataclass
class ServerInfo:
    name:           str
    group:          str
    dynamic:        bool
    broadcast_port: int   = 8777
    running:        bool  = False
    sessions:       int   = 0
    last_close:     float = field(default_factory=time.monotonic)
    game_state:     str   = 'Unknown'   # live value from broadcast port
    player_count:   int   = 0           # number of players currently in the server

# ── Manager ────────────────────────────────────────────────────────────────────

class ServerManager:
    def __init__(self, docker_client):
        self._docker  = docker_client
        self._servers: dict[str, ServerInfo] = {}
        self._lock    = asyncio.Lock()
        self._start_events: dict[str, asyncio.Event] = {}

    def register_static(self, name: str, group_prefix: str, broadcast_port: int, running: bool) -> None:
        self._servers[name] = ServerInfo(
            name=name, group=group_prefix, dynamic=False,
            broadcast_port=broadcast_port, running=running,
        )

    # ── Public API ─────────────────────────────────────────────────────────────

    async def get_backend(self, mode: str) -> tuple[str, int]:
        cfg = _GROUPS.get(mode, _GROUPS['s'])

        async with self._lock:
            # Route to the most-populated server that is still accepting players
            # (WaitingForPlayers / WaitingToStart / booting states).  This fills
            # one lobby before a second server is ever needed.  A new server is
            # created only when every running server for this mode is non-joinable
            # (e.g. InProgress) or has hit the session cap.
            candidates = [
                s for s in self._servers.values()
                if s.group == cfg.prefix
                and s.running
                and s.game_state in _JOINABLE
                and s.sessions < MAX_SESSIONS
            ]
            if candidates:
                best = max(candidates, key=lambda s: s.player_count)
                best.sessions += 1
                log.info('ASSIGN  %-15s  mode=%s  state=%-20s  players=%d  sessions=%d',
                         best.name, mode, best.game_state, best.player_count, best.sessions)
                return (best.name, 7777)

            # No joinable server — all are in-progress or at capacity; need another one.
            target = next(
                (s for s in self._servers.values()
                 if s.group == cfg.prefix and not s.dynamic and not s.running),
                None,
            )
            if target is None:
                name = self._next_name(cfg.prefix)
                self._servers[name] = ServerInfo(
                    name=name, group=cfg.prefix, dynamic=True,
                    broadcast_port=cfg.broadcast_port,
                )
                target = self._servers[name]

            name = target.name

            if name not in self._start_events:
                evt = asyncio.Event()
                self._start_events[name] = evt
                asyncio.ensure_future(self._boot_server(target, cfg, evt))
            else:
                evt = self._start_events[name]

        log.info('WAITING %-15s  (booting)', name)
        try:
            await asyncio.wait_for(evt.wait(), timeout=SERVER_START_WAIT + 15)
        except asyncio.TimeoutError:
            raise RuntimeError(f'{name} did not become ready in time')

        async with self._lock:
            srv = self._servers.get(name)
            if srv is None or not srv.running:
                raise RuntimeError(f'{name} failed to start')
            srv.sessions += 1
            log.info('ASSIGN  %-15s  mode=%s  state=%-20s  sessions=%d',
                     name, mode, srv.game_state, srv.sessions)
            return (name, 7777)

    async def close_session(self, host: str) -> None:
        async with self._lock:
            srv = self._servers.get(host)
            if srv is None:
                return
            if srv.sessions > 0:
                srv.sessions -= 1
            srv.last_close = time.monotonic()
            log.info('RELEASE %-15s  sessions=%d', host, srv.sessions)

    # ── Broadcast state monitor ────────────────────────────────────────────────

    async def monitor_broadcast(self, srv: ServerInfo) -> None:
        """Maintain a persistent TCP connection to a server's broadcast port.
        Updates srv.game_state from the JSON stream emitted by match_tracker."""
        RECONNECT_DELAY = 10
        while True:
            # Don't try to connect while the server isn't running.
            async with self._lock:
                still_tracked = srv.name in self._servers
                is_running    = srv.running
            if not still_tracked:
                return
            if not is_running:
                await asyncio.sleep(RECONNECT_DELAY)
                continue

            try:
                reader, writer = await asyncio.open_connection(srv.name, srv.broadcast_port)
                log.info('BROADCAST %-15s  connected on :%d', srv.name, srv.broadcast_port)
                buf = ''
                while True:
                    chunk = await reader.read(4096)
                    if not chunk:
                        break
                    buf += chunk.decode('utf-8', errors='replace')
                    while '\n' in buf:
                        line, buf = buf.split('\n', 1)
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            data         = json.loads(line)
                            state        = data.get('state', 'Unknown')
                            player_count = len(data.get('players', []))
                            async with self._lock:
                                if srv.game_state != state:
                                    log.info('STATE   %-15s  %s → %s',
                                             srv.name, srv.game_state, state)
                                    srv.game_state = state
                                if srv.player_count != player_count:
                                    log.info('PLAYERS %-15s  %d → %d',
                                             srv.name, srv.player_count, player_count)
                                    srv.player_count = player_count
                        except json.JSONDecodeError:
                            pass
                writer.close()
            except Exception as exc:
                log.warning('BROADCAST %-15s  lost connection: %s', srv.name, exc)

            async with self._lock:
                srv.game_state   = 'Unknown'
                srv.player_count = 0
            await asyncio.sleep(RECONNECT_DELAY)

    # ── Container lifecycle ────────────────────────────────────────────────────

    async def _boot_server(
        self, srv: ServerInfo, cfg: GroupConfig, evt: asyncio.Event,
    ) -> None:
        try:
            if srv.dynamic:
                log.info('CREATE  %s  image=%s', srv.name, GAME_SERVER_IMAGE)
                env = {k: os.environ[k] for k in _FORWARD_ENV if k in os.environ}
                env['GAMEMODE'] = cfg.gamemode
                await self._run_in_thread(
                    self._docker.containers.run,
                    GAME_SERVER_IMAGE,
                    name=srv.name,
                    network=DOCKER_NETWORK,
                    environment=env,
                    detach=True,
                )
            else:
                log.info('START   %s', srv.name)
                container = await self._run_in_thread(
                    self._docker.containers.get, srv.name,
                )
                await self._run_in_thread(container.start)

            log.info('BOOTING %s  waiting %.0fs for UE4 startup', srv.name, SERVER_START_WAIT)
            await asyncio.sleep(SERVER_START_WAIT)

            async with self._lock:
                srv.running = True
            log.info('READY   %s', srv.name)

            # Start monitoring broadcast state for this server.
            asyncio.ensure_future(self.monitor_broadcast(srv))

        except Exception as exc:
            log.error('Boot failed for %s: %s', srv.name, exc)
            async with self._lock:
                if srv.dynamic:
                    self._servers.pop(srv.name, None)
        finally:
            evt.set()
            self._start_events.pop(srv.name, None)

    async def _stop_server(self, srv: ServerInfo) -> None:
        async with self._lock:
            if srv.sessions > 0:
                return
            srv.running = False

        log.info('STOPPING %s  (idle %.0f min)', srv.name, IDLE_TIMEOUT / 60)
        try:
            container = await self._run_in_thread(self._docker.containers.get, srv.name)
            await self._run_in_thread(container.stop)
            log.info('STOPPED  %s', srv.name)
            if srv.dynamic:
                await self._run_in_thread(container.remove)
                log.info('REMOVED  %s', srv.name)
                async with self._lock:
                    self._servers.pop(srv.name, None)
        except Exception as exc:
            log.error('Stop failed for %s: %s', srv.name, exc)
            async with self._lock:
                srv.running = True

    # ── Idle cleanup loop ──────────────────────────────────────────────────────

    async def run_cleanup(self) -> None:
        while True:
            await asyncio.sleep(60)
            now = time.monotonic()
            async with self._lock:
                idle = [
                    s for s in self._servers.values()
                    if s.running and s.sessions == 0
                    and (now - s.last_close) > IDLE_TIMEOUT
                ]
            for srv in idle:
                asyncio.ensure_future(self._stop_server(srv))

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _next_name(self, prefix: str) -> str:
        n = 1
        while f'{prefix}-{n}' in self._servers:
            n += 1
        return f'{prefix}-{n}'

    @staticmethod
    async def _run_in_thread(fn, *args, **kwargs):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: fn(*args, **kwargs))

# ── HTTP handlers ──────────────────────────────────────────────────────────────

async def handle_backend(req: web.Request) -> web.Response:
    manager: ServerManager = req.app['manager']
    mode = req.rel_url.query.get('mode', 's')
    try:
        host, port = await manager.get_backend(mode)
        return web.json_response({'host': host, 'port': port})
    except Exception as exc:
        log.error('get_backend(mode=%s) failed: %s', mode, exc)
        return web.json_response({'error': str(exc)}, status=503)


async def handle_session_close(req: web.Request) -> web.Response:
    manager: ServerManager = req.app['manager']
    body = await req.json()
    await manager.close_session(body.get('host', ''))
    return web.json_response({'ok': True})


async def handle_status(req: web.Request) -> web.Response:
    manager: ServerManager = req.app['manager']
    async with manager._lock:
        servers = [
            {
                'name':           s.name,
                'group':          s.group,
                'dynamic':        s.dynamic,
                'running':        s.running,
                'sessions':       s.sessions,
                'game_state':     s.game_state,
                'player_count':   s.player_count,
                'broadcast_port': s.broadcast_port,
            }
            for s in manager._servers.values()
        ]
    return web.json_response({'servers': servers})

# ── Entry point ────────────────────────────────────────────────────────────────

async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(name)s] %(levelname)s %(message)s',
    )

    loop = asyncio.get_running_loop()
    docker_client = await loop.run_in_executor(None, docker.from_env)

    manager = ServerManager(docker_client)

    static_str = os.environ.get('STATIC_SERVERS', 'solo-dev-1:s,arenas-dev-1:c')
    for entry in static_str.split(','):
        entry = entry.strip()
        if ':' not in entry:
            continue
        name, _, mode_char = entry.partition(':')
        name = name.strip()
        cfg  = _GROUPS.get(mode_char.strip(), _GROUPS['s'])
        try:
            container = await loop.run_in_executor(None, docker_client.containers.get, name)
            running = container.status == 'running'
        except Exception:
            running = False
        manager.register_static(name, cfg.prefix, cfg.broadcast_port, running)
        log.info('STATIC  %-15s  group=%-12s  bcast=%-5d  running=%s',
                 name, cfg.prefix, cfg.broadcast_port, running)
        if running:
            asyncio.ensure_future(manager.monitor_broadcast(manager._servers[name]))

    asyncio.ensure_future(manager.run_cleanup())

    app = web.Application()
    app['manager'] = manager
    app.router.add_get( '/backend',       handle_backend)
    app.router.add_post('/session/close', handle_session_close)
    app.router.add_get( '/status',        handle_status)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, MANAGER_HOST, MANAGER_PORT)
    await site.start()
    log.info('Manager listening on %s:%d', MANAGER_HOST, MANAGER_PORT)

    await asyncio.Future()


if __name__ == '__main__':
    asyncio.run(main())
