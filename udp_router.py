#!/usr/bin/env python3
"""
UDP game-mode router.

Routes the first packet from each client to the DEFAULT backend immediately
(no handshake delay). Buffers all packets until ?EFMode=<char> is seen in a
join URL. If EFMode indicates a different backend, replays the full buffer
to the correct server and discards the initial upstream connection.

Mode chars:
  s → SOLO_HOST:SOLO_PORT   (solo / duo / squad — default)
  d → SOLO_HOST:SOLO_PORT
  q → SOLO_HOST:SOLO_PORT
  c → ARENA_HOST:ARENA_PORT (capture / dominion / arenas)
"""

import asyncio
import logging
import os
import time
import urllib.request
import json
import aiosqlite
import re
import secrets
from typing import Optional

log = logging.getLogger('router')
auth_log = logging.getLogger('auth')

# ── Config ────────────────────────────────────────────────────────────────────

LISTEN_HOST     = os.environ.get('LISTEN_HOST', '0.0.0.0')
LISTEN_PORT     = int(os.environ.get('LISTEN_PORT', '7777'))
SESSION_TIMEOUT = int(os.environ.get('SESSION_TIMEOUT', '120'))
REDIRECT_BUFFER = int(os.environ.get('REDIRECT_BUFFER', '60'))

MODE_PREF_TTL = float(os.environ.get('MODE_PREF_TTL', '30'))

AUTH_HOST    = os.environ.get('AUTH_HOST',    '0.0.0.0')
AUTH_PORT    = int(os.environ.get('AUTH_PORT',    '4948'))
AUTH_DB      = os.environ.get('AUTH_DB',      '/data/elefrac.db')
CONTROL_HOST = os.environ.get('CONTROL_HOST', '0.0.0.0')
CONTROL_PORT = int(os.environ.get('CONTROL_PORT', '3387'))
CONTROL_PASS = os.environ.get('CONTROL_PASSWORD', '')

# When MANAGER_URL is set, all backend selection goes through the server manager.
# Without it, falls back to the hardcoded dict below (standalone / testing mode).
MANAGER_URL = os.environ.get('MANAGER_URL', '').rstrip('/')

_BACKENDS: dict[str, tuple[str, int]] = {
    's': (os.environ.get('SOLO_HOST',  'solo-dev-1'),   int(os.environ.get('SOLO_PORT',  '7777'))),
    'd': (os.environ.get('SOLO_HOST',  'solo-dev-1'),   int(os.environ.get('SOLO_PORT',  '7777'))),
    'q': (os.environ.get('SOLO_HOST',  'solo-dev-1'),   int(os.environ.get('SOLO_PORT',  '7777'))),
    'c': (os.environ.get('ARENA_HOST', 'arenas-dev-1'), int(os.environ.get('ARENA_PORT', '7777'))),
}
_DEFAULT_BACKEND = _BACKENDS['s']


async def _fetch_backend(mode: str) -> tuple[str, int]:
    """Ask the server manager which backend to use, and increment its session counter."""
    url = f'{MANAGER_URL}/backend?mode={mode}'
    loop = asyncio.get_running_loop()
    def _get():
        with urllib.request.urlopen(url, timeout=60) as resp:
            return json.loads(resp.read())
    data = await loop.run_in_executor(None, _get)
    return (data['host'], int(data['port']))


async def _notify_close(backend: tuple[str, int]) -> None:
    """Tell the server manager a session has ended."""
    url = f'{MANAGER_URL}/session/close'
    body = json.dumps({'host': backend[0], 'port': backend[1]}).encode()
    loop = asyncio.get_running_loop()
    def _post():
        req = urllib.request.Request(url, data=body,
                                     headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(req, timeout=5):
            pass
    try:
        await loop.run_in_executor(None, _post)
    except Exception as exc:
        log.warning('session/close notify failed: %s', exc)

# ── Packet inspection ─────────────────────────────────────────────────────────

_ENCODED_HEADER = bytes([94, 142, 194, 218, 202, 94, 154, 194, 224, 230, 94])

# Side-channel mode-selection packets sent by server_router.dll before the UE4 handshake.
# Format: b'EFM:s'  (exactly 5 bytes: prefix + mode char)
_EFM_PREFIX = b'EFM:'


def _extract_efm_control(data: bytes) -> Optional[str]:
    if len(data) == 5 and data[:4] == _EFM_PREFIX:
        return chr(data[4])
    return None


def _decode_join_url(data: bytes) -> Optional[str]:
    idx = data.find(_ENCODED_HEADER)
    if idx == -1:
        return None
    chars = []
    for b in data[idx + len(_ENCODED_HEADER):]:
        if b == 0:
            break
        c = b >> 1
        if 0x20 <= c <= 0x7E:
            chars.append(chr(c))
        else:
            break
    url = ''.join(chars)
    return ('/Game/Maps/' + url) if url else None


def _extract_efmode(data: bytes) -> Optional[str]:
    url = _decode_join_url(data)
    if not url:
        return None
    for part in url.split('?'):
        if part.startswith('EFMode='):
            return part[7:8]
    return None

# ── Per-session upstream connection ──────────────────────────────────────────

ClientAddr = tuple[str, int]


class UpstreamProtocol(asyncio.DatagramProtocol):
    def __init__(self, client_addr: ClientAddr, router: 'RouterProtocol'):
        self._client = client_addr
        self._router = router
        self.transport: Optional[asyncio.DatagramTransport] = None

    def connection_made(self, t: asyncio.DatagramTransport) -> None:
        self.transport = t

    def datagram_received(self, data: bytes, addr: ClientAddr) -> None:
        log.debug('BACKEND→CLIENT %s  %d bytes', self._client[0], len(data))
        self._router.refresh_session(self._client)
        self._router.send_to_client(self._client, data)

    def error_received(self, exc: Exception) -> None:
        log.warning('Upstream error (%s): %s', self._client, exc)

    def connection_lost(self, exc: Optional[Exception]) -> None:
        pass

# ── Session record ────────────────────────────────────────────────────────────
#
# Each session stores:
#   upstream  — UpstreamProtocol (may be replaced on redirect)
#   backend   — (host, port) currently connected to
#   last_seen — monotonic timestamp for idle cleanup
#   buf       — ring buffer of recent packets (for redirect replay)
#   committed — True once EFMode has been seen and backend is final

class Session:
    __slots__ = ('upstream', 'backend', 'last_seen', 'buf', 'committed')

    def __init__(self, upstream: UpstreamProtocol, backend: tuple):
        self.upstream  = upstream
        self.backend   = backend
        self.last_seen = time.monotonic()
        self.buf: list[bytes] = []
        self.committed = False

# ── Main router ───────────────────────────────────────────────────────────────

class RouterProtocol(asyncio.DatagramProtocol):
    def __init__(self):
        self._transport: Optional[asyncio.DatagramTransport] = None
        self._sessions: dict[ClientAddr, Session] = {}
        self._pending: set[ClientAddr] = set()
        # ip → (mode_char, timestamp) from EFM side-channel packets
        self._mode_prefs: dict[str, tuple[str, float]] = {}

    def connection_made(self, transport: asyncio.DatagramTransport) -> None:
        self._transport = transport
        log.info('Router listening on %s:%d', LISTEN_HOST, LISTEN_PORT)
        logged = set()
        for ch, backend in _BACKENDS.items():
            if backend not in logged:
                log.info('  %s → %s:%d', ch, *backend)
                logged.add(backend)

    def refresh_session(self, addr: ClientAddr) -> None:
        sess = self._sessions.get(addr)
        if sess:
            sess.last_seen = time.monotonic()

    def send_to_client(self, addr: ClientAddr, data: bytes) -> None:
        if self._transport:
            log.debug('FORWARD→CLIENT %s  %d bytes', addr[0], len(data))
            self._transport.sendto(data, addr)

    def datagram_received(self, data: bytes, addr: ClientAddr) -> None:
        asyncio.ensure_future(self._handle(data, addr))

    def error_received(self, exc: Exception) -> None:
        log.warning('Router socket error: %s', exc)

    # ── Per-packet handler ────────────────────────────────────────────────────

    async def _handle(self, data: bytes, addr: ClientAddr) -> None:
        # ── EFM side-channel: mode preference from client DLL ─────────────────
        efm = _extract_efm_control(data)
        if efm is not None:
            self._mode_prefs[addr[0]] = (efm, time.monotonic())
            log.info('MODE_PREF %-20s  → %s', addr[0], efm)
            return  # not a real game packet; don't forward

        # ── Established session ───────────────────────────────────────────────
        if addr in self._sessions:
            sess = self._sessions[addr]
            sess.last_seen = time.monotonic()

            if not sess.committed:
                # Still watching for EFMode to confirm (or redirect) backend.
                if len(sess.buf) < REDIRECT_BUFFER:
                    sess.buf.append(data)

                mode = _extract_efmode(data)
                if mode is not None:
                    target = _BACKENDS.get(mode, _DEFAULT_BACKEND)
                    if target != sess.backend:
                        log.info(
                            'REDIRECT %-20s  mode=%s  %s:%d → %s:%d',
                            addr[0], mode, *sess.backend, *target,
                        )
                        # Close wrong upstream, notify manager, open correct one.
                        if sess.upstream.transport:
                            sess.upstream.transport.close()
                        if MANAGER_URL:
                            asyncio.ensure_future(_notify_close(sess.backend))
                        buf = list(sess.buf)
                        del self._sessions[addr]
                        asyncio.ensure_future(self._open(addr, target, buf))
                        return
                    else:
                        sess.committed = True
                        sess.buf.clear()
                        log.info('CONFIRMED %-20s  mode=%s', addr[0], mode)

            if sess.upstream.transport:
                sess.upstream.transport.sendto(data)
            return

        # ── New client — route using EFM preference or default ────────────────
        if addr in self._pending:
            return
        self._pending.add(addr)
        try:
            stored = self._mode_prefs.get(addr[0])
            if stored and (time.monotonic() - stored[1]) < MODE_PREF_TTL:
                mode = stored[0]
            else:
                mode = 's'
            if MANAGER_URL:
                try:
                    backend = await _fetch_backend(mode)
                except Exception as exc:
                    log.error('Manager unreachable, falling back to hardcoded: %s', exc)
                    backend = _BACKENDS.get(mode, _DEFAULT_BACKEND)
            else:
                backend = _BACKENDS.get(mode, _DEFAULT_BACKEND)
            await self._open(addr, backend, [data])
        finally:
            self._pending.discard(addr)

    async def _open(
        self, addr: ClientAddr, backend: tuple, initial_packets: list[bytes]
    ) -> None:
        log.info('ROUTE  %-20s  → %s:%d', addr[0], *backend)
        loop = asyncio.get_running_loop()
        upstream = UpstreamProtocol(addr, self)
        try:
            transport, _ = await loop.create_datagram_endpoint(
                lambda: upstream,
                remote_addr=backend,
            )
            upstream.transport = transport
        except Exception as exc:
            log.error('Cannot reach backend %s:%d: %s', *backend, exc)
            return

        sess = Session(upstream, backend)
        sess.buf = list(initial_packets)
        self._sessions[addr] = sess

        for pkt in initial_packets:
            upstream.transport.sendto(pkt)

    # ── Idle cleanup ──────────────────────────────────────────────────────────

    async def run_cleanup(self) -> None:
        while True:
            await asyncio.sleep(30)
            now = time.monotonic()
            stale = [a for a, s in self._sessions.items()
                     if now - s.last_seen > SESSION_TIMEOUT]
            for addr in stale:
                sess = self._sessions.pop(addr)
                if sess.upstream.transport:
                    sess.upstream.transport.close()
                if MANAGER_URL:
                    asyncio.ensure_future(_notify_close(sess.backend))
            if stale:
                log.info('Expired %d idle sessions', len(stale))

# ── Pre-auth TCP server ───────────────────────────────────────────────────────
#
# Clients (auth_injector.dll) connect, send their token as a single line, and
# receive auth info before joining any game server.
#
# Protocol (newline-terminated ASCII):
#   Client → Server : <token>\n
#   Server → Client : OK <username> <is_staff> <is_dev>\n
#                  OR: FAIL\n

_USERNAME_RE = re.compile(r"^[A-Za-z0-9 '._-]{1,20}$")


async def _setup_auth_db() -> None:
    async with aiosqlite.connect(AUTH_DB) as db:
        await db.executescript('''
            CREATE TABLE IF NOT EXISTS users (
                id               INTEGER PRIMARY KEY,
                username         TEXT NOT NULL UNIQUE,
                password_hash    TEXT NOT NULL DEFAULT '',
                created_at       INTEGER NOT NULL DEFAULT (strftime('%s','now')),
                uid              TEXT,
                ip_address       TEXT,
                first_connection INTEGER,
                discord_id       TEXT UNIQUE,
                is_staff         INTEGER NOT NULL DEFAULT 0,
                is_dev           INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS tokens (
                id         INTEGER PRIMARY KEY,
                user_id    INTEGER NOT NULL,
                token      TEXT NOT NULL,
                created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
                used_at    INTEGER
            );
        ''')
        await db.commit()


async def _db_get_user_by_discord(discord_id: str) -> Optional[aiosqlite.Row]:
    async with aiosqlite.connect(AUTH_DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            'SELECT * FROM users WHERE discord_id = ?', (discord_id,)
        ) as cur:
            return await cur.fetchone()


async def _db_get_user_by_username(username: str) -> Optional[aiosqlite.Row]:
    async with aiosqlite.connect(AUTH_DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            'SELECT * FROM users WHERE username = ?', (username,)
        ) as cur:
            return await cur.fetchone()


async def _db_create_user(username: str, discord_id: str) -> Optional[int]:
    try:
        async with aiosqlite.connect(AUTH_DB) as db:
            cur = await db.execute(
                'INSERT INTO users (username, password_hash, discord_id) VALUES (?, \'\', ?)',
                (username, discord_id),
            )
            await db.commit()
            return cur.lastrowid
    except aiosqlite.IntegrityError:
        return None


async def _db_create_token(user_id: int, token: str) -> bool:
    try:
        async with aiosqlite.connect(AUTH_DB) as db:
            # Invalidate old tokens for this user first.
            await db.execute('DELETE FROM tokens WHERE user_id = ?', (user_id,))
            await db.execute(
                'INSERT INTO tokens (user_id, token) VALUES (?, ?)', (user_id, token)
            )
            await db.commit()
            return True
    except Exception:
        return False


async def _db_set_staff(user_id: int, value: bool) -> None:
    async with aiosqlite.connect(AUTH_DB) as db:
        await db.execute('UPDATE users SET is_staff = ? WHERE id = ?', (int(value), user_id))
        await db.commit()


async def _db_set_dev(user_id: int, value: bool) -> None:
    async with aiosqlite.connect(AUTH_DB) as db:
        await db.execute(
            'UPDATE users SET is_dev = ?, is_staff = ? WHERE id = ?',
            (int(value), int(value), user_id),
        )
        await db.commit()


async def _validate_token(token: str) -> Optional[aiosqlite.Row]:
    async with aiosqlite.connect(AUTH_DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            '''SELECT u.username, u.is_staff, u.is_dev
               FROM tokens t JOIN users u ON t.user_id = u.id
               WHERE t.token LIKE ? || '%'
               ORDER BY length(t.token) ASC
               LIMIT 1''',
            (token,),
        ) as cur:
            return await cur.fetchone()


async def _handle_auth_client(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    addr = writer.get_extra_info('peername')
    try:
        try:
            line = await asyncio.wait_for(reader.readline(), timeout=10)
        except asyncio.TimeoutError:
            return
        token = line.decode('utf-8', errors='replace').strip()
        if not token:
            writer.write(b'FAIL\n')
            await writer.drain()
            return
        row = await _validate_token(token)
        if row is None:
            auth_log.debug('Auth rejected for token %s... from %s', token[:8], addr)
            writer.write(b'FAIL\n')
        else:
            auth_log.info('Auth OK: %s (staff=%d dev=%d) from %s',
                          row['username'], row['is_staff'], row['is_dev'], addr)
            writer.write(
                f'OK {row["username"]} {int(row["is_staff"])} {int(row["is_dev"])}\n'.encode()
            )
        await writer.drain()
    except Exception as exc:
        auth_log.debug('Auth session error (%s): %s', addr, exc)
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


async def _handle_control_client(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    ctrl_log = logging.getLogger('control')
    addr = writer.get_extra_info('peername')
    ctrl_log.info('Control connection from %s', addr)
    authed = not CONTROL_PASS

    async def reply(data: dict) -> None:
        try:
            writer.write((json.dumps(data) + '\n').encode())
            await writer.drain()
        except Exception:
            pass

    try:
        while True:
            try:
                line = await asyncio.wait_for(reader.readline(), timeout=300)
            except asyncio.TimeoutError:
                break
            if not line:
                break
            parts = line.decode('utf-8', errors='replace').strip().split(None, 3)
            if not parts:
                continue
            cmd = parts[0].upper()

            if cmd == 'AUTH':
                pw = parts[1] if len(parts) > 1 else ''
                authed = pw == CONTROL_PASS
                await reply({'ok': authed} if authed else {'ok': False, 'error': 'bad password'})
                continue

            if cmd == 'QUIT':
                break

            if not authed:
                await reply({'ok': False, 'error': 'not authenticated'})
                continue

            # ── Legacy no-ops ──────────────────────────────────────────────────
            if cmd in ('CMD_REFRESH', 'BOT_REFRESH', 'MATCH_COMPLETE',
                       'PURGE_CONNECTIONS', 'RESTART_MATCHMAKING', 'RESTART'):
                await reply({'ok': True})

            # ── Registration ───────────────────────────────────────────────────
            elif cmd == 'REGISTER':
                discord_id = parts[1] if len(parts) > 1 else ''
                username   = ' '.join(parts[2:]).strip() if len(parts) > 2 else ''
                if not discord_id or not username:
                    await reply({'ok': False, 'error': 'discord_id and username required'})
                    continue
                if not _USERNAME_RE.match(username):
                    await reply({'ok': False, 'error': 'invalid_username'})
                    continue
                if await _db_get_user_by_discord(discord_id):
                    await reply({'ok': False, 'error': 'already_registered'})
                    continue
                if await _db_get_user_by_username(username):
                    await reply({'ok': False, 'error': 'username_taken'})
                    continue
                user_id = await _db_create_user(username, discord_id)
                if user_id is None:
                    await reply({'ok': False, 'error': 'username_taken'})
                    continue
                token = secrets.token_hex(32)
                if not await _db_create_token(user_id, token):
                    await reply({'ok': False, 'error': 'token_create_failed'})
                    continue
                ctrl_log.info('REGISTER discord=%s username=%s', discord_id, username)
                await reply({'ok': True, 'token': token, 'user_id': user_id})

            elif cmd == 'ROTATE_TOKEN':
                discord_id = parts[1] if len(parts) > 1 else ''
                if not discord_id:
                    await reply({'ok': False, 'error': 'discord_id required'})
                    continue
                user = await _db_get_user_by_discord(discord_id)
                if not user:
                    await reply({'ok': False, 'error': 'not_registered'})
                    continue
                token = secrets.token_hex(32)
                if not await _db_create_token(user['id'], token):
                    await reply({'ok': False, 'error': 'token_create_failed'})
                    continue
                ctrl_log.info('ROTATE_TOKEN discord=%s username=%s', discord_id, user['username'])
                await reply({'ok': True, 'token': token, 'username': user['username']})

            elif cmd == 'GRANT_STAFF':
                discord_id = parts[1] if len(parts) > 1 else ''
                user = await _db_get_user_by_discord(discord_id) if discord_id else None
                if not user:
                    await reply({'ok': False, 'error': 'not_registered'})
                    continue
                await _db_set_staff(user['id'], True)
                await reply({'ok': True, 'username': user['username'], 'is_staff': True})

            elif cmd == 'REVOKE_STAFF':
                discord_id = parts[1] if len(parts) > 1 else ''
                user = await _db_get_user_by_discord(discord_id) if discord_id else None
                if not user:
                    await reply({'ok': False, 'error': 'not_registered'})
                    continue
                await _db_set_staff(user['id'], False)
                await reply({'ok': True, 'username': user['username'], 'is_staff': False})

            elif cmd == 'GRANT_DEV':
                discord_id = parts[1] if len(parts) > 1 else ''
                user = await _db_get_user_by_discord(discord_id) if discord_id else None
                if not user:
                    await reply({'ok': False, 'error': 'not_registered'})
                    continue
                await _db_set_dev(user['id'], True)
                await reply({'ok': True, 'username': user['username'], 'is_dev': True, 'is_staff': True})

            elif cmd == 'REVOKE_DEV':
                discord_id = parts[1] if len(parts) > 1 else ''
                user = await _db_get_user_by_discord(discord_id) if discord_id else None
                if not user:
                    await reply({'ok': False, 'error': 'not_registered'})
                    continue
                await _db_set_dev(user['id'], False)
                await reply({'ok': True, 'username': user['username'], 'is_dev': False, 'is_staff': False})

            else:
                await reply({'ok': False, 'error': f'unknown command: {cmd}'})

    except Exception as exc:
        ctrl_log.error('Control session error (%s): %s', addr, exc)
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


async def run_control_server() -> None:
    server = await asyncio.start_server(_handle_control_client, CONTROL_HOST, CONTROL_PORT)
    logging.getLogger('control').info(
        'Control server on %s:%d  password=%s',
        CONTROL_HOST, CONTROL_PORT, 'set' if CONTROL_PASS else 'none',
    )
    async with server:
        await server.serve_forever()


async def run_auth_server() -> None:
    await _setup_auth_db()
    server = await asyncio.start_server(_handle_auth_client, AUTH_HOST, AUTH_PORT)
    auth_log.info('Pre-auth server on %s:%d  db=%s', AUTH_HOST, AUTH_PORT, AUTH_DB)
    async with server:
        await server.serve_forever()


# ── Entry point ───────────────────────────────────────────────────────────────

async def main() -> None:
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(name)s] %(levelname)s %(message)s',
    )
    loop = asyncio.get_running_loop()
    proto = RouterProtocol()
    await loop.create_datagram_endpoint(
        lambda: proto,
        local_addr=(LISTEN_HOST, LISTEN_PORT),
    )
    asyncio.ensure_future(proto.run_cleanup())
    asyncio.ensure_future(run_auth_server())
    asyncio.ensure_future(run_control_server())
    await asyncio.Future()


if __name__ == '__main__':
    asyncio.run(main())
