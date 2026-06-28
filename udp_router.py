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
from typing import Optional

log = logging.getLogger('router')

# ── Config ────────────────────────────────────────────────────────────────────

LISTEN_HOST     = os.environ.get('LISTEN_HOST', '0.0.0.0')
LISTEN_PORT     = int(os.environ.get('LISTEN_PORT', '7777'))
SESSION_TIMEOUT = int(os.environ.get('SESSION_TIMEOUT', '120'))
REDIRECT_BUFFER = int(os.environ.get('REDIRECT_BUFFER', '60'))

MODE_PREF_TTL = float(os.environ.get('MODE_PREF_TTL', '30'))

_BACKENDS: dict[str, tuple[str, int]] = {
    's': (os.environ.get('SOLO_HOST',  'solo-dev-1'),   int(os.environ.get('SOLO_PORT',  '7777'))),
    'd': (os.environ.get('SOLO_HOST',  'solo-dev-1'),   int(os.environ.get('SOLO_PORT',  '7777'))),
    'q': (os.environ.get('SOLO_HOST',  'solo-dev-1'),   int(os.environ.get('SOLO_PORT',  '7777'))),
    'c': (os.environ.get('ARENA_HOST', 'arenas-dev-1'), int(os.environ.get('ARENA_PORT', '7777'))),
}
_DEFAULT_BACKEND = _BACKENDS['s']

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
                        # Close wrong upstream, open correct one, replay buffer.
                        if sess.upstream.transport:
                            sess.upstream.transport.close()
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
            await self._open(addr, _BACKENDS.get(mode, _DEFAULT_BACKEND), [data])
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
            if stale:
                log.info('Expired %d idle sessions', len(stale))

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
    await asyncio.Future()


if __name__ == '__main__':
    asyncio.run(main())
