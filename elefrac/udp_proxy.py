"""
UDP proxy: sits between Spellbreak clients and the game server.

First packet from a new client address:
  1. IP ban check — drop if banned.
  2. If it looks like a join packet:
       a. Token-auth path (require_auth=True or token present):
          Extract the Name field as an auth token, validate + consume from
          the database, rewrite the name to the player's registered username,
          then open an upstream connection.  The player's hardware UID is
          also extracted (if present) and stored for future identification.
       b. UID fallback path (require_auth=False and no valid token):
          Treat the Name field as the player's actual username, extract the
          hardware UID, and resolve the player via the Elixir priority chain
          (UID → IP → alias → create new).  Impersonation is detected and
          logged automatically by resolve_player.

  3. If require_auth=True and no valid token: packet is dropped.

All subsequent packets forward transparently in both directions.
Sessions expire after session_timeout seconds of inactivity.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from .ban_handler import BanHandler
from .config import Config
from .database import Database
from .packet_parser import decode_join_url, extract_auth_uid_token, extract_name, extract_uid, is_join_packet, rewrite_name

log = logging.getLogger(__name__)

ClientAddr = tuple[str, int]


@dataclass
class Session:
    client_addr: ClientAddr
    username: str
    user_id: Optional[int]
    connection_id: Optional[int]
    uid: Optional[str] = None
    upstream: 'UpstreamProtocol' = field(repr=False, default=None)
    last_seen: float = field(default_factory=time.monotonic)
    authenticated: bool = False
    uid_auth: bool = False   # True = authenticated via UID field; Name untouched
    kicked: bool = False


class UpstreamProtocol(asyncio.DatagramProtocol):
    """One instance per client session; receives server→client packets."""

    def __init__(self, client_addr: ClientAddr, proxy: 'ProxyProtocol'):
        self._client = client_addr
        self._proxy = proxy
        self.transport: Optional[asyncio.DatagramTransport] = None

    def connection_made(self, transport: asyncio.DatagramTransport) -> None:
        self.transport = transport

    def datagram_received(self, data: bytes, addr: ClientAddr) -> None:
        self._proxy.send_to_client(self._client, data)

    def error_received(self, exc: Exception) -> None:
        log.debug('Upstream socket error for %s: %s', self._client, exc)

    def connection_lost(self, exc: Optional[Exception]) -> None:
        pass


class ProxyProtocol(asyncio.DatagramProtocol):
    """Single protocol instance shared across all client connections."""

    def __init__(
        self,
        config: Config,
        db: Database,
        ban_handler: BanHandler,
        match_state,
    ):
        self._cfg = config
        self._db = db
        self._bans = ban_handler
        self._match_state = match_state
        self._transport: Optional[asyncio.DatagramTransport] = None
        self._sessions: dict[ClientAddr, Session] = {}
        self._pending: set[ClientAddr] = set()

    def connection_made(self, transport: asyncio.DatagramTransport) -> None:
        self._transport = transport
        log.info(
            'UDP proxy listening on %s:%d  →  %s:%d',
            self._cfg.proxy_host,
            self._cfg.proxy_port,
            self._cfg.game_host,
            self._cfg.game_port,
        )

    def send_to_client(self, client_addr: ClientAddr, data: bytes) -> None:
        if self._transport:
            self._transport.sendto(data, client_addr)

    def datagram_received(self, data: bytes, addr: ClientAddr) -> None:
        asyncio.ensure_future(self._handle(data, addr))

    def error_received(self, exc: Exception) -> None:
        log.warning('Proxy socket error: %s', exc)

    # ── Internal ─────────────────────────────────────────────────────────────

    async def _handle(self, data: bytes, addr: ClientAddr) -> None:
        ip, _ = addr

        if addr in self._sessions:
            session = self._sessions[addr]
            if session.kicked:
                return
            if is_join_packet(data):
                if session.user_id is None:
                    # Not yet identified — scan for join URL and resolve identity.
                    data = await self._identify_session(session, data, ip)
                    if session.kicked:
                        return
                elif not session.uid_auth and session.username != 'unknown':
                    # Legacy name-field auth: rewrite retransmissions so the game
                    # server always sees the registered name, not the raw token.
                    token, name_start, name_end, name_encoded = extract_name(data)
                    if token and token != session.username:
                        data = rewrite_name(data, name_start, name_end, session.username, name_encoded)
            self._forward(data, addr)
            return

        # Serialize first-packet handling per client to avoid duplicate sessions
        if addr in self._pending:
            return
        self._pending.add(addr)
        try:
            banned, reason = await self._bans.is_banned(ip)
            if banned:
                log.info('Dropped packet from banned IP %s (%s)', ip, reason)
                return

            data = await self._open_session(data, addr)
            if data is None:
                return
        finally:
            self._pending.discard(addr)

        self._forward(data, addr)

    async def _identify_session(
        self, session: Session, data: bytes, ip: str
    ) -> bytes:
        """Resolve identity from a late-arriving join packet for an unidentified session."""
        is_staff = False
        is_dev = False

        join_url = decode_join_url(data)
        log.info('JOIN URL (identify) from %s: %s', ip, join_url)

        # ── UID-field auth (preferred) ────────────────────────────────────────
        uid_token = extract_auth_uid_token(data)
        if uid_token:
            row = await self._db.validate_token(uid_token)
            if row is None:
                log.info('Unknown UID token from %s: %.8s…', ip, uid_token)
                if self._cfg.require_auth:
                    session.kicked = True
                    return data
            else:
                steam_name, _, _, _ = extract_name(data)
                if not steam_name or steam_name.lower() != row['username'].lower():
                    log.info(
                        'Name mismatch from %s: got %r expected %r — dropped',
                        ip, steam_name, row['username'],
                    )
                    session.kicked = True
                    return data

                banned, reason = await self._bans.is_banned(ip, row['user_id'])
                if banned:
                    log.info('Rejected banned account %s (%s) from %s', row['username'], reason, ip)
                    session.kicked = True
                    return data

                # Name field untouched — Steam name passes through unchanged.
                session.username = row['username']
                session.user_id = row['user_id']
                session.authenticated = True
                session.uid_auth = True
                is_staff = bool(row['is_staff'])
                is_dev   = bool(row['is_dev'])
                log.info('Auth OK (UID): %s (%.8s…) from %s', session.username, uid_token, ip)
                await self._db.update_player_ip(session.user_id, ip)
                # Fall through to IDENTIFIED log below.

        if not session.authenticated:
            # ── Name-field auth fallback (legacy / unauthenticated) ──────────
            token, name_start, name_end, name_encoded = extract_name(data)
            if not token:
                if self._cfg.require_auth:
                    log.info('No token in join packet from %s — dropped (require_auth)', ip)
                    session.kicked = True
                return data

            row = await self._db.validate_token(token)
            if row is not None:
                banned, reason = await self._bans.is_banned(ip, row['user_id'])
                if banned:
                    log.info('Rejected banned account %s (%s) from %s', row['username'], reason, ip)
                    session.kicked = True
                    return data
                data = rewrite_name(data, name_start, name_end, row['username'], name_encoded)
                session.username = row['username']
                session.user_id = row['user_id']
                session.authenticated = True
                is_staff = bool(row['is_staff'])
                is_dev   = bool(row['is_dev'])
                log.info('Auth OK (name): %s (%.8s…) from %s', session.username, token, ip)
                uid = extract_uid(data)
                if uid:
                    session.uid = uid
                    await self._db.update_player_uid(session.user_id, uid, ip)
                else:
                    await self._db.update_player_ip(session.user_id, ip)

            elif self._cfg.require_auth:
                log.info('Invalid token from %s: %.8s… — dropped (require_auth)', ip, token)
                session.kicked = True
                return data

            elif not self._cfg.require_auth:
                uid = extract_uid(data)
                user_id, _ = await self._db.resolve_player(ip, token, uid, track_name=False)
                if user_id is not None:
                    user = await self._db.get_user_by_id(user_id)
                    if user:
                        session.username = user['username']
                        session.user_id = user_id
                        session.uid = uid
                        if session.username != token:
                            data = rewrite_name(data, name_start, name_end, session.username, name_encoded)

        if session.user_id is not None:
            auth_tag = 'auth' if session.authenticated else 'open'
            uid_tag = f' uid={session.uid}' if session.uid else ''
            log.info(
                'IDENTIFIED %-20s  %-24s  [%s] id=%d%s',
                ip, session.username, auth_tag, session.user_id, uid_tag,
            )
            if session.connection_id is not None:
                await self._db.update_connection_user(
                    session.connection_id, session.user_id, session.username
                )
            await self._match_state.player_connected(session.username, session.user_id)

        # Send dev-menu access flags now that identity is resolved.
        if session.authenticated and self._transport:
            cf = 't' if is_staff else 'f'
            df = 't' if is_dev else 'f'
            ef_pkt = f'EF_AUTH:{cf}:{df}:{session.username}'.encode()
            self._transport.sendto(ef_pkt, session.client_addr)
            log.debug('EF_AUTH:%s:%s → %s (%s)', cf, df, session.username, ip)

        return data

    async def _open_session(self, data: bytes, addr: ClientAddr) -> Optional[bytes]:
        ip, _ = addr

        username = 'unknown'
        user_id = None
        uid = None
        authenticated = False
        is_staff = False
        is_dev = False

        uid_auth = False
        if is_join_packet(data):
            join_url = decode_join_url(data)
            log.info('JOIN URL from %s: %s', ip, join_url)
            # ── UID-field auth (preferred) ────────────────────────────────────
            uid_token = extract_auth_uid_token(data)
            if uid_token:
                row = await self._db.validate_token(uid_token)
                if row is None:
                    log.info('Unknown UID token from %s: %.8s…', ip, uid_token)
                    if self._cfg.require_auth:
                        return None
                else:
                    steam_name, _, _, _ = extract_name(data)
                    if not steam_name or steam_name.lower() != row['username'].lower():
                        log.info(
                            'Name mismatch from %s: got %r expected %r — dropped',
                            ip, steam_name, row['username'],
                        )
                        return None

                    banned, reason = await self._bans.is_banned(ip, row['user_id'])
                    if banned:
                        log.info('Rejected banned account %s (%s) from %s', row['username'], reason, ip)
                        return None

                    # Name field untouched — Steam name propagates unchanged.
                    username = row['username']
                    user_id = row['user_id']
                    authenticated = True
                    uid_auth = True
                    is_staff = bool(row['is_staff'])
                    is_dev   = bool(row['is_dev'])
                    log.info('Auth OK (UID): %s (%.8s…) from %s', username, uid_token, ip)
                    await self._db.update_player_ip(user_id, ip)

            if not authenticated:
                # ── Name-field auth fallback (legacy / unauthenticated) ───────
                token, name_start, name_end, name_encoded = extract_name(data)

                if token:
                    row = await self._db.validate_token(token)

                    if row is not None:
                        banned, reason = await self._bans.is_banned(ip, row['user_id'])
                        if banned:
                            log.info('Rejected banned account %s (%s) from %s', row['username'], reason, ip)
                            return None
                        data = rewrite_name(data, name_start, name_end, row['username'], name_encoded)
                        username = row['username']
                        user_id = row['user_id']
                        authenticated = True
                        is_staff = bool(row['is_staff'])
                        is_dev   = bool(row['is_dev'])
                        log.info('Auth OK (name): %s (%.8s…) from %s', username, token, ip)
                        uid = extract_uid(data)
                        if uid:
                            log.info('UID for %s: %s', username, uid)
                            await self._db.update_player_uid(user_id, uid, ip)
                        else:
                            await self._db.update_player_ip(user_id, ip)

                    elif self._cfg.require_auth:
                        log.info('Invalid token from %s: %.8s…', ip, token)
                        return None

                    else:
                        uid = extract_uid(data)
                        user_id, is_new = await self._db.resolve_player(ip, token, uid)
                        if user_id is not None:
                            user = await self._db.get_user_by_id(user_id)
                            if user:
                                username = user['username']
                                if username != token:
                                    data = rewrite_name(data, name_start, name_end, username, name_encoded)

                elif self._cfg.require_auth:
                    log.debug('No token in join packet from %s — dropped (require_auth)', ip)
                    return None

        # Send dev-menu cheat-access status back to the client.  The client's
        # auth_injector hooks recvfrom and silently absorbs this packet before
        # the game ever sees it.
        if authenticated and self._transport:
            flag = 't' if is_staff else 'f'
            ef_pkt = f'EF_AUTH:{flag}:{username}'.encode()
            self._transport.sendto(ef_pkt, addr)
            log.debug('EF_AUTH:%s → %s (%s)', flag, username, ip)

        conn_id = await self._db.log_connection(ip, username, user_id)

        loop = asyncio.get_running_loop()
        upstream = UpstreamProtocol(addr, self)
        transport, _ = await loop.create_datagram_endpoint(
            lambda: upstream,
            remote_addr=(self._cfg.game_host, self._cfg.game_port),
        )

        session = Session(
            client_addr=addr,
            username=username,
            user_id=user_id,
            connection_id=conn_id,
            uid=uid,
            upstream=upstream,
            authenticated=authenticated,
            uid_auth=uid_auth,
        )
        session.upstream.transport = transport
        self._sessions[addr] = session

        auth_tag = 'auth' if authenticated else 'open'
        uid_tag = f' uid={uid}' if uid else ''
        id_tag = f' id={user_id}' if user_id else ''
        log.info('CONNECT  %-20s  %-24s  [%s]%s%s', ip, username, auth_tag, id_tag, uid_tag)

        if user_id is not None:
            await self._match_state.player_connected(username, user_id)

        return data

    def _forward(self, data: bytes, addr: ClientAddr) -> None:
        session = self._sessions.get(addr)
        if session is None or session.kicked:
            return
        session.last_seen = time.monotonic()
        if session.upstream and session.upstream.transport:
            session.upstream.transport.sendto(data)

    # ── Public API ────────────────────────────────────────────────────────────

    async def kick_client(self, addr: ClientAddr, reason: str = '') -> bool:
        session = self._sessions.get(addr)
        if not session:
            return False
        session.kicked = True
        log.info('Kicked %s from %s: %s', session.username, addr[0], reason)
        await self._close_session(addr)
        return True

    async def kick_by_username(self, username: str) -> int:
        targets = [
            addr for addr, s in self._sessions.items()
            if s.username.lower() == username.lower()
        ]
        for addr in targets:
            await self.kick_client(addr, 'admin kick')
        return len(targets)

    async def clear_all_sessions(self) -> None:
        addrs = list(self._sessions)
        for addr in addrs:
            await self._close_session(addr)
        if addrs:
            log.info('Cleared %d sessions', len(addrs))

    def get_connected_players(self) -> list[dict]:
        return [
            {
                'username': s.username,
                'ip': s.client_addr[0],
                'authenticated': s.authenticated,
                'uid': s.uid,
                'idle_secs': int(time.monotonic() - s.last_seen),
            }
            for s in self._sessions.values()
            if not s.kicked
        ]

    async def _close_session(self, addr: ClientAddr) -> None:
        session = self._sessions.pop(addr, None)
        if session is None:
            return
        if session.upstream and session.upstream.transport:
            session.upstream.transport.close()
        if session.connection_id is not None:
            await self._db.log_disconnection(session.connection_id)
        duration = int(time.monotonic() - session.last_seen)
        id_tag = f' id={session.user_id}' if session.user_id else ''
        log.info('DISCONNECT %-20s  %-24s  idle=%ds%s', addr[0], session.username, duration, id_tag)
        if session.user_id is not None:
            await self._match_state.player_disconnected(session.username)

    async def run_cleanup_loop(self) -> None:
        while True:
            await asyncio.sleep(30)
            now = time.monotonic()
            stale = [
                addr for addr, s in self._sessions.items()
                if now - s.last_seen > self._cfg.session_timeout
            ]
            for addr in stale:
                await self._close_session(addr)
            if stale:
                log.info('Expired %d idle sessions', len(stale))


async def start_proxy(
    config: Config,
    db: Database,
    ban_handler: BanHandler,
    match_state,
) -> ProxyProtocol:
    loop = asyncio.get_running_loop()
    protocol = ProxyProtocol(config, db, ban_handler, match_state)
    await loop.create_datagram_endpoint(
        lambda: protocol,
        local_addr=(config.proxy_host, config.proxy_port),
    )
    asyncio.ensure_future(protocol.run_cleanup_loop())
    return protocol
