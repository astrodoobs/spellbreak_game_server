"""
TCP control interface for remote management (Discord bot, admin CLI).

All commands are newline-terminated ASCII. The session must authenticate
with AUTH before any other command (unless control_password is empty).

Standard commands:
    AUTH <password>
    GET_PLAYERS                   — match state JSON (same format as match_tracker.dll port 4951)
    STATUS                        — server + match state summary
    PLAYERS                       — live proxy connection list
    KICK <username>               — drop a player from the proxy
    BAN_IP <ip> [reason]          — permanent IP ban + immediate kick
    BAN_IP_TEMP <ip> <secs> [reason]
    UNBAN_IP <ip>
    BAN_USER <username> [reason]  — permanent account ban + kick
    BAN_USER_TEMP <username> <secs> [reason]
    UNBAN_USER <username>
    BANS                          — list all active bans
    REGISTER <discord_id> <username> — create user + persistent 64-char token; returns {"ok":true,"token":"..."}
    ROTATE_TOKEN <discord_id>    — replace token for existing user; returns {"ok":true,"token":"..."}
    GRANT_STAFF <discord_id>     — set is_staff=true; client dev menu unlocks cheat commands on next connect
    REVOKE_STAFF <discord_id>    — set is_staff=false; takes effect on next connect
    GRANT_DEV <discord_id>       — set is_dev=true + is_staff=true; unlocks Dev Settings panel on next connect
    REVOKE_DEV <discord_id>      — set is_dev=false + is_staff=false; takes effect on next connect
    RESTART                       — request game server restart
    QUIT

Legacy Elixir-compatible commands (accepted without response wait):
    CMD_REFRESH                   — no-op (push model keeps state current)
    BOT_REFRESH                   — no-op (same)
    MATCH_COMPLETE                — clear all UDP sessions
    PURGE_CONNECTIONS             — clear all UDP sessions
    RESTART_MATCHMAKING           — clear sessions + restart game server

Responses are JSON objects, one per line: {"ok": true, ...} or {"ok": false, "error": "..."}.

Modding toolkit hook point:
    To extend auth via match_tracker.dll, add a TRACKER_CMD command here
    that opens a TCP connection to 127.0.0.1:4951, sends the command, and
    returns the response — no proxy changes needed.
"""

import asyncio
import json
import logging
import re
import secrets
from typing import Optional

from .config import Config
from .database import Database
from .match_state import MatchStateManager

log = logging.getLogger(__name__)

_USERNAME_RE = re.compile(r"^[A-Za-z0-9 '._-]{1,20}$")


class ControlServer:
    def __init__(
        self,
        config: Config,
        db: Database,
        match_state: MatchStateManager,
        proxy=None,
        supervisor=None,
    ):
        self._cfg = config
        self._db = db
        self._state = match_state
        self._proxy = proxy
        self._supervisor = supervisor

    def set_proxy(self, proxy) -> None:
        self._proxy = proxy

    def set_supervisor(self, supervisor) -> None:
        self._supervisor = supervisor

    async def start(self) -> None:
        server = await asyncio.start_server(
            self._handle_client,
            self._cfg.control_host,
            self._cfg.control_port,
        )
        log.info('Control server on %s:%d', self._cfg.control_host, self._cfg.control_port)
        async with server:
            await server.serve_forever()

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        addr = writer.get_extra_info('peername')
        log.info('Control connection from %s', addr)
        authed = not self._cfg.control_password

        async def reply(data: dict) -> None:
            try:
                writer.write(json.dumps(data).encode() + b'\n')
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

                # ── Auth ──────────────────────────────────────────────────────
                if cmd == 'AUTH':
                    pw = parts[1] if len(parts) > 1 else ''
                    if pw == self._cfg.control_password:
                        authed = True
                        await reply({'ok': True})
                    else:
                        await reply({'ok': False, 'error': 'bad password'})
                    continue

                if cmd == 'QUIT':
                    break

                if not authed:
                    await reply({'ok': False, 'error': 'not authenticated'})
                    continue

                # ── Legacy Elixir-compatible commands (no auth required if password empty) ──
                if cmd in ('CMD_REFRESH', 'BOT_REFRESH'):
                    # No-op: push model keeps state current without clearing sessions.
                    log.info('Legacy command %s from %s (no-op)', cmd, addr)
                    await reply({'ok': True})

                elif cmd in ('MATCH_COMPLETE', 'PURGE_CONNECTIONS'):
                    log.info('Legacy command %s from %s', cmd, addr)
                    if self._proxy:
                        await self._proxy.clear_all_sessions()
                    await reply({'ok': True})

                elif cmd == 'RESTART_MATCHMAKING':
                    log.info('RESTART_MATCHMAKING from %s', addr)
                    if self._proxy:
                        await self._proxy.clear_all_sessions()
                    if self._supervisor:
                        self._supervisor.request_restart()
                    await reply({'ok': True})

                # ── Standard commands ──────────────────────────────────────────
                elif cmd == 'GET_PLAYERS':
                    # Same JSON format as match_tracker.dll port 4951 — for bot compatibility.
                    try:
                        writer.write(self._state.state.to_json().encode() + b'\n')
                        await writer.drain()
                    except Exception:
                        pass

                elif cmd == 'STATUS':
                    s = self._state.state
                    conns = self._proxy.get_connected_players() if self._proxy else []
                    await reply({
                        'ok': True,
                        'status': s.status,
                        'map': s.map_name,
                        'tracker_players': len(s.players),
                        'proxy_connections': len(conns),
                    })

                elif cmd == 'PLAYERS':
                    conns = self._proxy.get_connected_players() if self._proxy else []
                    await reply({'ok': True, 'players': conns})

                elif cmd == 'KICK':
                    username = parts[1] if len(parts) > 1 else ''
                    if not username:
                        await reply({'ok': False, 'error': 'username required'})
                        continue
                    n = await self._proxy.kick_by_username(username) if self._proxy else 0
                    await reply({'ok': True, 'kicked': n})

                elif cmd in ('BAN_IP', 'BAN_IP_TEMP'):
                    ip = parts[1] if len(parts) > 1 else ''
                    if not ip:
                        await reply({'ok': False, 'error': 'ip required'})
                        continue
                    duration: Optional[int] = None
                    reason = ''
                    if cmd == 'BAN_IP_TEMP':
                        try:
                            duration = int(parts[2]) if len(parts) > 2 else 3600
                        except ValueError:
                            await reply({'ok': False, 'error': 'invalid duration'})
                            continue
                        reason = parts[3] if len(parts) > 3 else ''
                    else:
                        reason = parts[2] if len(parts) > 2 else ''
                    await self._db.ban_ip(ip, reason, banned_by='admin', duration_secs=duration)
                    if self._proxy:
                        for a, s in list(self._proxy._sessions.items()):
                            if a[0] == ip:
                                await self._proxy.kick_client(a, 'banned')
                    await reply({'ok': True})

                elif cmd == 'UNBAN_IP':
                    ip = parts[1] if len(parts) > 1 else ''
                    if not ip:
                        await reply({'ok': False, 'error': 'ip required'})
                        continue
                    n = await self._db.unban_ip(ip)
                    await reply({'ok': True, 'removed': n})

                elif cmd in ('BAN_USER', 'BAN_USER_TEMP'):
                    username = parts[1] if len(parts) > 1 else ''
                    if not username:
                        await reply({'ok': False, 'error': 'username required'})
                        continue
                    user = await self._db.get_user_by_username(username)
                    if not user:
                        await reply({'ok': False, 'error': 'user not found'})
                        continue
                    duration = None
                    reason = ''
                    if cmd == 'BAN_USER_TEMP':
                        try:
                            duration = int(parts[2]) if len(parts) > 2 else 3600
                        except ValueError:
                            await reply({'ok': False, 'error': 'invalid duration'})
                            continue
                        reason = parts[3] if len(parts) > 3 else ''
                    else:
                        reason = parts[2] if len(parts) > 2 else ''
                    await self._db.ban_user(user['id'], reason, banned_by='admin', duration_secs=duration)
                    if self._proxy:
                        await self._proxy.kick_by_username(username)
                    await reply({'ok': True})

                elif cmd == 'UNBAN_USER':
                    username = parts[1] if len(parts) > 1 else ''
                    if not username:
                        await reply({'ok': False, 'error': 'username required'})
                        continue
                    user = await self._db.get_user_by_username(username)
                    if not user:
                        await reply({'ok': False, 'error': 'user not found'})
                        continue
                    n = await self._db.unban_user(user['id'])
                    await reply({'ok': True, 'removed': n})

                elif cmd == 'BANS':
                    rows = await self._db.list_bans()
                    await reply({'ok': True, 'bans': [dict(r) for r in rows]})

                elif cmd == 'RESTART':
                    if self._supervisor:
                        self._supervisor.request_restart()
                    await reply({'ok': True})

                elif cmd == 'REGISTER':
                    # REGISTER <discord_id> <username>
                    discord_id = parts[1] if len(parts) > 1 else ''
                    username   = ' '.join(parts[2:]).strip() if len(parts) > 2 else ''
                    if not discord_id or not username:
                        await reply({'ok': False, 'error': 'discord_id and username required'})
                        continue
                    if not _USERNAME_RE.match(username):
                        await reply({'ok': False, 'error': 'invalid_username'})
                        continue
                    existing_discord = await self._db.get_user_by_discord_id(discord_id)
                    if existing_discord:
                        await reply({
                            'ok': False,
                            'error': 'already_registered',
                            'username': existing_discord['username'],
                        })
                        continue
                    existing_name = await self._db.get_user_by_username(username)
                    if existing_name:
                        await reply({'ok': False, 'error': 'username_taken'})
                        continue
                    user_id = await self._db.create_user(username, password_hash='', discord_id=discord_id)
                    if user_id is None:
                        await reply({'ok': False, 'error': 'username_taken'})
                        continue
                    token = secrets.token_hex(32)
                    success = await self._db.create_token(user_id, token)
                    if not success:
                        await reply({'ok': False, 'error': 'token_create_failed'})
                        continue
                    log.info('REGISTER discord=%s username=%s user_id=%d', discord_id, username, user_id)
                    await reply({'ok': True, 'token': token, 'user_id': user_id})

                elif cmd == 'ROTATE_TOKEN':
                    # ROTATE_TOKEN <discord_id>
                    discord_id = parts[1] if len(parts) > 1 else ''
                    if not discord_id:
                        await reply({'ok': False, 'error': 'discord_id required'})
                        continue
                    user = await self._db.get_user_by_discord_id(discord_id)
                    if not user:
                        await reply({'ok': False, 'error': 'not_registered'})
                        continue
                    token = secrets.token_hex(32)
                    success = await self._db.create_token(user['id'], token)
                    if not success:
                        await reply({'ok': False, 'error': 'token_create_failed'})
                        continue
                    log.info('ROTATE_TOKEN discord=%s username=%s', discord_id, user['username'])
                    await reply({'ok': True, 'token': token, 'username': user['username']})

                elif cmd == 'DELETE_REGISTRATION':
                    # DELETE_REGISTRATION <discord_id>
                    discord_id = parts[1] if len(parts) > 1 else ''
                    if not discord_id:
                        await reply({'ok': False, 'error': 'discord_id required'})
                        continue
                    user = await self._db.get_user_by_discord_id(discord_id)
                    if not user:
                        await reply({'ok': False, 'error': 'not_registered'})
                        continue
                    if self._proxy:
                        await self._proxy.kick_by_username(user['username'])
                    await self._db.delete_user(user['id'])
                    log.info('DELETE_REGISTRATION discord=%s username=%s', discord_id, user['username'])
                    await reply({'ok': True, 'username': user['username']})

                elif cmd == 'LOOKUP_DISCORD':
                    # LOOKUP_DISCORD <discord_id>
                    discord_id = parts[1] if len(parts) > 1 else ''
                    if not discord_id:
                        await reply({'ok': False, 'error': 'discord_id required'})
                        continue
                    user = await self._db.get_user_by_discord_id(discord_id)
                    if not user:
                        await reply({'ok': False, 'error': 'not_registered'})
                        continue
                    await reply({'ok': True, 'user': dict(user)})

                elif cmd in ('GRANT_STAFF', 'REVOKE_STAFF'):
                    # GRANT_STAFF <discord_id> / REVOKE_STAFF <discord_id>
                    discord_id = parts[1] if len(parts) > 1 else ''
                    if not discord_id:
                        await reply({'ok': False, 'error': 'discord_id required'})
                        continue
                    user = await self._db.get_user_by_discord_id(discord_id)
                    if not user:
                        await reply({'ok': False, 'error': 'not_registered'})
                        continue
                    granting = cmd == 'GRANT_STAFF'
                    await self._db.set_staff(user['id'], granting)
                    log.info('%s discord=%s username=%s', cmd, discord_id, user['username'])
                    await reply({'ok': True, 'username': user['username'], 'is_staff': granting})

                elif cmd in ('GRANT_DEV', 'REVOKE_DEV'):
                    # GRANT_DEV <discord_id> / REVOKE_DEV <discord_id>
                    discord_id = parts[1] if len(parts) > 1 else ''
                    if not discord_id:
                        await reply({'ok': False, 'error': 'discord_id required'})
                        continue
                    user = await self._db.get_user_by_discord_id(discord_id)
                    if not user:
                        await reply({'ok': False, 'error': 'not_registered'})
                        continue
                    granting = cmd == 'GRANT_DEV'
                    await self._db.set_dev(user['id'], granting)
                    log.info('%s discord=%s username=%s', cmd, discord_id, user['username'])
                    await reply({'ok': True, 'username': user['username'], 'is_dev': granting, 'is_staff': granting})

                else:
                    await reply({'ok': False, 'error': f'unknown command: {cmd}'})

        except Exception as exc:
            log.error('Control session error (%s): %s', addr, exc)
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            log.debug('Control connection closed: %s', addr)
