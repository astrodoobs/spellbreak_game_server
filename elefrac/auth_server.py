"""
Lightweight pre-auth TCP server for game-launch token validation.

Clients (auth_injector.dll) connect at game start, send their token as a
single line, and receive auth info before joining any game server.

Protocol (newline-terminated ASCII):
    Client → Server : <token>\n
    Server → Client : OK <username> <is_staff> <is_dev>\n
                   OR: FAIL\n

The connection is closed immediately after the reply.
"""

import asyncio
import logging

from .config import Config
from .database import Database

log = logging.getLogger(__name__)


class AuthServer:
    def __init__(self, config: Config, db: Database):
        self._cfg = config
        self._db  = db

    async def start(self) -> None:
        server = await asyncio.start_server(
            self._handle,
            self._cfg.auth_host,
            self._cfg.auth_port,
        )
        log.info('Auth server on %s:%d', self._cfg.auth_host, self._cfg.auth_port)
        async with server:
            await server.serve_forever()

    async def _handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
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

            row = await self._db.validate_token(token)
            if row is None:
                log.debug('Auth rejected for token prefix %s... from %s', token[:8], addr)
                writer.write(b'FAIL\n')
            else:
                username = row['username']
                is_staff = int(row['is_staff'] or 0)
                is_dev   = int(row['is_dev']   or 0)
                log.info('Pre-auth OK: %s (staff=%d dev=%d) from %s',
                         username, is_staff, is_dev, addr)
                writer.write(
                    f'OK {username} {is_staff} {is_dev}\n'.encode()
                )
            await writer.drain()
        except Exception as exc:
            log.debug('Auth session error (%s): %s', addr, exc)
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
