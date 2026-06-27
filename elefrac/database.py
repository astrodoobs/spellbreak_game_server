import logging
import time
from typing import Optional

import aiosqlite

log = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    username      TEXT    UNIQUE NOT NULL COLLATE NOCASE,
    password_hash TEXT    NOT NULL DEFAULT '',
    created_at    INTEGER NOT NULL DEFAULT (strftime('%s','now')),
    uid           TEXT    UNIQUE,
    ip_address    TEXT,
    first_connection INTEGER,
    discord_id    TEXT    UNIQUE
);

CREATE TABLE IF NOT EXISTS user_aliases (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id  INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    username TEXT    NOT NULL COLLATE NOCASE,
    UNIQUE(user_id, username)
);

CREATE TABLE IF NOT EXISTS tokens (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token      TEXT    UNIQUE NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
    used_at    INTEGER
);

CREATE TABLE IF NOT EXISTS connection_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id         INTEGER REFERENCES users(id) ON DELETE SET NULL,
    ip_address      TEXT    NOT NULL,
    username        TEXT    NOT NULL,
    connected_at    INTEGER NOT NULL DEFAULT (strftime('%s','now')),
    disconnected_at INTEGER
);

CREATE TABLE IF NOT EXISTS bans (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    ip_address TEXT,
    user_id    INTEGER REFERENCES users(id) ON DELETE CASCADE,
    reason     TEXT    NOT NULL DEFAULT '',
    banned_by  TEXT    NOT NULL DEFAULT 'system',
    banned_at  INTEGER NOT NULL DEFAULT (strftime('%s','now')),
    expires_at INTEGER
);

CREATE TABLE IF NOT EXISTS rate_limits (
    key    TEXT    NOT NULL,
    hit_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tokens_token      ON tokens(token);
CREATE INDEX IF NOT EXISTS idx_bans_ip           ON bans(ip_address);
CREATE INDEX IF NOT EXISTS idx_bans_user         ON bans(user_id);
CREATE INDEX IF NOT EXISTS idx_connlog_user      ON connection_log(user_id);
CREATE INDEX IF NOT EXISTS idx_ratelimit_key     ON rate_limits(key);
CREATE INDEX IF NOT EXISTS idx_aliases_username  ON user_aliases(username);
CREATE INDEX IF NOT EXISTS idx_aliases_user      ON user_aliases(user_id);
"""

# Columns added after the initial schema — applied idempotently on every start.
_MIGRATIONS = [
    'ALTER TABLE users ADD COLUMN uid TEXT UNIQUE',
    'ALTER TABLE users ADD COLUMN ip_address TEXT',
    'ALTER TABLE users ADD COLUMN first_connection INTEGER',
    'ALTER TABLE users ADD COLUMN discord_id TEXT UNIQUE',
    'ALTER TABLE users ADD COLUMN is_staff INTEGER NOT NULL DEFAULT 0',
    'ALTER TABLE users ADD COLUMN is_dev   INTEGER NOT NULL DEFAULT 0',
]


class Database:
    def __init__(self, path: str):
        self._path = path
        self._db: Optional[aiosqlite.Connection] = None

    async def connect(self) -> None:
        self._db = await aiosqlite.connect(self._path)
        self._db.row_factory = aiosqlite.Row
        await self._db.executescript(_SCHEMA)
        await self._db.execute('PRAGMA journal_mode=WAL')
        await self._db.execute('PRAGMA foreign_keys=ON')
        await self._db.commit()
        await self._apply_migrations()
        log.info('Database ready: %s', self._path)

    async def _apply_migrations(self) -> None:
        for sql in _MIGRATIONS:
            try:
                await self._db.execute(sql)
            except aiosqlite.OperationalError:
                pass  # Column already exists
        await self._db.commit()

    async def close(self) -> None:
        if self._db:
            await self._db.close()

    # ── Users ────────────────────────────────────────────────────────────────

    async def create_user(
        self, username: str, password_hash: str, discord_id: Optional[str] = None
    ) -> Optional[int]:
        try:
            async with self._db.execute(
                'INSERT INTO users (username, password_hash, discord_id) VALUES (?, ?, ?)',
                (username, password_hash, discord_id),
            ) as cur:
                await self._db.commit()
                return cur.lastrowid
        except aiosqlite.IntegrityError:
            return None

    async def get_user_by_username(self, username: str) -> Optional[aiosqlite.Row]:
        async with self._db.execute(
            'SELECT * FROM users WHERE username = ?', (username,)
        ) as cur:
            return await cur.fetchone()

    async def get_user_by_id(self, user_id: int) -> Optional[aiosqlite.Row]:
        async with self._db.execute(
            'SELECT * FROM users WHERE id = ?', (user_id,)
        ) as cur:
            return await cur.fetchone()

    async def get_user_by_discord_id(self, discord_id: str) -> Optional[aiosqlite.Row]:
        async with self._db.execute(
            'SELECT * FROM users WHERE discord_id = ?', (discord_id,)
        ) as cur:
            return await cur.fetchone()

    async def get_user_by_uid(self, uid: str) -> Optional[aiosqlite.Row]:
        async with self._db.execute(
            'SELECT * FROM users WHERE uid = ?', (uid,)
        ) as cur:
            return await cur.fetchone()

    async def get_user_by_ip(self, ip: str) -> Optional[aiosqlite.Row]:
        """Returns the oldest user record with this IP address."""
        async with self._db.execute(
            'SELECT * FROM users WHERE ip_address = ? ORDER BY id ASC LIMIT 1', (ip,)
        ) as cur:
            return await cur.fetchone()

    async def set_staff(self, user_id: int, is_staff: bool) -> bool:
        """Grant or revoke staff status. Returns True if the user was found."""
        async with self._db.execute(
            'UPDATE users SET is_staff = ? WHERE id = ?',
            (1 if is_staff else 0, user_id),
        ) as cur:
            await self._db.commit()
            return cur.rowcount > 0

    async def set_dev(self, user_id: int, is_dev: bool) -> bool:
        """Grant or revoke dev status (also sets is_staff to match). Returns True if found."""
        async with self._db.execute(
            'UPDATE users SET is_dev = ?, is_staff = ? WHERE id = ?',
            (1 if is_dev else 0, 1 if is_dev else 0, user_id),
        ) as cur:
            await self._db.commit()
            return cur.rowcount > 0

    async def delete_user(self, user_id: int) -> bool:
        """Remove a user and all their tokens/aliases (cascaded by FK)."""
        async with self._db.execute('DELETE FROM users WHERE id = ?', (user_id,)) as cur:
            await self._db.commit()
            return cur.rowcount > 0

    async def get_user_by_alias(self, username: str) -> Optional[aiosqlite.Row]:
        """Check users.username first, then user_aliases."""
        user = await self.get_user_by_username(username)
        if user:
            return user
        async with self._db.execute(
            '''SELECT u.* FROM users u
               JOIN user_aliases ua ON ua.user_id = u.id
               WHERE ua.username = ?
               ORDER BY u.id ASC LIMIT 1''',
            (username,),
        ) as cur:
            return await cur.fetchone()

    # ── User aliases ─────────────────────────────────────────────────────────

    async def ensure_alias(self, user_id: int, username: str) -> None:
        try:
            await self._db.execute(
                'INSERT INTO user_aliases (user_id, username) VALUES (?, ?)',
                (user_id, username),
            )
            await self._db.commit()
        except aiosqlite.IntegrityError:
            pass  # Already recorded

    async def get_aliases(self, user_id: int) -> list:
        async with self._db.execute(
            'SELECT username FROM user_aliases WHERE user_id = ?', (user_id,)
        ) as cur:
            return await cur.fetchall()

    # ── UID / IP tracking ────────────────────────────────────────────────────

    async def update_player_uid(self, user_id: int, uid: str, ip: str) -> None:
        await self._db.execute(
            'UPDATE users SET uid = ?, ip_address = ? WHERE id = ? AND (uid IS NULL OR uid != ?)',
            (uid, ip, user_id, uid),
        )
        await self._db.commit()

    async def update_player_ip(self, user_id: int, ip: str) -> None:
        await self._db.execute(
            'UPDATE users SET ip_address = ? WHERE id = ?', (ip, user_id)
        )
        await self._db.commit()

    async def resolve_player(
        self, ip: str, username: str, uid: Optional[str], track_name: bool = True
    ) -> tuple[Optional[int], bool]:
        """
        Find or create a player using the Elixir priority chain:
          1. Hardware UID (most trusted)
          2. Known IP address
          3. Username / alias match
          4. Create new record

        track_name=False skips updating the stored username (use when the
        caller is passing an auth token rather than a real display name).

        Returns (user_id, is_new). Logs impersonation attempts.
        """
        user_by_uid = await self.get_user_by_uid(uid) if uid else None
        user_by_name = await self.get_user_by_alias(username)
        user_by_ip = await self.get_user_by_ip(ip)

        if user_by_uid:
            if user_by_name and user_by_name['id'] != user_by_uid['id']:
                log.warning(
                    'IMPERSONATION: %s (uid=%s, id=%d) using name owned by id=%d',
                    username, uid, user_by_uid['id'], user_by_name['id'],
                )
            user = user_by_uid

        elif user_by_ip:
            if user_by_name and user_by_name['id'] != user_by_ip['id']:
                log.warning(
                    'NAME CLASH: %s (ip=%s, id=%d) using name owned by id=%d',
                    username, ip, user_by_ip['id'], user_by_name['id'],
                )
            user = user_by_ip

        elif user_by_name:
            # Known name but unknown machine — block impersonation, create isolated record.
            log.error(
                'PREVENTED IMPERSONATION: new machine at %s claiming "%s" (owned by id=%d)',
                ip, username, user_by_name['id'],
            )
            user = None

        else:
            user = None

        if user is not None:
            user_id = user['id']
            updates: dict = {}
            if uid and not user['uid']:
                updates['uid'] = uid
            if user['ip_address'] != ip:
                updates['ip_address'] = ip
            if track_name and user['username'].lower() != username.lower():
                updates['username'] = username
            if updates:
                set_clause = ', '.join(f'{k} = ?' for k in updates)
                await self._db.execute(
                    f'UPDATE users SET {set_clause} WHERE id = ?',
                    (*updates.values(), user_id),
                )
                await self._db.commit()
            await self.ensure_alias(user_id, username)
            return user_id, False

        # Create new player record.
        try:
            async with self._db.execute(
                '''INSERT INTO users (username, password_hash, uid, ip_address, first_connection)
                   VALUES (?, '', ?, ?, strftime('%s','now'))''',
                (username, uid, ip),
            ) as cur:
                await self._db.commit()
                user_id = cur.lastrowid
            await self.ensure_alias(user_id, username)
            log.info('New player: %s ip=%s uid=%s (id=%d)', username, ip, uid, user_id)
            return user_id, True
        except aiosqlite.IntegrityError:
            # Rare race condition: username just appeared, fetch it.
            existing = await self.get_user_by_username(username)
            if existing:
                return existing['id'], False
            return None, False

    # ── Tokens ───────────────────────────────────────────────────────────────

    async def create_token(self, user_id: int, token: str) -> bool:
        await self._db.execute(
            'DELETE FROM tokens WHERE user_id = ? AND used_at IS NULL', (user_id,)
        )
        try:
            await self._db.execute(
                'INSERT INTO tokens (user_id, token) VALUES (?, ?)', (user_id, token)
            )
            await self._db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False

    async def validate_token(self, token: str) -> Optional[aiosqlite.Row]:
        """Validate a persistent token. Never consumed — reusable across connections.

        Prefix matching: the injector truncates the 64-char token to the player's
        Steam name slot length, so we match any stored token that starts with the
        injected prefix.
        """
        async with self._db.execute(
            '''SELECT u.id AS user_id, u.username, u.discord_id, u.is_staff, u.is_dev
               FROM tokens t JOIN users u ON t.user_id = u.id
               WHERE t.token LIKE ? || '%'
               ORDER BY length(t.token) ASC
               LIMIT 1''',
            (token,),
        ) as cur:
            return await cur.fetchone()

    # ── Bans ─────────────────────────────────────────────────────────────────

    async def is_ip_banned(self, ip_address: str) -> bool:
        now = int(time.time())
        async with self._db.execute(
            '''SELECT 1 FROM bans
               WHERE ip_address = ?
               AND (expires_at IS NULL OR expires_at > ?)
               LIMIT 1''',
            (ip_address, now),
        ) as cur:
            return await cur.fetchone() is not None

    async def is_user_banned(self, user_id: int) -> bool:
        now = int(time.time())
        async with self._db.execute(
            '''SELECT 1 FROM bans
               WHERE user_id = ?
               AND (expires_at IS NULL OR expires_at > ?)
               LIMIT 1''',
            (user_id, now),
        ) as cur:
            return await cur.fetchone() is not None

    async def ban_ip(
        self,
        ip: str,
        reason: str = '',
        banned_by: str = 'system',
        duration_secs: Optional[int] = None,
    ) -> None:
        expires_at = int(time.time()) + duration_secs if duration_secs else None
        await self._db.execute(
            'INSERT INTO bans (ip_address, reason, banned_by, expires_at) VALUES (?, ?, ?, ?)',
            (ip, reason, banned_by, expires_at),
        )
        await self._db.commit()

    async def ban_user(
        self,
        user_id: int,
        reason: str = '',
        banned_by: str = 'system',
        duration_secs: Optional[int] = None,
    ) -> None:
        expires_at = int(time.time()) + duration_secs if duration_secs else None
        await self._db.execute(
            'INSERT INTO bans (user_id, reason, banned_by, expires_at) VALUES (?, ?, ?, ?)',
            (user_id, reason, banned_by, expires_at),
        )
        await self._db.commit()

    async def unban_ip(self, ip: str) -> int:
        async with self._db.execute(
            'DELETE FROM bans WHERE ip_address = ?', (ip,)
        ) as cur:
            await self._db.commit()
            return cur.rowcount

    async def unban_user(self, user_id: int) -> int:
        async with self._db.execute(
            'DELETE FROM bans WHERE user_id = ?', (user_id,)
        ) as cur:
            await self._db.commit()
            return cur.rowcount

    async def list_bans(self) -> list:
        async with self._db.execute('SELECT * FROM bans ORDER BY banned_at DESC') as cur:
            return await cur.fetchall()

    # ── Connection log ───────────────────────────────────────────────────────

    async def log_connection(
        self, ip_address: str, username: str, user_id: Optional[int] = None
    ) -> int:
        async with self._db.execute(
            'INSERT INTO connection_log (user_id, ip_address, username) VALUES (?, ?, ?)',
            (user_id, ip_address, username),
        ) as cur:
            await self._db.commit()
            return cur.lastrowid

    async def update_connection_user(
        self, connection_id: int, user_id: int, username: str
    ) -> None:
        """Backfill user_id and username once identity is resolved from a later packet."""
        await self._db.execute(
            'UPDATE connection_log SET user_id = ?, username = ? WHERE id = ?',
            (user_id, username, connection_id),
        )
        await self._db.commit()

    async def log_disconnection(self, connection_id: int) -> None:
        await self._db.execute(
            'UPDATE connection_log SET disconnected_at = strftime(\'%s\',\'now\') WHERE id = ?',
            (connection_id,),
        )
        await self._db.commit()
