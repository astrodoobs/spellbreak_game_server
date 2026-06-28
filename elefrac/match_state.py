import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Callable, List, Optional

log = logging.getLogger(__name__)


@dataclass
class Player:
    username: str
    player_id: int
    ping: int
    is_bot: bool
    is_spectator: bool = False
    joined_at: float = field(default_factory=time.time)


@dataclass
class MatchState:
    status: str = 'WaitingForServer'
    map_name: str = ''
    players: List[Player] = field(default_factory=list)
    match_started_at: Optional[float] = None
    last_updated: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        """Serialise to the wire format expected by the Discord bot."""
        return {
            'state': self.status,
            'map': self.map_name,
            'players': [
                {
                    'username': p.username,
                    'id': p.player_id,
                    'ping': p.ping,
                    'is_spectator': p.is_spectator,
                    'is_bot': p.is_bot,
                }
                for p in self.players
            ],
            'match_start_time': int(self.match_started_at) if self.match_started_at else 0,
            'events': [],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict())


class MatchStateManager:
    def __init__(self):
        self._state = MatchState()
        self._lock = asyncio.Lock()
        self._on_state_change: List[Callable] = []
        self._on_match_end: List[Callable] = []
        self._on_player_join: List[Callable] = []
        self._on_player_leave: List[Callable] = []

    @property
    def state(self) -> MatchState:
        return self._state

    # ── Callback registration ─────────────────────────────────────────────────

    def on_state_change(self, cb: Callable) -> None:
        """Register a callback invoked with the serialised state dict on any change."""
        self._on_state_change.append(cb)

    def on_match_end(self, cb: Callable) -> None:
        self._on_match_end.append(cb)

    def on_player_join(self, cb: Callable) -> None:
        self._on_player_join.append(cb)

    def on_player_leave(self, cb: Callable) -> None:
        self._on_player_leave.append(cb)

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _fire_state_change(self) -> None:
        snapshot = self._state.to_dict()
        for cb in self._on_state_change:
            asyncio.ensure_future(cb(snapshot))

    # ── State mutations ───────────────────────────────────────────────────────

    async def player_connected(self, username: str, player_id: int = 0) -> None:
        """Called by the UDP proxy when a player session is opened."""
        async with self._lock:
            if any(p.username == username for p in self._state.players):
                return
            # If the server is still in WaitingForServer when a player connects,
            # the ready log line was missed — advance the status.
            if self._state.status == 'WaitingForServer':
                self._state.status = 'WaitingForPlayers'
            self._state.players.append(
                Player(username=username, player_id=player_id, ping=0, is_bot=False)
            )
            self._state.last_updated = time.time()
        self._fire_state_change()
        for cb in self._on_player_join:
            asyncio.ensure_future(cb(username))

    async def player_disconnected(self, username: str) -> None:
        """Called by the UDP proxy when a player session closes."""
        async with self._lock:
            before = len(self._state.players)
            self._state.players = [p for p in self._state.players if p.username != username]
            if len(self._state.players) == before:
                return
            self._state.last_updated = time.time()
        self._fire_state_change()
        for cb in self._on_player_leave:
            asyncio.ensure_future(cb(username))

    async def set_match_started(self) -> None:
        async with self._lock:
            if self._state.status == 'InProgress':
                return
            self._state.status = 'InProgress'
            self._state.match_started_at = time.time()
            self._state.last_updated = time.time()
        log.info('Match started')
        self._fire_state_change()

    async def update_from_tracker(self, data: dict) -> None:
        """Merge richer DLL data (ping, spectator, bot) and reconcile disconnects."""
        departed: list[str] = []
        async with self._lock:
            changed = False
            new_status = data.get('state') or ''
            if new_status and new_status != self._state.status:
                self._state.status = new_status
                if new_status == 'InProgress' and not self._state.match_started_at:
                    self._state.match_started_at = time.time()
                changed = True

            tracker_map = {p['username']: p for p in data.get('players', [])}

            # Enrich existing proxy-tracked players with DLL data.
            for player in self._state.players:
                if player.username in tracker_map:
                    tp = tracker_map[player.username]
                    player.ping = tp.get('ping', player.ping)
                    player.is_bot = tp.get('is_bot', player.is_bot)
                    player.is_spectator = tp.get('is_spectator', player.is_spectator)
                    changed = True

            # Add any DLL-reported players the proxy hasn't seen yet.
            existing = {p.username for p in self._state.players}
            for username, tp in tracker_map.items():
                if username not in existing:
                    self._state.players.append(Player(
                        username=username,
                        player_id=tp.get('id', 0),
                        ping=tp.get('ping', 0),
                        is_bot=tp.get('is_bot', False),
                        is_spectator=tp.get('is_spectator', False),
                    ))
                    changed = True

            # When the DLL explicitly sends a player list, treat it as authoritative:
            # remove anyone the DLL no longer reports (they disconnected on the UE4 side).
            if 'players' in data:
                departed = [p.username for p in self._state.players if p.username not in tracker_map]
                if departed:
                    gone = set(departed)
                    self._state.players = [p for p in self._state.players if p.username not in gone]
                    changed = True

            if changed:
                self._state.last_updated = time.time()

        if changed:
            self._fire_state_change()

        for username in departed:
            log.info('DLL: %s left — firing disconnect', username)
            for cb in self._on_player_leave:
                asyncio.ensure_future(cb(username))

    async def set_server_ready(self) -> None:
        async with self._lock:
            self._state.status = 'WaitingForPlayers'
            self._state.last_updated = time.time()
        self._fire_state_change()

    async def set_map_name(self, map_name: str) -> None:
        async with self._lock:
            if self._state.map_name == map_name:
                return
            self._state.map_name = map_name
            self._state.last_updated = time.time()
        self._fire_state_change()

    async def signal_match_end(self) -> None:
        log.info('Match ended')
        async with self._lock:
            self._state.status = 'Ended'
            self._state.last_updated = time.time()
        self._fire_state_change()
        for cb in self._on_match_end:
            asyncio.ensure_future(cb())

    async def set_offline(self) -> None:
        async with self._lock:
            self._state.status = 'Offline'
            self._state.players = []
            self._state.last_updated = time.time()
        self._fire_state_change()

    async def reset(self) -> None:
        async with self._lock:
            self._state = MatchState()
        self._fire_state_change()
