"""
Game server process supervisor.

Spawns the game executable (or via Wine on Linux), tails its log file for
match events, and receives pushed state from match_tracker.dll over a
persistent TCP connection (port 4950). Restarts cleanly on crash, match
completion, or idle timeout.
"""

import asyncio
import json
import logging
import re
import time
from pathlib import Path
from typing import Optional

from .config import Config
from .match_state import MatchStateManager

log = logging.getLogger(__name__)

_RE_READY       = re.compile(r'LogInit:Display: Game Engine Initialized')
_RE_MATCH_START = re.compile(r'LogInteractive:.*\bOnMatchStarted\b')
_RE_MATCH_END   = re.compile(r'R:GameServer: The match was complete')
_RE_MAP         = re.compile(r'LogWorld: Bringing World .*/([^/\.]+) up for play')


class GameSupervisor:
    def __init__(self, config: Config, match_state: MatchStateManager):
        self._cfg = config
        self._state = match_state
        self._proc: Optional[asyncio.subprocess.Process] = None
        self._log_path = Path(config.log_path) if config.log_path else None
        self._running = False
        self._restart_event = asyncio.Event()

    async def start(self) -> None:
        self._running = True
        await self._run_loop()

    async def stop(self) -> None:
        self._running = False
        self._restart_event.set()
        if self._proc and self._proc.returncode is None:
            self._proc.terminate()
            try:
                await asyncio.wait_for(self._proc.wait(), timeout=10)
            except asyncio.TimeoutError:
                self._proc.kill()

    def request_restart(self) -> None:
        self._restart_event.set()

    # ── Tracker push listener ──────────────────────────────────────────────────

    async def listen_for_tracker(self) -> None:
        """Persistent TCP server that accepts push connections from match_tracker.dll."""
        server = await asyncio.start_server(
            self._handle_tracker_push,
            '127.0.0.1',
            self._cfg.tracker_push_port,
        )
        log.info('Tracker push receiver on :%d', self._cfg.tracker_push_port)
        async with server:
            await server.serve_forever()

    async def _handle_tracker_push(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        addr = writer.get_extra_info('peername')
        log.info('match_tracker connected from %s', addr)
        try:
            while True:
                line = await asyncio.wait_for(reader.readline(), timeout=60.0)
                if not line:
                    break
                try:
                    data = json.loads(line.decode('utf-8'))
                    await self._state.update_from_tracker(data)
                except (json.JSONDecodeError, Exception) as exc:
                    log.debug('Tracker push parse error: %s', exc)
        except (asyncio.TimeoutError, ConnectionResetError, asyncio.IncompleteReadError, OSError):
            pass
        finally:
            log.info('match_tracker disconnected from %s', addr)
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def _run_loop(self) -> None:
        while self._running:
            self._restart_event.clear()
            await self._state.reset()
            await self._launch()

            if self._proc is None:
                log.error('Failed to launch game server; retrying in 15s')
                await self._state.set_offline()
                await asyncio.sleep(15)
                continue

            tasks = [
                asyncio.ensure_future(self._watch_process()),
                asyncio.ensure_future(self._watch_log()),
                asyncio.ensure_future(self._restart_event.wait()),
            ]

            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for t in pending:
                t.cancel()
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass

            if self._proc and self._proc.returncode is None:
                log.info('Terminating game server process')
                self._proc.terminate()
                try:
                    await asyncio.wait_for(self._proc.wait(), timeout=15)
                except asyncio.TimeoutError:
                    self._proc.kill()

            if not self._running:
                break

            await self._state.set_offline()
            log.info('Restarting in 5s...')
            await asyncio.sleep(5)

    async def _launch(self) -> None:
        cmd: list[str] = []
        if self._cfg.use_wine:
            cmd.append('wine')
        cmd.append(self._cfg.game_exe)
        if self._cfg.game_args:
            args = self._cfg.game_args
            if self._cfg.gamemode:
                import re as _re
                args = _re.sub(r'\?game=[^?&\s]+', f'?game={self._cfg.gamemode}', args)
                if '?game=' not in args:
                    args += f'?game={self._cfg.gamemode}'
            cmd.extend(args.split())
        cmd.append(f'-port={self._cfg.server_port}')

        log.info('Launching: %s', ' '.join(cmd))
        try:
            self._proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            log.info('Game server PID: %d', self._proc.pid)
            asyncio.ensure_future(self._pipe_game_output(self._proc))
        except Exception as exc:
            log.error('Launch failed: %s', exc)
            self._proc = None

    # ── Watchers ──────────────────────────────────────────────────────────────

    async def _watch_process(self) -> None:
        if self._proc is None:
            return
        code = await self._proc.wait()
        log.warning('Game server exited (code %d)', code)

    async def _pipe_game_output(self, proc: asyncio.subprocess.Process) -> None:
        """Forward game/Wine stdout lines that contain mod tags to Python logging."""
        if proc.stdout is None:
            return
        try:
            async for raw in proc.stdout:
                line = raw.decode('utf-8', errors='replace').rstrip()
                if '[match_tracker]' in line or '[mod_loader]' in line:
                    log.info('[game] %s', line)
        except (asyncio.CancelledError, Exception):
            pass

    def _find_active_log(self) -> Optional[Path]:
        if not self._log_path:
            return None
        if self._log_path.exists():
            return self._log_path
        candidates = sorted(
            [p for p in self._log_path.parent.glob('g3*.log') if 'backup' not in p.name],
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        return candidates[0] if candidates else None

    async def _watch_log(self) -> None:
        if not self._log_path:
            return

        for _ in range(60):
            if self._find_active_log():
                break
            await asyncio.sleep(1)
        else:
            log.warning('Log file never appeared: %s', self._log_path)
            return

        current_path: Optional[Path] = None
        fh = None
        try:
            while True:
                active = self._find_active_log()
                if active != current_path:
                    if fh:
                        fh.close()
                        fh = None
                    if active:
                        log.info('Watching log: %s', active)
                        fh = active.open('r', encoding='utf-8', errors='replace')
                        fh.seek(0, 2)
                        current_path = active

                if fh:
                    line = fh.readline()
                    if line:
                        await self._handle_log_line(line.rstrip())
                        continue
                await asyncio.sleep(0.25)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.error('Log watcher error: %s', exc)
        finally:
            if fh:
                fh.close()

    async def _handle_log_line(self, line: str) -> None:
        if _RE_READY.search(line):
            log.info('Game server ready')
            await self._state.set_server_ready()
        elif _RE_MATCH_START.search(line):
            await self._state.set_match_started()
        elif _RE_MATCH_END.search(line):
            log.info('Match complete (log signal)')
            await self._state.signal_match_end()
            if self._cfg.restart_on_match_end:
                self.request_restart()
        elif m := _RE_MAP.search(line):
            await self._state.set_map_name(m.group(1))

    # ── Idle watchdog ─────────────────────────────────────────────────────────

    async def run_idle_watchdog(self) -> None:
        threshold = self._cfg.idle_restart_minutes * 60
        while True:
            await asyncio.sleep(60)
            s = self._state.state
            if (
                s.status == 'WaitingForPlayers'
                and not s.players
                and (time.time() - s.last_updated) > threshold
            ):
                log.info('Server idle for %d min — restarting', self._cfg.idle_restart_minutes)
                self.request_restart()
