import asyncio
import logging
import os
import signal

from .ban_handler import BanHandler
from .broadcast import BroadcastServer
from .config import Config
from .database import Database
from .game_supervisor import GameSupervisor
from .logging_setup import setup as setup_logging
from .match_state import MatchStateManager
from .tcp_control import ControlServer
from .udp_proxy import start_proxy


async def _main() -> None:
    setup_logging()
    log = logging.getLogger('elefrac')

    cfg = Config(os.environ.get('ELEFRAC_CONFIG', 'config.ini'))

    db = Database(cfg.db_path)
    await db.connect()

    match_state = MatchStateManager()
    ban_handler = BanHandler(db)

    proxy = await start_proxy(cfg, db, ban_handler, match_state)

    supervisor = GameSupervisor(cfg, match_state)
    broadcast = BroadcastServer(cfg, match_state)
    control = ControlServer(cfg, db, match_state, proxy=proxy, supervisor=supervisor)

    async def _on_match_end() -> None:
        await proxy.clear_all_sessions()

    async def _on_state_change(data: dict) -> None:
        await broadcast.push_update(data)

    async def _on_player_leave(username: str) -> None:
        await proxy.kick_by_username(username)

    match_state.on_match_end(_on_match_end)
    match_state.on_state_change(_on_state_change)
    match_state.on_player_leave(_on_player_leave)

    log.info('Elemental Fracture starting')

    loop = asyncio.get_running_loop()

    def _shutdown() -> None:
        log.info('Shutdown signal received')
        for task in asyncio.all_tasks(loop):
            task.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _shutdown)
        except (NotImplementedError, OSError):
            pass  # Windows does not support add_signal_handler

    try:
        await asyncio.gather(
            supervisor.start(),
            supervisor.listen_for_tracker(),
            broadcast.start(),
            control.start(),
            supervisor.run_idle_watchdog(),
        )
    except asyncio.CancelledError:
        log.info('Shutting down...')
    finally:
        await supervisor.stop()
        await db.close()
        log.info('Shutdown complete')


def main() -> None:
    asyncio.run(_main())
