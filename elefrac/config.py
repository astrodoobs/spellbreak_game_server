import configparser
import os


_TRUE_VALS  = {'1', 'true',  'yes', 'on'}
_FALSE_VALS = {'0', 'false', 'no',  'off'}

def _env_bool(name: str):
    """Return True/False if the env var is set to a recognised value, else None."""
    val = os.environ.get(name, '').strip().lower()
    if val in _TRUE_VALS:  return True
    if val in _FALSE_VALS: return False
    return None


class Config:
    def __init__(self, path: str = 'config.ini'):
        self._cfg = configparser.ConfigParser()
        if not self._cfg.read(path):
            raise FileNotFoundError(f'Config file not found: {path}')

    def _s(self, section, key, fallback=None):
        return self._cfg.get(section, key, fallback=fallback)

    def _i(self, section, key, fallback=None):
        return self._cfg.getint(section, key, fallback=fallback)

    def _f(self, section, key, fallback=None):
        return self._cfg.getfloat(section, key, fallback=fallback)

    def _b(self, section, key, fallback=None):
        return self._cfg.getboolean(section, key, fallback=fallback)

    # Proxy
    @property
    def proxy_host(self):       return self._s('Proxy', 'listen_host', '0.0.0.0')
    @property
    def proxy_port(self):       return self._i('Proxy', 'listen_port', 7776)
    @property
    def game_host(self):        return self._s('Proxy', 'game_host', '127.0.0.1')
    @property
    def game_port(self):        return self._i('Proxy', 'game_port', 7777)
    @property
    def session_timeout(self):
        env = os.environ.get('SESSION_TIMEOUT', '').strip()
        if env.isdigit():
            return int(env)
        return self._i('Proxy', 'session_timeout', 600)
    @property
    def require_auth(self):
        env = _env_bool('REQUIRE_AUTH')
        return env if env is not None else self._b('Proxy', 'require_auth', True)

    # Database
    @property
    def db_path(self):          return self._s('Database', 'path', 'elefrac.db')

    # GameServer
    @property
    def game_exe(self):                 return self._s('GameServer', 'exe_path', '')
    @property
    def game_args(self):                return self._s('GameServer', 'args', '')
    @property
    def log_path(self):                 return self._s('GameServer', 'log_path', '')
    @property
    def gamemode(self):
        return os.environ.get('GAMEMODE', '').strip() or self._s('GameServer', 'gamemode', '')
    @property
    def server_port(self):              return self._i('GameServer', 'port', 7777)
    @property
    def restart_on_match_end(self):     return self._b('GameServer', 'restart_on_match_end', True)
    @property
    def idle_restart_minutes(self):     return self._i('GameServer', 'idle_restart_minutes', 30)
    @property
    def use_wine(self):                 return self._b('GameServer', 'use_wine', False)

    # MatchTracker
    @property
    def tracker_port(self):             return self._i('MatchTracker', 'port', 4951)
    @property
    def tracker_push_port(self):        return self._i('MatchTracker', 'push_port', 4950)

    # Broadcast
    @property
    def broadcast_host(self):   return self._s('Broadcast', 'host', '0.0.0.0')
    @property
    def broadcast_port(self):   return self._i('Broadcast', 'port', 4947)

    # Control
    @property
    def control_host(self):     return self._s('Control', 'host', '127.0.0.1')
    @property
    def control_port(self):     return self._i('Control', 'port', 8880)
    @property
    def control_password(self): return self._s('Control', 'password', '')
