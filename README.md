# Elemental Fracture — Game Server

Python-based infrastructure layer for the Elemental Fracture Spellbreak private server. Sits between game clients and the UE4 dedicated server binary, providing authentication, session management, moderation, and real-time match broadcasting.

## Architecture

```
Client (UDP) ──► elefrac UDP proxy :7777 ──► g3Server.exe :7778
                        │
                        ├── Auth: validates token from packet UID field
                        ├── SQLite DB: players, tokens, bans
                        ├── TCP control :3387 ← Discord bot / admin CLI
                        └── Broadcast :8777 → match state stream
```

**Key components:**

| Module | Purpose |
|---|---|
| `elefrac/udp_proxy.py` | Intercepts join packets, authenticates players, rewrites names |
| `elefrac/packet_parser.py` | Decodes Spellbreak's 2×-encoded UDP packets |
| `elefrac/tcp_control.py` | Admin control protocol (register, ban, kick, token rotation) |
| `elefrac/database.py` | SQLite schema and async query layer |
| `elefrac/ban_handler.py` | IP and account ban enforcement |
| `elefrac/game_supervisor.py` | Watches and restarts the game server process |
| `elefrac/broadcast.py` | TCP stream of live match state for the Discord bot |

## Authentication

Players authenticate via the `auth_injector` client mod, which writes the first 32 chars of their 64-char token into the hardware UID field of the join packet (2×-encoded). The proxy extracts this, validates it as a token prefix against the DB, and checks that the Steam display name matches the registered username. Mismatches and unknown tokens are dropped.

Token registration and rotation is handled by the Discord bot via the TCP control protocol.

## Running with Docker

The recommended way. The game binary (`g3Server.exe`) is provisioned at container start via `PATCH_URL` — it is not included in this repo.

```bash
# Copy and configure
cp config.ini.example config.ini
# edit config.ini — set control password, paths, ports

# Build and run
docker compose -f ../docker-compose-test.yaml --env-file ../.env-dev up --build
```

Environment variables (passed via docker-compose):

| Variable | Description |
|---|---|
| `PATCH_URL` | URL to prod patch zip (game content) |
| `PATCH_TEST_URL` | URL to dev patch zip |
| `PATCH_ENV` | `prod`, `dev`, or `vanilla` (skip patch) |

## Running locally

```bash
pip install -r requirements.txt
cp config.ini.example config.ini
# edit config.ini
python3 -m elefrac
```

## Configuration

`config.ini.example` documents all options. Copy it to `config.ini` and adjust:

- **`[Proxy]`** — listen port, game host/port, `require_auth`
- **`[GameServer]`** — path to `g3Server.exe`, map args, restart behaviour
- **`[Control]`** — TCP control host/port and password (set a strong password in production)
- **`[Broadcast]`** — match state broadcast port

## Mods

Place server-side mod DLLs in `BaseServer/Mods/dlls/`. The `match_tracker.dll` mod streams live match state to the proxy via TCP on port 4951.

Subdirectory layout:
```
BaseServer/Mods/
  dlls/       ← server mod DLLs (not committed — distribute separately)
  commands/   ← server-side command configs
  scripts/    ← mod scripts
  packages/   ← mod packages
  contents/   ← mod content assets
  servers/    ← per-server overrides
```

## TCP Control Protocol

The Discord bot and admin tools connect to `host:control_port` and issue newline-terminated commands. If a password is set, send `AUTH <password>` first.

```
STATUS                          server + match state
PLAYERS                         live session list
KICK <username>
BAN_USER <username> [reason]
UNBAN_USER <username>
BAN_IP <ip> [reason]
UNBAN_IP <ip>
BANS                            list active bans
REGISTER <discord_id> <username>   create account + token (username: alphanumeric, spaces, `'._-`, max 20 chars)
ROTATE_TOKEN <discord_id>          replace token
DELETE_REGISTRATION <discord_id>   remove account + tokens
LOOKUP_DISCORD <discord_id>        look up account by Discord ID
RESTART
```

All responses are JSON: `{"ok": true, ...}` or `{"ok": false, "error": "..."}`.
