# zeus-signaling-server

WebSocket signaling and relay server for BeerRock multiplayer.

## Runtime gates

Set these environment variables in production to block outdated multiplayer clients at the server:

| env | Default | Description |
| --- | --- | --- |
| `MULTIPLAYER_MIN_APP_VERSION_CODE` | `1` | Minimum Android `clientVersionCode` allowed to use lobby/match start messages |
| `MULTIPLAYER_MIN_PROTOCOL_VERSION` | `1` | Minimum network protocol version |
| `MULTIPLAYER_MIN_BALANCE_VERSION` | `1` | Minimum gameplay balance/data version |
| `HEARTBEAT_INTERVAL_MS` | `15000` | Server WebSocket ping interval |
| `HEARTBEAT_TIMEOUT_MS` | `45000` | Time since last message/pong before a socket is terminated |
| `MAX_CONNECTIONS` | unset | Optional hard cap for simultaneous WebSocket clients |
| `MAX_ACTIVE_ROOMS` | unset | Optional cap for total in-memory rooms |
| `MAX_ACTIVE_MATCHES` | unset | Optional cap for active two-player matches |
| `CAPACITY_BUSY_RATIO` | `0.9` | When below `1`, block new matchmaking before a cap is fully reached |
| `CAPACITY_RETRY_AFTER_SEC` | `30` | Suggested retry delay returned by `/capacity` and server-busy WebSocket errors |
| `MAINTENANCE_MODE` | `false` | When true, block new multiplayer entry with a maintenance message |
| `MAINTENANCE_MESSAGE` | Korean default | Message shown to clients during maintenance |
| `DATABASE_URL` | unset | PostgreSQL connection string. When set, match results/rankings use Postgres instead of memory |
| `PGSSL` / `PGSSLMODE` | unset | Set `PGSSL=true` or `PGSSLMODE=require` if the Postgres provider requires SSL |

If a client is missing or below the version values, the server returns `{ type: "error", code: "update_required", message }`.
If capacity or maintenance gates block entry, the server returns `{ type: "error", code: "server_busy" | "server_maintenance", message, retryAfterSec }`.

## Lobby messages

| type | Direction | Description |
| --- | --- | --- |
| `create_room` | Client -> Server | Create room with compatibility metadata and `{ hostCharacterId, hostPassiveId, arenaId, hostNickname, hostPlayerId, networkMode }`; missing/invalid `networkMode` becomes `relay` |
| `room_created` | Server -> Host | `{ code, networkMode }` |
| `join_room` | Client -> Server | Compatibility metadata and `{ code, guestCharacterId, guestPassiveId, arenaId, guestNickname, guestPlayerId }` |
| `room_joined` | Server -> Guest | `{ code, matchId, networkMode, hostCharacterId, hostPassiveId, arenaId, hostNickname, hostPlayerId }` |
| `guest_joined` | Server -> Host | `{ matchId, networkMode, guestCharacterId, guestPassiveId, arenaId, guestNickname, guestPlayerId }` |
| `selection_update` | Client -> Server -> Peer | Live setup selection and ready state: `{ characterId, passiveId, arenaId, nickname, playerId, ready, networkMode }` |
| `ping_check` | Client -> Server | `{ clientTime, rttMs? }` |
| `ping_check_ack` | Server -> Client | `{ clientTime, serverTime }` |
| `get_room_list` | Client -> Server | Open rooms |
| `room_list` | Server -> Client | `{ rooms: [{ code, pingMs, quality, hostCharacterId?, arenaId?, networkMode }] }` |

`create_room` and `join_room` also enforce the same capacity gates as `/capacity`, so clients cannot bypass overload protection by skipping the HTTP preflight.

Relay game packets (`game_start`, `game_ready`, `game_countdown_sync`, `game_state`, `game_skill`, `game_damage`, `game_state_hp`, `game_emote`, `game_over`, `game_start_failed`, `rematch_request`, `rematch_cancel`, `rematch_reselect`, `rematch_ready`, rematch accept/decline, etc.) are forwarded unchanged to the peer. WebRTC `offer` / `answer` / `ice_candidate` messages are forwarded for P2P DataChannel setup.

Rate limits are type-specific so lobby/signaling traffic stays low while `game_state` can sustain 20 Hz relay traffic.

Room quality uses smoothed client-reported RTT samples. Server labels are `good` at `<=120ms`, `casual` at `<=240ms`, and `poor` above that.

## HTTP stats API

The same process also exposes HTTP JSON APIs. With `DATABASE_URL` set, the server creates and uses PostgreSQL tables. Without `DATABASE_URL`, it falls back to process-local memory for local development and data is lost on restart.

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/health` | Process health, version, storage mode, room/player counts |
| `GET` | `/capacity` | Multiplayer entry status, current counts, optional caps, retry delay, and minimum version requirements |
| `POST` | `/matches/result` | Store a single-player result for one player |
| `POST` | `/matches/pvp-result` | Store a PvP result for both players and return local reward/MMR data |
| `GET` | `/rankings?mode=single|multi` | Return ranking rows from the active stats storage |
| `GET` | `/players/:playerIdOrNickname/stats?mode=single|multi` | Return one player's aggregate stats |

Recommended Railway rollout:

- Hobby: use for development, two-device QA, and small closed tests. Set conservative caps such as `MAX_CONNECTIONS`, `MAX_ACTIVE_ROOMS`, and `MAX_ACTIVE_MATCHES`, then verify the app shows the busy/maintenance popup instead of entering multiplayer.
- Pro: switch before public SLT/release multiplayer, then raise caps based on load-test results and Railway CPU/RAM/egress metrics.

PvP results prefer `playerId` as the identity key and use nickname only as a fallback/display value. Normal PvP wins return the full coin reward; disconnect/forfeit wins return reduced reward so abuse-prone outcomes can still count for MMR without becoming a farming path. Duplicate `serverMatchId`/`clientMatchId` submissions are idempotent, so both devices can submit the same match without double-counting.

PostgreSQL tables are created automatically at startup:

- `br_player_stats`: aggregate MMR, wins/losses/draws, streaks, favorite character data, and coin totals.
- `br_match_results`: idempotency records and stored result payloads.
