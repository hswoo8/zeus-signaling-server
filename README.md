# zeus-signaling-server

WebSocket signaling and relay server for BeerRock multiplayer.

## Runtime gates

Set these environment variables in production to block outdated multiplayer clients at the server:

| env | Default | Description |
| --- | --- | --- |
| `SERVICE_ROLE` | `game` | Run `game` signaling/stats server or stateless `router` from the same image |
| `SERVER_CHANNEL` | `unrestricted` | Game environment label such as `production` or `beta` |
| `SERVER_POOL_ID` | `default` | Blue/green or beta pool identifier returned by health/capacity |
| `SERVER_ALLOWED_CHANNELS` | channel default | Comma-separated app channels accepted by WebSocket and stats endpoints |
| `MULTIPLAYER_MIN_APP_VERSION_CODE` | `1` | Minimum Android `clientVersionCode` allowed to use lobby/match start messages |
| `MULTIPLAYER_MAX_APP_VERSION_CODE` | unset | Optional maximum Android version for this pool |
| `MULTIPLAYER_MIN_PROTOCOL_VERSION` | `1` | Minimum network protocol version |
| `MULTIPLAYER_MAX_PROTOCOL_VERSION` | unset | Optional maximum protocol version for this pool |
| `MULTIPLAYER_RULESET_VERSION` | unset | Exact gameplay ruleset required by this pool |
| `MULTIPLAYER_MIN_BALANCE_VERSION` | `1` | Minimum gameplay balance/data version |
| `MULTIPLAYER_MAX_BALANCE_VERSION` | unset | Optional maximum balance version for this pool |
| `HEARTBEAT_INTERVAL_MS` | `15000` | Server WebSocket ping interval |
| `HEARTBEAT_TIMEOUT_MS` | `45000` | Time since last message/pong before a socket is terminated |
| `WS_BACKPRESSURE_SOFT_BYTES` | `262144` | Drop superseded `game_state` packets above this pending-send buffer |
| `WS_BACKPRESSURE_HARD_BYTES` | `1048576` | Terminate a slow socket above this pending-send buffer to protect server memory |
| `MAX_CONNECTIONS` | unset | Optional hard cap for simultaneous WebSocket clients |
| `MAX_ACTIVE_ROOMS` | unset | Optional cap for total in-memory rooms |
| `MAX_ACTIVE_MATCHES` | unset | Optional cap for active two-player matches |
| `CAPACITY_BUSY_RATIO` | `0.9` | When below `1`, block new matchmaking before a cap is fully reached |
| `CAPACITY_RETRY_AFTER_SEC` | `30` | Suggested retry delay returned by `/capacity` and server-busy WebSocket errors |
| `MAINTENANCE_MODE` | `false` | When true, block new multiplayer entry with a maintenance message |
| `MAINTENANCE_MESSAGE` | Korean default | Message shown to clients during maintenance |
| `DATABASE_URL` | unset | PostgreSQL connection string. When set, match results/rankings use Postgres instead of memory |
| `CONFIRMED_MATCH_TTL_MS` | `86400000` | Time a server-confirmed PvP result remains eligible for stats submission |
| `ANALYTICS_INGEST_ENABLED` | `true` | Accept debug/beta analytics events at `/analytics/events` |
| `ANALYTICS_RETENTION_DAYS` | `90` | Analytics event retention and dashboard aggregation window |
| `ANALYTICS_RATE_LIMIT_PER_MINUTE` | `120` | Per-source in-memory rate limit for analytics ingestion |
| `ADMIN_DASHBOARD_USERNAME` | `admin` | Basic-auth username for `/admin` |
| `ADMIN_DASHBOARD_PASSWORD` | unset | Required password; `/admin` stays disabled when unset |
| `PGSSL` / `PGSSLMODE` | unset | Set `PGSSL=true` or `PGSSLMODE=require` if the Postgres provider requires SSL |

If a client is missing or below the version values, the server returns `{ type: "error", code: "update_required", message }`.
If capacity or maintenance gates block entry, the server returns `{ type: "error", code: "server_busy" | "server_maintenance", message, retryAfterSec }`.

## Match router

Set `SERVICE_ROLE=router` to run the stateless `/route` service. Configure `ROUTER_CHANNEL`, `ROUTER_ALLOWED_CHANNELS`, and `ROUTER_ROUTES_JSON`. Each route defines a pool, secure WebSocket/stats URLs, app version bounds, an exact protocol/ruleset, and balance bounds. Unsupported combinations return `update_required`; cross-environment requests return `wrong_environment`.

Clients must route before capacity/WebSocket connection. The selected game server repeats the channel and version validation, so a bad or stale route cannot mix beta and production rooms. Android debug uses the beta router; production candidates use the production router.

## Lobby messages

| type | Direction | Description |
| --- | --- | --- |
| `create_room` | Client -> Server | Create room with compatibility metadata and `{ hostCharacterId, hostPassiveId, arenaId, battleType, hostNickname, hostPlayerId, networkMode }`; missing/invalid `networkMode` becomes `relay`, missing/invalid `battleType` becomes `short` |
| `room_created` | Server -> Host | `{ code, networkMode, battleType }` |
| `join_room` | Client -> Server | Compatibility metadata and `{ code, guestCharacterId, guestPassiveId, arenaId, guestNickname, guestPlayerId }` |
| `room_joined` | Server -> Guest | `{ code, networkMode, battleType, hostCharacterId, hostPassiveId, arenaId, hostNickname, hostPlayerId }` |
| `guest_joined` | Server -> Host | `{ networkMode, battleType, guestCharacterId, guestPassiveId, arenaId, guestNickname, guestPlayerId }` |
| `leave_room` | Client -> Server | Leave the waiting/rematch room while keeping the WebSocket connected |
| `room_left` | Server -> Client | Confirms that room state was cleared; the client can return to room-list polling |
| `selection_update` | Client -> Server -> Peer | Live setup selection and ready state: `{ characterId, passiveId, arenaId, battleType, nickname, playerId, ready, networkMode }`; room `battleType` is fixed at `create_room` |
| `ping_check` | Client -> Server | `{ clientTime, rttMs? }` |
| `ping_check_ack` | Server -> Client | `{ clientTime, serverTime }` |
| `get_room_list` | Client -> Server | Open rooms |
| `room_list` | Server -> Client | `{ rooms: [{ code, pingMs, quality, hostCharacterId?, arenaId?, battleType, networkMode }] }` |
| `room_updated` / `room_removed` | Server -> Client | Push room-list deltas after the initial snapshot; clients periodically request a new snapshot for recovery |

`create_room` and `join_room` also enforce the same capacity gates as `/capacity`, so clients cannot bypass overload protection by skipping the HTTP preflight.

Each accepted `game_start` assigns a new `matchId`, including rematches. The guest receives it on `game_start`, the host receives `match_assigned`, and both receive it again on `game_countdown_sync`. Relay game packets remain peer-compatible. On `game_over` or an active-match socket disconnect, the server emits perspective-correct `match_result { matchId, outcome, finishReason, serverConfirmed }` packets to both sides. After `game_ready`, the server sends `game_countdown_sync { matchId, serverTimeMs, battleStartAtMs }` so battle countdown starts from the same server-authoritative time. WebRTC `offer` / `answer` / `ice_candidate` messages are forwarded for P2P DataChannel setup.

Rate limits are type-specific so lobby/signaling traffic stays low while `game_state` can sustain 20 Hz relay traffic.

Room quality uses smoothed client-reported RTT samples. Server labels are `good` at `<=120ms`, `casual` at `<=240ms`, and `poor` above that.

## HTTP stats API

The same process also exposes HTTP JSON APIs. With `DATABASE_URL` set, the server creates and uses PostgreSQL tables. Without `DATABASE_URL`, it falls back to process-local memory for local development and data is lost on restart.

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/health` | Process health, version, channel, pool, ruleset, storage mode, room/player counts |
| `GET` | `/capacity` | Multiplayer entry status, current counts, optional caps, retry delay, and minimum version requirements |
| `POST` | `/matches/result` | Store a single-player result for one player; requires an allowed `X-App-Channel` |
| `POST` | `/matches/pvp-result` | Store a server-confirmed PvP result for both players; requires an allowed `X-App-Channel` |
| `GET` | `/rankings?mode=single|multi` | Return ranking rows from the active stats storage |
| `GET` | `/players/:playerIdOrNickname/stats?mode=single|multi` | Return one player's aggregate stats |
| `POST` | `/analytics/events` | Accept allowlisted launch, screen, feature, and single-match analytics events |
| `GET` | `/admin` | Basic-auth operations dashboard; disabled until admin password is configured |
| `GET` | `/admin/api/stats` | Basic-auth JSON snapshot used for operations or future admin tooling |

Recommended Railway rollout:

- Hobby: use for development, two-device QA, and small closed tests. Set conservative caps such as `MAX_CONNECTIONS`, `MAX_ACTIVE_ROOMS`, and `MAX_ACTIVE_MATCHES`, then verify the app shows the busy/maintenance popup instead of entering multiplayer.
- Pro: switch before public SLT/release multiplayer, then raise caps based on load-test results and Railway CPU/RAM/egress metrics.

PvP results prefer `playerId` as the identity key and use nickname only as a fallback/display value. `/matches/pvp-result` rejects unknown match IDs, participant mismatches, and outcomes that differ from the WebSocket-confirmed result. Normal PvP wins return the full coin reward; disconnect/forfeit wins return reduced reward so abuse-prone outcomes can still count for MMR without becoming a farming path. Duplicate `serverMatchId` submissions are idempotent, so both devices can submit the same match without double-counting.

PostgreSQL tables are created automatically at startup:

- `br_player_stats`: aggregate MMR, wins/losses/draws, streaks, favorite character data, and coin totals.
- `br_match_results`: idempotency records and stored result payloads.
- `br_pvp_match_confirmations`: short-lived server-confirmed match participants and outcomes used to validate PvP result submissions.
- `br_analytics_events`: anonymized launches, single matches, and server-confirmed multiplayer matches retained for the configured analytics window.

## Admin analytics

The dashboard reports DAU/WAU/MAU, DAU/MAU stickiness, launches, screen and feature usage, single/multiplayer match counts, matches per active user, live connections, versions, country/locale codes, User-Agent distribution, finish reasons, and repeated-opponent risk signals. It includes a 30-day activity line chart plus country, feature, screen, and finish-reason distribution bars. Multiplayer matches are recorded from server-confirmed results. All metrics can be filtered by `dev`, `beta`, or `production` distribution channel. Android debug and release builds upload launch, screen, feature, and single-match events; closed-beta release artifacts must set `ANALYTICS_CHANNEL=beta` at build time.

Set `ADMIN_DASHBOARD_PASSWORD` in Railway, optionally change `ADMIN_DASHBOARD_USERNAME`, redeploy, then open `/admin`. The browser uses HTTP Basic authentication. Do not put the password in source control, a static webpage, or a query parameter.

Analytics stores a one-way short hash of the anonymous player ID. It does not persist raw IP addresses. Country prefers a trusted proxy country header and falls back to the Android locale country code, so it is operational grouping rather than precise location. Persistent history requires `DATABASE_URL`; memory mode resets dashboard history whenever the server restarts.
