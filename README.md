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
| `RELAY_MATCHES_ENABLED` | `true` | When false, keep signaling/P2P available but reject new Relay rooms and AUTO-to-Relay starts |
| `MAX_ACTIVE_RELAY_MATCHES` | unset | Optional cap for concurrently active Relay transports; existing matches are not interrupted |
| `RELAY_EGRESS_WARNING_MB_PER_HOUR` | unset | Mark Relay traffic warning state when rolling 60-minute WebSocket payload bytes reach this value |
| `RELAY_EGRESS_LIMIT_MB_PER_HOUR` | unset | Reject new Relay starts when rolling 60-minute WebSocket payload bytes reach this value |
| `CAPACITY_BUSY_RATIO` | `0.9` | When below `1`, block new matchmaking before a cap is fully reached |
| `CAPACITY_RETRY_AFTER_SEC` | `30` | Suggested retry delay returned by `/capacity` and server-busy WebSocket errors |
| `MAINTENANCE_MODE` | `false` | When true, block new multiplayer entry with a maintenance message |
| `MAINTENANCE_MESSAGE` | Korean default | Message shown to clients during maintenance |
| `DEPLOYMENT_DRAIN_MESSAGE` | Korean default | Message shown while the runtime deployment drain blocks new entry |
| `DATABASE_URL` | unset | PostgreSQL connection string. When set, match results/rankings use Postgres instead of memory |
| `CONFIRMED_MATCH_TTL_MS` | `86400000` | Time a server-confirmed PvP result remains eligible for stats submission |
| `RANK_PLACEMENT_MATCHES` | `10` | Ranked matches that use the placement K-factor |
| `RANK_PLACEMENT_K` | `48` | Elo K-factor while either player is in placement |
| `RANK_ESTABLISHED_K` | `24` | Elo K-factor after placement |
| `RANK_ELO_SPREAD` | `400` | Rating spread used by the Elo expected-score curve |
| `INTEGRITY_INVALID_FLAG_THRESHOLD` | `3` | Invalid ranked audit reports before an observe-only flag |
| `INTEGRITY_HP_MISMATCH_FLAG_THRESHOLD` | `3` | Consecutive paired HP mismatches before an observe-only flag |
| `INTEGRITY_REPORT_MAX_SKEW_MS` | `5000` | Maximum receive-time skew for comparing two audit reports |
| `ANALYTICS_INGEST_ENABLED` | `true` | Accept legacy-client analytics events at `/analytics/events`; current Android builds use Firebase Analytics |
| `ANALYTICS_RETENTION_DAYS` | `90` | Analytics event retention and dashboard aggregation window |
| `ANALYTICS_RATE_LIMIT_PER_MINUTE` | `120` | Per-source in-memory rate limit for analytics ingestion |
| `SUPPORT_INGEST_ENABLED` | `true` | Accept in-app support inquiries at `/support/inquiries` |
| `SUPPORT_RETENTION_DAYS` | `180` | Retention period for support messages and optional reply email |
| `SUPPORT_RATE_LIMIT_PER_HOUR` | `5` | Per-anonymous-player support inquiry rate limit |
| `SUPPORT_ADDRESS_RATE_LIMIT_PER_HOUR` | `10` | Per-source-address in-memory support inquiry rate limit; raw address is never persisted |
| `ADMIN_DASHBOARD_USERNAME` | `admin` | Basic-auth username for `/admin` |
| `ADMIN_DASHBOARD_PASSWORD` | unset | Required password; `/admin` stays disabled when unset |
| `AUTH_TOKEN_SECRET` | unset | At least 32 characters enables guest/access/WebSocket ticket authentication |
| `AUTH_ACCESS_TTL_SEC` | `3600` | Access-token lifetime used by the shared API auth client |
| `AUTH_GUEST_TTL_SEC` | `15552000` | Long-lived guest credential lifetime |
| `AUTH_WS_TICKET_TTL_SEC` | `60` | One-time WebSocket connection ticket lifetime |
| `AUTH_RATE_LIMIT_PER_MINUTE` | `30` | Per-source authentication endpoint rate limit |
| `PGSSL` / `PGSSLMODE` | unset | Set `PGSSL=true` or `PGSSLMODE=require` if the Postgres provider requires SSL |

If a client is missing or below the version values, the server returns `{ type: "error", code: "update_required", message }`.
If capacity or maintenance gates block entry, the server returns `{ type: "error", code: "server_busy" | "server_maintenance", message, retryAfterSec }`.
Relay admission failures return `relay_disabled`, `relay_capacity`, or `relay_egress_limited`. An AUTO room may still start over P2P while Relay admission is blocked. If AUTO falls back to Relay and admission fails, the server removes the waiting room and returns both players to the lobby without interrupting existing matches.
If a running P2P match later sends `game_state` over WebSocket, the server reclassifies it as a runtime Relay fallback for metrics and admission decisions. The existing match continues even when this temporarily moves active Relay usage above its configured cap.

For a controlled pool smoke test, pass the exact compatibility tuple to the load
client. Production targets always require the explicit safety flag, including
provider-owned candidate URLs:

```bash
npm run load-test -- \
  --url wss://candidate.example.com \
  --clients 1 \
  --duration 5 \
  --mode lobby \
  --channel production \
  --app-version 2 \
  --protocol-version 1 \
  --ruleset-version 1 \
  --balance-version 1 \
  --allow-production
```

## Match router

Set `SERVICE_ROLE=router` to run the stateless `/route` service. Configure `ROUTER_CHANNEL`, `ROUTER_ALLOWED_CHANNELS`, and `ROUTER_ROUTES_JSON`. Each route defines a pool, secure WebSocket/stats URLs, app version bounds, an exact protocol/ruleset, and balance bounds. Unsupported combinations return `update_required`; cross-environment requests return `wrong_environment`.

Clients must route before capacity/WebSocket connection. The selected game server repeats the channel and version validation, so a bad or stale route cannot mix beta and production rooms. Android debug uses the beta router; production candidates use the production router.

The router also exposes the app-wide update policy independently of multiplayer:

| env | Default | Description |
| --- | --- | --- |
| `APP_LATEST_VERSION_CODE` | `1` | Latest published positive Android version code |
| `APP_MIN_SUPPORTED_VERSION_CODE` | `1` | Oldest version code allowed to continue; must not exceed latest |
| `APP_UPDATE_MODE` | `none` | Policy for versions below latest but still supported: `none`, `soft`, or `force` |
| `APP_UPDATE_MESSAGE` | unset | Optional controlled message returned for `soft` or `force`; the current Android Splash keeps localized built-in copy |
| `ROUTER_STORE_URL` | unset | Optional store URL returned for `soft` or `force` |

Call `GET /app-policy?channel=production&versionCode=123`. The response includes `policy`, `currentVersionCode`, `latestVersionCode`, `minSupportedVersionCode`, and optional update message/store URL. A version below the minimum always gets `force`. A supported version below latest gets the configured policy: `none` hides the update, `soft` offers an optional update, and `force` blocks entry. A version at or above latest gets `none`.

```json
{
  "status": "ok",
  "code": "ok",
  "policy": "soft",
  "channel": "production",
  "currentVersionCode": 123,
  "latestVersionCode": 125,
  "minSupportedVersionCode": 120,
  "message": "새 버전을 설치해주세요.",
  "storeUrl": "https://play.google.com/store/apps/details?id=example"
}
```

An unapproved channel returns HTTP 409 with `wrong_environment`. A missing, malformed, zero, negative, or unsafe `versionCode` returns HTTP 400 with `invalid_request`. Policy responses and `/health` use `Cache-Control: no-store`; `/health.appPolicy` reports only the safe latest/minimum/mode configuration metadata.

## Lobby messages

| type | Direction | Description |
| --- | --- | --- |
| `create_room` | Client -> Server | Create room with compatibility metadata and `{ hostCharacterId, hostPassiveId, arenaId, battleType, debugNoKo, debugNoTime, hostNickname, hostPlayerId, networkMode }`; missing/invalid `networkMode` becomes `relay`, missing/invalid `battleType` becomes `short` |
| `room_created` | Server -> Host | `{ code, networkMode, arenaId, battleType, debugNoKo, debugNoTime }` |
| `join_room` | Client -> Server | Compatibility metadata and `{ code, guestCharacterId, guestPassiveId, arenaId, guestNickname, guestPlayerId }` |
| `room_joined` | Server -> Guest | `{ code, networkMode, battleType, debugNoKo, debugNoTime, hostCharacterId, hostPassiveId, arenaId, hostNickname, hostPlayerId }` |
| `guest_joined` | Server -> Host | `{ networkMode, battleType, debugNoKo, debugNoTime, guestCharacterId, guestPassiveId, arenaId, guestNickname, guestPlayerId }` |
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

Ranked rooms use the `short` battle type. The server selects one supported arena when the room is created and keeps that arena for both players. Internal `debugNoKo` and `debugNoTime` rules are fixed from the room creator's request and relayed to the guest.

Rate limits are type-specific so lobby/signaling traffic stays low while `game_state` can sustain 20 Hz relay traffic.

Room quality uses smoothed client-reported RTT samples. Server labels are `good` at `<=120ms`, `casual` at `<=240ms`, and `poor` above that.

## HTTP stats API

The same process also exposes HTTP JSON APIs. With `DATABASE_URL` set, the server creates and uses PostgreSQL tables. Without `DATABASE_URL`, it falls back to process-local memory for local development and data is lost on restart.

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/health` | Lightweight process readiness plus channel, pool, ruleset, room counts, and deployment-drain state |
| `GET` | `/capacity` | Multiplayer entry status, current counts, optional caps, retry delay, and minimum version requirements |
| `POST` | `/admin/api/deployment/drain` | Basic-auth runtime switch that blocks new entry while existing matches finish |
| `POST` | `/auth/guest/register` | Issue a long-lived signed guest credential for one player ID |
| `POST` | `/auth/token` | Exchange a valid guest credential for a short-lived API access token |
| `POST` | `/auth/ws-ticket` | Issue a one-time WebSocket connection ticket from an access token |
| `POST` | `/matches/result` | Store a single-player result for one player; requires an allowed `X-App-Channel` |
| `POST` | `/matches/pvp-result` | Store a server-confirmed PvP result for both players; requires an allowed `X-App-Channel` |
| `GET` | `/rankings?mode=single|multi` | Return ranking rows from the active stats storage |
| `GET` | `/players/:playerIdOrNickname/stats?mode=single|multi` | Return one player's aggregate stats |
| `POST` | `/analytics/events` | Legacy compatibility for older Android launch, screen, feature, and single-match analytics events |
| `POST` | `/support/inquiries` | Store one in-app support inquiry; requires `X-Player-Id`, allowed `X-App-Channel`, app metadata headers, category, message, and optional reply email |
| `GET` | `/admin` | Basic-auth operations dashboard; disabled until admin password is configured |
| `GET` | `/admin/api/stats` | Basic-auth JSON snapshot used for operations or future admin tooling |
| `GET` | `/admin/support` | Basic-auth support inbox with linked single/multi MMR and record context |
| `GET` | `/admin/api/support` | Basic-auth JSON support inbox; filter with `channel` and `status` |
| `POST` | `/admin/api/support/:inquiryId/status` | Basic-auth support status update: `open`, `review`, or `closed` |

`/health`, `/capacity`, and the admin snapshot expose process-local operational metrics: active Relay/P2P matches, Relay packet/byte totals, rolling 60-minute Relay bytes, Relay/capacity rejections, WebSocket backpressure drops/closes, disconnect sources, ranked integrity-audit counts, and rolling 60-second event-loop lag. Counters reset when the service restarts. The rolling egress value measures WebSocket payload bytes and is an application-level estimate; Railway Network Egress remains the billing source of truth.

`/health` deliberately does not query PostgreSQL. Railway calls it while a new deployment starts, so it must reflect process readiness without making deployment activation depend on a dashboard query. It remains HTTP 200 while runtime drain is active and reports `acceptingConnections=false`; `/capacity` and WebSocket admission enforce the actual entry block.

For a single-replica deployment, open `/admin`, select `배포 드레인 시작`, and wait until the dashboard reports zero active matches. The router remains online and continues returning the one stable game-service domain, while `/capacity` prevents new clients from entering. Deploy the game service only after active matches reach zero. A successful restart clears the in-memory drain automatically; if deployment is cancelled, select `신규 입장 재개`.

Ranked P2P clients send a compact `game_audit` directly over the signaling WebSocket every two seconds. The server compares each side's local/remote HP view and basic sequence/position bounds. Configurable repeated mismatch and invalid-report thresholds mark the in-memory match observation and the existing confirmed-match analytics record. This is observe-only: it does not reject a result or alter MMR until real beta data establishes safe thresholds.

Recommended Railway rollout:

- Hobby: use for development, two-device QA, and small closed tests. Set conservative caps such as `MAX_CONNECTIONS`, `MAX_ACTIVE_ROOMS`, and `MAX_ACTIVE_MATCHES`, then verify the app shows the busy/maintenance popup instead of entering multiplayer.
- Pro: switch before public SLT/release multiplayer, then raise caps based on load-test results and Railway CPU/RAM/egress metrics.

PvP results prefer `playerId` as the identity key and use nickname only as a fallback/display value. `/matches/pvp-result` rejects unknown match IDs, participant mismatches, and outcomes that differ from the WebSocket-confirmed result. Ranked MMR uses opponent-relative Elo: placement matches use K=48 by default, established players use K=24, and the response includes each player's rating before, delta, and rating after the match. Normal PvP wins return the full coin reward; disconnect/forfeit wins return reduced reward so abuse-prone outcomes can still count for MMR without becoming a farming path. Duplicate `serverMatchId` submissions are idempotent, so both devices can submit the same match without double-counting.

PostgreSQL tables are created automatically at startup:

- `br_player_stats`: aggregate MMR, wins/losses/draws, streaks, favorite character data, and coin totals.
- `br_match_results`: idempotency records and stored result payloads.
- `br_pvp_match_confirmations`: short-lived server-confirmed match participants and outcomes used to validate PvP result submissions.
- `br_analytics_events`: anonymized launches, single matches, and server-confirmed multiplayer matches retained for the configured analytics window.
- `br_support_inquiries`: in-app support messages, optional reply email, anonymous player ID, app/device/country metadata, and support status retained separately from analytics.

## Admin analytics

The dashboard reports DAU/WAU/MAU, DAU/MAU stickiness, launches, screen and feature usage, single/multiplayer match counts, matches per active user, live connections, versions, country/locale codes, User-Agent distribution, finish reasons, and repeated-opponent risk signals. It includes a 30-day activity line chart plus country, feature, screen, and finish-reason distribution bars. Multiplayer matches are recorded from server-confirmed results. All metrics can be filtered by `dev`, `beta`, or `production` distribution channel. Android debug and release builds upload launch, screen, feature, and single-match events; closed-beta release artifacts must set `ANALYTICS_CHANNEL=beta` at build time.

Set `ADMIN_DASHBOARD_PASSWORD` in Railway, optionally change `ADMIN_DASHBOARD_USERNAME`, redeploy, then open `/admin`. The browser uses HTTP Basic authentication. Do not put the password in source control, a static webpage, or a query parameter.

The same administrator credentials protect `/admin/support`. Each inquiry links the submitted anonymous `playerId` to the current `br_player_stats` single/multi MMR and record snapshot for support handling. Support messages and optional reply emails are not analytics events. They are retained for `SUPPORT_RETENTION_DAYS` and should only be accessed for support operations.

Current Android builds send app-usage analytics to Firebase Analytics and do not call `/analytics/events`. The legacy endpoint stores a one-way short hash of the anonymous player ID and does not persist raw IP addresses. Country prefers a trusted proxy country header and falls back to the Android locale country code, so it is operational grouping rather than precise location. Persistent legacy history requires `DATABASE_URL`; memory mode resets dashboard history whenever the server restarts. Set `ANALYTICS_INGEST_ENABLED=false` after versions that still use the endpoint are no longer supported.
