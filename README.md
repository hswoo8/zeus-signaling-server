# zeus-signaling-server

WebSocket signaling and relay server for BeerRock multiplayer.

## Runtime gates

Set these environment variables in production to block outdated multiplayer clients at the server:

| env | Default | Description |
| --- | --- | --- |
| `MULTIPLAYER_MIN_APP_VERSION_CODE` | `1` | Minimum Android `clientVersionCode` allowed to use lobby/match start messages |
| `MULTIPLAYER_MIN_PROTOCOL_VERSION` | `1` | Minimum network protocol version |
| `MULTIPLAYER_MIN_BALANCE_VERSION` | `1` | Minimum gameplay balance/data version |

If a client is missing or below these values, the server returns `{ type: "error", code: "update_required", message }`.

## Lobby messages

| type | Direction | Description |
| --- | --- | --- |
| `create_room` | Client -> Server | Create room with compatibility metadata and `{ hostCharacterId, hostPassiveId, arenaId, networkMode }`; missing/invalid `networkMode` becomes `relay` |
| `room_created` | Server -> Host | `{ code, networkMode }` |
| `join_room` | Client -> Server | Compatibility metadata and `{ code }` |
| `room_joined` | Server -> Guest | `{ code, matchId, networkMode }` |
| `guest_joined` | Server -> Host | `{ matchId, networkMode }` |
| `ping_check` | Client -> Server | `{ clientTime, rttMs? }` |
| `ping_check_ack` | Server -> Client | `{ clientTime, serverTime }` |
| `get_room_list` | Client -> Server | Open rooms |
| `room_list` | Server -> Client | `{ rooms: [{ code, pingMs, quality, hostCharacterId?, arenaId?, networkMode }] }` |

Relay game packets (`game_start`, `game_ready`, `game_state`, `game_skill`, `game_damage`, `game_state_hp`, `game_emote`, `game_over`, `game_start_failed`, `rematch_request`, `rematch_cancel`, `rematch_reselect`, `rematch_ready`, rematch accept/decline, etc.) are forwarded unchanged to the peer. WebRTC `offer` / `answer` / `ice_candidate` messages are forwarded for P2P DataChannel setup.

Rate limits are type-specific so lobby/signaling traffic stays low while `game_state` can sustain 20 Hz relay traffic.

Room quality uses smoothed client-reported RTT samples. Server labels are `good` at `<=120ms`, `casual` at `<=240ms`, and `poor` above that.
