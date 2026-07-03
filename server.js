const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;
const MAX_MSG_BYTES = Number(process.env.MAX_MSG_BYTES || 16 * 1024);
const RATE_LIMIT_WINDOW_MS = 10000;
const RATE_LIMITS = {
    lobby: Number(process.env.RATE_LIMIT_LOBBY_MAX || 120),
    signaling: Number(process.env.RATE_LIMIT_SIGNALING_MAX || 240),
    gameState: Number(process.env.RATE_LIMIT_GAME_STATE_MAX || 420),
    gameEvent: Number(process.env.RATE_LIMIT_GAME_EVENT_MAX || 240),
};
const NETWORK_MODES = new Set(['auto', 'relay', 'p2p']);
const wss = new WebSocket.Server({ port: PORT });

// rooms[roomCode] = { host, guest, networkMode, hostCharacterId, hostPassiveId, arenaId, matchId }
const rooms = {};

const LOBBY_TYPES = new Set([
    'create_room', 'join_room', 'get_room_list', 'ping_check',
    'offer', 'answer', 'ice_candidate',
]);

const GAME_TYPES = new Set([
    'game_start', 'game_ready', 'game_state', 'game_skill', 'game_damage',
    'game_state_hp', 'game_over', 'game_start_failed', 'rematch_accept', 'rematch_decline',
    'rematch_request', 'rematch_cancel', 'rematch_reselect', 'rematch_ready',
]);

const ALL_TYPES = new Set([...LOBBY_TYPES, ...GAME_TYPES]);

function generateCode() {
    return Math.floor(1000 + Math.random() * 9000).toString();
}

function send(ws, data) {
    if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(data));
    }
}

function makeMatchId() {
    const now = Date.now().toString(36).toUpperCase();
    const random = Math.random().toString(36).slice(2, 10).toUpperCase();
    return `PVP-${now}-${random}`;
}

function enumToken(value) {
    return typeof value === 'string' && /^[A-Z0-9_]+$/.test(value) ? value : undefined;
}

function networkMode(value) {
    const normalized = typeof value === 'string' ? value.toLowerCase() : 'relay';
    return NETWORK_MODES.has(normalized) ? normalized : 'relay';
}

function validateJoinCode(code) {
    return typeof code === 'string' && /^\d{4}$/.test(code);
}

function socketRttMs(ws) {
    return Number.isFinite(ws?.lastRttMs) ? ws.lastRttMs : null;
}

function roomQuality(hostRttMs, guestRttMs) {
    if (!Number.isFinite(hostRttMs) || !Number.isFinite(guestRttMs)) {
        return { pingMs: -1, quality: 'unknown' };
    }
    const pingMs = Math.round(hostRttMs + guestRttMs);
    return {
        pingMs,
        quality: pingMs > 150 ? 'poor' : 'good',
    };
}

function rateLimitBucket(type) {
    if (type === 'game_state') return 'gameState';
    if (type === 'offer' || type === 'answer' || type === 'ice_candidate') return 'signaling';
    if (GAME_TYPES.has(type)) return 'gameEvent';
    return 'lobby';
}

function rateLimitOk(ws, type) {
    const now = Date.now();
    const bucket = rateLimitBucket(type);
    if (!ws._rateBuckets) ws._rateBuckets = {};
    const state = ws._rateBuckets[bucket] || { windowStart: now, count: 0 };
    if (now - state.windowStart > RATE_LIMIT_WINDOW_MS) {
        state.windowStart = now;
        state.count = 0;
    }
    state.count += 1;
    ws._rateBuckets[bucket] = state;
    return state.count <= RATE_LIMITS[bucket];
}

wss.on('connection', (ws) => {
    ws.roomCode = null;
    ws.role = null; // 'host' | 'guest'

    ws.on('message', (raw) => {
        if (typeof raw !== 'string' && !Buffer.isBuffer(raw)) return;
        const text = raw.toString();
        if (text.length > MAX_MSG_BYTES) return;

        let msg;
        try {
            msg = JSON.parse(text);
        } catch {
            return;
        }

        if (!msg || typeof msg.type !== 'string' || !ALL_TYPES.has(msg.type)) return;
        if (!rateLimitOk(ws, msg.type)) return;

        switch (msg.type) {
            case 'ping_check': {
                const reportedRttMs = msg.rttMs;
                if (typeof reportedRttMs === 'number' &&
                    Number.isFinite(reportedRttMs) &&
                    reportedRttMs >= 0 &&
                    reportedRttMs < 60000) {
                    ws.lastRttMs = reportedRttMs;
                    if (ws.role === 'host' && ws.roomCode && rooms[ws.roomCode]) {
                        rooms[ws.roomCode].hostRttMs = reportedRttMs;
                    }
                }
                send(ws, { type: 'ping_check_ack', clientTime: msg.clientTime, serverTime: Date.now() });
                break;
            }

            // ── 방 만들기 ────────────────────────────────────────────────
            case 'create_room': {
                if (ws.roomCode) {
                    send(ws, { type: 'error', message: 'Already in a room' });
                    return;
                }
                // 기존 방 코드와 겹치지 않도록 재생성
                let code;
                do { code = generateCode(); } while (rooms[code]);

                rooms[code] = {
                    host: ws,
                    guest: null,
                    createdAt: Date.now(),
                    hostRttMs: socketRttMs(ws),
                    hostCharacterId: enumToken(msg.hostCharacterId),
                    hostPassiveId: enumToken(msg.hostPassiveId),
                    arenaId: enumToken(msg.arenaId),
                    networkMode: networkMode(msg.networkMode),
                    matchId: null,
                };
                ws.roomCode = code;
                ws.role = 'host';

                send(ws, { type: 'room_created', code, networkMode: rooms[code].networkMode });
                console.log(`[+] Room created: ${code}`);
                break;
            }

            // ── 방 참가 ──────────────────────────────────────────────────
            case 'join_room': {
                const code = msg.code;
                if (!validateJoinCode(code)) {
                    send(ws, { type: 'error', message: 'Invalid room code' });
                    return;
                }
                const room = rooms[code];

                if (!room) {
                    send(ws, { type: 'error', message: 'Room not found' });
                    return;
                }
                if (room.guest && room.guest.readyState === WebSocket.OPEN) {
                    send(ws, { type: 'error', message: 'Room is full' });
                    return;
                }

                room.guest = ws;
                if (!room.matchId) room.matchId = makeMatchId();
                ws.roomCode = code;
                ws.role = 'guest';

                // 양쪽에게 준비 알림
                send(ws, { type: 'room_joined', code, matchId: room.matchId, networkMode: room.networkMode || 'relay' });
                send(room.host, { type: 'guest_joined', matchId: room.matchId, networkMode: room.networkMode || 'relay' });
                console.log(`[+] Room joined: ${code}`);
                break;
            }

            // ── WebRTC 시그널링 릴레이 ───────────────────────────────────
            // offer / answer / ice_candidate 모두 상대방에게 그대로 중계
            case 'offer':
            case 'answer':
            case 'ice_candidate': {
                const code = ws.roomCode;
                const room = rooms[code];
                if (!room) return;

                const peer = ws.role === 'host' ? room.guest : room.host;
                send(peer, msg); // 메시지 타입 유지하여 그대로 전달
                break;
            }

            // ── 방 목록 조회 ─────────────────────────────────────────────
            case 'get_room_list': {
                const guestRttMs = socketRttMs(ws);
                const list = Object.keys(rooms)
                    .map(code => {
                        const room = rooms[code];
                        if (room.host?.readyState !== WebSocket.OPEN ||
                            (room.guest && room.guest.readyState === WebSocket.OPEN)) {
                            return null;
                        }
                        return {
                            code,
                            ...roomQuality(room.hostRttMs, guestRttMs),
                            hostCharacterId: room.hostCharacterId,
                            arenaId: room.arenaId,
                            networkMode: room.networkMode || 'relay',
                        };
                    })
                    .filter(Boolean);
                send(ws, { type: 'room_list', rooms: list });
                break;
            }

            // ── 게임 패킷 릴레이 ─────────────────────────────────────────
            case 'game_over':
            case 'game_start_failed':
            case 'rematch_accept':
            case 'rematch_decline':
            case 'rematch_request':
            case 'rematch_cancel':
            case 'rematch_reselect':
            case 'rematch_ready':
            case 'game_start':
            case 'game_ready':
            case 'game_state':
            case 'game_skill':
            case 'game_damage':
            case 'game_state_hp': {
                const code = ws.roomCode;
                const room = rooms[code];
                if (!room) return;

                const peer = ws.role === 'host' ? room.guest : room.host;
                send(peer, msg);
                break;
            }

            default:
                break;
        }
    });

    ws.on('close', () => {
        const code = ws.roomCode;
        if (!code || !rooms[code]) return;

        const room = rooms[code];
        const peer = ws.role === 'host' ? room.guest : room.host;

        // 상대방에게 연결 끊김 알림
        send(peer, { type: 'peer_disconnected' });

        // 방 삭제
        delete rooms[code];
        console.log(`[-] Room removed: ${code}`);
    });
});

console.log(`Signaling server running on port ${PORT}`);
