const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;
const wss = new WebSocket.Server({ port: PORT });

// rooms[roomCode] = { host: ws, guest: ws | null }
const rooms = {};

function generateCode() {
    return Math.floor(1000 + Math.random() * 9000).toString();
}

function send(ws, data) {
    if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(data));
    }
}

wss.on('connection', (ws) => {
    ws.roomCode = null;
    ws.role = null; // 'host' | 'guest'

    ws.on('message', (raw) => {
        let msg;
        try {
            msg = JSON.parse(raw);
        } catch {
            return;
        }

        switch (msg.type) {
            // ── 방 만들기 ────────────────────────────────────────────────
            case 'create_room': {
                // 기존 방 코드와 겹치지 않도록 재생성
                let code;
                do { code = generateCode(); } while (rooms[code]);

                rooms[code] = { host: ws, guest: null };
                ws.roomCode = code;
                ws.role = 'host';

                send(ws, { type: 'room_created', code });
                console.log(`[+] Room created: ${code}`);
                break;
            }

            // ── 방 참가 ──────────────────────────────────────────────────
            case 'join_room': {
                const code = msg.code;
                const room = rooms[code];

                if (!room) {
                    send(ws, { type: 'error', message: 'Room not found' });
                    return;
                }
                if (room.guest) {
                    send(ws, { type: 'error', message: 'Room is full' });
                    return;
                }

                room.guest = ws;
                ws.roomCode = code;
                ws.role = 'guest';

                // 양쪽에게 준비 알림
                send(ws,       { type: 'room_joined', code });
                send(room.host, { type: 'guest_joined' });
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
                const list = Object.keys(rooms)
                    .filter(code => !rooms[code].guest)
                    .map(code => ({ code }));
                send(ws, { type: 'room_list', rooms: list });
                break;
            }

            // ── 게임 패킷 릴레이 ─────────────────────────────────────────
            case 'game_over':
            case 'rematch_accept':
            case 'rematch_decline':
            case 'game_start':
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
