const http = require('http');
const WebSocket = require('ws');
const { Pool } = require('pg');
const packageJson = require('./package.json');

const PORT = process.env.PORT || 8080;
const DATABASE_URL = (process.env.DATABASE_URL || '').trim();
const MAX_MSG_BYTES = Number(process.env.MAX_MSG_BYTES || 16 * 1024);
const HTTP_BODY_LIMIT_BYTES = Number(process.env.HTTP_BODY_LIMIT_BYTES || MAX_MSG_BYTES);
const HEARTBEAT_INTERVAL_MS = Number(process.env.HEARTBEAT_INTERVAL_MS || 15000);
const HEARTBEAT_TIMEOUT_MS = Number(process.env.HEARTBEAT_TIMEOUT_MS || 45000);
const ROOM_READY_TIMEOUT_MS = envInt(['ROOM_READY_TIMEOUT_MS', 'READY_TIMEOUT_MS'], 90000);
const MAX_CONNECTIONS = envOptionalInt(['MAX_CONNECTIONS', 'MULTIPLAYER_MAX_CONNECTIONS']);
const MAX_ACTIVE_ROOMS = envOptionalInt(['MAX_ACTIVE_ROOMS', 'MULTIPLAYER_MAX_ACTIVE_ROOMS']);
const MAX_ACTIVE_MATCHES = envOptionalInt(['MAX_ACTIVE_MATCHES', 'MULTIPLAYER_MAX_ACTIVE_MATCHES']);
const CAPACITY_BUSY_RATIO = envFloat(['CAPACITY_BUSY_RATIO', 'MULTIPLAYER_CAPACITY_BUSY_RATIO'], 0.9, 0.1, 1);
const CAPACITY_RETRY_AFTER_SEC = envInt(['CAPACITY_RETRY_AFTER_SEC', 'MULTIPLAYER_CAPACITY_RETRY_AFTER_SEC'], 30);
const MAINTENANCE_MODE = envBool(['MAINTENANCE_MODE', 'MULTIPLAYER_MAINTENANCE'], false);
const MAINTENANCE_MESSAGE = process.env.MAINTENANCE_MESSAGE ||
    process.env.MULTIPLAYER_MAINTENANCE_MESSAGE ||
    '대전 서버 점검 중입니다. 잠시 후 다시 시도해주세요.';
const SERVER_BUSY_MESSAGE = process.env.SERVER_BUSY_MESSAGE ||
    process.env.MULTIPLAYER_SERVER_BUSY_MESSAGE ||
    '현재 대전 서버가 혼잡합니다. 잠시 후 다시 시도해주세요.';
const RATE_LIMIT_WINDOW_MS = 10000;
const RATE_LIMITS = {
    lobby: Number(process.env.RATE_LIMIT_LOBBY_MAX || 120),
    signaling: Number(process.env.RATE_LIMIT_SIGNALING_MAX || 240),
    gameState: Number(process.env.RATE_LIMIT_GAME_STATE_MAX || 420),
    gameEvent: Number(process.env.RATE_LIMIT_GAME_EVENT_MAX || 240),
};
const MIN_CLIENT_VERSION_CODE = envInt(
    ['MULTIPLAYER_MIN_APP_VERSION_CODE', 'MIN_CLIENT_VERSION_CODE'],
    1
);
const MIN_PROTOCOL_VERSION = envInt(
    ['MULTIPLAYER_MIN_PROTOCOL_VERSION', 'MIN_PROTOCOL_VERSION'],
    1
);
const MIN_BALANCE_VERSION = envInt(
    ['MULTIPLAYER_MIN_BALANCE_VERSION', 'MIN_BALANCE_VERSION'],
    1
);
const NETWORK_MODES = new Set(['auto', 'relay', 'p2p']);
const statsPool = DATABASE_URL ? new Pool({
    connectionString: DATABASE_URL,
    ssl: postgresSslConfig(),
}) : null;
const server = http.createServer((req, res) => {
    handleHttpRequest(req, res).catch((err) => {
        console.error('[http] unexpected error:', err?.message || err);
        sendJson(res, 500, {
            error: { code: 'internal_error', message: 'Internal server error' },
        });
    });
});
const wss = new WebSocket.Server({ server });

// rooms[roomCode] = { host, guest, networkMode, hostCharacterId, hostPassiveId, arenaId, matchId, hostNickname, guestNickname, readyDeadlineAt }
const rooms = {};
const statsPlayers = new Map();
const statsIdempotency = new Map();
let serverMatchCounter = 0;

const LOBBY_TYPES = new Set([
    'create_room', 'join_room', 'get_room_list', 'ping_check', 'selection_update',
    'offer', 'answer', 'ice_candidate',
]);

const GAME_TYPES = new Set([
    'game_start', 'game_ready', 'game_state', 'game_skill', 'game_damage',
    'game_state_hp', 'game_emote', 'game_over', 'game_start_failed', 'rematch_accept', 'rematch_decline',
    'rematch_request', 'rematch_cancel', 'rematch_reselect', 'rematch_ready',
    'game_pause', 'game_resume',
]);

const ALL_TYPES = new Set([...LOBBY_TYPES, ...GAME_TYPES]);

const COMPATIBILITY_TYPES = new Set([
    'create_room', 'join_room', 'get_room_list', 'ping_check',
    'selection_update', 'game_start', 'game_ready', 'rematch_ready',
]);

function envInt(names, fallback) {
    for (const name of names) {
        const parsed = Number.parseInt(process.env[name] || '', 10);
        if (Number.isFinite(parsed) && parsed > 0) return parsed;
    }
    return fallback;
}

function envOptionalInt(names) {
    for (const name of names) {
        const parsed = Number.parseInt(process.env[name] || '', 10);
        if (Number.isFinite(parsed) && parsed > 0) return parsed;
    }
    return 0;
}

function envFloat(names, fallback, min, max) {
    for (const name of names) {
        const parsed = Number.parseFloat(process.env[name] || '');
        if (Number.isFinite(parsed)) return Math.min(max, Math.max(min, parsed));
    }
    return fallback;
}

function envBool(names, fallback) {
    for (const name of names) {
        const raw = String(process.env[name] || '').trim().toLowerCase();
        if (!raw) continue;
        if (['1', 'true', 'yes', 'on'].includes(raw)) return true;
        if (['0', 'false', 'no', 'off'].includes(raw)) return false;
    }
    return fallback;
}

function postgresSslConfig() {
    const sslMode = String(process.env.PGSSLMODE || '').toLowerCase();
    const explicit = String(process.env.PGSSL || '').toLowerCase();
    if (sslMode === 'require' || sslMode === 'no-verify' || explicit === 'true') {
        return { rejectUnauthorized: false };
    }
    return undefined;
}

function storageMode() {
    return statsPool ? 'postgres' : 'memory';
}

async function initializeStatsStorage() {
    if (!statsPool) return;
    await statsPool.query(`
        CREATE TABLE IF NOT EXISTS br_player_stats (
            mode TEXT NOT NULL,
            identity_key TEXT NOT NULL,
            player_id TEXT,
            nickname TEXT NOT NULL,
            nickname_key TEXT NOT NULL,
            rating INTEGER NOT NULL DEFAULT 1000,
            wins INTEGER NOT NULL DEFAULT 0,
            losses INTEGER NOT NULL DEFAULT 0,
            draws INTEGER NOT NULL DEFAULT 0,
            current_streak INTEGER NOT NULL DEFAULT 0,
            best_streak INTEGER NOT NULL DEFAULT 0,
            total_duration_sec INTEGER NOT NULL DEFAULT 0,
            character_counts JSONB NOT NULL DEFAULT '{}'::jsonb,
            last_played_at TIMESTAMPTZ,
            coins_earned INTEGER NOT NULL DEFAULT 0,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (mode, identity_key)
        )
    `);
    await statsPool.query(`
        CREATE INDEX IF NOT EXISTS br_player_stats_mode_rating_idx
            ON br_player_stats (mode, rating DESC, wins DESC, nickname_key ASC)
    `);
    await statsPool.query(`
        CREATE TABLE IF NOT EXISTS br_match_results (
            idempotency_key TEXT PRIMARY KEY,
            mode TEXT NOT NULL,
            match_id TEXT NOT NULL,
            record JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    `);
}

function generateCode() {
    return Math.floor(1000 + Math.random() * 9000).toString();
}

function send(ws, data) {
    if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(data));
    }
}

function markSocketAlive(ws) {
    ws.isAlive = true;
    ws.lastSeenAt = Date.now();
}

function sendJson(res, statusCode, data) {
    const body = JSON.stringify(data);
    res.writeHead(statusCode, {
        'Content-Type': 'application/json; charset=utf-8',
        'Content-Length': Buffer.byteLength(body),
    });
    res.end(body);
}

function sendHttpError(res, statusCode, code, message) {
    sendJson(res, statusCode, { error: { code, message } });
}

function openConnectionCount() {
    return Array.from(wss.clients).filter((client) => client.readyState === WebSocket.OPEN).length;
}

function roomCounts() {
    const roomValues = Object.values(rooms);
    const waitingRooms = roomValues.filter((room) =>
        room.host?.readyState === WebSocket.OPEN &&
        !(room.guest && room.guest.readyState === WebSocket.OPEN)
    ).length;
    const activeMatches = roomValues.filter((room) =>
        room.host?.readyState === WebSocket.OPEN &&
        room.guest?.readyState === WebSocket.OPEN &&
        room.matchStarted
    ).length;
    return {
        rooms: roomValues.length,
        waitingRooms,
        activeMatches,
    };
}

function limitValue(limit) {
    return limit > 0 ? limit : null;
}

function exceedsLimit(count, limit, extra = 0) {
    return limit > 0 && (count + extra) > limit;
}

function exceedsBusyRatio(count, limit) {
    return limit > 0 &&
        CAPACITY_BUSY_RATIO < 1 &&
        count > 0 &&
        (count / limit) >= CAPACITY_BUSY_RATIO;
}

function blockedReason(count, limit, extra = 0) {
    if (exceedsLimit(count, limit, extra)) return 'full';
    if (exceedsBusyRatio(count, limit, extra)) return 'busy';
    return null;
}

function capacitySnapshot(options = {}) {
    const connectionExtra = Number.isInteger(options.connectionExtra) ? options.connectionExtra : 1;
    const roomStats = roomCounts();
    const counts = {
        connections: openConnectionCount(),
        rooms: roomStats.rooms,
        waitingRooms: roomStats.waitingRooms,
        activeMatches: roomStats.activeMatches,
    };
    const limits = {
        connections: limitValue(MAX_CONNECTIONS),
        rooms: limitValue(MAX_ACTIVE_ROOMS),
        activeMatches: limitValue(MAX_ACTIVE_MATCHES),
        busyRatio: CAPACITY_BUSY_RATIO,
    };

    const connectReason = blockedReason(counts.connections, MAX_CONNECTIONS, connectionExtra);
    const createReason = blockedReason(counts.rooms, MAX_ACTIVE_ROOMS, 1);
    const joinReason = blockedReason(counts.activeMatches, MAX_ACTIVE_MATCHES, 1);
    const canConnect = !MAINTENANCE_MODE && !connectReason;
    const canCreateRoom = !MAINTENANCE_MODE && !createReason;
    const canJoinRoom = !MAINTENANCE_MODE && !joinReason;
    const canAcceptMatchmaking = canConnect && (canCreateRoom || canJoinRoom);

    let status = 'available';
    let code = 'ok';
    let message = '대전 서버 이용 가능';
    if (MAINTENANCE_MODE) {
        status = 'maintenance';
        code = 'server_maintenance';
        message = MAINTENANCE_MESSAGE;
    } else if (!canAcceptMatchmaking) {
        const reason = connectReason || createReason || joinReason || 'busy';
        status = reason === 'full' ? 'full' : 'busy';
        code = 'server_busy';
        message = SERVER_BUSY_MESSAGE;
    } else if (connectReason || createReason || joinReason) {
        status = 'busy';
        code = 'server_busy';
        message = SERVER_BUSY_MESSAGE;
    }

    return {
        ok: true,
        service: 'beerock-signaling-server',
        version: packageJson.version || '1.0.0',
        status,
        code,
        message,
        canConnect,
        canCreateRoom,
        canJoinRoom,
        canAcceptMatchmaking,
        retryAfterSec: status === 'available' ? 0 : CAPACITY_RETRY_AFTER_SEC,
        counts,
        limits,
        requiredVersionCode: MIN_CLIENT_VERSION_CODE,
        requiredProtocolVersion: MIN_PROTOCOL_VERSION,
        requiredBalanceVersion: MIN_BALANCE_VERSION,
    };
}

function sendCapacityWsError(ws, snapshot) {
    send(ws, {
        type: 'error',
        code: snapshot.code || 'server_busy',
        message: snapshot.message || SERVER_BUSY_MESSAGE,
        status: snapshot.status || 'busy',
        retryAfterSec: snapshot.retryAfterSec || CAPACITY_RETRY_AFTER_SEC,
    });
}

function readJsonBody(req) {
    return new Promise((resolve, reject) => {
        const contentType = String(req.headers['content-type'] || '').toLowerCase();
        if (!contentType.includes('application/json')) {
            reject(Object.assign(new Error('Content-Type must be application/json'), { statusCode: 415, code: 'unsupported_media_type' }));
            return;
        }

        let raw = '';
        req.setEncoding('utf8');
        req.on('data', (chunk) => {
            raw += chunk;
            if (Buffer.byteLength(raw, 'utf8') > HTTP_BODY_LIMIT_BYTES) {
                reject(Object.assign(new Error('Request body too large'), { statusCode: 413, code: 'payload_too_large' }));
                req.destroy();
            }
        });
        req.on('end', () => {
            try {
                resolve(raw.trim() ? JSON.parse(raw) : {});
            } catch {
                reject(Object.assign(new Error('Malformed JSON body'), { statusCode: 400, code: 'invalid_json' }));
            }
        });
        req.on('error', (err) => reject(err));
    });
}

async function handleHttpRequest(req, res) {
    const url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
    const pathname = url.pathname;

    if (req.method === 'GET' && pathname === '/health') {
        const players = statsPool ? await postgresPlayerCount() : statsPlayers.size;
        sendJson(res, 200, {
            ok: true,
            service: 'beerock-signaling-server',
            version: packageJson.version || '1.0.0',
            uptimeSec: Math.floor(process.uptime()),
            storage: storageMode(),
            rooms: Object.keys(rooms).length,
            players,
        });
        return;
    }

    if (req.method === 'GET' && pathname === '/capacity') {
        const compatibilityMessage = compatibilityErrorFromQuery(url.searchParams);
        if (compatibilityMessage) {
            sendJson(res, 200, {
                ...capacitySnapshot(),
                status: 'update_required',
                code: 'update_required',
                message: compatibilityMessage,
                canConnect: false,
                canCreateRoom: false,
                canJoinRoom: false,
                canAcceptMatchmaking: false,
                retryAfterSec: 0,
            });
            return;
        }
        sendJson(res, 200, capacitySnapshot());
        return;
    }

    if (req.method === 'POST' && pathname === '/matches/result') {
        const body = await readJsonRequest(req, res);
        if (!body) return;
        if (statsPool) {
            await handlePostgresSingleMatchResult(res, body);
        } else {
            handleSingleMatchResult(res, body);
        }
        return;
    }

    if (req.method === 'POST' && pathname === '/matches/pvp-result') {
        const body = await readJsonRequest(req, res);
        if (!body) return;
        if (statsPool) {
            await handlePostgresPvpMatchResult(res, body);
        } else {
            handlePvpMatchResult(res, body);
        }
        return;
    }

    if (req.method === 'GET' && pathname === '/rankings') {
        const mode = normalizeMode(url.searchParams.get('mode') || 'single');
        if (!mode) {
            sendHttpError(res, 400, 'invalid_request', 'mode must be single or multi');
            return;
        }
        const limit = parseBoundedInt(url.searchParams.get('limit'), 50, 1, 100);
        const offset = parseBoundedInt(url.searchParams.get('offset'), 0, 0, 1000000);
        sendJson(res, 200, statsPool
            ? await postgresRankingsResponse(mode, limit, offset)
            : rankingsResponse(mode, limit, offset)
        );
        return;
    }

    const playerStatsMatch = pathname.match(/^\/players\/([^/]+)\/stats$/);
    if (req.method === 'GET' && playerStatsMatch) {
        const mode = normalizeMode(url.searchParams.get('mode') || 'single');
        if (!mode) {
            sendHttpError(res, 400, 'invalid_request', 'mode must be single or multi');
            return;
        }
        const playerRef = decodeURIComponent(playerStatsMatch[1]);
        const player = statsPool
            ? await postgresFindPlayerByRef(mode, playerRef)
            : findPlayerByRef(mode, playerRef);
        if (!player) {
            sendHttpError(res, 404, 'player_not_found', 'Player stats were not found');
            return;
        }
        const rank = statsPool
            ? await postgresRankForPlayer(mode, player.key)
            : rankForPlayer(mode, player.key);
        sendJson(res, 200, playerStatsResponse(player, rank));
        return;
    }

    if (['/matches/result', '/matches/pvp-result'].includes(pathname) ||
        pathname === '/health' ||
        pathname === '/capacity' ||
        pathname === '/rankings' ||
        playerStatsMatch) {
        sendHttpError(res, 405, 'method_not_allowed', 'Method not allowed');
        return;
    }

    sendHttpError(res, 404, 'not_found', 'Route not found');
}

async function readJsonRequest(req, res) {
    try {
        return await readJsonBody(req);
    } catch (err) {
        sendHttpError(
            res,
            err.statusCode || 400,
            err.code || 'invalid_request',
            err.message || 'Invalid request'
        );
        return null;
    }
}

function normalizeMode(value) {
    return value === 'single' || value === 'multi' ? value : null;
}

function parseBoundedInt(value, fallback, min, max) {
    const parsed = Number.parseInt(value || '', 10);
    if (!Number.isFinite(parsed)) return fallback;
    return Math.min(max, Math.max(min, parsed));
}

function makeServerMatchId() {
    serverMatchCounter += 1;
    const now = Date.now().toString(36).toUpperCase();
    const seq = serverMatchCounter.toString(36).toUpperCase().padStart(4, '0');
    const random = Math.random().toString(36).slice(2, 8).toUpperCase();
    return `srv_${now}_${seq}_${random}`;
}

function normalizeNicknameInput(value, fallback = null) {
    if (typeof value !== 'string') return fallback;
    const trimmed = value.trim();
    if (!trimmed || trimmed.length > 24 || /[\u0000-\u001F\u007F]/.test(trimmed)) return fallback;
    return trimmed;
}

function nicknameKey(value) {
    return normalizeNicknameInput(value, '')?.toLocaleLowerCase('en-US') || '';
}

function normalizePlayerId(value) {
    if (typeof value !== 'string') return null;
    const trimmed = value.trim();
    if (!trimmed || trimmed.length > 128 || /[\u0000-\u001F\u007F]/.test(trimmed)) return null;
    return trimmed;
}

function playerIdentityKey(player) {
    const playerId = normalizePlayerId(player?.playerId);
    if (playerId) return `id:${playerId}`;
    const nick = nicknameKey(player?.nickname);
    return nick ? `name:${nick}` : null;
}

function statsKey(mode, identityKey) {
    return `${mode}:${identityKey}`;
}

function validEnumToken(value, required = false) {
    if (value === undefined || value === null || value === '') return !required;
    return typeof value === 'string' && /^[A-Z0-9_]+$/.test(value);
}

function validStringId(value) {
    if (value === undefined || value === null || value === '') return true;
    return typeof value === 'string' && value.length <= 128 && !/[\u0000-\u001F\u007F]/.test(value);
}

function validIsoTime(value) {
    if (value === undefined || value === null || value === '') return true;
    if (typeof value !== 'string' || Number.isNaN(Date.parse(value))) return false;
    return value.endsWith('Z');
}

function intField(value, min, max) {
    return Number.isInteger(value) && value >= min && value <= max;
}

function outcomeValue(value) {
    return value === 'win' || value === 'loss' || value === 'draw' ? value : null;
}

function reverseOutcome(outcome) {
    if (outcome === 'win') return 'loss';
    if (outcome === 'loss') return 'win';
    return 'draw';
}

function rewardForResult(outcome, finishReason) {
    if (outcome === 'loss') return 0;
    if (outcome === 'draw') return 25;
    if (finishReason === 'remote_disconnect' || finishReason === 'remote_forfeit') return 20;
    return 100;
}

function ratingDelta(outcome, mode) {
    if (outcome === 'win') return mode === 'multi' ? 16 : 16;
    if (outcome === 'loss') return mode === 'multi' ? -16 : -12;
    return mode === 'multi' ? 1 : 2;
}

function getOrCreatePlayer(mode, playerInput) {
    const nickname = normalizeNicknameInput(playerInput.nickname, 'Player');
    const identityKey = playerIdentityKey({ ...playerInput, nickname });
    if (!identityKey) return null;
    const key = statsKey(mode, identityKey);
    let player = statsPlayers.get(key);
    if (!player) {
        player = {
            key,
            mode,
            playerId: normalizePlayerId(playerInput.playerId),
            nickname,
            nicknameKey: nicknameKey(nickname),
            rating: 1000,
            wins: 0,
            losses: 0,
            draws: 0,
            currentStreak: 0,
            bestStreak: 0,
            totalDurationSec: 0,
            characterCounts: new Map(),
            lastPlayedAt: null,
            coinsEarned: 0,
        };
        statsPlayers.set(key, player);
    } else {
        player.nickname = nickname;
        player.nicknameKey = nicknameKey(nickname);
        player.playerId = normalizePlayerId(playerInput.playerId) || player.playerId;
    }
    return player;
}

function applyPlayerResult(player, outcome, metadata) {
    player.rating = Math.max(0, player.rating + ratingDelta(outcome, player.mode));
    if (outcome === 'win') {
        player.wins += 1;
        player.currentStreak = Math.max(1, player.currentStreak + 1);
        player.bestStreak = Math.max(player.bestStreak, player.currentStreak);
    } else if (outcome === 'loss') {
        player.losses += 1;
        player.currentStreak = Math.min(-1, player.currentStreak - 1);
    } else {
        player.draws += 1;
        player.currentStreak = 0;
    }
    player.totalDurationSec += metadata.durationSec;
    if (metadata.characterId) {
        player.characterCounts.set(
            metadata.characterId,
            (player.characterCounts.get(metadata.characterId) || 0) + 1
        );
    }
    player.lastPlayedAt = metadata.completedAt;
    player.coinsEarned += metadata.rewardCoins || 0;
}

function favoriteCharacterId(player) {
    let bestId = null;
    let bestCount = -1;
    for (const [id, count] of player.characterCounts.entries()) {
        if (count > bestCount || (count === bestCount && (bestId === null || id < bestId))) {
            bestId = id;
            bestCount = count;
        }
    }
    return bestId;
}

function playerMatchCount(player) {
    return player.wins + player.losses + player.draws;
}

function winRate(player) {
    const matches = playerMatchCount(player);
    return matches > 0 ? Math.round((player.wins / matches) * 1000) / 10 : 0;
}

function averageDurationSec(player) {
    const matches = playerMatchCount(player);
    return matches > 0 ? Math.round((player.totalDurationSec / matches) * 10) / 10 : 0;
}

function sortRankedPlayers(players) {
    return players.sort((a, b) => {
        if (b.rating !== a.rating) return b.rating - a.rating;
        if (b.wins !== a.wins) return b.wins - a.wins;
        if (winRate(b) !== winRate(a)) return winRate(b) - winRate(a);
        return a.nicknameKey.localeCompare(b.nicknameKey);
    });
}

function rankedPlayers(mode) {
    return sortRankedPlayers(Array.from(statsPlayers.values()).filter((player) => player.mode === mode));
}

function rankForPlayer(mode, key) {
    const rows = rankedPlayers(mode);
    const index = rows.findIndex((player) => player.key === key);
    return index >= 0 ? index + 1 : null;
}

function playerRankRow(player, rank) {
    const matches = playerMatchCount(player);
    return {
        rank,
        nickname: player.nickname,
        playerId: player.playerId || null,
        rating: player.rating,
        wins: player.wins,
        losses: player.losses,
        draws: player.draws,
        matches,
        winRate: winRate(player),
        bestStreak: player.bestStreak,
        lastPlayedAt: player.lastPlayedAt,
    };
}

function playerStatsResponse(player, rank) {
    return {
        nickname: player.nickname,
        playerId: player.playerId || null,
        rating: player.rating,
        rank,
        wins: player.wins,
        losses: player.losses,
        draws: player.draws,
        matches: playerMatchCount(player),
        winRate: winRate(player),
        bestStreak: player.bestStreak,
        currentStreak: player.currentStreak,
        averageDurationSec: averageDurationSec(player),
        favoriteCharacterId: favoriteCharacterId(player),
        coinsEarned: player.coinsEarned,
        lastPlayedAt: player.lastPlayedAt,
    };
}

function pvpPlayerSummary(player, rewardCoins) {
    return {
        nickname: player.nickname,
        playerId: player.playerId || null,
        rating: player.rating,
        wins: player.wins,
        losses: player.losses,
        draws: player.draws,
        rewardCoins,
    };
}

function pvpResponseFromRecord(record, localIdentity, remoteIdentity, duplicate = false) {
    const local = record.players[localIdentity] || record.players.local;
    const remote = record.players[remoteIdentity] || record.players.remote;
    return {
        matchId: record.matchId,
        accepted: true,
        mode: 'multi',
        finishReason: record.finishReason,
        players: {
            local,
            remote,
        },
        ...(duplicate ? { duplicate: true } : {}),
    };
}

function rankingsResponse(mode, limit, offset) {
    const rows = rankedPlayers(mode).map((player, index) => playerRankRow(player, index + 1));
    return {
        mode,
        limit,
        offset,
        players: rows.slice(offset, offset + limit),
    };
}

function findPlayerByRef(mode, ref) {
    const safeRef = normalizePlayerId(ref);
    if (!safeRef) return null;
    const byId = statsPlayers.get(statsKey(mode, `id:${safeRef}`));
    if (byId) return byId;
    const byName = statsPlayers.get(statsKey(mode, `name:${nicknameKey(safeRef)}`));
    if (byName) return byName;
    return Array.from(statsPlayers.values()).find(
        (player) => player.mode === mode && player.nicknameKey === nicknameKey(safeRef)
    ) || null;
}

function characterCountsObject(player) {
    return Object.fromEntries(player.characterCounts.entries());
}

function dbPlayerFromRow(row) {
    const counts = row.character_counts || {};
    return {
        key: statsKey(row.mode, row.identity_key),
        mode: row.mode,
        playerId: row.player_id,
        nickname: row.nickname,
        nicknameKey: row.nickname_key,
        rating: Number(row.rating) || 0,
        wins: Number(row.wins) || 0,
        losses: Number(row.losses) || 0,
        draws: Number(row.draws) || 0,
        currentStreak: Number(row.current_streak) || 0,
        bestStreak: Number(row.best_streak) || 0,
        totalDurationSec: Number(row.total_duration_sec) || 0,
        characterCounts: new Map(Object.entries(counts)),
        lastPlayedAt: row.last_played_at ? new Date(row.last_played_at).toISOString() : null,
        coinsEarned: Number(row.coins_earned) || 0,
    };
}

async function postgresPlayerCount() {
    const result = await statsPool.query('SELECT COUNT(*)::int AS count FROM br_player_stats');
    return result.rows[0]?.count || 0;
}

async function postgresPlayersByMode(mode) {
    const result = await statsPool.query(
        'SELECT * FROM br_player_stats WHERE mode = $1',
        [mode]
    );
    return sortRankedPlayers(result.rows.map(dbPlayerFromRow));
}

async function postgresRankingsResponse(mode, limit, offset) {
    const rows = (await postgresPlayersByMode(mode)).map((player, index) => playerRankRow(player, index + 1));
    return {
        mode,
        limit,
        offset,
        players: rows.slice(offset, offset + limit),
    };
}

async function postgresFindPlayerByRef(mode, ref) {
    const safeRef = normalizePlayerId(ref);
    if (!safeRef) return null;
    const idKey = `id:${safeRef}`;
    const nameKey = `name:${nicknameKey(safeRef)}`;
    const result = await statsPool.query(
        `SELECT *
           FROM br_player_stats
          WHERE mode = $1
            AND (identity_key = $2 OR identity_key = $3 OR nickname_key = $4)
          ORDER BY CASE
              WHEN identity_key = $2 THEN 0
              WHEN identity_key = $3 THEN 1
              ELSE 2
          END
          LIMIT 1`,
        [mode, idKey, nameKey, nicknameKey(safeRef)]
    );
    return result.rows[0] ? dbPlayerFromRow(result.rows[0]) : null;
}

async function postgresRankForPlayer(mode, key) {
    const rows = await postgresPlayersByMode(mode);
    const index = rows.findIndex((player) => player.key === key);
    return index >= 0 ? index + 1 : null;
}

async function upsertPostgresPlayerResult(client, mode, playerInput, outcome, metadata) {
    const nickname = normalizeNicknameInput(playerInput.nickname, 'Player');
    const identityKey = playerIdentityKey({ ...playerInput, nickname });
    if (!identityKey) throw new Error('player identity is invalid');
    const playerId = normalizePlayerId(playerInput.playerId);
    await client.query(
        `INSERT INTO br_player_stats (mode, identity_key, player_id, nickname, nickname_key)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (mode, identity_key) DO UPDATE
            SET player_id = COALESCE(EXCLUDED.player_id, br_player_stats.player_id),
                nickname = EXCLUDED.nickname,
                nickname_key = EXCLUDED.nickname_key,
                updated_at = NOW()`,
        [mode, identityKey, playerId, nickname, nicknameKey(nickname)]
    );
    const locked = await client.query(
        'SELECT * FROM br_player_stats WHERE mode = $1 AND identity_key = $2 FOR UPDATE',
        [mode, identityKey]
    );
    const player = dbPlayerFromRow(locked.rows[0]);
    applyPlayerResult(player, outcome, metadata);
    await client.query(
        `UPDATE br_player_stats
            SET player_id = $3,
                nickname = $4,
                nickname_key = $5,
                rating = $6,
                wins = $7,
                losses = $8,
                draws = $9,
                current_streak = $10,
                best_streak = $11,
                total_duration_sec = $12,
                character_counts = $13::jsonb,
                last_played_at = $14,
                coins_earned = $15,
                updated_at = NOW()
          WHERE mode = $1 AND identity_key = $2`,
        [
            mode,
            identityKey,
            player.playerId,
            player.nickname,
            player.nicknameKey,
            player.rating,
            player.wins,
            player.losses,
            player.draws,
            player.currentStreak,
            player.bestStreak,
            player.totalDurationSec,
            JSON.stringify(characterCountsObject(player)),
            player.lastPlayedAt,
            player.coinsEarned,
        ]
    );
    return player;
}

function validateCommonResult(body, mode) {
    if (!normalizeMode(mode)) return 'mode must be single or multi';
    if (!outcomeValue(body.outcome)) return 'outcome must be win, loss, or draw';
    if (!intField(body.durationSec, 1, 3600)) return 'durationSec must be between 1 and 3600';
    if (!validStringId(body.clientMatchId)) return 'clientMatchId is invalid';
    if (!validStringId(body.serverMatchId)) return 'serverMatchId is invalid';
    if (!validIsoTime(body.completedAt)) return 'completedAt must be an ISO-8601 UTC timestamp';
    if (!validEnumToken(body.arenaId, false)) return 'arenaId is invalid';
    return null;
}

function validatePlayerPayload(player, prefix) {
    if (!player || typeof player !== 'object') return `${prefix} is required`;
    if (!normalizeNicknameInput(player.nickname)) return `${prefix}.nickname is invalid`;
    if (!validStringId(player.playerId)) return `${prefix}.playerId is invalid`;
    if (!validEnumToken(player.characterId, true)) return `${prefix}.characterId is invalid`;
    if (!validEnumToken(player.passiveId, false)) return `${prefix}.passiveId is invalid`;
    if (!intField(player.hp, 0, 9999)) return `${prefix}.hp must be between 0 and 9999`;
    return null;
}

function handleSingleMatchResult(res, body) {
    if (body.mode !== 'single') {
        sendHttpError(res, 400, 'invalid_request', 'mode must be single on /matches/result');
        return;
    }
    const commonError = validateCommonResult(body, 'single');
    const playerError = validatePlayerPayload({
        nickname: body.nickname,
        playerId: body.playerId,
        characterId: body.characterId,
        passiveId: body.passiveId,
        hp: body.localHp,
    }, 'player');
    if (commonError || playerError || !intField(body.remoteHp, 0, 9999)) {
        sendHttpError(res, 400, 'invalid_request', commonError || playerError || 'remoteHp must be between 0 and 9999');
        return;
    }

    const player = getOrCreatePlayer('single', {
        nickname: body.nickname,
        playerId: body.playerId,
    });
    const clientMatchId = normalizePlayerId(body.clientMatchId);
    const duplicateKey = clientMatchId ? `single:${player.key}:${clientMatchId}` : null;
    if (duplicateKey && statsIdempotency.has(duplicateKey)) {
        sendJson(res, 200, statsIdempotency.get(duplicateKey));
        return;
    }

    const outcome = outcomeValue(body.outcome);
    const completedAt = body.completedAt || new Date().toISOString();
    const rewardCoins = rewardForResult(outcome, body.finishReason);
    applyPlayerResult(player, outcome, {
        durationSec: body.durationSec,
        characterId: body.characterId,
        completedAt,
        rewardCoins,
    });
    const response = {
        matchId: makeServerMatchId(),
        accepted: true,
        mode: 'single',
        rewardCoins,
        player: {
            nickname: player.nickname,
            playerId: player.playerId || null,
            rating: player.rating,
            wins: player.wins,
            losses: player.losses,
            draws: player.draws,
            rewardCoins,
        },
    };
    if (duplicateKey) statsIdempotency.set(duplicateKey, { ...response, duplicate: true });
    sendJson(res, 201, response);
}

function handlePvpMatchResult(res, body) {
    const mode = body.mode === undefined ? 'multi' : body.mode;
    if (mode !== 'multi') {
        sendHttpError(res, 400, 'invalid_request', 'mode must be multi on /matches/pvp-result');
        return;
    }
    const commonError = validateCommonResult(body, 'multi');
    const localError = validatePlayerPayload(body.localPlayer, 'localPlayer');
    const remoteError = validatePlayerPayload(body.remotePlayer, 'remotePlayer');
    if (commonError || localError || remoteError) {
        sendHttpError(res, 400, 'invalid_request', commonError || localError || remoteError);
        return;
    }

    const localIdentity = playerIdentityKey(body.localPlayer);
    const remoteIdentity = playerIdentityKey(body.remotePlayer);
    if (!localIdentity || !remoteIdentity || localIdentity === remoteIdentity) {
        sendHttpError(res, 400, 'invalid_request', 'localPlayer and remotePlayer must be different');
        return;
    }

    const matchRef = normalizePlayerId(body.serverMatchId) || normalizePlayerId(body.clientMatchId);
    const duplicateKey = matchRef ? `multi:${matchRef}` : null;
    if (duplicateKey && statsIdempotency.has(duplicateKey)) {
        const record = statsIdempotency.get(duplicateKey);
        if (record?.type === 'pvp') {
            sendJson(res, 200, pvpResponseFromRecord(record, localIdentity, remoteIdentity, true));
        } else {
            sendJson(res, 200, { ...record, duplicate: true });
        }
        return;
    }

    const outcome = outcomeValue(body.outcome);
    const remoteOutcome = reverseOutcome(outcome);
    const completedAt = body.completedAt || new Date().toISOString();
    const finishReason = typeof body.finishReason === 'string' ? body.finishReason : 'normal';
    const localReward = rewardForResult(outcome, finishReason);
    const remoteReward = rewardForResult(remoteOutcome, finishReason);
    const local = getOrCreatePlayer('multi', body.localPlayer);
    const remote = getOrCreatePlayer('multi', body.remotePlayer);

    applyPlayerResult(local, outcome, {
        durationSec: body.durationSec,
        characterId: body.localPlayer.characterId,
        completedAt,
        rewardCoins: localReward,
    });
    applyPlayerResult(remote, remoteOutcome, {
        durationSec: body.durationSec,
        characterId: body.remotePlayer.characterId,
        completedAt,
        rewardCoins: remoteReward,
    });

    const matchId = matchRef || makeServerMatchId();
    const record = {
        type: 'pvp',
        matchId,
        finishReason,
        players: {
            [localIdentity]: pvpPlayerSummary(local, localReward),
            [remoteIdentity]: pvpPlayerSummary(remote, remoteReward),
            local: pvpPlayerSummary(local, localReward),
            remote: pvpPlayerSummary(remote, remoteReward),
        },
    };
    const response = {
        matchId,
        accepted: true,
        mode: 'multi',
        finishReason,
        players: {
            local: record.players[localIdentity],
            remote: record.players[remoteIdentity],
        },
    };
    if (duplicateKey) statsIdempotency.set(duplicateKey, record);
    sendJson(res, 201, response);
}

async function handlePostgresSingleMatchResult(res, body) {
    if (body.mode !== 'single') {
        sendHttpError(res, 400, 'invalid_request', 'mode must be single on /matches/result');
        return;
    }
    const commonError = validateCommonResult(body, 'single');
    const playerPayload = {
        nickname: body.nickname,
        playerId: body.playerId,
        characterId: body.characterId,
        passiveId: body.passiveId,
        hp: body.localHp,
    };
    const playerError = validatePlayerPayload(playerPayload, 'player');
    if (commonError || playerError || !intField(body.remoteHp, 0, 9999)) {
        sendHttpError(res, 400, 'invalid_request', commonError || playerError || 'remoteHp must be between 0 and 9999');
        return;
    }

    const nickname = normalizeNicknameInput(body.nickname, 'Player');
    const identityKey = playerIdentityKey({ nickname, playerId: body.playerId });
    const clientMatchId = normalizePlayerId(body.clientMatchId);
    const duplicateKey = clientMatchId ? `single:${statsKey('single', identityKey)}:${clientMatchId}` : null;
    const matchId = makeServerMatchId();
    const outcome = outcomeValue(body.outcome);
    const completedAt = body.completedAt || new Date().toISOString();
    const rewardCoins = rewardForResult(outcome, body.finishReason);
    const client = await statsPool.connect();

    try {
        await client.query('BEGIN');
        if (duplicateKey) {
            const inserted = await client.query(
                `INSERT INTO br_match_results (idempotency_key, mode, match_id, record)
                 VALUES ($1, 'single', $2, $3::jsonb)
                 ON CONFLICT DO NOTHING
                 RETURNING idempotency_key`,
                [duplicateKey, matchId, JSON.stringify({ type: 'pending' })]
            );
            if (inserted.rowCount === 0) {
                const existing = await client.query(
                    'SELECT record FROM br_match_results WHERE idempotency_key = $1',
                    [duplicateKey]
                );
                await client.query('COMMIT');
                const record = existing.rows[0]?.record;
                sendJson(res, 200, { ...(record?.response || record || {}), duplicate: true });
                return;
            }
        }

        const player = await upsertPostgresPlayerResult(client, 'single', {
            nickname: body.nickname,
            playerId: body.playerId,
        }, outcome, {
            durationSec: body.durationSec,
            characterId: body.characterId,
            completedAt,
            rewardCoins,
        });

        const response = {
            matchId,
            accepted: true,
            mode: 'single',
            rewardCoins,
            player: {
                nickname: player.nickname,
                playerId: player.playerId || null,
                rating: player.rating,
                wins: player.wins,
                losses: player.losses,
                draws: player.draws,
                rewardCoins,
            },
        };
        if (duplicateKey) {
            await client.query(
                'UPDATE br_match_results SET record = $2::jsonb WHERE idempotency_key = $1',
                [duplicateKey, JSON.stringify({ type: 'single', response })]
            );
        }
        await client.query('COMMIT');
        sendJson(res, 201, response);
    } catch (err) {
        await client.query('ROLLBACK').catch(() => {});
        throw err;
    } finally {
        client.release();
    }
}

async function handlePostgresPvpMatchResult(res, body) {
    const mode = body.mode === undefined ? 'multi' : body.mode;
    if (mode !== 'multi') {
        sendHttpError(res, 400, 'invalid_request', 'mode must be multi on /matches/pvp-result');
        return;
    }
    const commonError = validateCommonResult(body, 'multi');
    const localError = validatePlayerPayload(body.localPlayer, 'localPlayer');
    const remoteError = validatePlayerPayload(body.remotePlayer, 'remotePlayer');
    if (commonError || localError || remoteError) {
        sendHttpError(res, 400, 'invalid_request', commonError || localError || remoteError);
        return;
    }

    const localIdentity = playerIdentityKey(body.localPlayer);
    const remoteIdentity = playerIdentityKey(body.remotePlayer);
    if (!localIdentity || !remoteIdentity || localIdentity === remoteIdentity) {
        sendHttpError(res, 400, 'invalid_request', 'localPlayer and remotePlayer must be different');
        return;
    }

    const matchRef = normalizePlayerId(body.serverMatchId) || normalizePlayerId(body.clientMatchId);
    const duplicateKey = matchRef ? `multi:${matchRef}` : null;
    const outcome = outcomeValue(body.outcome);
    const remoteOutcome = reverseOutcome(outcome);
    const completedAt = body.completedAt || new Date().toISOString();
    const finishReason = typeof body.finishReason === 'string' ? body.finishReason : 'normal';
    const localReward = rewardForResult(outcome, finishReason);
    const remoteReward = rewardForResult(remoteOutcome, finishReason);
    const matchId = matchRef || makeServerMatchId();
    const client = await statsPool.connect();

    try {
        await client.query('BEGIN');
        if (duplicateKey) {
            const inserted = await client.query(
                `INSERT INTO br_match_results (idempotency_key, mode, match_id, record)
                 VALUES ($1, 'multi', $2, $3::jsonb)
                 ON CONFLICT DO NOTHING
                 RETURNING idempotency_key`,
                [duplicateKey, matchId, JSON.stringify({ type: 'pending' })]
            );
            if (inserted.rowCount === 0) {
                const existing = await client.query(
                    'SELECT record FROM br_match_results WHERE idempotency_key = $1',
                    [duplicateKey]
                );
                await client.query('COMMIT');
                const record = existing.rows[0]?.record;
                if (record?.type === 'pvp') {
                    sendJson(res, 200, pvpResponseFromRecord(record, localIdentity, remoteIdentity, true));
                } else {
                    sendJson(res, 200, { ...(record || {}), duplicate: true });
                }
                return;
            }
        }

        const local = await upsertPostgresPlayerResult(client, 'multi', body.localPlayer, outcome, {
            durationSec: body.durationSec,
            characterId: body.localPlayer.characterId,
            completedAt,
            rewardCoins: localReward,
        });
        const remote = await upsertPostgresPlayerResult(client, 'multi', body.remotePlayer, remoteOutcome, {
            durationSec: body.durationSec,
            characterId: body.remotePlayer.characterId,
            completedAt,
            rewardCoins: remoteReward,
        });
        const record = {
            type: 'pvp',
            matchId,
            finishReason,
            players: {
                [localIdentity]: pvpPlayerSummary(local, localReward),
                [remoteIdentity]: pvpPlayerSummary(remote, remoteReward),
                local: pvpPlayerSummary(local, localReward),
                remote: pvpPlayerSummary(remote, remoteReward),
            },
        };
        const response = {
            matchId,
            accepted: true,
            mode: 'multi',
            finishReason,
            players: {
                local: record.players[localIdentity],
                remote: record.players[remoteIdentity],
            },
        };
        if (duplicateKey) {
            await client.query(
                'UPDATE br_match_results SET record = $2::jsonb WHERE idempotency_key = $1',
                [duplicateKey, JSON.stringify(record)]
            );
        }
        await client.query('COMMIT');
        sendJson(res, 201, response);
    } catch (err) {
        await client.query('ROLLBACK').catch(() => {});
        throw err;
    } finally {
        client.release();
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
        quality: pingMs <= 120 ? 'good' : (pingMs <= 240 ? 'casual' : 'poor'),
    };
}

function smoothedRttMs(previous, sample) {
    if (!Number.isFinite(sample)) return previous;
    if (!Number.isFinite(previous)) return sample;
    return Math.round(previous * 0.65 + sample * 0.35);
}

function clearReadyDeadline(room) {
    if (!room) return;
    if (room.readyTimer) {
        clearTimeout(room.readyTimer);
    }
    room.readyTimer = null;
    room.readyDeadlineAt = null;
}

function resetGuestSlot(room) {
    if (!room) return;
    room.guest = null;
    room.guestCharacterId = undefined;
    room.guestPassiveId = undefined;
    room.guestArenaId = undefined;
    room.guestNickname = undefined;
    room.guestPlayerId = undefined;
    room.guestReady = false;
    room.hostReady = false;
    room.matchId = null;
}

function startReadyDeadline(code, room) {
    if (!room || room.matchStarted) return;
    if (room.host?.readyState !== WebSocket.OPEN || room.guest?.readyState !== WebSocket.OPEN) {
        clearReadyDeadline(room);
        return;
    }
    clearReadyDeadline(room);
    room.hostReady = false;
    room.guestReady = false;
    room.readyDeadlineAt = Date.now() + ROOM_READY_TIMEOUT_MS;
    room.readyTimer = setTimeout(() => {
        handleReadyTimeout(code);
    }, ROOM_READY_TIMEOUT_MS);
    const payload = { type: 'room_ready_deadline', deadlineAtMs: room.readyDeadlineAt };
    send(room.host, payload);
    send(room.guest, payload);
}

function updateRoomReady(room, role, ready) {
    if (!room || typeof ready !== 'boolean') return;
    if (role === 'host') room.hostReady = ready;
    if (role === 'guest') room.guestReady = ready;
    if (room.hostReady && room.guestReady) {
        clearReadyDeadline(room);
    }
}

function handleReadyTimeout(code) {
    const room = rooms[code];
    if (!room || room.matchStarted) return;
    room.readyTimer = null;
    room.readyDeadlineAt = null;
    if (room.host?.readyState !== WebSocket.OPEN || room.guest?.readyState !== WebSocket.OPEN) {
        clearReadyDeadline(room);
        return;
    }

    const hostReady = room.hostReady === true;
    const guestReady = room.guestReady === true;
    if (hostReady && guestReady) return;

    if (!hostReady && !guestReady) {
        send(room.host, { type: 'room_ready_timeout', action: 'room_closed', reason: 'both_not_ready' });
        send(room.guest, { type: 'room_ready_timeout', action: 'room_closed', reason: 'both_not_ready' });
        room.host.roomCode = null;
        room.host.role = null;
        room.guest.roomCode = null;
        room.guest.role = null;
        delete rooms[code];
        console.log(`[timeout] Room closed because both players were not ready: ${code}`);
        return;
    }

    if (hostReady && !guestReady) {
        const kicked = room.guest;
        send(kicked, { type: 'room_ready_timeout', action: 'kicked', reason: 'not_ready' });
        send(room.host, { type: 'room_ready_timeout', action: 'peer_removed', reason: 'not_ready' });
        kicked.roomCode = null;
        kicked.role = null;
        resetGuestSlot(room);
        console.log(`[timeout] Guest kicked for not ready: ${code}`);
        return;
    }

    const kicked = room.host;
    const promotedHost = room.guest;
    send(kicked, { type: 'room_ready_timeout', action: 'kicked', reason: 'not_ready' });
    send(promotedHost, { type: 'room_ready_timeout', action: 'peer_removed', reason: 'not_ready' });
    kicked.roomCode = null;
    kicked.role = null;
    room.host = promotedHost;
    room.guest = null;
    room.hostRttMs = socketRttMs(promotedHost);
    room.hostCharacterId = room.guestCharacterId;
    room.hostPassiveId = room.guestPassiveId;
    room.arenaId = room.guestArenaId || room.arenaId;
    room.hostNickname = room.guestNickname;
    room.hostPlayerId = room.guestPlayerId;
    resetGuestSlot(room);
    promotedHost.role = 'host';
    promotedHost.roomCode = code;
    send(promotedHost, {
        type: 'host_migrated',
        code,
        networkMode: room.networkMode || 'relay',
        arenaId: room.arenaId,
    });
    console.log(`[timeout] Host kicked for not ready; guest promoted: ${code}`);
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

function packetInt(msg, field) {
    const value = msg[field];
    return Number.isInteger(value) ? value : null;
}

function queryInt(params, field) {
    const raw = params.get(field);
    if (raw === null || raw === '') return null;
    const parsed = Number.parseInt(raw, 10);
    return Number.isFinite(parsed) ? parsed : null;
}

function clientCompatibilityError(clientVersionCode, protocolVersion, balanceVersion, requireAll) {
    if (requireAll && clientVersionCode === null) {
        return '앱 버전 정보를 확인할 수 없습니다. 최신 앱으로 업데이트 후 다시 대전해주세요.';
    }
    if (clientVersionCode !== null && clientVersionCode < MIN_CLIENT_VERSION_CODE) {
        return `앱 업데이트가 필요합니다. 필요 버전 코드 ${MIN_CLIENT_VERSION_CODE} 이상에서 대전할 수 있습니다.`;
    }
    if (requireAll && protocolVersion === null) {
        return '대전 프로토콜 정보를 확인할 수 없습니다. 최신 앱으로 업데이트 후 다시 대전해주세요.';
    }
    if (protocolVersion !== null && protocolVersion < MIN_PROTOCOL_VERSION) {
        return '대전 프로토콜이 오래되었습니다. 최신 앱으로 업데이트 후 다시 대전해주세요.';
    }
    if (requireAll && balanceVersion === null) {
        return '대전 밸런스 정보를 확인할 수 없습니다. 최신 앱으로 업데이트 후 다시 대전해주세요.';
    }
    if (balanceVersion !== null && balanceVersion < MIN_BALANCE_VERSION) {
        return '대전 밸런스 데이터가 오래되었습니다. 최신 앱으로 업데이트 후 다시 대전해주세요.';
    }
    return null;
}

function compatibilityErrorFromQuery(params) {
    const clientVersionCode = queryInt(params, 'clientVersionCode');
    const protocolVersion = queryInt(params, 'protocolVersion');
    const balanceVersion = queryInt(params, 'balanceVersion');
    const hasAnyVersionField = clientVersionCode !== null || protocolVersion !== null || balanceVersion !== null;
    return clientCompatibilityError(clientVersionCode, protocolVersion, balanceVersion, hasAnyVersionField);
}

function compatibilityError(msg) {
    if (!COMPATIBILITY_TYPES.has(msg.type)) return null;

    const clientVersionCode = packetInt(msg, 'clientVersionCode');
    const protocolVersion = packetInt(msg, 'protocolVersion');
    const balanceVersion = packetInt(msg, 'balanceVersion');

    return clientCompatibilityError(clientVersionCode, protocolVersion, balanceVersion, true);
}

wss.on('connection', (ws) => {
    ws.roomCode = null;
    ws.role = null; // 'host' | 'guest'
    markSocketAlive(ws);

    const connectionCapacity = capacitySnapshot({ connectionExtra: 0 });
    if (!connectionCapacity.canConnect) {
        sendCapacityWsError(ws, connectionCapacity);
        ws.close(1013, connectionCapacity.status || 'server_busy');
        return;
    }

    ws.on('pong', () => {
        markSocketAlive(ws);
    });

    ws.on('message', (raw) => {
        markSocketAlive(ws);
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
        const compatibilityMessage = compatibilityError(msg);
        if (compatibilityMessage) {
            send(ws, {
                type: 'error',
                code: 'update_required',
                message: compatibilityMessage,
                requiredVersionCode: MIN_CLIENT_VERSION_CODE,
                requiredProtocolVersion: MIN_PROTOCOL_VERSION,
                requiredBalanceVersion: MIN_BALANCE_VERSION,
            });
            return;
        }

        switch (msg.type) {
            case 'ping_check': {
                const reportedRttMs = msg.rttMs;
                if (typeof reportedRttMs === 'number' &&
                    Number.isFinite(reportedRttMs) &&
                    reportedRttMs >= 0 &&
                    reportedRttMs < 60000) {
                    ws.lastRttMs = smoothedRttMs(ws.lastRttMs, reportedRttMs);
                    if (ws.role === 'host' && ws.roomCode && rooms[ws.roomCode]) {
                        rooms[ws.roomCode].hostRttMs = ws.lastRttMs;
                    }
                }
                send(ws, { type: 'ping_check_ack', clientTime: msg.clientTime, serverTime: Date.now() });
                break;
            }

            // ── 방 만들기 ────────────────────────────────────────────────
            case 'create_room': {
                const capacity = capacitySnapshot({ connectionExtra: 0 });
                if (!capacity.canCreateRoom) {
                    sendCapacityWsError(ws, capacity);
                    return;
                }
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
                    hostNickname: typeof msg.hostNickname === 'string' ? msg.hostNickname : undefined,
                    hostPlayerId: typeof msg.hostPlayerId === 'string' ? msg.hostPlayerId : undefined,
                    guestCharacterId: undefined,
                    guestPassiveId: undefined,
                    guestArenaId: undefined,
                    guestNickname: undefined,
                    guestPlayerId: undefined,
                    networkMode: networkMode(msg.networkMode),
                    matchStarted: false,
                    matchId: null,
                    hostReady: false,
                    guestReady: false,
                    readyDeadlineAt: null,
                    readyTimer: null,
                };
                ws.roomCode = code;
                ws.role = 'host';

                send(ws, { type: 'room_created', code, networkMode: rooms[code].networkMode });
                console.log(`[+] Room created: ${code}`);
                break;
            }

            // ── 방 참가 ──────────────────────────────────────────────────
            case 'join_room': {
                const capacity = capacitySnapshot({ connectionExtra: 0 });
                if (!capacity.canJoinRoom) {
                    sendCapacityWsError(ws, capacity);
                    return;
                }
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
                room.guestCharacterId = enumToken(msg.guestCharacterId);
                room.guestPassiveId = enumToken(msg.guestPassiveId);
                room.guestArenaId = enumToken(msg.arenaId);
                room.guestNickname = typeof msg.guestNickname === 'string' ? msg.guestNickname : undefined;
                room.guestPlayerId = typeof msg.guestPlayerId === 'string' ? msg.guestPlayerId : undefined;
                if (!room.matchId) room.matchId = makeMatchId();
                ws.roomCode = code;
                ws.role = 'guest';

                // 양쪽에게 준비 알림
                send(ws, {
                    type: 'room_joined',
                    code,
                    matchId: room.matchId,
                    networkMode: room.networkMode || 'relay',
                    hostCharacterId: room.hostCharacterId,
                    hostPassiveId: room.hostPassiveId,
                    arenaId: room.arenaId,
                    hostNickname: room.hostNickname,
                    hostPlayerId: room.hostPlayerId,
                });
                send(room.host, {
                    type: 'guest_joined',
                    matchId: room.matchId,
                    networkMode: room.networkMode || 'relay',
                    guestCharacterId: room.guestCharacterId,
                    guestPassiveId: room.guestPassiveId,
                    arenaId: room.guestArenaId,
                    guestNickname: room.guestNickname,
                    guestPlayerId: room.guestPlayerId,
                });
                startReadyDeadline(code, room);
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

            case 'selection_update': {
                const code = ws.roomCode;
                const room = rooms[code];
                if (!room) return;

                if (ws.role === 'host') {
                    room.hostCharacterId = enumToken(msg.characterId) || room.hostCharacterId;
                    room.hostPassiveId = enumToken(msg.passiveId) || room.hostPassiveId;
                    room.arenaId = enumToken(msg.arenaId) || room.arenaId;
                    room.hostNickname = typeof msg.nickname === 'string' ? msg.nickname : room.hostNickname;
                    room.hostPlayerId = typeof msg.playerId === 'string' ? msg.playerId : room.hostPlayerId;
                } else if (ws.role === 'guest') {
                    room.guestCharacterId = enumToken(msg.characterId) || room.guestCharacterId;
                    room.guestPassiveId = enumToken(msg.passiveId) || room.guestPassiveId;
                    room.guestArenaId = enumToken(msg.arenaId) || room.guestArenaId;
                    room.guestNickname = typeof msg.nickname === 'string' ? msg.nickname : room.guestNickname;
                    room.guestPlayerId = typeof msg.playerId === 'string' ? msg.playerId : room.guestPlayerId;
                }
                updateRoomReady(room, ws.role, msg.ready);

                const peer = ws.role === 'host' ? room.guest : room.host;
                send(peer, msg);
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
            case 'game_state_hp':
            case 'game_emote':
            case 'game_pause':
            case 'game_resume': {
                const code = ws.roomCode;
                const room = rooms[code];
                if (!room) return;

                if (msg.type === 'game_start') {
                    room.matchStarted = true;
                    clearReadyDeadline(room);
                }
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

        if (ws.role === 'guest' && !room.matchStarted) {
            send(room.host, { type: 'peer_disconnected' });
            clearReadyDeadline(room);
            resetGuestSlot(room);
            console.log(`[-] Guest left waiting room: ${code}`);
            return;
        }

        if (ws.role === 'host' &&
            !room.matchStarted &&
            room.guest?.readyState === WebSocket.OPEN) {
            const promotedHost = room.guest;
            room.host = promotedHost;
            room.guest = null;
            room.hostRttMs = socketRttMs(promotedHost);
            room.hostCharacterId = room.guestCharacterId;
            room.hostPassiveId = room.guestPassiveId;
            room.arenaId = room.guestArenaId || room.arenaId;
            room.hostNickname = room.guestNickname;
            room.hostPlayerId = room.guestPlayerId;
            clearReadyDeadline(room);
            resetGuestSlot(room);
            promotedHost.role = 'host';
            promotedHost.roomCode = code;
            send(promotedHost, {
                type: 'host_migrated',
                code,
                networkMode: room.networkMode || 'relay',
                arenaId: room.arenaId,
            });
            console.log(`[~] Host migrated after waiting host left: ${code}`);
            return;
        }

        const peer = ws.role === 'host' ? room.guest : room.host;

        // 상대방에게 연결 끊김 알림
        send(peer, { type: 'peer_disconnected' });

        // 방 삭제
        clearReadyDeadline(room);
        delete rooms[code];
        console.log(`[-] Room removed: ${code}`);
    });
});

const heartbeatInterval = setInterval(() => {
    const now = Date.now();
    wss.clients.forEach((ws) => {
        if (ws.readyState !== WebSocket.OPEN) return;
        const lastSeenAt = Number.isFinite(ws.lastSeenAt) ? ws.lastSeenAt : now;
        if (now - lastSeenAt > HEARTBEAT_TIMEOUT_MS) {
            ws.terminate();
            return;
        }
        ws.isAlive = false;
        try {
            ws.ping();
        } catch {
            ws.terminate();
        }
    });
}, HEARTBEAT_INTERVAL_MS);

wss.on('close', () => {
    clearInterval(heartbeatInterval);
});

initializeStatsStorage()
    .then(() => {
        server.listen(PORT, () => {
            console.log(`Signaling server running on port ${PORT} (${storageMode()} stats)`);
        });
    })
    .catch((err) => {
        console.error('[stats] failed to initialize storage:', err?.message || err);
        process.exit(1);
    });
