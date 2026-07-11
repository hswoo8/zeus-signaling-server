const http = require('http');
const crypto = require('crypto');
const WebSocket = require('ws');
const { Pool } = require('pg');
const packageJson = require('./package.json');
const {
    AnalyticsStore,
    eventFromHttpRequest,
    hashIdentifier,
    normalizeAnalyticsChannel,
    pairHash,
    renderAdminPage,
    requestCountry,
    safeUserAgent,
} = require('./analytics');

const PORT = process.env.PORT || 8080;
const DATABASE_URL = (process.env.DATABASE_URL || '').trim();
const MAX_MSG_BYTES = Number(process.env.MAX_MSG_BYTES || 16 * 1024);
const HTTP_BODY_LIMIT_BYTES = Number(process.env.HTTP_BODY_LIMIT_BYTES || MAX_MSG_BYTES);
const HEARTBEAT_INTERVAL_MS = Number(process.env.HEARTBEAT_INTERVAL_MS || 15000);
const HEARTBEAT_TIMEOUT_MS = Number(process.env.HEARTBEAT_TIMEOUT_MS || 45000);
const WS_BACKPRESSURE_SOFT_BYTES = Number(process.env.WS_BACKPRESSURE_SOFT_BYTES || 256 * 1024);
const WS_BACKPRESSURE_HARD_BYTES = Number(process.env.WS_BACKPRESSURE_HARD_BYTES || 1024 * 1024);
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
const BATTLE_COUNTDOWN_SYNC_DELAY_MS = Number(process.env.BATTLE_COUNTDOWN_SYNC_DELAY_MS || 4500);
const CONFIRMED_MATCH_TTL_MS = Number(process.env.CONFIRMED_MATCH_TTL_MS || 24 * 60 * 60 * 1000);
const ANALYTICS_RETENTION_DAYS = Number(process.env.ANALYTICS_RETENTION_DAYS || 90);
const ANALYTICS_INGEST_ENABLED = envBool(['ANALYTICS_INGEST_ENABLED'], true);
const ANALYTICS_RATE_LIMIT_PER_MINUTE = Number(process.env.ANALYTICS_RATE_LIMIT_PER_MINUTE || 120);
const ADMIN_DASHBOARD_USERNAME = (process.env.ADMIN_DASHBOARD_USERNAME || 'admin').trim();
const ADMIN_DASHBOARD_PASSWORD = (process.env.ADMIN_DASHBOARD_PASSWORD || '').trim();
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
const MAX_CLIENT_VERSION_CODE = envOptionalInt(
    ['MULTIPLAYER_MAX_APP_VERSION_CODE', 'MAX_CLIENT_VERSION_CODE']
) || Number.MAX_SAFE_INTEGER;
const MAX_PROTOCOL_VERSION = envOptionalInt(
    ['MULTIPLAYER_MAX_PROTOCOL_VERSION', 'MAX_PROTOCOL_VERSION']
) || Number.MAX_SAFE_INTEGER;
const MAX_BALANCE_VERSION = envOptionalInt(
    ['MULTIPLAYER_MAX_BALANCE_VERSION', 'MAX_BALANCE_VERSION']
) || Number.MAX_SAFE_INTEGER;
const RULESET_VERSION = envOptionalInt(['MULTIPLAYER_RULESET_VERSION', 'RULESET_VERSION']);
const SERVER_CHANNEL = envToken('SERVER_CHANNEL', 'unrestricted');
const SERVER_POOL_ID = envToken('SERVER_POOL_ID', 'default');
const SERVER_ALLOWED_CHANNELS = envTokenSet(
    'SERVER_ALLOWED_CHANNELS',
    SERVER_CHANNEL === 'unrestricted' ? [] : [SERVER_CHANNEL]
);
const NETWORK_MODES = new Set(['auto', 'relay', 'p2p']);
const BATTLE_TYPES = new Set(['short', 'standard', 'long']);
const statsPool = DATABASE_URL ? new Pool({
    connectionString: DATABASE_URL,
    ssl: postgresSslConfig(),
}) : null;
const analyticsStore = new AnalyticsStore(statsPool, { retentionDays: ANALYTICS_RETENTION_DAYS });
const server = http.createServer((req, res) => {
    handleHttpRequest(req, res).catch((err) => {
        console.error('[http] unexpected error:', err?.message || err);
        sendJson(res, 500, {
            error: { code: 'internal_error', message: 'Internal server error' },
        });
    });
});
const wss = new WebSocket.Server({ server });

// rooms[roomCode] = { host, guest, networkMode, hostCharacterId, hostPassiveId, arenaId, matchId, hostNickname, guestNickname }
const rooms = {};
const statsPlayers = new Map();
const statsIdempotency = new Map();
const confirmedPvpMatches = new Map();
const analyticsRequestBuckets = new Map();
let serverMatchCounter = 0;
let backpressureDroppedStatePackets = 0;
let backpressureClosedConnections = 0;

const LOBBY_TYPES = new Set([
    'create_room', 'join_room', 'leave_room', 'get_room_list', 'ping_check', 'selection_update',
    'offer', 'answer', 'ice_candidate',
]);

const GAME_TYPES = new Set([
    'game_start', 'game_ready', 'game_state', 'game_skill', 'game_damage',
    'game_state_hp', 'game_emote', 'game_over', 'game_start_failed', 'rematch_accept', 'rematch_decline',
    'rematch_request', 'rematch_cancel', 'rematch_reselect', 'rematch_ready',
    'game_pause', 'game_resume', 'game_countdown_sync',
]);

const ALL_TYPES = new Set([...LOBBY_TYPES, ...GAME_TYPES]);

const COMPATIBILITY_TYPES = new Set([
    'create_room', 'join_room', 'leave_room', 'get_room_list', 'ping_check',
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

function envToken(name, fallback) {
    const value = String(process.env[name] || fallback || '')
        .trim()
        .toLowerCase()
        .replace(/[^a-z0-9._-]/g, '')
        .slice(0, 48);
    return value || fallback;
}

function envTokenSet(name, fallback) {
    const source = String(process.env[name] || '').trim()
        ? String(process.env[name])
        : fallback.join(',');
    return new Set(source.split(',').map((value) => value.trim().toLowerCase()).filter(Boolean));
}

function appChannelAllowed(channel) {
    return SERVER_ALLOWED_CHANNELS.size === 0 || SERVER_ALLOWED_CHANNELS.has(channel);
}

function requestAppChannel(req) {
    return String(req.headers['x-app-channel'] || '').trim().toLowerCase();
}

function requireHttpAppChannel(req, res) {
    const channel = requestAppChannel(req);
    if (appChannelAllowed(channel)) return true;
    sendHttpError(
        res,
        409,
        'wrong_environment',
        `${channel || 'unknown'} 앱은 ${SERVER_CHANNEL} 서버 데이터를 사용할 수 없습니다.`
    );
    return false;
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
    await statsPool.query(`
        CREATE TABLE IF NOT EXISTS br_pvp_match_confirmations (
            match_id TEXT PRIMARY KEY,
            record JSONB NOT NULL,
            expires_at TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    `);
}

function generateCode() {
    return Math.floor(1000 + Math.random() * 9000).toString();
}

function battleType(value) {
    const normalized = String(value || '').trim().toLowerCase();
    return BATTLE_TYPES.has(normalized) ? normalized : 'short';
}

function send(ws, data) {
    if (!ws || ws.readyState !== WebSocket.OPEN) return false;
    const bufferedBytes = Number(ws.bufferedAmount || 0);
    if (bufferedBytes >= WS_BACKPRESSURE_HARD_BYTES) {
        backpressureClosedConnections += 1;
        ws.terminate();
        return false;
    }
    if (data?.type === 'game_state' && bufferedBytes >= WS_BACKPRESSURE_SOFT_BYTES) {
        backpressureDroppedStatePackets += 1;
        return false;
    }
    try {
        ws.send(JSON.stringify(data));
        return true;
    } catch {
        return false;
    }
}

function sendCountdownSync(room) {
    if (!room || !room.host || !room.guest) return;
    const serverTimeMs = Date.now();
    if (!Number.isFinite(room.battleStartAtMs) || room.battleStartAtMs <= serverTimeMs) {
        room.battleStartAtMs = serverTimeMs + BATTLE_COUNTDOWN_SYNC_DELAY_MS;
    }
    const packet = {
        type: 'game_countdown_sync',
        matchId: room.matchId || null,
        serverTimeMs,
        battleStartAtMs: room.battleStartAtMs,
        countdownDelayMs: Math.max(0, room.battleStartAtMs - serverTimeMs),
    };
    send(room.host, packet);
    send(room.guest, packet);
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

function sendHtml(res, statusCode, body) {
    res.writeHead(statusCode, {
        'Content-Type': 'text/html; charset=utf-8',
        'Content-Length': Buffer.byteLength(body),
        'Cache-Control': 'no-store',
        'X-Content-Type-Options': 'nosniff',
        'X-Frame-Options': 'DENY',
        'Referrer-Policy': 'no-referrer',
        'Content-Security-Policy': "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'",
    });
    res.end(body);
}

function sendHttpError(res, statusCode, code, message) {
    sendJson(res, statusCode, { error: { code, message } });
}

function secureEqual(left, right) {
    const leftBuffer = Buffer.from(String(left));
    const rightBuffer = Buffer.from(String(right));
    return leftBuffer.length === rightBuffer.length && crypto.timingSafeEqual(leftBuffer, rightBuffer);
}

function adminDashboardEnabled() {
    return Boolean(ADMIN_DASHBOARD_USERNAME && ADMIN_DASHBOARD_PASSWORD);
}

function adminAuthorized(req) {
    const authorization = String(req.headers.authorization || '');
    if (!authorization.startsWith('Basic ')) return false;
    let decoded;
    try {
        decoded = Buffer.from(authorization.slice(6), 'base64').toString('utf8');
    } catch {
        return false;
    }
    const separator = decoded.indexOf(':');
    if (separator < 0) return false;
    return secureEqual(decoded.slice(0, separator), ADMIN_DASHBOARD_USERNAME) &&
        secureEqual(decoded.slice(separator + 1), ADMIN_DASHBOARD_PASSWORD);
}

function requireAdmin(req, res) {
    if (!adminDashboardEnabled()) {
        sendHttpError(res, 503, 'admin_disabled', 'Admin dashboard credentials are not configured');
        return false;
    }
    if (!adminAuthorized(req)) {
        res.writeHead(401, {
            'WWW-Authenticate': 'Basic realm="MiniZeus Admin", charset="UTF-8"',
            'Cache-Control': 'no-store',
        });
        res.end('Authentication required');
        return false;
    }
    return true;
}

function analyticsRuntimeSnapshot() {
    const roomStats = roomCounts();
    return {
        storage: storageMode(),
        uptimeSec: Math.floor(process.uptime()),
        live: {
            connections: openConnectionCount(),
            rooms: roomStats.rooms,
            waitingRooms: roomStats.waitingRooms,
            activeMatches: roomStats.activeMatches,
        },
        backpressure: {
            droppedStatePackets: backpressureDroppedStatePackets,
            closedConnections: backpressureClosedConnections,
        },
    };
}

function analyticsRequestAllowed(req) {
    const forwarded = String(req.headers['x-forwarded-for'] || '').split(',')[0].trim();
    const addressHash = hashIdentifier(forwarded || req.socket?.remoteAddress || 'unknown') || 'unknown';
    const now = Date.now();
    const bucket = analyticsRequestBuckets.get(addressHash) || { windowStart: now, count: 0 };
    if (now - bucket.windowStart >= 60000) {
        bucket.windowStart = now;
        bucket.count = 0;
    }
    bucket.count += 1;
    analyticsRequestBuckets.set(addressHash, bucket);
    if (analyticsRequestBuckets.size > 5000) {
        for (const [key, value] of analyticsRequestBuckets) {
            if (now - value.windowStart >= 120000) analyticsRequestBuckets.delete(key);
        }
    }
    return bucket.count <= ANALYTICS_RATE_LIMIT_PER_MINUTE;
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
    const matchSlots = roomValues.filter((room) =>
        room.host?.readyState === WebSocket.OPEN &&
        room.guest?.readyState === WebSocket.OPEN
    ).length;
    return {
        rooms: roomValues.length,
        waitingRooms,
        activeMatches,
        matchSlots,
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
        matchSlots: roomStats.matchSlots,
    };
    const limits = {
        connections: limitValue(MAX_CONNECTIONS),
        rooms: limitValue(MAX_ACTIVE_ROOMS),
        activeMatches: limitValue(MAX_ACTIVE_MATCHES),
        busyRatio: CAPACITY_BUSY_RATIO,
    };

    const connectReason = blockedReason(counts.connections, MAX_CONNECTIONS, connectionExtra);
    const createReason = blockedReason(counts.rooms, MAX_ACTIVE_ROOMS, 1);
    const joinReason = blockedReason(counts.matchSlots, MAX_ACTIVE_MATCHES, 1);
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
        backpressure: {
            droppedStatePackets: backpressureDroppedStatePackets,
            closedConnections: backpressureClosedConnections,
        },
        requiredVersionCode: MIN_CLIENT_VERSION_CODE,
        requiredProtocolVersion: MIN_PROTOCOL_VERSION,
        requiredBalanceVersion: MIN_BALANCE_VERSION,
        requiredRulesetVersion: RULESET_VERSION || null,
        maxVersionCode: MAX_CLIENT_VERSION_CODE === Number.MAX_SAFE_INTEGER ? null : MAX_CLIENT_VERSION_CODE,
        maxProtocolVersion: MAX_PROTOCOL_VERSION === Number.MAX_SAFE_INTEGER ? null : MAX_PROTOCOL_VERSION,
        maxBalanceVersion: MAX_BALANCE_VERSION === Number.MAX_SAFE_INTEGER ? null : MAX_BALANCE_VERSION,
        channel: SERVER_CHANNEL,
        poolId: SERVER_POOL_ID,
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

    if (req.method === 'GET' && (pathname === '/admin' || pathname === '/admin/')) {
        if (!requireAdmin(req, res)) return;
        const snapshot = await analyticsStore.snapshot(
            analyticsRuntimeSnapshot(),
            url.searchParams.get('channel')
        );
        sendHtml(res, 200, renderAdminPage(snapshot));
        return;
    }

    if (req.method === 'GET' && pathname === '/admin/api/stats') {
        if (!requireAdmin(req, res)) return;
        sendJson(res, 200, await analyticsStore.snapshot(
            analyticsRuntimeSnapshot(),
            url.searchParams.get('channel')
        ));
        return;
    }

    if (req.method === 'POST' && pathname === '/analytics/events') {
        if (!ANALYTICS_INGEST_ENABLED) {
            sendHttpError(res, 503, 'analytics_disabled', 'Analytics ingestion is disabled');
            return;
        }
        if (!analyticsRequestAllowed(req)) {
            sendHttpError(res, 429, 'rate_limited', 'Too many analytics events');
            return;
        }
        const body = await readJsonRequest(req, res);
        if (!body) return;
        const event = eventFromHttpRequest(req, body);
        if (!event) {
            sendHttpError(res, 400, 'invalid_event', 'Analytics event is invalid or unsupported');
            return;
        }
        if (!appChannelAllowed(event.analyticsChannel)) {
            sendHttpError(res, 409, 'wrong_environment', 'Analytics channel does not match this server');
            return;
        }
        const result = await analyticsStore.record(event);
        sendJson(res, result.duplicate ? 200 : 202, result);
        return;
    }

    if (req.method === 'GET' && pathname === '/health') {
        const players = statsPool ? await postgresPlayerCount() : statsPlayers.size;
        sendJson(res, 200, {
            ok: true,
            service: 'beerock-signaling-server',
            version: packageJson.version || '1.0.0',
            channel: SERVER_CHANNEL,
            poolId: SERVER_POOL_ID,
            rulesetVersion: RULESET_VERSION || null,
            uptimeSec: Math.floor(process.uptime()),
            storage: storageMode(),
            rooms: Object.keys(rooms).length,
            players,
            backpressure: {
                droppedStatePackets: backpressureDroppedStatePackets,
                closedConnections: backpressureClosedConnections,
            },
        });
        return;
    }

    if (req.method === 'GET' && pathname === '/capacity') {
        const compatibilityIssue = compatibilityErrorFromQuery(url.searchParams);
        if (compatibilityIssue) {
            sendJson(res, 200, {
                ...capacitySnapshot(),
                status: compatibilityIssue.code === 'wrong_environment' ? 'wrong_environment' : 'update_required',
                code: compatibilityIssue.code,
                message: compatibilityIssue.message,
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
        if (!requireHttpAppChannel(req, res)) return;
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
        if (!requireHttpAppChannel(req, res)) return;
        const submittedBody = await readJsonRequest(req, res);
        if (!submittedBody) return;
        const confirmed = await normalizeConfirmedPvpSubmission(submittedBody);
        if (confirmed.error) {
            sendHttpError(
                res,
                confirmed.error.status,
                confirmed.error.code,
                confirmed.error.message
            );
            return;
        }
        const body = confirmed.body;
        if (statsPool) {
            await handlePostgresPvpMatchResult(res, body);
        } else {
            handlePvpMatchResult(res, body);
        }
        return;
    }

    if (req.method === 'GET' && pathname === '/rankings') {
        if (!requireHttpAppChannel(req, res)) return;
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
        if (!requireHttpAppChannel(req, res)) return;
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

    if (['/matches/result', '/matches/pvp-result', '/analytics/events'].includes(pathname) ||
        pathname === '/health' ||
        pathname === '/capacity' ||
        pathname === '/rankings' ||
        pathname === '/admin' ||
        pathname === '/admin/' ||
        pathname === '/admin/api/stats' ||
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
    const remoteReward = rewardForResult(remoteOutcome, body.remoteFinishReason || finishReason);
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
    const remoteReward = rewardForResult(remoteOutcome, body.remoteFinishReason || finishReason);
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

function roomParticipant(room, role) {
    const prefix = role === 'host' ? 'host' : 'guest';
    return {
        playerId: room[`${prefix}PlayerId`] || null,
        nickname: room[`${prefix}Nickname`] || (role === 'host' ? 'Host' : 'Guest'),
        characterId: room[`${prefix}CharacterId`] || null,
        passiveId: room[`${prefix}PassiveId`] || null,
    };
}

function opponentRole(role) {
    return role === 'host' ? 'guest' : 'host';
}

function canonicalMatchReason(reason) {
    if (reason === 'forfeit' || reason === 'local_forfeit' || reason === 'remote_forfeit') {
        return 'forfeit';
    }
    if (reason === 'disconnect_timeout' || reason === 'remote_disconnect') {
        return 'disconnect_timeout';
    }
    if (reason === 'timeout') return 'timeout';
    return 'normal';
}

function reportedGameOutcome(msg) {
    const explicit = outcomeValue(msg.outcome);
    if (explicit) return explicit;
    if (canonicalMatchReason(msg.reason) === 'forfeit') return 'loss';
    return packetInt(msg, 'hp') === 0 ? 'loss' : 'win';
}

function resultOutcomeForRole(result, role) {
    if (!result.winnerRole) return 'draw';
    return result.winnerRole === role ? 'win' : 'loss';
}

function resultReasonForRole(result, role) {
    if (result.reason === 'forfeit') {
        return result.loserRole === role ? 'local_forfeit' : 'remote_forfeit';
    }
    if (result.reason === 'disconnect_timeout') {
        return result.loserRole === role ? 'local_forfeit' : 'remote_disconnect';
    }
    return result.reason;
}

function resultPacketForRole(result, role) {
    const localHp = role === 'host' ? result.hostHp : result.guestHp;
    const remoteHp = role === 'host' ? result.guestHp : result.hostHp;
    const packet = {
        type: 'match_result',
        matchId: result.matchId,
        roundId: result.roundId,
        outcome: resultOutcomeForRole(result, role),
        finishReason: resultReasonForRole(result, role),
        serverConfirmed: true,
    };
    if (Number.isInteger(localHp)) packet.localHp = localHp;
    if (Number.isInteger(remoteHp)) packet.remoteHp = remoteHp;
    return packet;
}

function confirmationRecord(room, result) {
    return {
        type: 'confirmed_pvp',
        matchId: result.matchId,
        host: roomParticipant(room, 'host'),
        guest: roomParticipant(room, 'guest'),
        hostOutcome: resultOutcomeForRole(result, 'host'),
        guestOutcome: resultOutcomeForRole(result, 'guest'),
        hostFinishReason: resultReasonForRole(result, 'host'),
        guestFinishReason: resultReasonForRole(result, 'guest'),
        hostHp: result.hostHp,
        guestHp: result.guestHp,
        arenaId: room.arenaId || null,
        rulesetVersion: room.rulesetVersion || null,
        durationSec: result.durationSec,
        completedAt: result.completedAt,
        expiresAt: new Date(result.expiresAt).toISOString(),
    };
}

function rememberConfirmedPvpMatch(room, result) {
    const record = confirmationRecord(room, result);
    confirmedPvpMatches.set(record.matchId, record);
    if (statsPool) {
        statsPool.query(
            `INSERT INTO br_pvp_match_confirmations (match_id, record, expires_at)
             VALUES ($1, $2::jsonb, $3)
             ON CONFLICT (match_id) DO UPDATE
                SET record = EXCLUDED.record, expires_at = EXCLUDED.expires_at`,
            [record.matchId, JSON.stringify(record), record.expiresAt]
        ).catch((err) => {
            console.error('[stats] failed to persist confirmed PvP match:', err?.message || err);
        });
    }
}

function recordMultiMatchAnalytics(room, result) {
    const hostPlayerHash = hashIdentifier(room.hostPlayerId || room.hostNickname);
    const guestPlayerHash = hashIdentifier(room.guestPlayerId || room.guestNickname);
    const winnerPlayerHash = result.winnerRole === 'host'
        ? hostPlayerHash
        : (result.winnerRole === 'guest' ? guestPlayerHash : null);
    analyticsStore.record({
        eventId: `multi:${result.matchId}`,
        eventName: 'multi_match_complete',
        occurredAt: result.completedAt,
        playerIdHash: winnerPlayerHash,
        appVersionName: room.hostVersionName || room.guestVersionName || 'unknown',
        appVersionCode: room.hostVersionCode || room.guestVersionCode || null,
        buildType: 'multiplayer',
        analyticsChannel: roomAnalyticsChannel(room),
        platform: 'server',
        userAgent: 'server-confirmed',
        countryCode: 'ZZ',
        properties: {
            pairHash: pairHash(room.hostPlayerId || room.hostNickname, room.guestPlayerId || room.guestNickname),
            hostPlayerHash,
            guestPlayerHash,
            winnerPlayerHash,
            hostVersionName: room.hostVersionName || 'unknown',
            hostVersionCode: room.hostVersionCode || null,
            hostAnalyticsChannel: room.hostAnalyticsChannel || 'unknown',
            guestVersionName: room.guestVersionName || 'unknown',
            guestVersionCode: room.guestVersionCode || null,
            guestAnalyticsChannel: room.guestAnalyticsChannel || 'unknown',
            hostCountryCode: room.hostCountryCode || 'ZZ',
            guestCountryCode: room.guestCountryCode || 'ZZ',
            hostUserAgent: room.hostUserAgent || 'unknown',
            guestUserAgent: room.guestUserAgent || 'unknown',
            finishReason: resultReasonForRole(result, result.winnerRole || 'host'),
            durationSec: result.durationSec,
            battleType: room.battleType || 'short',
            arenaId: room.arenaId || null,
            rulesetVersion: room.rulesetVersion || null,
        },
    }).catch((err) => {
        console.error('[analytics] failed to record multi match:', err?.message || err);
    });
}

function roomAnalyticsChannel(room) {
    const hostChannel = normalizeAnalyticsChannel(room.hostAnalyticsChannel);
    const guestChannel = normalizeAnalyticsChannel(room.guestAnalyticsChannel);
    if (hostChannel === guestChannel) return hostChannel;
    if (hostChannel === 'unknown') return guestChannel;
    if (guestChannel === 'unknown') return hostChannel;
    return 'mixed';
}

function finalizeRoomMatch(room, reporterRole, msg) {
    if (!room || !['host', 'guest'].includes(reporterRole)) return null;
    if (room.finalResult) return room.finalResult;
    if (!room.matchStarted || !room.matchId) return null;
    const reporterOutcome = reportedGameOutcome(msg);
    const winnerRole = reporterOutcome === 'draw'
        ? null
        : (reporterOutcome === 'win' ? reporterRole : opponentRole(reporterRole));
    const loserRole = winnerRole ? opponentRole(winnerRole) : null;
    const reporterHp = packetInt(msg, 'hp');
    const remoteHp = packetInt(msg, 'remoteHp');
    const completedAtMs = Date.now();
    const completedAt = new Date(completedAtMs).toISOString();
    const result = {
        matchId: room.matchId,
        roundId: packetInt(msg, 'roundId'),
        winnerRole,
        loserRole,
        reason: canonicalMatchReason(msg.reason),
        hostHp: reporterRole === 'host' ? reporterHp : remoteHp,
        guestHp: reporterRole === 'guest' ? reporterHp : remoteHp,
        durationSec: Math.max(1, Math.floor((completedAtMs - (room.matchStartedAtMs || completedAtMs)) / 1000)),
        completedAt,
        expiresAt: Date.now() + CONFIRMED_MATCH_TTL_MS,
    };

    room.finalResult = result;
    room.matchStarted = false;
    rememberConfirmedPvpMatch(room, result);
    recordMultiMatchAnalytics(room, result);
    send(room.host, resultPacketForRole(result, 'host'));
    send(room.guest, resultPacketForRole(result, 'guest'));
    return result;
}

async function confirmedPvpMatch(matchId) {
    const cached = confirmedPvpMatches.get(matchId);
    if (cached) {
        if (Date.parse(cached.expiresAt) > Date.now()) return cached;
        confirmedPvpMatches.delete(matchId);
    }
    if (!statsPool) return null;

    const result = await statsPool.query(
        `SELECT record
           FROM br_pvp_match_confirmations
          WHERE match_id = $1 AND expires_at > NOW()`,
        [matchId]
    );
    const record = result.rows[0]?.record || null;
    if (record) confirmedPvpMatches.set(matchId, record);
    return record;
}

function confirmationRole(record, player) {
    const identity = playerIdentityKey(player);
    if (!identity) return null;
    if (identity === playerIdentityKey(record.host)) return 'host';
    if (identity === playerIdentityKey(record.guest)) return 'guest';
    return null;
}

async function normalizeConfirmedPvpSubmission(body) {
    const matchId = normalizePlayerId(body.serverMatchId);
    if (!matchId) {
        return { error: { status: 409, code: 'match_not_confirmed', message: 'serverMatchId is required for PvP results' } };
    }
    const record = await confirmedPvpMatch(matchId);
    if (!record) {
        return { error: { status: 409, code: 'match_not_confirmed', message: 'PvP result was not confirmed by the battle server' } };
    }

    const localRole = confirmationRole(record, body.localPlayer);
    const remoteRole = confirmationRole(record, body.remotePlayer);
    if (!localRole || !remoteRole || localRole === remoteRole) {
        return { error: { status: 409, code: 'participant_mismatch', message: 'PvP result players do not match the confirmed battle' } };
    }

    const expectedOutcome = localRole === 'host' ? record.hostOutcome : record.guestOutcome;
    if (outcomeValue(body.outcome) !== expectedOutcome) {
        return { error: { status: 409, code: 'result_mismatch', message: 'PvP outcome does not match the confirmed battle result' } };
    }

    const localParticipant = localRole === 'host' ? record.host : record.guest;
    const remoteParticipant = remoteRole === 'host' ? record.host : record.guest;
    const localHp = localRole === 'host' ? record.hostHp : record.guestHp;
    const remoteHp = remoteRole === 'host' ? record.hostHp : record.guestHp;

    return {
        body: {
            ...body,
            serverMatchId: matchId,
            clientMatchId: matchId,
            outcome: expectedOutcome,
            finishReason: localRole === 'host' ? record.hostFinishReason : record.guestFinishReason,
            remoteFinishReason: remoteRole === 'host' ? record.hostFinishReason : record.guestFinishReason,
            arenaId: record.arenaId || body.arenaId,
            durationSec: record.durationSec || body.durationSec,
            completedAt: record.completedAt,
            localPlayer: {
                ...body.localPlayer,
                ...localParticipant,
                hp: Number.isInteger(localHp) ? localHp : body.localPlayer?.hp,
            },
            remotePlayer: {
                ...body.remotePlayer,
                ...remoteParticipant,
                hp: Number.isInteger(remoteHp) ? remoteHp : body.remotePlayer?.hp,
            },
        },
    };
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

function roomListEntry(code, room, viewer) {
    if (!room || room.host?.readyState !== WebSocket.OPEN ||
        (room.guest && room.guest.readyState === WebSocket.OPEN)) {
        return null;
    }
    return {
        code,
        ...roomQuality(room.hostRttMs, socketRttMs(viewer)),
        hostCharacterId: room.hostCharacterId,
        arenaId: room.arenaId,
        battleType: room.battleType,
        networkMode: room.networkMode || 'relay',
        region: room.hostRegion || null,
        relayRegion: SERVER_POOL_ID,
    };
}

function roomListSnapshot(viewer) {
    return Object.keys(rooms)
        .map((code) => roomListEntry(code, rooms[code], viewer))
        .filter(Boolean);
}

function broadcastRoomUpsert(code) {
    const room = rooms[code];
    wss.clients.forEach((client) => {
        if (!client.roomListSubscribed || client.readyState !== WebSocket.OPEN) return;
        const entry = roomListEntry(code, room, client);
        if (entry) {
            send(client, { type: 'room_updated', room: entry });
        } else {
            send(client, { type: 'room_removed', code });
        }
    });
}

function broadcastRoomRemoved(code) {
    wss.clients.forEach((client) => {
        if (client.roomListSubscribed) send(client, { type: 'room_removed', code });
    });
}

function smoothedRttMs(previous, sample) {
    if (!Number.isFinite(sample)) return previous;
    if (!Number.isFinite(previous)) return sample;
    return Math.round(previous * 0.65 + sample * 0.35);
}

function resetGuestSlot(room) {
    if (!room) return;
    room.guest = null;
    room.guestCharacterId = undefined;
    room.guestPassiveId = undefined;
    room.guestArenaId = undefined;
    room.guestNickname = undefined;
    room.guestPlayerId = undefined;
    room.guestVersionCode = undefined;
    room.guestVersionName = undefined;
    room.guestCountryCode = undefined;
    room.guestUserAgent = undefined;
    room.matchStarted = false;
    room.matchId = null;
    room.finalResult = null;
    room.battleStartAtMs = null;
    room.matchStartedAtMs = null;
}

function leaveWaitingRoom(ws, notifyLeaver = false) {
    const code = ws?.roomCode;
    const room = code ? rooms[code] : null;
    if (!room || room.matchStarted) return false;

    if (ws.role === 'guest') {
        send(room.host, { type: 'peer_disconnected' });
        resetGuestSlot(room);
        broadcastRoomUpsert(code);
        console.log(`[-] Guest left waiting room: ${code}`);
    } else if (ws.role === 'host' && room.guest?.readyState === WebSocket.OPEN) {
        const promotedHost = room.guest;
        room.host = promotedHost;
        room.guest = null;
        room.hostRttMs = socketRttMs(promotedHost);
        room.hostCharacterId = room.guestCharacterId;
        room.hostPassiveId = room.guestPassiveId;
        room.arenaId = room.guestArenaId || room.arenaId;
        room.hostNickname = room.guestNickname;
        room.hostPlayerId = room.guestPlayerId;
        room.hostVersionCode = room.guestVersionCode;
        room.hostVersionName = room.guestVersionName;
        room.hostAnalyticsChannel = room.guestAnalyticsChannel;
        room.hostCountryCode = room.guestCountryCode;
        room.hostUserAgent = room.guestUserAgent;
        resetGuestSlot(room);
        promotedHost.role = 'host';
        promotedHost.roomCode = code;
        send(promotedHost, {
            type: 'host_migrated',
            code,
            networkMode: room.networkMode || 'relay',
            arenaId: room.arenaId,
        });
        broadcastRoomUpsert(code);
        console.log(`[~] Host migrated after waiting host left: ${code}`);
    } else if (ws.role === 'host') {
        delete rooms[code];
        broadcastRoomRemoved(code);
        console.log(`[-] Empty waiting room removed: ${code}`);
    } else {
        return false;
    }

    ws.roomCode = null;
    ws.role = null;
    if (notifyLeaver) send(ws, { type: 'room_left', code });
    return true;
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

function clientCompatibilityError(
    clientVersionCode,
    protocolVersion,
    rulesetVersion,
    balanceVersion,
    channel,
    requireAll
) {
    if (SERVER_ALLOWED_CHANNELS.size > 0 && !SERVER_ALLOWED_CHANNELS.has(channel)) {
        return {
            code: 'wrong_environment',
            message: `${channel || 'unknown'} 앱은 ${SERVER_CHANNEL} 대전 서버를 사용할 수 없습니다.`,
        };
    }
    if (requireAll && clientVersionCode === null) {
        return { code: 'update_required', message: '앱 버전 정보를 확인할 수 없습니다. 최신 앱으로 업데이트 후 다시 대전해주세요.' };
    }
    if (clientVersionCode !== null && clientVersionCode < MIN_CLIENT_VERSION_CODE) {
        return { code: 'update_required', message: `앱 업데이트가 필요합니다. 필요 버전 코드 ${MIN_CLIENT_VERSION_CODE} 이상에서 대전할 수 있습니다.` };
    }
    if (clientVersionCode !== null && clientVersionCode > MAX_CLIENT_VERSION_CODE) {
        return { code: 'incompatible_version', message: '이 서버보다 새로운 앱 버전입니다. 호환되는 대전 서버로 다시 연결해주세요.' };
    }
    if (requireAll && protocolVersion === null) {
        return { code: 'update_required', message: '대전 프로토콜 정보를 확인할 수 없습니다. 최신 앱으로 업데이트 후 다시 대전해주세요.' };
    }
    if (protocolVersion !== null && protocolVersion < MIN_PROTOCOL_VERSION) {
        return { code: 'update_required', message: '대전 프로토콜이 오래되었습니다. 최신 앱으로 업데이트 후 다시 대전해주세요.' };
    }
    if (protocolVersion !== null && protocolVersion > MAX_PROTOCOL_VERSION) {
        return { code: 'incompatible_version', message: '이 서버와 대전 프로토콜이 맞지 않습니다.' };
    }
    if (RULESET_VERSION > 0 && rulesetVersion !== RULESET_VERSION) {
        return { code: 'incompatible_ruleset', message: '이 서버와 대전 규칙 버전이 맞지 않습니다.' };
    }
    if (requireAll && balanceVersion === null) {
        return { code: 'update_required', message: '대전 밸런스 정보를 확인할 수 없습니다. 최신 앱으로 업데이트 후 다시 대전해주세요.' };
    }
    if (balanceVersion !== null && balanceVersion < MIN_BALANCE_VERSION) {
        return { code: 'update_required', message: '대전 밸런스 데이터가 오래되었습니다. 최신 앱으로 업데이트 후 다시 대전해주세요.' };
    }
    if (balanceVersion !== null && balanceVersion > MAX_BALANCE_VERSION) {
        return { code: 'incompatible_version', message: '이 서버와 대전 밸런스 버전이 맞지 않습니다.' };
    }
    return null;
}

function compatibilityErrorFromQuery(params) {
    const clientVersionCode = queryInt(params, 'clientVersionCode');
    const protocolVersion = queryInt(params, 'protocolVersion');
    const rulesetVersion = queryInt(params, 'rulesetVersion');
    const balanceVersion = queryInt(params, 'balanceVersion');
    const channel = String(params.get('channel') || '').trim().toLowerCase();
    const hasAnyVersionField = clientVersionCode !== null || protocolVersion !== null || rulesetVersion !== null || balanceVersion !== null;
    return clientCompatibilityError(clientVersionCode, protocolVersion, rulesetVersion, balanceVersion, channel, hasAnyVersionField);
}

function compatibilityError(msg) {
    if (!COMPATIBILITY_TYPES.has(msg.type)) return null;

    const clientVersionCode = packetInt(msg, 'clientVersionCode');
    const protocolVersion = packetInt(msg, 'protocolVersion');
    const rulesetVersion = packetInt(msg, 'rulesetVersion');
    const balanceVersion = packetInt(msg, 'balanceVersion');
    const channel = typeof msg.analyticsChannel === 'string' ? msg.analyticsChannel.trim().toLowerCase() : '';

    return clientCompatibilityError(clientVersionCode, protocolVersion, rulesetVersion, balanceVersion, channel, true);
}

wss.on('connection', (ws, req) => {
    ws.roomCode = null;
    ws.role = null; // 'host' | 'guest'
    ws.roomListSubscribed = false;
    ws.analyticsCountryCode = requestCountry(req, null);
    ws.analyticsUserAgent = safeUserAgent(req);
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
        const compatibilityIssue = compatibilityError(msg);
        if (compatibilityIssue) {
            send(ws, {
                type: 'error',
                code: compatibilityIssue.code,
                message: compatibilityIssue.message,
                requiredVersionCode: MIN_CLIENT_VERSION_CODE,
                requiredProtocolVersion: MIN_PROTOCOL_VERSION,
                requiredBalanceVersion: MIN_BALANCE_VERSION,
                requiredRulesetVersion: RULESET_VERSION || null,
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
                    hostRegion: typeof msg.region === 'string' ? msg.region.slice(0, 24) : undefined,
                    hostCharacterId: enumToken(msg.hostCharacterId),
                    hostPassiveId: enumToken(msg.hostPassiveId),
                    arenaId: enumToken(msg.arenaId),
                    battleType: battleType(msg.battleType),
                    rulesetVersion: packetInt(msg, 'rulesetVersion'),
                    hostNickname: typeof msg.hostNickname === 'string' ? msg.hostNickname : undefined,
                    hostPlayerId: typeof msg.hostPlayerId === 'string' ? msg.hostPlayerId : undefined,
                    hostVersionCode: packetInt(msg, 'clientVersionCode'),
                    hostVersionName: typeof msg.clientVersionName === 'string' ? msg.clientVersionName : undefined,
                    hostAnalyticsChannel: normalizeAnalyticsChannel(msg.analyticsChannel),
                    hostCountryCode: ws.analyticsCountryCode,
                    hostUserAgent: ws.analyticsUserAgent,
                    guestCharacterId: undefined,
                    guestPassiveId: undefined,
                    guestArenaId: undefined,
                    guestNickname: undefined,
                    guestPlayerId: undefined,
                    guestVersionCode: undefined,
                    guestVersionName: undefined,
                    guestAnalyticsChannel: undefined,
                    guestCountryCode: undefined,
                    guestUserAgent: undefined,
                    networkMode: networkMode(msg.networkMode),
                    matchStarted: false,
                    matchId: null,
                    matchSequence: 0,
                    finalResult: null,
                    battleStartAtMs: null,
                    matchStartedAtMs: null,
                };
                ws.roomCode = code;
                ws.role = 'host';

                send(ws, {
                    type: 'room_created',
                    code,
                    networkMode: rooms[code].networkMode,
                    battleType: rooms[code].battleType,
                });
                broadcastRoomUpsert(code);
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
                room.guestVersionCode = packetInt(msg, 'clientVersionCode');
                room.guestVersionName = typeof msg.clientVersionName === 'string' ? msg.clientVersionName : undefined;
                room.guestAnalyticsChannel = normalizeAnalyticsChannel(msg.analyticsChannel);
                room.guestCountryCode = ws.analyticsCountryCode;
                room.guestUserAgent = ws.analyticsUserAgent;
                ws.roomCode = code;
                ws.role = 'guest';

                // 양쪽에게 준비 알림
                send(ws, {
                    type: 'room_joined',
                    code,
                    networkMode: room.networkMode || 'relay',
                    hostCharacterId: room.hostCharacterId,
                    hostPassiveId: room.hostPassiveId,
                    arenaId: room.arenaId,
                    battleType: room.battleType,
                    hostNickname: room.hostNickname,
                    hostPlayerId: room.hostPlayerId,
                });
                send(room.host, {
                    type: 'guest_joined',
                    networkMode: room.networkMode || 'relay',
                    guestCharacterId: room.guestCharacterId,
                    guestPassiveId: room.guestPassiveId,
                    arenaId: room.guestArenaId,
                    battleType: room.battleType,
                    guestNickname: room.guestNickname,
                    guestPlayerId: room.guestPlayerId,
                });
                broadcastRoomRemoved(code);
                console.log(`[+] Room joined: ${code}`);
                break;
            }

            case 'leave_room': {
                if (!ws.roomCode || !rooms[ws.roomCode]) {
                    ws.roomCode = null;
                    ws.role = null;
                    send(ws, { type: 'room_left', code: null });
                    break;
                }
                if (!leaveWaitingRoom(ws, true)) {
                    send(ws, {
                        type: 'error',
                        code: 'match_in_progress',
                        message: 'Cannot leave an active match from the lobby',
                    });
                }
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
                const peer = ws.role === 'host' ? room.guest : room.host;
                send(peer, msg);
                if (ws.role === 'host' && !room.guest) broadcastRoomUpsert(code);
                break;
            }

            // ── 방 목록 조회 ─────────────────────────────────────────────
            case 'get_room_list': {
                ws.roomListSubscribed = true;
                send(ws, { type: 'room_list', rooms: roomListSnapshot(ws) });
                break;
            }

            // ── 게임 패킷 릴레이 ─────────────────────────────────────────
            case 'game_over': {
                const code = ws.roomCode;
                const room = rooms[code];
                if (!room) return;

                const peer = ws.role === 'host' ? room.guest : room.host;
                send(peer, { ...msg, matchId: room.matchId || null });
                finalizeRoomMatch(room, ws.role, msg);
                break;
            }

            case 'game_start_failed':
            case 'rematch_accept':
            case 'rematch_decline':
            case 'rematch_request':
            case 'rematch_cancel':
            case 'rematch_reselect':
            case 'rematch_ready':
            case 'game_start':
            case 'game_countdown_sync':
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
                    if (ws.role !== 'host') return;
                    if (!room.matchStarted) {
                        room.matchStarted = true;
                        room.matchSequence = (room.matchSequence || 0) + 1;
                        room.matchId = makeMatchId();
                        room.finalResult = null;
                        room.battleStartAtMs = null;
                        room.matchStartedAtMs = Date.now();
                    }
                    room.hostCharacterId = enumToken(msg.hostCharacterId) || room.hostCharacterId;
                    room.hostPassiveId = enumToken(msg.hostPassiveId) || room.hostPassiveId;
                    room.hostNickname = typeof msg.hostNickname === 'string' ? msg.hostNickname : room.hostNickname;
                    room.hostPlayerId = typeof msg.hostPlayerId === 'string' ? msg.hostPlayerId : room.hostPlayerId;
                    room.hostVersionCode = packetInt(msg, 'clientVersionCode') || room.hostVersionCode;
                    room.hostVersionName = typeof msg.clientVersionName === 'string' ? msg.clientVersionName : room.hostVersionName;
                    room.hostAnalyticsChannel = normalizeAnalyticsChannel(msg.analyticsChannel || room.hostAnalyticsChannel);
                    room.arenaId = enumToken(msg.arenaId) || room.arenaId;
                    const gameStartPacket = { ...msg, matchId: room.matchId };
                    const peer = ws.role === 'host' ? room.guest : room.host;
                    send(peer, gameStartPacket);
                    send(ws, {
                        type: 'match_assigned',
                        matchId: room.matchId,
                        matchSequence: room.matchSequence,
                    });
                    break;
                }
                if (msg.type === 'game_ready' && ws.role === 'guest') {
                    room.guestCharacterId = enumToken(msg.guestCharacterId) || room.guestCharacterId;
                    room.guestPassiveId = enumToken(msg.guestPassiveId) || room.guestPassiveId;
                    room.guestNickname = typeof msg.guestNickname === 'string' ? msg.guestNickname : room.guestNickname;
                    room.guestPlayerId = typeof msg.guestPlayerId === 'string' ? msg.guestPlayerId : room.guestPlayerId;
                    room.guestVersionCode = packetInt(msg, 'clientVersionCode') || room.guestVersionCode;
                    room.guestVersionName = typeof msg.clientVersionName === 'string' ? msg.clientVersionName : room.guestVersionName;
                    room.guestAnalyticsChannel = normalizeAnalyticsChannel(msg.analyticsChannel || room.guestAnalyticsChannel);
                }
                if (msg.type === 'rematch_ready') {
                    const prefix = ws.role === 'host' ? 'host' : 'guest';
                    room[`${prefix}CharacterId`] = enumToken(msg.characterId) || room[`${prefix}CharacterId`];
                    room[`${prefix}PassiveId`] = enumToken(msg.passiveId) || room[`${prefix}PassiveId`];
                    room[`${prefix}Nickname`] = typeof msg.nickname === 'string' ? msg.nickname : room[`${prefix}Nickname`];
                    room[`${prefix}PlayerId`] = typeof msg.playerId === 'string' ? msg.playerId : room[`${prefix}PlayerId`];
                    room[`${prefix}AnalyticsChannel`] = normalizeAnalyticsChannel(
                        msg.analyticsChannel || room[`${prefix}AnalyticsChannel`]
                    );
                }
                const peer = ws.role === 'host' ? room.guest : room.host;
                send(peer, msg);
                if (msg.type === 'game_ready') {
                    sendCountdownSync(room);
                }
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
        if (leaveWaitingRoom(ws)) return;

        const peer = ws.role === 'host' ? room.guest : room.host;

        if (room.matchStarted) {
            finalizeRoomMatch(room, ws.role, {
                outcome: 'loss',
                reason: 'disconnect_timeout',
                hp: 0,
            });
        }

        // 상대방에게 연결 끊김 알림
        send(peer, { type: 'peer_disconnected' });

        // 방 삭제
        delete rooms[code];
        broadcastRoomRemoved(code);
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
    .then(() => analyticsStore.initialize())
    .then(() => {
        server.listen(PORT, () => {
            console.log(`Signaling server running on port ${PORT} (${storageMode()} stats)`);
        });
    })
    .catch((err) => {
        console.error('[stats] failed to initialize storage:', err?.message || err);
        process.exit(1);
    });
