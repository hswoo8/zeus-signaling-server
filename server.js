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
const RELAY_MATCHES_ENABLED = envBool(['RELAY_MATCHES_ENABLED'], true);
const MAX_ACTIVE_RELAY_MATCHES = envOptionalInt(['MAX_ACTIVE_RELAY_MATCHES']);
const RELAY_EGRESS_WARNING_MB_PER_HOUR = envOptionalInt(['RELAY_EGRESS_WARNING_MB_PER_HOUR']);
const RELAY_EGRESS_LIMIT_MB_PER_HOUR = envOptionalInt(['RELAY_EGRESS_LIMIT_MB_PER_HOUR']);
const CAPACITY_BUSY_RATIO = envFloat(['CAPACITY_BUSY_RATIO', 'MULTIPLAYER_CAPACITY_BUSY_RATIO'], 0.9, 0.1, 1);
const CAPACITY_RETRY_AFTER_SEC = envInt(['CAPACITY_RETRY_AFTER_SEC', 'MULTIPLAYER_CAPACITY_RETRY_AFTER_SEC'], 30);
const MAINTENANCE_MODE = envBool(['MAINTENANCE_MODE', 'MULTIPLAYER_MAINTENANCE'], false);
const MAINTENANCE_MESSAGE = process.env.MAINTENANCE_MESSAGE ||
    process.env.MULTIPLAYER_MAINTENANCE_MESSAGE ||
    '대전 서버 점검 중입니다. 잠시 후 다시 시도해주세요.';
const DEPLOYMENT_DRAIN_MESSAGE = process.env.DEPLOYMENT_DRAIN_MESSAGE ||
    '안전한 배포를 위해 신규 대전 입장을 잠시 중단했습니다.';
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
const RANK_PLACEMENT_MATCHES = envInt(['RANK_PLACEMENT_MATCHES'], 10);
const RANK_PLACEMENT_K = envInt(['RANK_PLACEMENT_K'], 48);
const RANK_ESTABLISHED_K = envInt(['RANK_ESTABLISHED_K'], 24);
const RANK_ELO_SPREAD = envInt(['RANK_ELO_SPREAD'], 400);
const INTEGRITY_INVALID_FLAG_THRESHOLD = envInt(['INTEGRITY_INVALID_FLAG_THRESHOLD'], 3);
const INTEGRITY_HP_MISMATCH_FLAG_THRESHOLD = envInt(['INTEGRITY_HP_MISMATCH_FLAG_THRESHOLD'], 3);
const INTEGRITY_REPORT_MAX_SKEW_MS = envInt(['INTEGRITY_REPORT_MAX_SKEW_MS'], 5000);
const ANALYTICS_RETENTION_DAYS = Number(process.env.ANALYTICS_RETENTION_DAYS || 90);
const ANALYTICS_INGEST_ENABLED = envBool(['ANALYTICS_INGEST_ENABLED'], true);
const ANALYTICS_RATE_LIMIT_PER_MINUTE = Number(process.env.ANALYTICS_RATE_LIMIT_PER_MINUTE || 120);
const SUPPORT_INGEST_ENABLED = envBool(['SUPPORT_INGEST_ENABLED'], true);
const SUPPORT_RETENTION_DAYS = Math.min(3650, envInt(['SUPPORT_RETENTION_DAYS'], 180));
const SUPPORT_RATE_LIMIT_PER_HOUR = Math.min(20, envInt(['SUPPORT_RATE_LIMIT_PER_HOUR'], 5));
const SUPPORT_ADDRESS_RATE_LIMIT_PER_HOUR = Math.min(
    100,
    envInt(['SUPPORT_ADDRESS_RATE_LIMIT_PER_HOUR'], Math.max(10, SUPPORT_RATE_LIMIT_PER_HOUR * 2))
);
const ANNOUNCEMENT_CATEGORIES = new Set(['update', 'maintenance', 'privacy', 'balance']);
const ANNOUNCEMENT_STATUSES = new Set(['draft', 'published', 'archived']);
const ANNOUNCEMENT_CHANNELS = new Set(['all', 'dev', 'beta', 'production']);
const ADMIN_DASHBOARD_USERNAME = (process.env.ADMIN_DASHBOARD_USERNAME || 'admin').trim();
const ADMIN_DASHBOARD_PASSWORD = (process.env.ADMIN_DASHBOARD_PASSWORD || '').trim();
const AUTH_TOKEN_SECRET = String(process.env.AUTH_TOKEN_SECRET || '').trim();
const AUTH_ACCESS_TTL_SEC = Number(process.env.AUTH_ACCESS_TTL_SEC || 60 * 60);
const AUTH_GUEST_TTL_SEC = Number(process.env.AUTH_GUEST_TTL_SEC || 180 * 24 * 60 * 60);
const AUTH_WS_TICKET_TTL_SEC = Number(process.env.AUTH_WS_TICKET_TTL_SEC || 60);
const AUTH_RATE_LIMIT_PER_MINUTE = Number(process.env.AUTH_RATE_LIMIT_PER_MINUTE || 30);
const AUTH_ENABLED = AUTH_TOKEN_SECRET.length >= 32;
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
const authRequestBuckets = new Map();
const usedWsTicketIds = new Map();
const wss = new WebSocket.Server({
    server,
    verifyClient: ({ req }) => authenticateWebSocketUpgrade(req),
});

// rooms[roomCode] = { host, guest, networkMode, hostCharacterId, hostPassiveId, arenaId, matchId, hostNickname, guestNickname }
const rooms = {};
const statsPlayers = new Map();
const statsIdempotency = new Map();
const confirmedPvpMatches = new Map();
const analyticsRequestBuckets = new Map();
const supportRequestBuckets = new Map();
const supportAddressBuckets = new Map();
const supportInquiries = new Map();
const announcements = new Map();
let serverMatchCounter = 0;
let backpressureDroppedStatePackets = 0;
let backpressureClosedConnections = 0;
let websocketDisconnects = 0;
let websocketAbnormalDisconnects = 0;
let websocketHeartbeatTimeouts = 0;
let websocketPingFailures = 0;
const websocketDisconnectSources = new Map();
let integrityAuditsReceived = 0;
let integrityAuditsInvalid = 0;
let integrityAuditMismatches = 0;
let integrityMatchesFlagged = 0;
let capacityRejections = 0;
let runtimeDrainEnabled = false;
let runtimeDrainStartedAtMs = null;
let relayedPackets = 0;
let relayedBytes = 0;
let relayAdmissionRejections = 0;
let relayRuntimeFallbacks = 0;
let eventLoopLagLatestMs = 0;
const eventLoopLagSamples = [];
const relayMinuteBytes = new Map();

const LOBBY_TYPES = new Set([
    'create_room', 'join_room', 'join_ranked_room', 'leave_room', 'get_room_list', 'ping_check', 'selection_update',
    'offer', 'answer', 'ice_candidate',
]);
const MATCH_MODES = new Set(['ranked', 'casual', 'friendly']);
const RANKED_BATTLE_TYPE = 'short';
const RANKED_ARENA_IDS = Object.freeze(['CLASSIC_OLYMPUS', 'SKY_OLYMPUS']);

function randomRankedArenaId() {
    return RANKED_ARENA_IDS[Math.floor(Math.random() * RANKED_ARENA_IDS.length)];
}

const GAME_TYPES = new Set([
    'game_start', 'game_ready', 'game_state', 'game_skill', 'game_damage',
    'game_state_hp', 'game_emote', 'game_over', 'game_start_failed', 'rematch_accept', 'rematch_decline',
    'rematch_request', 'rematch_cancel', 'rematch_reselect', 'rematch_ready',
    'game_pause', 'game_resume', 'game_countdown_sync', 'game_audit',
]);

const ALL_TYPES = new Set([...LOBBY_TYPES, ...GAME_TYPES]);

const COMPATIBILITY_TYPES = new Set([
    'create_room', 'join_room', 'join_ranked_room', 'leave_room', 'get_room_list', 'ping_check',
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
    await statsPool.query(`
        CREATE TABLE IF NOT EXISTS br_support_inquiries (
            id BIGSERIAL PRIMARY KEY,
            public_id TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL DEFAULT 'open',
            category TEXT NOT NULL,
            message TEXT NOT NULL,
            reply_email TEXT,
            player_id TEXT NOT NULL,
            player_id_hash TEXT NOT NULL,
            app_version_name TEXT NOT NULL,
            app_version_code INTEGER,
            build_type TEXT NOT NULL,
            analytics_channel TEXT NOT NULL,
            country_code TEXT NOT NULL,
            user_agent TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            closed_at TIMESTAMPTZ
        )
    `);
    await statsPool.query(`
        CREATE INDEX IF NOT EXISTS br_support_inquiries_channel_status_time_idx
            ON br_support_inquiries (analytics_channel, status, created_at DESC)
    `);
    await statsPool.query(`
        CREATE INDEX IF NOT EXISTS br_support_inquiries_player_time_idx
            ON br_support_inquiries (player_id, created_at DESC)
    `);
    await statsPool.query(
        `DELETE FROM br_support_inquiries
          WHERE created_at < NOW() - ($1::text || ' days')::interval`,
        [String(SUPPORT_RETENTION_DAYS)]
    );
    await statsPool.query(`
        CREATE TABLE IF NOT EXISTS br_announcements (
            id BIGSERIAL PRIMARY KEY,
            public_id TEXT NOT NULL UNIQUE,
            title TEXT NOT NULL,
            body TEXT NOT NULL,
            category TEXT NOT NULL,
            analytics_channel TEXT NOT NULL DEFAULT 'all',
            status TEXT NOT NULL DEFAULT 'draft',
            effective_at TIMESTAMPTZ NOT NULL,
            published_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    `);
    await statsPool.query(`
        CREATE INDEX IF NOT EXISTS br_announcements_public_idx
            ON br_announcements (analytics_channel, status, effective_at DESC, published_at DESC)
    `);
}

function generateCode() {
    return Math.floor(1000 + Math.random() * 9000).toString();
}

function battleType(value) {
    const normalized = String(value || '').trim().toLowerCase();
    return BATTLE_TYPES.has(normalized) ? normalized : 'short';
}

function matchMode(value) {
    const normalized = String(value || '').trim().toLowerCase();
    return MATCH_MODES.has(normalized) ? normalized : 'friendly';
}

function send(ws, data, options = {}) {
    if (!ws || ws.readyState !== WebSocket.OPEN) return false;
    const bufferedBytes = Number(ws.bufferedAmount || 0);
    if (bufferedBytes >= WS_BACKPRESSURE_HARD_BYTES) {
        backpressureClosedConnections += 1;
        ws.serverTerminationSource = 'backpressure';
        ws.terminate();
        return false;
    }
    if (data?.type === 'game_state' && bufferedBytes >= WS_BACKPRESSURE_SOFT_BYTES) {
        backpressureDroppedStatePackets += 1;
        return false;
    }
    try {
        const payload = JSON.stringify(data);
        ws.send(payload);
        if (options.relay === true) {
            relayedPackets += 1;
            const payloadBytes = Buffer.byteLength(payload, 'utf8');
            relayedBytes += payloadBytes;
            recordRelayBytes(payloadBytes);
        }
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
        matchSequence: room.matchSequence || 0,
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

function recordWebSocketDisconnect(ws, code, reason) {
    websocketDisconnects += 1;
    if (code !== 1000) websocketAbnormalDisconnects += 1;
    const source = ws.serverTerminationSource || (code === 1000 ? 'normal' : 'transport');
    websocketDisconnectSources.set(source, (websocketDisconnectSources.get(source) || 0) + 1);
    const reasonText = Buffer.isBuffer(reason) ? reason.toString('utf8') : String(reason || '');
    console.log(
        `[ws] closed code=${code} source=${source} room=${ws.roomCode || '-'} reason=${reasonText || '-'}`
    );
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

function sendRedirect(res, location) {
    res.writeHead(303, {
        Location: location,
        'Cache-Control': 'no-store',
    });
    res.end();
}

function escapeHtml(value) {
    return String(value ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

function supportStatsText(stats) {
    const lines = [];
    for (const mode of ['single', 'multi']) {
        const row = stats?.[mode];
        if (!row) continue;
        lines.push(`${mode} · MMR ${row.rating} · #${row.rank || '-'} · ${row.wins}W ${row.losses}L ${row.draws}D`);
    }
    return lines.join('\n') || '기록 없음';
}

function renderAdminSupportPage(inquiries, options = {}) {
    const selectedChannel = options.channel || 'all';
    const selectedStatus = options.status || 'all';
    const filters = [
        ['all', '전체'],
        ['beta', '베타'],
        ['production', '운영'],
        ['dev', '개발'],
    ].map(([channel, label]) => {
        const query = new URLSearchParams({ channel, status: selectedStatus }).toString();
        return `<a href="/admin/support?${query}" class="${channel === selectedChannel ? 'current' : ''}">${label}</a>`;
    }).join('');
    const statuses = ['all', 'open', 'review', 'closed'].map((status) => {
        const label = { all: '전체', open: '접수', review: '검토', closed: '완료' }[status];
        const query = new URLSearchParams({ channel: selectedChannel, status }).toString();
        return `<a href="/admin/support?${query}" class="${status === selectedStatus ? 'current' : ''}">${label}</a>`;
    }).join('');
    const rows = inquiries.length > 0 ? inquiries.map((inquiry) => {
        const encodedId = encodeURIComponent(inquiry.id);
        const statusButtons = ['open', 'review', 'closed'].map((status) => {
            const label = { open: '접수', review: '검토', closed: '완료' }[status];
            return `<form method="post" action="/admin/support/${encodedId}/status?status=${status}&channel=${encodeURIComponent(selectedChannel)}&filter=${encodeURIComponent(selectedStatus)}"><button${inquiry.status === status ? ' disabled' : ''}>${label}</button></form>`;
        }).join('');
        return `<article class="ticket">
            <header><strong>${escapeHtml(inquiry.category)}</strong><span class="status ${escapeHtml(inquiry.status)}">${escapeHtml(inquiry.status)}</span><time>${escapeHtml(new Date(inquiry.createdAt).toLocaleString('ko-KR', { timeZone: 'Asia/Seoul' }))}</time></header>
            <pre class="message">${escapeHtml(inquiry.message)}</pre>
            <dl>
                <div><dt>회신 이메일</dt><dd>${escapeHtml(inquiry.replyEmail || '-')}</dd></div>
                <div><dt>플레이어 ID</dt><dd><code>${escapeHtml(inquiry.playerId)}</code><small>${escapeHtml(inquiry.playerIdHash)}</small></dd></div>
                <div><dt>앱</dt><dd>${escapeHtml(inquiry.appVersionName)} (${escapeHtml(inquiry.appVersionCode ?? '-')}) · ${escapeHtml(inquiry.buildType)} · ${escapeHtml(inquiry.analyticsChannel)}</dd></div>
                <div><dt>국가 / User-Agent</dt><dd>${escapeHtml(inquiry.countryCode)} · ${escapeHtml(inquiry.userAgent)}</dd></div>
                <div><dt>현재 전적</dt><dd><pre>${escapeHtml(supportStatsText(inquiry.playerStats))}</pre></dd></div>
            </dl>
            <div class="actions">${statusButtons}</div>
        </article>`;
    }).join('') : '<p class="empty">조건에 맞는 문의가 없습니다.</p>';
    return `<!doctype html>
<html lang="ko"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="robots" content="noindex,nofollow,noarchive"><title>MiniZeus Support Admin</title>
<style>:root{--bg:#f4f5f2;--surface:#fff;--line:#d8ddd5;--text:#20231f;--muted:#697067;--green:#19764c;--orange:#c65d1b;--red:#b83b32;--ink:#38424b}*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:14px/1.45 system-ui,-apple-system,"Noto Sans KR",sans-serif}header.page{background:#20231f;color:#fff;padding:18px 24px;border-bottom:4px solid var(--green)}header.page h1{font-size:20px;margin:0 0 4px}header.page p{margin:0;color:#cbd2c8;font-size:12px}main{max-width:1240px;margin:0 auto;padding:20px 24px 48px}.toolbar{display:flex;justify-content:space-between;gap:12px;align-items:center;margin-bottom:14px}.toolbar a{color:#fff;background:var(--green);padding:8px 12px;border-radius:4px;text-decoration:none;font-weight:700}.filters{display:flex;gap:12px;flex-wrap:wrap;margin-bottom:20px}.filter{display:inline-grid;grid-auto-flow:column;border:1px solid var(--line);border-radius:6px;overflow:hidden;background:var(--surface)}.filter a{padding:8px 12px;color:var(--text);text-decoration:none;font-weight:700;border-right:1px solid var(--line)}.filter a:last-child{border-right:0}.filter a.current{background:var(--ink);color:#fff}.ticket{background:var(--surface);border:1px solid var(--line);border-radius:6px;padding:16px;margin:12px 0}.ticket>header{display:flex;align-items:center;gap:9px;flex-wrap:wrap}.ticket time{margin-left:auto;color:var(--muted);font-size:12px}.status{font-size:12px;font-weight:700;padding:2px 7px;border-radius:999px;background:#e9ece7}.status.review{background:#ffe1bb}.status.closed{background:#d8ecdf}.message{white-space:pre-wrap;overflow-wrap:anywhere;background:#f8faf7;border:1px solid #e7eae5;padding:10px;margin:12px 0;font:inherit}dl{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px;margin:0}dl div{border-top:1px solid #e7eae5;padding-top:8px}dt{font-size:12px;color:var(--muted)}dd{margin:2px 0 0;overflow-wrap:anywhere}dd small{display:block;color:var(--muted)}dd pre{margin:0;white-space:pre-wrap;font:inherit}.actions{display:flex;gap:8px;margin-top:14px}.actions form{margin:0}.actions button{border:1px solid var(--green);background:#fff;color:var(--green);border-radius:4px;padding:6px 10px;font-weight:700;cursor:pointer}.actions button:disabled{opacity:.45;cursor:default}.empty{color:var(--muted);padding:24px;text-align:center;background:var(--surface);border:1px solid var(--line);border-radius:6px}@media(max-width:760px){main{padding:14px}dl{grid-template-columns:1fr}.ticket time{margin-left:0;width:100%}.filter{overflow:auto;max-width:100%}}</style></head>
<body><header class="page"><h1>MiniZeus 문의 관리</h1><p>관리자 전용 · 문의 본문과 선택 회신 이메일은 지원 처리 목적으로만 보관</p></header><main>
<div class="toolbar"><span>보관 ${SUPPORT_RETENTION_DAYS}일 · ${escapeHtml(storageMode())}</span><span><a href="/admin/announcements?channel=${encodeURIComponent(selectedChannel)}">공지 관리</a> <a href="/admin?channel=${encodeURIComponent(selectedChannel)}">운영 통계</a></span></div>
<div class="filters"><nav class="filter">${filters}</nav><nav class="filter">${statuses}</nav></div>${rows}</main></body></html>`;
}

function announcementCategory(value, fallback = null) {
    const category = cleanSupportText(value, 24).toLowerCase();
    return ANNOUNCEMENT_CATEGORIES.has(category) ? category : fallback;
}

function announcementStatus(value, fallback = null) {
    const status = cleanSupportText(value, 24).toLowerCase();
    return ANNOUNCEMENT_STATUSES.has(status) ? status : fallback;
}

function announcementChannel(value, fallback = null) {
    const channel = cleanSupportText(value, 24).toLowerCase();
    return ANNOUNCEMENT_CHANNELS.has(channel) ? channel : fallback;
}

function announcementId() {
    return `ann_${crypto.randomBytes(12).toString('base64url')}`;
}

function announcementFromInput(body) {
    const title = cleanSupportText(body?.title, 120);
    const message = cleanSupportText(body?.body, 5000);
    const category = announcementCategory(body?.category);
    const analyticsChannel = announcementChannel(body?.channel, 'all');
    const status = announcementStatus(body?.status, 'draft');
    const requestedEffectiveAt = cleanSupportText(body?.effectiveAt, 64);
    const effectiveAt = requestedEffectiveAt ? new Date(requestedEffectiveAt) : new Date();
    if (title.length < 2 || message.length < 4 || !category || Number.isNaN(effectiveAt.getTime())) {
        return null;
    }
    return {
        title,
        body: message,
        category,
        analyticsChannel,
        status,
        effectiveAt: effectiveAt.toISOString(),
    };
}

function announcementFromRow(row) {
    return {
        id: row.public_id,
        title: row.title,
        body: row.body,
        category: announcementCategory(row.category, 'update'),
        channel: announcementChannel(row.analytics_channel, 'all'),
        status: announcementStatus(row.status, 'draft'),
        effectiveAt: new Date(row.effective_at).toISOString(),
        publishedAt: row.published_at ? new Date(row.published_at).toISOString() : null,
        createdAt: new Date(row.created_at).toISOString(),
        updatedAt: new Date(row.updated_at).toISOString(),
    };
}

function announcementVisibleToChannel(announcement, channel) {
    return announcement.status === 'published' &&
        (announcement.channel === 'all' || announcement.channel === channel);
}

async function createAnnouncement(input) {
    const id = announcementId();
    const now = new Date().toISOString();
    const publishedAt = input.status === 'published' ? now : null;
    if (!statsPool) {
        const announcement = {
            id,
            ...input,
            publishedAt,
            createdAt: now,
            updatedAt: now,
        };
        announcements.set(id, announcement);
        return announcement;
    }
    const result = await statsPool.query(
        `INSERT INTO br_announcements (
            public_id, title, body, category, analytics_channel, status, effective_at, published_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         RETURNING *`,
        [
            id,
            input.title,
            input.body,
            input.category,
            input.analyticsChannel,
            input.status,
            input.effectiveAt,
            publishedAt,
        ]
    );
    return announcementFromRow(result.rows[0]);
}

async function listAnnouncements(channel = 'all', status = 'all', limit = 100) {
    const selectedChannel = announcementChannel(channel, 'all');
    const selectedStatus = status === 'all' ? 'all' : announcementStatus(status, 'all');
    const safeLimit = Math.min(200, Math.max(1, Number(limit) || 100));
    if (!statsPool) {
        return Array.from(announcements.values())
            .filter((announcement) => selectedChannel === 'all' || announcement.channel === selectedChannel)
            .filter((announcement) => selectedStatus === 'all' || announcement.status === selectedStatus)
            .sort((left, right) => right.effectiveAt.localeCompare(left.effectiveAt))
            .slice(0, safeLimit);
    }
    const clauses = [];
    const params = [];
    if (selectedChannel !== 'all') {
        params.push(selectedChannel);
        clauses.push(`analytics_channel = $${params.length}`);
    }
    if (selectedStatus !== 'all') {
        params.push(selectedStatus);
        clauses.push(`status = $${params.length}`);
    }
    params.push(safeLimit);
    const where = clauses.length ? `WHERE ${clauses.join(' AND ')}` : '';
    const rows = await statsPool.query(
        `SELECT * FROM br_announcements ${where}
          ORDER BY effective_at DESC, published_at DESC NULLS LAST, created_at DESC
          LIMIT $${params.length}`,
        params
    );
    return rows.rows.map(announcementFromRow);
}

async function publicAnnouncements(channel, limit = 30) {
    const safeChannel = announcementChannel(channel, 'all');
    if (!statsPool) {
        return Array.from(announcements.values())
            .filter((announcement) => announcementVisibleToChannel(announcement, safeChannel))
            .sort((left, right) => right.effectiveAt.localeCompare(left.effectiveAt))
            .slice(0, Math.min(100, Math.max(1, Number(limit) || 30)));
    }
    const rows = await statsPool.query(
        `SELECT * FROM br_announcements
          WHERE status = 'published' AND (analytics_channel = 'all' OR analytics_channel = $1)
          ORDER BY effective_at DESC, published_at DESC NULLS LAST, created_at DESC
          LIMIT $2`,
        [safeChannel, Math.min(100, Math.max(1, Number(limit) || 30))]
    );
    return rows.rows.map(announcementFromRow);
}

async function findPublicAnnouncement(id, channel) {
    const safeId = cleanSupportText(id, 64);
    const safeChannel = announcementChannel(channel, 'all');
    if (!safeId) return null;
    if (!statsPool) {
        const announcement = announcements.get(safeId);
        return announcement && announcementVisibleToChannel(announcement, safeChannel) ? announcement : null;
    }
    const result = await statsPool.query(
        `SELECT * FROM br_announcements
          WHERE public_id = $1 AND status = 'published'
            AND (analytics_channel = 'all' OR analytics_channel = $2)`,
        [safeId, safeChannel]
    );
    return result.rows[0] ? announcementFromRow(result.rows[0]) : null;
}

async function updateAnnouncementStatus(id, status) {
    const safeId = cleanSupportText(id, 64);
    const safeStatus = announcementStatus(status);
    if (!safeId || !safeStatus) return null;
    if (!statsPool) {
        const announcement = announcements.get(safeId);
        if (!announcement) return null;
        announcement.status = safeStatus;
        announcement.publishedAt = safeStatus === 'published' ? announcement.publishedAt || new Date().toISOString() : null;
        announcement.updatedAt = new Date().toISOString();
        return announcement;
    }
    const result = await statsPool.query(
        `UPDATE br_announcements
            SET status = $2,
                published_at = CASE
                    WHEN $2 = 'published' THEN COALESCE(published_at, NOW())
                    ELSE NULL
                END,
                updated_at = NOW()
          WHERE public_id = $1
          RETURNING *`,
        [safeId, safeStatus]
    );
    return result.rows[0] ? announcementFromRow(result.rows[0]) : null;
}

function renderAdminAnnouncementsPage(items, options = {}) {
    const selectedChannel = announcementChannel(options.channel, 'all');
    const selectedStatus = options.status === 'all' ? 'all' : announcementStatus(options.status, 'all');
    const channelFilters = [['all', '전체'], ['production', '운영'], ['beta', '베타'], ['dev', '개발']]
        .map(([channel, label]) => `<a href="/admin/announcements?${new URLSearchParams({ channel, status: selectedStatus })}" class="${channel === selectedChannel ? 'current' : ''}">${label}</a>`)
        .join('');
    const statusFilters = [['all', '전체'], ['draft', '초안'], ['published', '공개'], ['archived', '보관']]
        .map(([status, label]) => `<a href="/admin/announcements?${new URLSearchParams({ channel: selectedChannel, status })}" class="${status === selectedStatus ? 'current' : ''}">${label}</a>`)
        .join('');
    const categoryLabels = { update: '업데이트', maintenance: '점검', privacy: '개인정보', balance: '밸런스' };
    const rows = items.length > 0 ? items.map((item) => `<article class="notice">
        <header><strong>${escapeHtml(item.title)}</strong><span>${escapeHtml(categoryLabels[item.category] || item.category)}</span><span>${escapeHtml(item.channel)}</span><span class="status ${escapeHtml(item.status)}">${escapeHtml(item.status)}</span></header>
        <time>시행 ${escapeHtml(new Date(item.effectiveAt).toLocaleString('ko-KR', { timeZone: 'Asia/Seoul' }))}</time>
        <pre>${escapeHtml(item.body)}</pre>
        <div class="actions">${['draft', 'published', 'archived'].map((status) => `<button type="button" onclick='setStatus(${JSON.stringify(item.id)}, ${JSON.stringify(status)})'${item.status === status ? ' disabled' : ''}>${escapeHtml({ draft: '초안', published: '공개', archived: '보관' }[status])}</button>`).join('')}</div>
    </article>`).join('') : '<p class="empty">등록된 공지가 없습니다.</p>';
    return `<!doctype html><html lang="ko"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="robots" content="noindex,nofollow,noarchive"><title>MiniZeus 공지 관리</title>
<style>:root{--bg:#f4f5f2;--surface:#fff;--line:#d8ddd5;--text:#20231f;--muted:#697067;--green:#19764c;--ink:#38424b}*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:14px/1.45 system-ui,-apple-system,"Noto Sans KR",sans-serif}header.page{background:#20231f;color:#fff;padding:18px 24px;border-bottom:4px solid var(--green)}header.page h1{font-size:20px;margin:0 0 4px}header.page p{margin:0;color:#cbd2c8;font-size:12px}main{max-width:1000px;margin:0 auto;padding:20px 24px 48px}.toolbar{display:flex;justify-content:space-between;gap:12px;align-items:center;margin-bottom:14px}.toolbar a{color:#fff;background:var(--green);padding:8px 12px;border-radius:4px;text-decoration:none;font-weight:700}.filters{display:flex;gap:12px;flex-wrap:wrap;margin-bottom:18px}.filter{display:inline-grid;grid-auto-flow:column;border:1px solid var(--line);border-radius:6px;overflow:hidden;background:var(--surface)}.filter a{padding:8px 12px;color:var(--text);text-decoration:none;font-weight:700;border-right:1px solid var(--line)}.filter a:last-child{border-right:0}.filter a.current{background:var(--ink);color:#fff}.composer,.notice{background:var(--surface);border:1px solid var(--line);border-radius:6px;padding:16px;margin:12px 0}.composer h2{font-size:16px;margin:0 0 12px}.composer label{display:block;font-weight:700;margin:9px 0 4px}.composer input,.composer textarea,.composer select{width:100%;font:inherit;padding:8px;border:1px solid var(--line);border-radius:4px}.composer textarea{min-height:150px;resize:vertical}.fields{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px}.composer button,.actions button{border:1px solid var(--green);background:#fff;color:var(--green);border-radius:4px;padding:7px 11px;font-weight:700;cursor:pointer}.composer button{background:var(--green);color:#fff;margin-top:14px}.notice header{display:flex;gap:8px;align-items:center;flex-wrap:wrap}.notice header strong{font-size:16px}.notice header span{font-size:12px;border-radius:999px;background:#e9ece7;padding:2px 7px}.notice header .published{background:#d8ecdf}.notice header .archived{background:#eee}.notice time{display:block;color:var(--muted);font-size:12px;margin-top:7px}.notice pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#f8faf7;border:1px solid #e7eae5;padding:10px;margin:12px 0;font:inherit}.actions{display:flex;gap:8px}.actions button:disabled{opacity:.45;cursor:default}.empty{color:var(--muted);padding:24px;text-align:center;background:var(--surface);border:1px solid var(--line);border-radius:6px}@media(max-width:700px){main{padding:14px}.fields{grid-template-columns:1fr}}</style></head>
<body><header class="page"><h1>MiniZeus 공지 관리</h1><p>관리자 전용 · 공개 공지는 앱의 배포 채널에 맞춰 표시됩니다.</p></header><main>
<div class="toolbar"><a href="/admin?channel=${encodeURIComponent(selectedChannel === 'all' ? 'all' : selectedChannel)}">운영 통계</a><a href="/admin/support?channel=${encodeURIComponent(selectedChannel === 'all' ? 'all' : selectedChannel)}">문의 관리</a></div>
<section class="composer"><h2>공지 등록</h2><form id="announcement-form"><label>제목<input name="title" maxlength="120" required></label><label>본문<textarea name="body" maxlength="5000" required></textarea></label><div class="fields"><label>유형<select name="category"><option value="update">업데이트</option><option value="maintenance">점검</option><option value="privacy">개인정보</option><option value="balance">밸런스</option></select></label><label>배포 채널<select name="channel"><option value="all">전체</option><option value="production">운영</option><option value="beta">베타</option><option value="dev">개발</option></select></label><label>공개 상태<select name="status"><option value="draft">초안</option><option value="published">공개</option></select></label></div><label>시행일<input name="effectiveAt" type="datetime-local"></label><button type="submit">공지 저장</button></form></section>
<div class="filters"><nav class="filter">${channelFilters}</nav><nav class="filter">${statusFilters}</nav></div>${rows}</main>
<script>const api='/admin/api/announcements';document.getElementById('announcement-form').addEventListener('submit',async event=>{event.preventDefault();const form=new FormData(event.currentTarget);const effective=form.get('effectiveAt');const payload=Object.fromEntries(form.entries());if(effective)payload.effectiveAt=new Date(effective).toISOString();const response=await fetch(api,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});if(!response.ok){alert('공지 저장에 실패했습니다.');return}location.reload()});async function setStatus(id,status){const response=await fetch(api+'/'+encodeURIComponent(id)+'/status',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({status})});if(!response.ok){alert('상태 변경에 실패했습니다.');return}location.reload()}</script></body></html>`;
}

function bearerToken(req) {
    const authorization = String(req.headers.authorization || '');
    return authorization.startsWith('Bearer ') ? authorization.slice(7).trim() : '';
}

function issueSignedToken(type, subject, channel, ttlSec, extra = {}) {
    const nowSec = Math.floor(Date.now() / 1000);
    const payload = {
        typ: type,
        sub: subject,
        channel,
        iat: nowSec,
        exp: nowSec + Math.max(1, ttlSec),
        jti: crypto.randomBytes(12).toString('base64url'),
        ...extra,
    };
    const encoded = Buffer.from(JSON.stringify(payload)).toString('base64url');
    const signature = crypto.createHmac('sha256', AUTH_TOKEN_SECRET).update(encoded).digest('base64url');
    return { token: `${encoded}.${signature}`, payload };
}

function verifySignedToken(token, expectedType) {
    if (!AUTH_ENABLED || typeof token !== 'string') return null;
    const [encoded, signature, extra] = token.split('.');
    if (!encoded || !signature || extra) return null;
    const expected = crypto.createHmac('sha256', AUTH_TOKEN_SECRET).update(encoded).digest('base64url');
    if (!secureEqual(signature, expected)) return null;
    let payload;
    try {
        payload = JSON.parse(Buffer.from(encoded, 'base64url').toString('utf8'));
    } catch {
        return null;
    }
    const nowSec = Math.floor(Date.now() / 1000);
    if (payload?.typ !== expectedType || !normalizePlayerId(payload.sub) ||
        !Number.isInteger(payload.exp) || payload.exp <= nowSec || !appChannelAllowed(payload.channel)) {
        return null;
    }
    return payload;
}

function cleanupUsedWsTickets(nowSec = Math.floor(Date.now() / 1000)) {
    for (const [ticketId, expiresAtSec] of usedWsTicketIds) {
        if (expiresAtSec <= nowSec) usedWsTicketIds.delete(ticketId);
    }
}

function authenticateWebSocketUpgrade(req) {
    if (!AUTH_ENABLED) return true;
    const payload = verifySignedToken(bearerToken(req), 'ws_ticket');
    if (!payload?.jti) return false;
    cleanupUsedWsTickets();
    if (usedWsTicketIds.has(payload.jti)) return false;
    usedWsTicketIds.set(payload.jti, payload.exp);
    req.authPrincipal = payload;
    return true;
}

function authRequestAllowed(req) {
    const forwarded = String(req.headers['x-forwarded-for'] || '').split(',')[0].trim();
    const key = hashIdentifier(forwarded || req.socket?.remoteAddress || 'unknown') || 'unknown';
    const now = Date.now();
    const bucket = authRequestBuckets.get(key) || { windowStart: now, count: 0 };
    if (now - bucket.windowStart >= 60000) {
        bucket.windowStart = now;
        bucket.count = 0;
    }
    bucket.count += 1;
    authRequestBuckets.set(key, bucket);
    if (authRequestBuckets.size > 5000) {
        for (const [entryKey, value] of authRequestBuckets) {
            if (now - value.windowStart >= 120000) authRequestBuckets.delete(entryKey);
        }
    }
    return bucket.count <= AUTH_RATE_LIMIT_PER_MINUTE;
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
            activeRelayMatches: roomStats.activeRelayMatches,
            activeP2pMatches: roomStats.activeP2pMatches,
        },
        operations: operationsSnapshot(),
        deployment: deploymentSnapshot(),
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

const SUPPORT_CATEGORIES = new Set(['bug', 'account', 'gameplay', 'other']);
const SUPPORT_STATUSES = new Set(['open', 'review', 'closed']);

function cleanSupportText(value, maxLength, fallback = '') {
    if (typeof value !== 'string') return fallback;
    const cleaned = value
        .replace(/\r\n?/g, '\n')
        .replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/g, ' ')
        .trim();
    return cleaned ? cleaned.slice(0, maxLength) : fallback;
}

function normalizeSupportCategory(value) {
    const category = cleanSupportText(value, 24).toLowerCase();
    return SUPPORT_CATEGORIES.has(category) ? category : null;
}

function normalizeSupportStatus(value, fallback = null) {
    const status = cleanSupportText(value, 24).toLowerCase();
    return SUPPORT_STATUSES.has(status) ? status : fallback;
}

function normalizeReplyEmail(value) {
    const email = cleanSupportText(value, 254);
    if (!email) return null;
    return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email) ? email : null;
}

function supportHeader(req, name, maxLength, fallback = '') {
    return cleanSupportText(String(req.headers[name] || ''), maxLength, fallback);
}

function supportInquiryFromRequest(req, body) {
    const playerId = normalizePlayerId(req.headers['x-player-id']);
    const category = normalizeSupportCategory(body?.category);
    const message = cleanSupportText(body?.message, 2000);
    const replyEmailRaw = cleanSupportText(body?.replyEmail, 254);
    const replyEmail = normalizeReplyEmail(replyEmailRaw);
    if (!playerId || !category || message.length < 4 || (replyEmailRaw && !replyEmail)) return null;
    const appVersionCode = Number.parseInt(String(req.headers['x-app-version-code'] || ''), 10);
    return {
        playerId,
        playerIdHash: hashIdentifier(playerId) || 'unknown',
        category,
        message,
        replyEmail,
        appVersionName: supportHeader(req, 'x-app-version-name', 40, 'unknown'),
        appVersionCode: Number.isFinite(appVersionCode) && appVersionCode >= 0 && appVersionCode <= 1000000000
            ? appVersionCode
            : null,
        buildType: supportHeader(req, 'x-build-type', 24, 'unknown'),
        analyticsChannel: normalizeAnalyticsChannel(requestAppChannel(req), supportHeader(req, 'x-build-type', 24, 'unknown')),
        countryCode: requestCountry(req),
        userAgent: safeUserAgent(req),
    };
}

function consumeRateLimit(bucketMap, key, maxRequests) {
    const now = Date.now();
    const bucket = bucketMap.get(key) || { windowStart: now, count: 0 };
    if (now - bucket.windowStart >= 3600000) {
        bucket.windowStart = now;
        bucket.count = 0;
    }
    bucket.count += 1;
    bucketMap.set(key, bucket);
    if (bucketMap.size > 5000) {
        for (const [entryKey, value] of bucketMap) {
            if (now - value.windowStart >= 7200000) bucketMap.delete(entryKey);
        }
    }
    return bucket.count <= maxRequests;
}

function supportRequestAllowed(req, playerId) {
    const forwarded = String(req.headers['x-forwarded-for'] || '').split(',')[0].trim();
    const addressKey = hashIdentifier(forwarded || req.socket?.remoteAddress || 'unknown') || 'unknown';
    const playerKey = hashIdentifier(playerId) || 'unknown';
    const playerAllowed = consumeRateLimit(
        supportRequestBuckets,
        playerKey,
        SUPPORT_RATE_LIMIT_PER_HOUR
    );
    const addressAllowed = consumeRateLimit(
        supportAddressBuckets,
        addressKey,
        SUPPORT_ADDRESS_RATE_LIMIT_PER_HOUR
    );
    return playerAllowed && addressAllowed;
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
    const activeRelayMatches = roomValues.filter((room) =>
        room.host?.readyState === WebSocket.OPEN &&
        room.guest?.readyState === WebSocket.OPEN &&
        room.matchStarted &&
        room.activeTransport === 'relay'
    ).length;
    const activeP2pMatches = roomValues.filter((room) =>
        room.host?.readyState === WebSocket.OPEN &&
        room.guest?.readyState === WebSocket.OPEN &&
        room.matchStarted &&
        room.activeTransport === 'p2p'
    ).length;
    return {
        rooms: roomValues.length,
        waitingRooms,
        activeMatches,
        matchSlots,
        activeRelayMatches,
        activeP2pMatches,
    };
}

function recordRelayBytes(bytes) {
    const minute = Math.floor(Date.now() / 60000);
    relayMinuteBytes.set(minute, (relayMinuteBytes.get(minute) || 0) + bytes);
    for (const key of relayMinuteBytes.keys()) {
        if (key < minute - 59) relayMinuteBytes.delete(key);
    }
}

function relayBytesLastHour() {
    const currentMinute = Math.floor(Date.now() / 60000);
    let bytes = 0;
    for (const [minute, value] of relayMinuteBytes) {
        if (minute >= currentMinute - 59) {
            bytes += value;
        } else {
            relayMinuteBytes.delete(minute);
        }
    }
    return bytes;
}

function relayAvailabilitySnapshot(additionalMatches = 1) {
    const activeMatches = roomCounts().activeRelayMatches;
    const lastHourBytes = relayBytesLastHour();
    const lastHourMb = Math.round(lastHourBytes * 10 / 1048576) / 10;
    const warning = RELAY_EGRESS_WARNING_MB_PER_HOUR > 0 &&
        lastHourBytes >= RELAY_EGRESS_WARNING_MB_PER_HOUR * 1048576;
    let code = 'ok';
    let message = 'Relay 대전 이용 가능';
    if (!RELAY_MATCHES_ENABLED) {
        code = 'relay_disabled';
        message = '현재 Relay 대전이 일시 중단되었습니다. P2P 모드로 다시 시도해주세요.';
    } else if (MAX_ACTIVE_RELAY_MATCHES > 0 && activeMatches + additionalMatches > MAX_ACTIVE_RELAY_MATCHES) {
        code = 'relay_capacity';
        message = '현재 Relay 대전이 혼잡합니다. P2P 모드로 다시 시도해주세요.';
    } else if (RELAY_EGRESS_LIMIT_MB_PER_HOUR > 0 &&
        lastHourBytes >= RELAY_EGRESS_LIMIT_MB_PER_HOUR * 1048576) {
        code = 'relay_egress_limited';
        message = 'Relay 트래픽 보호가 작동 중입니다. P2P 모드로 다시 시도해주세요.';
    }
    return {
        enabled: RELAY_MATCHES_ENABLED,
        canStartNewMatch: code === 'ok',
        code,
        message,
        activeMatches,
        maxActiveMatches: limitValue(MAX_ACTIVE_RELAY_MATCHES),
        lastHourBytes,
        lastHourMb,
        warning,
        warningMbPerHour: limitValue(RELAY_EGRESS_WARNING_MB_PER_HOUR),
        limitMbPerHour: limitValue(RELAY_EGRESS_LIMIT_MB_PER_HOUR),
    };
}

function eventLoopLagSnapshot() {
    if (eventLoopLagSamples.length === 0) {
        return { latest: 0, p95: 0, max: 0, samples: 0 };
    }
    const sorted = [...eventLoopLagSamples].sort((a, b) => a - b);
    const p95Index = Math.min(sorted.length - 1, Math.ceil(sorted.length * 0.95) - 1);
    return {
        latest: eventLoopLagLatestMs,
        p95: sorted[p95Index],
        max: sorted[sorted.length - 1],
        samples: sorted.length,
    };
}

function operationsSnapshot() {
    return {
        capacityRejections,
        relay: {
            packets: relayedPackets,
            bytes: relayedBytes,
            admissionRejections: relayAdmissionRejections,
            runtimeFallbacks: relayRuntimeFallbacks,
            ...relayAvailabilitySnapshot(),
        },
        backpressure: {
            droppedStatePackets: backpressureDroppedStatePackets,
            closedConnections: backpressureClosedConnections,
        },
        websocketDisconnects: {
            total: websocketDisconnects,
            abnormal: websocketAbnormalDisconnects,
            heartbeatTimeouts: websocketHeartbeatTimeouts,
            pingFailures: websocketPingFailures,
            sources: Object.fromEntries(websocketDisconnectSources),
        },
        integrityAudits: {
            received: integrityAuditsReceived,
            invalid: integrityAuditsInvalid,
            hpMismatches: integrityAuditMismatches,
            flaggedMatches: integrityMatchesFlagged,
            thresholds: {
                invalidReports: INTEGRITY_INVALID_FLAG_THRESHOLD,
                consecutiveHpMismatches: INTEGRITY_HP_MISMATCH_FLAG_THRESHOLD,
                reportSkewMs: INTEGRITY_REPORT_MAX_SKEW_MS,
            },
        },
        eventLoopLagMs: eventLoopLagSnapshot(),
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

function deploymentSnapshot() {
    const roomStats = roomCounts();
    const admissionPaused = MAINTENANCE_MODE || runtimeDrainEnabled;
    return {
        ready: true,
        acceptingConnections: !admissionPaused,
        staticMaintenance: MAINTENANCE_MODE,
        draining: runtimeDrainEnabled,
        drainStartedAt: runtimeDrainStartedAtMs
            ? new Date(runtimeDrainStartedAtMs).toISOString()
            : null,
        activeMatchesDrained: roomStats.activeMatches === 0,
        activeMatches: roomStats.activeMatches,
        waitingRooms: roomStats.waitingRooms,
        rooms: roomStats.rooms,
        connections: openConnectionCount(),
    };
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
        activeRelayMatches: roomStats.activeRelayMatches,
        activeP2pMatches: roomStats.activeP2pMatches,
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
    const admissionPaused = MAINTENANCE_MODE || runtimeDrainEnabled;
    const canConnect = !admissionPaused && !connectReason;
    const canCreateRoom = !admissionPaused && !createReason;
    const canJoinRoom = !admissionPaused && !joinReason;
    const canAcceptMatchmaking = canConnect && (canCreateRoom || canJoinRoom);

    let status = 'available';
    let code = 'ok';
    let message = '대전 서버 이용 가능';
    if (admissionPaused) {
        status = 'maintenance';
        code = 'server_maintenance';
        message = runtimeDrainEnabled ? DEPLOYMENT_DRAIN_MESSAGE : MAINTENANCE_MESSAGE;
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
        operations: operationsSnapshot(),
        relay: relayAvailabilitySnapshot(),
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
    capacityRejections += 1;
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
    const adminSupportStatusMatch = pathname.match(/^\/admin\/support\/([^/]+)\/status$/);
    const adminSupportApiStatusMatch = pathname.match(/^\/admin\/api\/support\/([^/]+)\/status$/);
    const announcementMatch = pathname.match(/^\/announcements\/([^/]+)$/);
    const adminAnnouncementStatusMatch = pathname.match(/^\/admin\/api\/announcements\/([^/]+)\/status$/);

    if (req.method === 'POST' && pathname === '/auth/guest/register') {
        if (!AUTH_ENABLED) {
            sendHttpError(res, 503, 'auth_disabled', 'Guest authentication is not configured');
            return;
        }
        if (!authRequestAllowed(req)) {
            sendHttpError(res, 429, 'rate_limited', 'Too many authentication requests');
            return;
        }
        const body = await readJsonRequest(req, res);
        if (!body) return;
        const playerId = normalizePlayerId(body.playerId);
        const channel = normalizeAnalyticsChannel(body.analyticsChannel, body.buildType);
        if (!playerId || !appChannelAllowed(channel)) {
            sendHttpError(res, 400, 'invalid_guest_registration', 'Player or app channel is invalid');
            return;
        }
        const guest = issueSignedToken('guest', playerId, channel, AUTH_GUEST_TTL_SEC);
        sendJson(res, 201, {
            playerId,
            guestToken: guest.token,
            expiresAtMs: guest.payload.exp * 1000,
        });
        return;
    }

    if (req.method === 'POST' && pathname === '/auth/token') {
        if (!AUTH_ENABLED) {
            sendHttpError(res, 503, 'auth_disabled', 'Guest authentication is not configured');
            return;
        }
        if (!authRequestAllowed(req)) {
            sendHttpError(res, 429, 'rate_limited', 'Too many authentication requests');
            return;
        }
        const body = await readJsonRequest(req, res);
        if (!body) return;
        const guest = verifySignedToken(body.guestToken, 'guest');
        if (!guest) {
            sendHttpError(res, 401, 'invalid_guest_token', 'Guest token is invalid or expired');
            return;
        }
        const access = issueSignedToken('access', guest.sub, guest.channel, AUTH_ACCESS_TTL_SEC);
        sendJson(res, 200, {
            accessToken: access.token,
            expiresAtMs: access.payload.exp * 1000,
        });
        return;
    }

    if (req.method === 'POST' && pathname === '/auth/ws-ticket') {
        if (!AUTH_ENABLED) {
            sendHttpError(res, 503, 'auth_disabled', 'Guest authentication is not configured');
            return;
        }
        if (!authRequestAllowed(req)) {
            sendHttpError(res, 429, 'rate_limited', 'Too many authentication requests');
            return;
        }
        const access = verifySignedToken(bearerToken(req), 'access');
        if (!access) {
            sendHttpError(res, 401, 'invalid_access_token', 'Access token is invalid or expired');
            return;
        }
        const ticket = issueSignedToken('ws_ticket', access.sub, access.channel, AUTH_WS_TICKET_TTL_SEC);
        sendJson(res, 201, {
            ticket: ticket.token,
            expiresAtMs: ticket.payload.exp * 1000,
        });
        return;
    }

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

    if (req.method === 'POST' && pathname === '/admin/api/deployment/drain') {
        if (!requireAdmin(req, res)) return;
        const body = await readJsonRequest(req, res);
        if (!body) return;
        if (typeof body.enabled !== 'boolean') {
            sendHttpError(res, 400, 'invalid_drain_state', 'enabled must be a boolean');
            return;
        }
        runtimeDrainEnabled = body.enabled;
        runtimeDrainStartedAtMs = runtimeDrainEnabled ? Date.now() : null;
        const deployment = deploymentSnapshot();
        console.info(
            `[deployment] drain ${runtimeDrainEnabled ? 'enabled' : 'disabled'} ` +
            `(activeMatches=${deployment.activeMatches}, rooms=${deployment.rooms}, connections=${deployment.connections})`
        );
        sendJson(res, 200, deployment);
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

    if (req.method === 'POST' && pathname === '/support/inquiries') {
        if (!SUPPORT_INGEST_ENABLED) {
            sendHttpError(res, 503, 'support_disabled', 'Support inquiries are temporarily unavailable');
            return;
        }
        if (!requireHttpAppChannel(req, res)) return;
        const body = await readJsonRequest(req, res);
        if (!body) return;
        const inquiry = supportInquiryFromRequest(req, body);
        if (!inquiry) {
            sendHttpError(res, 400, 'invalid_inquiry', 'Inquiry content or metadata is invalid');
            return;
        }
        if (!supportRequestAllowed(req, inquiry.playerId)) {
            sendHttpError(res, 429, 'rate_limited', 'Too many support inquiries. Please try again later.');
            return;
        }
        const saved = await createSupportInquiry(inquiry);
        sendJson(res, 201, {
            inquiryId: saved.id,
            status: saved.status,
            receivedAt: saved.createdAt,
        });
        return;
    }

    if (req.method === 'GET' && (pathname === '/admin/support' || pathname === '/admin/support/')) {
        if (!requireAdmin(req, res)) return;
        const channel = url.searchParams.get('channel') || 'all';
        const status = url.searchParams.get('status') || 'all';
        const inquiries = await supportInquiryList(channel, status);
        sendHtml(res, 200, renderAdminSupportPage(inquiries, {
            channel: normalizeAnalyticsChannel(channel, 'unknown') === 'unknown' && channel !== 'unknown' ? 'all' : channel,
            status: normalizeSupportStatus(status) || 'all',
        }));
        return;
    }

    if (req.method === 'GET' && pathname === '/admin/api/support') {
        if (!requireAdmin(req, res)) return;
        const channel = url.searchParams.get('channel') || 'all';
        const status = url.searchParams.get('status') || 'all';
        sendJson(res, 200, {
            storage: storageMode(),
            retentionDays: SUPPORT_RETENTION_DAYS,
            inquiries: await supportInquiryList(channel, status),
        });
        return;
    }

    if (req.method === 'POST' && adminSupportStatusMatch) {
        if (!requireAdmin(req, res)) return;
        const status = normalizeSupportStatus(url.searchParams.get('status'));
        const updated = await updateSupportInquiryStatus(
            decodeURIComponent(adminSupportStatusMatch[1]),
            status
        );
        if (!updated) {
            sendHttpError(res, 404, 'inquiry_not_found', 'Support inquiry was not found');
            return;
        }
        const channel = url.searchParams.get('channel') || 'all';
        const filter = url.searchParams.get('filter') || 'all';
        sendRedirect(
            res,
            `/admin/support?${new URLSearchParams({ channel, status: filter }).toString()}`
        );
        return;
    }

    if (req.method === 'POST' && adminSupportApiStatusMatch) {
        if (!requireAdmin(req, res)) return;
        const body = await readJsonRequest(req, res);
        if (!body) return;
        const updated = await updateSupportInquiryStatus(
            decodeURIComponent(adminSupportApiStatusMatch[1]),
            body.status
        );
        if (!updated) {
            sendHttpError(res, 404, 'inquiry_not_found', 'Support inquiry was not found');
            return;
        }
        sendJson(res, 200, updated);
        return;
    }

    if (req.method === 'GET' && (pathname === '/admin/announcements' || pathname === '/admin/announcements/')) {
        if (!requireAdmin(req, res)) return;
        const channel = url.searchParams.get('channel') || 'all';
        const status = url.searchParams.get('status') || 'all';
        const validChannel = channel === 'all' || ANNOUNCEMENT_CHANNELS.has(channel);
        const validStatus = status === 'all' || ANNOUNCEMENT_STATUSES.has(status);
        const announcements = await listAnnouncements(
            validChannel ? channel : 'all',
            validStatus ? status : 'all'
        );
        sendHtml(res, 200, renderAdminAnnouncementsPage(announcements, {
            channel: validChannel ? channel : 'all',
            status: validStatus ? status : 'all',
        }));
        return;
    }

    if (req.method === 'GET' && pathname === '/admin/api/announcements') {
        if (!requireAdmin(req, res)) return;
        const channel = url.searchParams.get('channel') || 'all';
        const status = url.searchParams.get('status') || 'all';
        sendJson(res, 200, {
            storage: storageMode(),
            announcements: await listAnnouncements(
                channel === 'all' || ANNOUNCEMENT_CHANNELS.has(channel) ? channel : 'all',
                status === 'all' || ANNOUNCEMENT_STATUSES.has(status) ? status : 'all'
            ),
        });
        return;
    }

    if (req.method === 'POST' && pathname === '/admin/api/announcements') {
        if (!requireAdmin(req, res)) return;
        const body = await readJsonRequest(req, res);
        if (!body) return;
        const announcement = announcementFromInput(body);
        if (!announcement) {
            sendHttpError(res, 400, 'invalid_announcement', 'Announcement title or body is invalid');
            return;
        }
        sendJson(res, 201, await createAnnouncement(announcement));
        return;
    }

    if (req.method === 'POST' && adminAnnouncementStatusMatch) {
        if (!requireAdmin(req, res)) return;
        const body = await readJsonRequest(req, res);
        if (!body) return;
        const updated = await updateAnnouncementStatus(
            decodeURIComponent(adminAnnouncementStatusMatch[1]),
            body.status
        );
        if (!updated) {
            sendHttpError(res, 404, 'announcement_not_found', 'Announcement was not found');
            return;
        }
        sendJson(res, 200, updated);
        return;
    }

    if (req.method === 'GET' && pathname === '/announcements') {
        if (!requireHttpAppChannel(req, res)) return;
        const limit = parseBoundedInt(url.searchParams.get('limit'), 30, 1, 100);
        sendJson(res, 200, {
            announcements: await publicAnnouncements(requestAppChannel(req), limit),
        });
        return;
    }

    if (req.method === 'GET' && announcementMatch) {
        if (!requireHttpAppChannel(req, res)) return;
        const announcement = await findPublicAnnouncement(
            decodeURIComponent(announcementMatch[1]),
            requestAppChannel(req)
        );
        if (!announcement) {
            sendHttpError(res, 404, 'announcement_not_found', 'Announcement was not found');
            return;
        }
        sendJson(res, 200, announcement);
        return;
    }

    if (req.method === 'GET' && pathname === '/health') {
        const deployment = deploymentSnapshot();
        sendJson(res, 200, {
            ok: true,
            ready: deployment.ready,
            acceptingConnections: deployment.acceptingConnections,
            service: 'beerock-signaling-server',
            version: packageJson.version || '1.0.0',
            channel: SERVER_CHANNEL,
            poolId: SERVER_POOL_ID,
            rulesetVersion: RULESET_VERSION || null,
            uptimeSec: Math.floor(process.uptime()),
            storage: storageMode(),
            authEnabled: AUTH_ENABLED,
            rooms: Object.keys(rooms).length,
            players: statsPool ? null : statsPlayers.size,
            deployment,
            backpressure: {
                droppedStatePackets: backpressureDroppedStatePackets,
                closedConnections: backpressureClosedConnections,
            },
            operations: operationsSnapshot(),
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

    if (['/matches/result', '/matches/pvp-result', '/analytics/events', '/support/inquiries', '/announcements'].includes(pathname) ||
        pathname === '/health' ||
        pathname === '/capacity' ||
        pathname === '/rankings' ||
        pathname === '/auth/guest/register' ||
        pathname === '/auth/token' ||
        pathname === '/auth/ws-ticket' ||
        pathname === '/admin' ||
        pathname === '/admin/' ||
        pathname === '/admin/api/stats' ||
        pathname === '/admin/api/deployment/drain' ||
        pathname === '/admin/support' ||
        pathname === '/admin/support/' ||
        pathname === '/admin/api/support' ||
        pathname === '/admin/announcements' ||
        pathname === '/admin/announcements/' ||
        pathname === '/admin/api/announcements' ||
        adminSupportStatusMatch ||
        adminSupportApiStatusMatch ||
        adminAnnouncementStatusMatch ||
        announcementMatch ||
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

function outcomeScore(outcome) {
    if (outcome === 'win') return 1;
    if (outcome === 'loss') return 0;
    return 0.5;
}

function rankKFactor(matches) {
    return matches < RANK_PLACEMENT_MATCHES ? RANK_PLACEMENT_K : RANK_ESTABLISHED_K;
}

function ratingDelta(outcome, mode, rating = 1000, opponentRating = 1000, matches = RANK_PLACEMENT_MATCHES) {
    if (mode !== 'multi') {
        if (outcome === 'win') return 16;
        if (outcome === 'loss') return -12;
        return 2;
    }
    const expected = 1 / (1 + (10 ** ((opponentRating - rating) / RANK_ELO_SPREAD)));
    return Math.round(rankKFactor(matches) * (outcomeScore(outcome) - expected));
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
    const delta = Number.isInteger(metadata.ratingDelta)
        ? metadata.ratingDelta
        : ratingDelta(outcome, player.mode);
    player.rating = Math.max(0, player.rating + delta);
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

function applyPvpPlayerResults(local, remote, localOutcome, localMetadata, remoteMetadata) {
    const remoteOutcome = reverseOutcome(localOutcome);
    const localBefore = local.rating;
    const remoteBefore = remote.rating;
    const localDelta = ratingDelta(
        localOutcome,
        'multi',
        localBefore,
        remoteBefore,
        playerMatchCount(local)
    );
    const remoteDelta = ratingDelta(
        remoteOutcome,
        'multi',
        remoteBefore,
        localBefore,
        playerMatchCount(remote)
    );
    applyPlayerResult(local, localOutcome, { ...localMetadata, ratingDelta: localDelta });
    applyPlayerResult(remote, remoteOutcome, { ...remoteMetadata, ratingDelta: remoteDelta });
    return {
        local: {
            ratingBefore: localBefore,
            ratingDelta: local.rating - localBefore,
        },
        remote: {
            ratingBefore: remoteBefore,
            ratingDelta: remote.rating - remoteBefore,
        },
    };
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

function pvpPlayerSummary(player, rewardCoins, ratingChange = {}) {
    return {
        nickname: player.nickname,
        playerId: player.playerId || null,
        ratingBefore: Number.isInteger(ratingChange.ratingBefore)
            ? ratingChange.ratingBefore
            : player.rating,
        ratingDelta: Number.isInteger(ratingChange.ratingDelta) ? ratingChange.ratingDelta : 0,
        rating: player.rating,
        wins: player.wins,
        losses: player.losses,
        draws: player.draws,
        matches: playerMatchCount(player),
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

function supportInquiryId() {
    return `sup_${crypto.randomBytes(12).toString('base64url')}`;
}

function supportInquiryFromRow(row) {
    return {
        id: row.public_id,
        status: normalizeSupportStatus(row.status, 'open'),
        category: row.category,
        message: row.message,
        replyEmail: row.reply_email || null,
        playerId: row.player_id,
        playerIdHash: row.player_id_hash,
        appVersionName: row.app_version_name,
        appVersionCode: row.app_version_code,
        buildType: row.build_type,
        analyticsChannel: normalizeAnalyticsChannel(row.analytics_channel, row.build_type),
        countryCode: row.country_code,
        userAgent: row.user_agent,
        createdAt: new Date(row.created_at).toISOString(),
        updatedAt: new Date(row.updated_at).toISOString(),
        closedAt: row.closed_at ? new Date(row.closed_at).toISOString() : null,
    };
}

async function createSupportInquiry(input) {
    const publicId = supportInquiryId();
    if (!statsPool) {
        const now = new Date().toISOString();
        const inquiry = {
            id: publicId,
            status: 'open',
            category: input.category,
            message: input.message,
            replyEmail: input.replyEmail,
            playerId: input.playerId,
            playerIdHash: input.playerIdHash,
            appVersionName: input.appVersionName,
            appVersionCode: input.appVersionCode,
            buildType: input.buildType,
            analyticsChannel: input.analyticsChannel,
            countryCode: input.countryCode,
            userAgent: input.userAgent,
            createdAt: now,
            updatedAt: now,
            closedAt: null,
        };
        supportInquiries.set(publicId, inquiry);
        return inquiry;
    }
    const result = await statsPool.query(
        `INSERT INTO br_support_inquiries (
            public_id, category, message, reply_email, player_id, player_id_hash,
            app_version_name, app_version_code, build_type, analytics_channel,
            country_code, user_agent
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
         RETURNING *`,
        [
            publicId,
            input.category,
            input.message,
            input.replyEmail,
            input.playerId,
            input.playerIdHash,
            input.appVersionName,
            input.appVersionCode,
            input.buildType,
            input.analyticsChannel,
            input.countryCode,
            input.userAgent,
        ]
    );
    return supportInquiryFromRow(result.rows[0]);
}

async function supportPlayerStats(playerId) {
    const result = {};
    for (const mode of ['single', 'multi']) {
        const player = statsPool
            ? await postgresFindPlayerByRef(mode, playerId)
            : findPlayerByRef(mode, playerId);
        if (!player) continue;
        const rank = statsPool
            ? await postgresRankForPlayer(mode, player.key)
            : rankForPlayer(mode, player.key);
        result[mode] = playerStatsResponse(player, rank);
    }
    return result;
}

async function supportInquiryList(channel = 'all', status = 'all') {
    const selectedChannel = normalizeAnalyticsChannel(channel, 'unknown') === 'unknown' && channel !== 'unknown'
        ? 'all'
        : channel;
    const selectedStatus = normalizeSupportStatus(status) || 'all';
    let inquiries;
    if (!statsPool) {
        inquiries = Array.from(supportInquiries.values())
            .filter((inquiry) => selectedChannel === 'all' || inquiry.analyticsChannel === selectedChannel)
            .filter((inquiry) => selectedStatus === 'all' || inquiry.status === selectedStatus)
            .sort((left, right) => right.createdAt.localeCompare(left.createdAt))
            .slice(0, 200);
    } else {
        const clauses = [];
        const params = [];
        if (selectedChannel !== 'all') {
            params.push(selectedChannel);
            clauses.push(`analytics_channel = $${params.length}`);
        }
        if (selectedStatus !== 'all') {
            params.push(selectedStatus);
            clauses.push(`status = $${params.length}`);
        }
        const where = clauses.length ? `WHERE ${clauses.join(' AND ')}` : '';
        const rows = await statsPool.query(
            `SELECT * FROM br_support_inquiries ${where} ORDER BY created_at DESC LIMIT 200`,
            params
        );
        inquiries = rows.rows.map(supportInquiryFromRow);
    }
    return Promise.all(inquiries.map(async (inquiry) => ({
        ...inquiry,
        playerStats: await supportPlayerStats(inquiry.playerId),
    })));
}

async function updateSupportInquiryStatus(publicId, status) {
    const safeId = cleanSupportText(publicId, 64);
    const safeStatus = normalizeSupportStatus(status);
    if (!safeId || !safeStatus) return null;
    if (!statsPool) {
        const inquiry = supportInquiries.get(safeId);
        if (!inquiry) return null;
        inquiry.status = safeStatus;
        inquiry.updatedAt = new Date().toISOString();
        inquiry.closedAt = safeStatus === 'closed' ? inquiry.updatedAt : null;
        return inquiry;
    }
    const result = await statsPool.query(
        `UPDATE br_support_inquiries
            SET status = $2,
                updated_at = NOW(),
                closed_at = CASE WHEN $2 = 'closed' THEN NOW() ELSE NULL END
          WHERE public_id = $1
        RETURNING *`,
        [safeId, safeStatus]
    );
    return result.rows[0] ? supportInquiryFromRow(result.rows[0]) : null;
}

async function ensurePostgresPlayer(client, mode, playerInput) {
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
    return identityKey;
}

async function persistPostgresPlayer(client, player) {
    const identityKey = player.key.slice(`${player.mode}:`.length);
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
            player.mode,
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
}

async function upsertPostgresPlayerResult(client, mode, playerInput, outcome, metadata) {
    const identityKey = await ensurePostgresPlayer(client, mode, playerInput);
    const locked = await client.query(
        'SELECT * FROM br_player_stats WHERE mode = $1 AND identity_key = $2 FOR UPDATE',
        [mode, identityKey]
    );
    const player = dbPlayerFromRow(locked.rows[0]);
    applyPlayerResult(player, outcome, metadata);
    await persistPostgresPlayer(client, player);
    return player;
}

async function upsertPostgresPvpResults(
    client,
    localInput,
    remoteInput,
    localOutcome,
    localMetadata,
    remoteMetadata
) {
    const localIdentity = await ensurePostgresPlayer(client, 'multi', localInput);
    const remoteIdentity = await ensurePostgresPlayer(client, 'multi', remoteInput);
    const identities = [localIdentity, remoteIdentity].sort();
    const locked = await client.query(
        `SELECT * FROM br_player_stats
          WHERE mode = 'multi' AND identity_key = ANY($1::text[])
          ORDER BY identity_key
          FOR UPDATE`,
        [identities]
    );
    const playersByIdentity = new Map(
        locked.rows.map((row) => [row.identity_key, dbPlayerFromRow(row)])
    );
    const local = playersByIdentity.get(localIdentity);
    const remote = playersByIdentity.get(remoteIdentity);
    if (!local || !remote) throw new Error('failed to lock PvP player records');
    const ratingChanges = applyPvpPlayerResults(
        local,
        remote,
        localOutcome,
        localMetadata,
        remoteMetadata
    );
    await persistPostgresPlayer(client, local);
    await persistPostgresPlayer(client, remote);
    return { local, remote, ratingChanges };
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

    const ratingChanges = applyPvpPlayerResults(
        local,
        remote,
        outcome,
        {
            durationSec: body.durationSec,
            characterId: body.localPlayer.characterId,
            completedAt,
            rewardCoins: localReward,
        },
        {
            durationSec: body.durationSec,
            characterId: body.remotePlayer.characterId,
            completedAt,
            rewardCoins: remoteReward,
        }
    );

    const matchId = matchRef || makeServerMatchId();
    const record = {
        type: 'pvp',
        matchId,
        finishReason,
        players: {
            [localIdentity]: pvpPlayerSummary(local, localReward, ratingChanges.local),
            [remoteIdentity]: pvpPlayerSummary(remote, remoteReward, ratingChanges.remote),
            local: pvpPlayerSummary(local, localReward, ratingChanges.local),
            remote: pvpPlayerSummary(remote, remoteReward, ratingChanges.remote),
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

        const { local, remote, ratingChanges } = await upsertPostgresPvpResults(
            client,
            body.localPlayer,
            body.remotePlayer,
            outcome,
            {
                durationSec: body.durationSec,
                characterId: body.localPlayer.characterId,
                completedAt,
                rewardCoins: localReward,
            },
            {
                durationSec: body.durationSec,
                characterId: body.remotePlayer.characterId,
                completedAt,
                rewardCoins: remoteReward,
            }
        );
        const record = {
            type: 'pvp',
            matchId,
            finishReason,
            players: {
                [localIdentity]: pvpPlayerSummary(local, localReward, ratingChanges.local),
                [remoteIdentity]: pvpPlayerSummary(remote, remoteReward, ratingChanges.remote),
                local: pvpPlayerSummary(local, localReward, ratingChanges.local),
                remote: pvpPlayerSummary(remote, remoteReward, ratingChanges.remote),
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
        matchMode: room.matchMode || 'friendly',
        rulesetVersion: room.rulesetVersion || null,
        durationSec: result.durationSec,
        integrity: roomIntegritySummary(room),
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
            integrity: roomIntegritySummary(room),
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

function roomIntegritySummary(room) {
    const state = room?.integrityState;
    return {
        audits: state?.audits || 0,
        invalid: state?.invalid || 0,
        hpMismatches: state?.hpMismatches || 0,
        flagged: state?.flagged === true,
    };
}

function resetRoomIntegrity(room) {
    room.integrityReports = {};
    room.integrityState = {
        audits: 0,
        invalid: 0,
        hpMismatches: 0,
        consecutiveHpMismatches: 0,
        flagged: false,
        comparedHostAuditSeq: 0,
        comparedGuestAuditSeq: 0,
    };
}

function finitePacketNumber(msg, field) {
    const value = msg[field];
    return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function flagRoomIntegrity(room, reason) {
    if (room.integrityState.flagged) return;
    room.integrityState.flagged = true;
    integrityMatchesFlagged += 1;
    console.warn(`[integrity] observe-only flag match=${room.matchId || '-'} reason=${reason}`);
}

function recordGameAudit(room, ws, msg) {
    integrityAuditsReceived += 1;
    if (!room?.matchStarted || room.matchMode !== 'ranked' || !['host', 'guest'].includes(ws.role)) return;
    if (!room.integrityState) resetRoomIntegrity(room);
    const state = room.integrityState;
    state.audits += 1;

    const matchId = typeof msg.matchId === 'string' ? msg.matchId : '';
    const auditSeq = packetInt(msg, 'auditSeq');
    const stateSeq = packetInt(msg, 'stateSeq');
    const elapsedSec = packetInt(msg, 'elapsedSec');
    const localHp = packetInt(msg, 'localHp');
    const remoteHp = packetInt(msg, 'remoteHp');
    const x = finitePacketNumber(msg, 'x');
    const y = finitePacketNumber(msg, 'y');
    const previous = room.integrityReports[ws.role];
    const invalid = matchId !== room.matchId ||
        !Number.isInteger(auditSeq) || auditSeq <= 0 ||
        !Number.isInteger(stateSeq) || stateSeq < 0 ||
        !Number.isInteger(elapsedSec) || elapsedSec < 0 || elapsedSec > 900 ||
        !Number.isInteger(localHp) || localHp < 0 || localHp > 1000000 ||
        !Number.isInteger(remoteHp) || remoteHp < 0 || remoteHp > 1000000 ||
        x === null || x < -64 || x > 1344 ||
        y === null || y < -64 || y > 784 ||
        (previous && auditSeq <= previous.auditSeq) ||
        (previous && stateSeq < previous.stateSeq);
    if (invalid) {
        integrityAuditsInvalid += 1;
        state.invalid += 1;
        if (state.invalid >= INTEGRITY_INVALID_FLAG_THRESHOLD) {
            flagRoomIntegrity(room, 'repeated_invalid_audit');
        }
        return;
    }

    room.integrityReports[ws.role] = {
        auditSeq,
        stateSeq,
        elapsedSec,
        localHp,
        remoteHp,
        receivedAtMs: Date.now(),
    };
    const host = room.integrityReports.host;
    const guest = room.integrityReports.guest;
    if (!host || !guest ||
        host.auditSeq === state.comparedHostAuditSeq ||
        guest.auditSeq === state.comparedGuestAuditSeq) return;
    state.comparedHostAuditSeq = host.auditSeq;
    state.comparedGuestAuditSeq = guest.auditSeq;
    if (Math.abs(host.receivedAtMs - guest.receivedAtMs) > INTEGRITY_REPORT_MAX_SKEW_MS) return;

    const hpMismatch = host.localHp !== guest.remoteHp || guest.localHp !== host.remoteHp;
    if (hpMismatch) {
        integrityAuditMismatches += 1;
        state.hpMismatches += 1;
        state.consecutiveHpMismatches += 1;
        if (state.consecutiveHpMismatches >= INTEGRITY_HP_MISMATCH_FLAG_THRESHOLD) {
            flagRoomIntegrity(room, 'repeated_hp_mismatch');
        }
    } else {
        state.consecutiveHpMismatches = 0;
    }
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
    if (record.matchMode !== 'ranked') {
        return { error: { status: 409, code: 'not_ranked_match', message: 'Only ranked matches submit MMR results' } };
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

function battleTransport(value, roomMode) {
    const normalized = typeof value === 'string' ? value.toLowerCase() : '';
    if (normalized === 'p2p' || normalized === 'relay') return normalized;
    return roomMode === 'p2p' ? 'p2p' : 'relay';
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

async function matchmakingRatingForSocket(ws, claimedPlayerId, refresh = false) {
    const now = Date.now();
    if (!refresh && Number.isFinite(ws.matchmakingRating) &&
        now - (ws.matchmakingRatingLoadedAt || 0) < 30000) {
        return ws.matchmakingRating;
    }
    const playerId = normalizePlayerId(ws.authPlayerId) || normalizePlayerId(claimedPlayerId);
    let player = null;
    try {
        player = statsPool
            ? await postgresFindPlayerByRef('multi', playerId)
            : findPlayerByRef('multi', playerId);
    } catch (error) {
        console.warn(`[matchmaking] MMR lookup failed: ${error?.message || 'unknown error'}`);
    }
    ws.matchmakingRating = Number.isFinite(player?.rating) ? player.rating : 1000;
    ws.matchmakingRatingLoadedAt = now;
    return ws.matchmakingRating;
}

function playerIdForSocket(ws, claimedPlayerId) {
    return normalizePlayerId(ws?.authPlayerId) || normalizePlayerId(claimedPlayerId);
}

function roomOwnedByViewer(room, viewer) {
    if (!room || !viewer) return false;
    if (room.host === viewer) return true;
    const viewerPlayerId = playerIdForSocket(viewer, viewer.matchmakingPlayerId);
    const hostPlayerId = normalizePlayerId(room.hostPlayerId);
    return Boolean(viewerPlayerId && hostPlayerId && viewerPlayerId === hostPlayerId);
}

function removeDuplicateWaitingRoomsForPlayer(playerId, currentSocket) {
    const normalizedPlayerId = normalizePlayerId(playerId);
    if (!normalizedPlayerId) return 0;
    let removed = 0;
    for (const [code, room] of Object.entries(rooms)) {
        if (!room || room.host === currentSocket || room.matchStarted) continue;
        if (room.guest?.readyState === WebSocket.OPEN) continue;
        if (normalizePlayerId(room.hostPlayerId) !== normalizedPlayerId) continue;

        const previousHost = room.host;
        if (previousHost) {
            previousHost.roomCode = null;
            previousHost.role = null;
            send(previousHost, {
                type: 'room_left',
                code,
                reason: 'replaced_by_new_connection',
            });
        }
        delete rooms[code];
        broadcastRoomRemoved(code);
        removed += 1;
        console.log(`[-] Replaced duplicate waiting room: ${code}`);
    }
    return removed;
}

function roomListEntry(code, room, viewer, viewerRating = 1000) {
    if (!room || room.host?.readyState !== WebSocket.OPEN ||
        (room.guest && room.guest.readyState === WebSocket.OPEN)) {
        return null;
    }
    if (roomOwnedByViewer(room, viewer)) {
        return null;
    }
    if (room.networkMode === 'relay' && !relayAvailabilitySnapshot().canStartNewMatch) {
        return null;
    }
    const ranked = room.matchMode === 'ranked';
    return {
        code,
        ...roomQuality(room.hostRttMs, socketRttMs(viewer)),
        hostCharacterId: room.hostCharacterId,
        arenaId: room.arenaId,
        battleType: room.battleType,
        matchMode: room.matchMode || 'friendly',
        networkMode: room.networkMode || 'relay',
        region: room.hostRegion || null,
        relayRegion: SERVER_POOL_ID,
        ...(ranked ? {
            ratingDifference: Math.abs((Number(room.hostRating) || 1000) - viewerRating),
            waitingMs: Math.max(0, Date.now() - room.createdAt),
        } : {}),
    };
}

function roomListSnapshot(viewer, requestedMatchMode = 'friendly', viewerRating = 1000) {
    return Object.keys(rooms)
        .map((code) => roomListEntry(code, rooms[code], viewer, viewerRating))
        .filter((entry) => entry && entry.matchMode === requestedMatchMode);
}

function broadcastRoomUpsert(code) {
    const room = rooms[code];
    wss.clients.forEach((client) => {
        if (!client.roomListSubscribed || client.readyState !== WebSocket.OPEN) return;
        if ((client.roomListMatchMode || 'friendly') !== (room.matchMode || 'friendly')) return;
        const entry = roomListEntry(code, room, client, client.matchmakingRating || 1000);
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
    room.activeTransport = null;
    room.matchId = null;
    room.finalResult = null;
    room.battleStartAtMs = null;
    room.matchStartedAtMs = null;
}

function sendRelayAdmissionError(ws, relayStatus) {
    relayAdmissionRejections += 1;
    send(ws, {
        type: 'error',
        code: relayStatus.code,
        message: relayStatus.message,
        retryAfterSec: CAPACITY_RETRY_AFTER_SEC,
        relay: relayStatus,
    });
}

function rejectRoomRelayStart(code, room, relayStatus) {
    relayAdmissionRejections += 1;
    const packet = {
        type: 'error',
        code: relayStatus.code,
        message: relayStatus.message,
        retryAfterSec: CAPACITY_RETRY_AFTER_SEC,
        relay: relayStatus,
    };
    for (const participant of [room.host, room.guest]) {
        send(participant, packet);
        if (participant) {
            participant.roomCode = null;
            participant.role = null;
        }
    }
    delete rooms[code];
    broadcastRoomRemoved(code);
    console.log(`[!] Relay match rejected: ${code} (${relayStatus.code})`);
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
            battleType: room.battleType,
            matchMode: room.matchMode,
            debugNoKo: room.debugNoKo,
            debugNoTime: room.debugNoTime,
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
    ws.authPlayerId = normalizePlayerId(req.authPrincipal?.sub);
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

    ws.on('message', async (raw) => {
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
                const requestedMatchMode = matchMode(msg.matchMode);
                const requestedNetworkMode = networkMode(msg.networkMode);
                if (requestedNetworkMode === 'relay') {
                    const relayStatus = relayAvailabilitySnapshot();
                    if (!relayStatus.canStartNewMatch) {
                        sendRelayAdmissionError(ws, relayStatus);
                        return;
                    }
                }
                if (ws.roomCode) {
                    send(ws, { type: 'error', code: 'already_in_room', message: 'Already in a room' });
                    return;
                }
                const hostPlayerId = playerIdForSocket(ws, msg.hostPlayerId);
                removeDuplicateWaitingRoomsForPlayer(hostPlayerId, ws);
                const capacity = capacitySnapshot({ connectionExtra: 0 });
                if (!capacity.canCreateRoom) {
                    sendCapacityWsError(ws, capacity);
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
                    arenaId: requestedMatchMode === 'ranked'
                        ? randomRankedArenaId()
                        : enumToken(msg.arenaId),
                    battleType: requestedMatchMode === 'ranked'
                        ? RANKED_BATTLE_TYPE
                        : battleType(msg.battleType),
                    debugNoKo: msg.debugNoKo === true,
                    debugNoTime: msg.debugNoTime === true,
                    matchMode: requestedMatchMode,
                    rulesetVersion: packetInt(msg, 'rulesetVersion'),
                    hostNickname: typeof msg.hostNickname === 'string' ? msg.hostNickname : undefined,
                    hostPlayerId: hostPlayerId || undefined,
                    hostRating: requestedMatchMode === 'ranked'
                        ? await matchmakingRatingForSocket(ws, msg.hostPlayerId)
                        : null,
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
                    networkMode: requestedNetworkMode,
                    activeTransport: null,
                    matchStarted: false,
                    matchId: null,
                    matchSequence: 0,
                    finalResult: null,
                    battleStartAtMs: null,
                    matchStartedAtMs: null,
                };
                ws.roomCode = code;
                ws.role = 'host';
                ws.matchmakingPlayerId = hostPlayerId;

                send(ws, {
                    type: 'room_created',
                    code,
                    networkMode: rooms[code].networkMode,
                    arenaId: rooms[code].arenaId,
                    battleType: rooms[code].battleType,
                    debugNoKo: rooms[code].debugNoKo,
                    debugNoTime: rooms[code].debugNoTime,
                    matchMode: rooms[code].matchMode,
                });
                broadcastRoomUpsert(code);
                console.log(`[+] Room created: ${code}`);
                break;
            }

            // ── 방 참가 ──────────────────────────────────────────────────
            case 'join_room':
            case 'join_ranked_room': {
                const capacity = capacitySnapshot({ connectionExtra: 0 });
                if (!capacity.canJoinRoom) {
                    sendCapacityWsError(ws, capacity);
                    return;
                }
                const code = msg.code;
                if (!validateJoinCode(code)) {
                    send(ws, { type: 'error', code: 'invalid_room_code', message: 'Invalid room code' });
                    return;
                }
                if (ws.roomCode) {
                    send(ws, { type: 'error', code: 'already_in_room', message: 'Already in a room' });
                    return;
                }
                const room = rooms[code];

                if (!room) {
                    send(ws, { type: 'error', code: 'room_not_found', message: 'Room not found' });
                    return;
                }
                if (room.matchMode === 'ranked' && msg.type !== 'join_ranked_room') {
                    send(ws, {
                        type: 'error',
                        code: 'ranked_auto_match_only',
                        message: 'Ranked matches can only be joined through ranked matchmaking',
                    });
                    return;
                }
                if (room.matchMode !== 'ranked' && msg.type === 'join_ranked_room') {
                    send(ws, {
                        type: 'error',
                        code: 'ranked_room_unavailable',
                        message: 'Ranked room is no longer available',
                    });
                    return;
                }
                const guestPlayerId = playerIdForSocket(ws, msg.guestPlayerId);
                const hostPlayerId = normalizePlayerId(room.hostPlayerId);
                if (room.host === ws || (guestPlayerId && hostPlayerId && guestPlayerId === hostPlayerId)) {
                    send(ws, {
                        type: 'error',
                        code: 'self_join_not_allowed',
                        message: 'Cannot join a room owned by the same player',
                    });
                    return;
                }
                if (room.guest && room.guest.readyState === WebSocket.OPEN) {
                    send(ws, { type: 'error', code: 'room_full', message: 'Room is full' });
                    return;
                }
                if (room.networkMode === 'relay') {
                    const relayStatus = relayAvailabilitySnapshot();
                    if (!relayStatus.canStartNewMatch) {
                        sendRelayAdmissionError(ws, relayStatus);
                        return;
                    }
                }

                room.guest = ws;
                room.guestCharacterId = enumToken(msg.guestCharacterId);
                room.guestPassiveId = enumToken(msg.guestPassiveId);
                room.guestArenaId = enumToken(msg.arenaId);
                room.guestNickname = typeof msg.guestNickname === 'string' ? msg.guestNickname : undefined;
                room.guestPlayerId = guestPlayerId || undefined;
                room.guestVersionCode = packetInt(msg, 'clientVersionCode');
                room.guestVersionName = typeof msg.clientVersionName === 'string' ? msg.clientVersionName : undefined;
                room.guestAnalyticsChannel = normalizeAnalyticsChannel(msg.analyticsChannel);
                room.guestCountryCode = ws.analyticsCountryCode;
                room.guestUserAgent = ws.analyticsUserAgent;
                ws.roomCode = code;
                ws.role = 'guest';
                ws.matchmakingPlayerId = guestPlayerId;

                // 양쪽에게 준비 알림
                send(ws, {
                    type: 'room_joined',
                    code,
                    networkMode: room.networkMode || 'relay',
                    hostCharacterId: room.hostCharacterId,
                    hostPassiveId: room.hostPassiveId,
                    arenaId: room.arenaId,
                    battleType: room.battleType,
                    debugNoKo: room.debugNoKo,
                    debugNoTime: room.debugNoTime,
                    matchMode: room.matchMode,
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
                    debugNoKo: room.debugNoKo,
                    debugNoTime: room.debugNoTime,
                    matchMode: room.matchMode,
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
                    if (room.matchMode !== 'ranked') {
                        room.arenaId = enumToken(msg.arenaId) || room.arenaId;
                    }
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
                ws.roomListMatchMode = matchMode(msg.matchMode);
                ws.matchmakingPlayerId = playerIdForSocket(ws, msg.playerId);
                const viewerRating = ws.roomListMatchMode === 'ranked'
                    ? await matchmakingRatingForSocket(ws, msg.playerId, true)
                    : 1000;
                send(ws, {
                    type: 'room_list',
                    matchMode: ws.roomListMatchMode,
                    rooms: roomListSnapshot(ws, ws.roomListMatchMode, viewerRating),
                });
                break;
            }

            // ── 게임 패킷 릴레이 ─────────────────────────────────────────
            case 'game_over': {
                const code = ws.roomCode;
                const room = rooms[code];
                if (!room) return;

                const peer = ws.role === 'host' ? room.guest : room.host;
                send(peer, { ...msg, matchId: room.matchId || null }, { relay: true });
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
            case 'game_resume':
            case 'game_audit': {
                const code = ws.roomCode;
                const room = rooms[code];
                if (!room) return;

                if (msg.type === 'game_start') {
                    if (ws.role !== 'host') return;
                    const requestedTransport = room.matchStarted
                        ? (room.activeTransport || battleTransport(msg.activeTransport, room.networkMode))
                        : battleTransport(msg.activeTransport, room.networkMode);
                    if (!room.matchStarted && requestedTransport === 'relay') {
                        const relayStatus = relayAvailabilitySnapshot();
                        if (!relayStatus.canStartNewMatch) {
                            rejectRoomRelayStart(code, room, relayStatus);
                            return;
                        }
                    }
                    if (!room.matchStarted) {
                        room.matchStarted = true;
                        room.activeTransport = requestedTransport;
                        room.matchSequence = (room.matchSequence || 0) + 1;
                        room.matchId = makeMatchId();
                        room.finalResult = null;
                        room.battleStartAtMs = null;
                        room.matchStartedAtMs = Date.now();
                        resetRoomIntegrity(room);
                    }
                    room.hostCharacterId = enumToken(msg.hostCharacterId) || room.hostCharacterId;
                    room.hostPassiveId = enumToken(msg.hostPassiveId) || room.hostPassiveId;
                    room.hostNickname = typeof msg.hostNickname === 'string' ? msg.hostNickname : room.hostNickname;
                    room.hostPlayerId = typeof msg.hostPlayerId === 'string' ? msg.hostPlayerId : room.hostPlayerId;
                    room.hostVersionCode = packetInt(msg, 'clientVersionCode') || room.hostVersionCode;
                    room.hostVersionName = typeof msg.clientVersionName === 'string' ? msg.clientVersionName : room.hostVersionName;
                    room.hostAnalyticsChannel = normalizeAnalyticsChannel(msg.analyticsChannel || room.hostAnalyticsChannel);
                    room.arenaId = room.matchMode === 'ranked'
                        ? room.arenaId
                        : (enumToken(msg.arenaId) || room.arenaId);
                    room.battleType = room.matchMode === 'ranked'
                        ? RANKED_BATTLE_TYPE
                        : room.battleType;
                    const gameStartPacket = {
                        ...msg,
                        arenaId: room.arenaId,
                        battleType: room.battleType,
                        debugNoKo: room.debugNoKo,
                        debugNoTime: room.debugNoTime,
                        matchMode: room.matchMode || 'friendly',
                        activeTransport: requestedTransport,
                        matchId: room.matchId,
                        matchSequence: room.matchSequence,
                    };
                    const peer = ws.role === 'host' ? room.guest : room.host;
                    send(peer, gameStartPacket, { relay: true });
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
                if (msg.type === 'game_audit') {
                    recordGameAudit(room, ws, msg);
                    break;
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
                if (msg.type === 'game_state' && room.matchStarted && room.activeTransport === 'p2p') {
                    room.activeTransport = 'relay';
                    relayRuntimeFallbacks += 1;
                }
                const peer = ws.role === 'host' ? room.guest : room.host;
                send(peer, msg, { relay: true });
                if (msg.type === 'game_ready') {
                    sendCountdownSync(room);
                }
                break;
            }

            default:
                break;
        }
    });

    ws.on('close', (closeCode, closeReason) => {
        recordWebSocketDisconnect(ws, closeCode, closeReason);
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
            websocketHeartbeatTimeouts += 1;
            ws.serverTerminationSource = 'heartbeat_timeout';
            ws.terminate();
            return;
        }
        ws.isAlive = false;
        try {
            ws.ping();
        } catch {
            websocketPingFailures += 1;
            ws.serverTerminationSource = 'ping_failure';
            ws.terminate();
        }
    });
}, HEARTBEAT_INTERVAL_MS);

let eventLoopExpectedAt = Date.now() + 1000;
const eventLoopLagInterval = setInterval(() => {
    const now = Date.now();
    eventLoopLagLatestMs = Math.max(0, now - eventLoopExpectedAt);
    eventLoopLagSamples.push(eventLoopLagLatestMs);
    if (eventLoopLagSamples.length > 60) eventLoopLagSamples.shift();
    eventLoopExpectedAt = now + 1000;
}, 1000);

wss.on('close', () => {
    clearInterval(heartbeatInterval);
    clearInterval(eventLoopLagInterval);
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
