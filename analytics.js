const crypto = require('crypto');

const EXTERNAL_EVENT_NAMES = new Set([
    'app_launch',
    'single_match_complete',
    'screen_view',
    'feature_use',
]);
const INTERNAL_EVENT_NAMES = new Set([...EXTERNAL_EVENT_NAMES, 'multi_match_complete']);
const ANALYTICS_CHANNELS = new Set(['dev', 'beta', 'production', 'mixed', 'unknown']);
const ANALYTICS_FILTERS = new Set(['all', ...ANALYTICS_CHANNELS]);

function cleanText(value, maxLength = 160, fallback = '') {
    if (typeof value !== 'string') return fallback;
    const cleaned = value.trim().replace(/[\u0000-\u001F\u007F]/g, ' ');
    return cleaned ? cleaned.slice(0, maxLength) : fallback;
}

function cleanInteger(value, fallback = null) {
    return Number.isInteger(value) ? value : fallback;
}

function cleanCountry(value) {
    const country = cleanText(value, 8).toUpperCase();
    return /^[A-Z]{2}$/.test(country) ? country : 'ZZ';
}

function normalizeAnalyticsChannel(value, buildType = 'unknown') {
    const channel = cleanText(value, 24).toLowerCase();
    if (ANALYTICS_CHANNELS.has(channel)) return channel;
    const normalizedBuildType = cleanText(buildType, 24).toLowerCase();
    if (normalizedBuildType === 'debug') return 'dev';
    if (normalizedBuildType === 'release') return 'production';
    return 'unknown';
}

function normalizeAnalyticsFilter(value) {
    const channel = cleanText(value, 24, 'all').toLowerCase();
    return ANALYTICS_FILTERS.has(channel) ? channel : 'all';
}

function requestCountry(req, fallback) {
    const candidates = [
        req?.headers?.['cf-ipcountry'],
        req?.headers?.['x-vercel-ip-country'],
        req?.headers?.['x-country-code'],
        fallback,
    ];
    for (const candidate of candidates) {
        const country = cleanCountry(candidate);
        if (country !== 'ZZ') return country;
    }
    return 'ZZ';
}

function safeUserAgent(req) {
    return cleanText(req?.headers?.['user-agent'], 200, 'unknown');
}

function hashIdentifier(value) {
    const normalized = cleanText(value, 128);
    if (!normalized) return null;
    return crypto.createHash('sha256').update(normalized).digest('hex').slice(0, 16);
}

function pairHash(first, second) {
    const participants = [hashIdentifier(first), hashIdentifier(second)].filter(Boolean).sort();
    if (participants.length !== 2 || participants[0] === participants[1]) return null;
    return crypto.createHash('sha256').update(participants.join(':')).digest('hex').slice(0, 16);
}

function sanitizeProperties(eventName, value) {
    const source = value && typeof value === 'object' && !Array.isArray(value) ? value : {};
    if (eventName === 'app_launch') {
        return {
            osVersion: cleanText(source.osVersion, 40) || null,
            deviceModel: cleanText(source.deviceModel, 80) || null,
        };
    }
    if (eventName === 'single_match_complete') {
        return {
            outcome: cleanText(source.outcome, 16) || null,
            finishReason: cleanText(source.finishReason, 32) || null,
            durationSec: cleanInteger(source.durationSec),
            characterId: cleanText(source.characterId, 40) || null,
            passiveId: cleanText(source.passiveId, 40) || null,
            arenaId: cleanText(source.arenaId, 40) || null,
            battleType: cleanText(source.battleType, 24) || null,
        };
    }
    if (eventName === 'screen_view') {
        return { screen: cleanText(source.screen, 48) || null };
    }
    if (eventName === 'feature_use') {
        return { feature: cleanText(source.feature, 64) || null };
    }
    return source;
}

function eventFromHttpRequest(req, body) {
    const eventName = cleanText(body?.eventName, 48);
    if (!EXTERNAL_EVENT_NAMES.has(eventName)) return null;
    const buildType = cleanText(body.buildType, 24, 'unknown');
    return {
        eventId: cleanText(body.eventId, 128),
        eventName,
        occurredAt: new Date().toISOString(),
        playerIdHash: hashIdentifier(body.playerId),
        appVersionName: cleanText(body.appVersionName, 40, 'unknown'),
        appVersionCode: cleanInteger(body.appVersionCode),
        buildType,
        analyticsChannel: normalizeAnalyticsChannel(body.analyticsChannel, buildType),
        platform: cleanText(body.platform, 24, 'android'),
        userAgent: safeUserAgent(req),
        countryCode: requestCountry(req, body.countryCode),
        properties: sanitizeProperties(eventName, body.properties),
    };
}

function normalizedEvent(event) {
    const eventName = cleanText(event?.eventName, 48);
    if (!INTERNAL_EVENT_NAMES.has(eventName)) return null;
    const occurredAt = new Date(event.occurredAt || Date.now());
    if (Number.isNaN(occurredAt.getTime())) return null;
    const buildType = cleanText(event.buildType, 24, 'unknown');
    return {
        eventId: cleanText(event.eventId, 128),
        eventName,
        occurredAt: occurredAt.toISOString(),
        playerIdHash: cleanText(event.playerIdHash, 32) || null,
        appVersionName: cleanText(event.appVersionName, 40, 'unknown'),
        appVersionCode: cleanInteger(event.appVersionCode),
        buildType,
        analyticsChannel: normalizeAnalyticsChannel(event.analyticsChannel, buildType),
        platform: cleanText(event.platform, 24, 'server'),
        userAgent: cleanText(event.userAgent, 200, 'unknown'),
        countryCode: cleanCountry(event.countryCode),
        properties: event.properties && typeof event.properties === 'object' ? event.properties : {},
    };
}

class AnalyticsStore {
    constructor(pool, options = {}) {
        this.pool = pool;
        this.retentionDays = Math.max(7, Number(options.retentionDays || 90));
        this.maxMemoryEvents = Math.max(1000, Number(options.maxMemoryEvents || 50000));
        this.memoryEvents = [];
        this.memoryEventIds = new Set();
    }

    async initialize() {
        if (!this.pool) return;
        await this.pool.query(`
            CREATE TABLE IF NOT EXISTS br_analytics_events (
                id BIGSERIAL PRIMARY KEY,
                event_id TEXT UNIQUE,
                event_name TEXT NOT NULL,
                occurred_at TIMESTAMPTZ NOT NULL,
                player_id_hash TEXT,
                app_version_name TEXT NOT NULL,
                app_version_code INTEGER,
                build_type TEXT NOT NULL,
                analytics_channel TEXT NOT NULL DEFAULT 'unknown',
                platform TEXT NOT NULL,
                user_agent TEXT NOT NULL,
                country_code TEXT NOT NULL,
                properties JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        `);
        await this.pool.query(`
            ALTER TABLE br_analytics_events
            ADD COLUMN IF NOT EXISTS analytics_channel TEXT NOT NULL DEFAULT 'unknown'
        `);
        await this.pool.query(`
            UPDATE br_analytics_events
               SET analytics_channel = CASE
                   WHEN build_type = 'debug' THEN 'dev'
                   WHEN build_type = 'release' THEN 'production'
                   ELSE 'unknown'
               END
             WHERE analytics_channel = 'unknown'
        `);
        await this.pool.query(`
            CREATE INDEX IF NOT EXISTS br_analytics_events_time_idx
                ON br_analytics_events (occurred_at DESC)
        `);
        await this.pool.query(`
            CREATE INDEX IF NOT EXISTS br_analytics_events_name_time_idx
                ON br_analytics_events (event_name, occurred_at DESC)
        `);
        await this.pool.query(`
            CREATE INDEX IF NOT EXISTS br_analytics_events_channel_time_idx
                ON br_analytics_events (analytics_channel, occurred_at DESC)
        `);
        await this.pool.query(
            `DELETE FROM br_analytics_events
              WHERE occurred_at < NOW() - ($1::text || ' days')::interval`,
            [String(this.retentionDays)]
        );
    }

    async record(rawEvent) {
        const event = normalizedEvent(rawEvent);
        if (!event || !event.eventId) return { accepted: false, reason: 'invalid_event' };

        if (!this.pool) {
            if (this.memoryEventIds.has(event.eventId)) return { accepted: true, duplicate: true };
            this.memoryEventIds.add(event.eventId);
            this.memoryEvents.push(event);
            while (this.memoryEvents.length > this.maxMemoryEvents) {
                const removed = this.memoryEvents.shift();
                if (removed) this.memoryEventIds.delete(removed.eventId);
            }
            return { accepted: true, duplicate: false };
        }

        const inserted = await this.pool.query(
            `INSERT INTO br_analytics_events (
                event_id, event_name, occurred_at, player_id_hash,
                app_version_name, app_version_code, build_type, analytics_channel,
                platform, user_agent, country_code, properties
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb)
             ON CONFLICT (event_id) DO NOTHING
             RETURNING id`,
            [
                event.eventId,
                event.eventName,
                event.occurredAt,
                event.playerIdHash,
                event.appVersionName,
                event.appVersionCode,
                event.buildType,
                event.analyticsChannel,
                event.platform,
                event.userAgent,
                event.countryCode,
                JSON.stringify(event.properties),
            ]
        );
        return { accepted: true, duplicate: inserted.rowCount === 0 };
    }

    async recentEvents(channel = 'all') {
        const selectedChannel = normalizeAnalyticsFilter(channel);
        if (!this.pool) {
            return selectedChannel === 'all'
                ? [...this.memoryEvents]
                : this.memoryEvents.filter((event) => event.analyticsChannel === selectedChannel);
        }
        const channelClause = selectedChannel === 'all' ? '' : 'AND analytics_channel = $2';
        const params = selectedChannel === 'all'
            ? [String(this.retentionDays)]
            : [String(this.retentionDays), selectedChannel];
        const result = await this.pool.query(
            `SELECT event_id, event_name, occurred_at, player_id_hash,
                    app_version_name, app_version_code, build_type, analytics_channel, platform,
                    user_agent, country_code, properties
               FROM br_analytics_events
              WHERE occurred_at >= NOW() - ($1::text || ' days')::interval
                ${channelClause}
              ORDER BY occurred_at DESC
              LIMIT 100000`,
            params
        );
        return result.rows.map((row) => ({
            eventId: row.event_id,
            eventName: row.event_name,
            occurredAt: new Date(row.occurred_at).toISOString(),
            playerIdHash: row.player_id_hash,
            appVersionName: row.app_version_name,
            appVersionCode: row.app_version_code,
            buildType: row.build_type,
            analyticsChannel: normalizeAnalyticsChannel(row.analytics_channel, row.build_type),
            platform: row.platform,
            userAgent: row.user_agent,
            countryCode: row.country_code,
            properties: row.properties || {},
        }));
    }

    async snapshot(runtime = {}, channel = 'all') {
        const selectedChannel = normalizeAnalyticsFilter(channel);
        return aggregateEvents(await this.recentEvents(selectedChannel), {
            retentionDays: this.retentionDays,
            selectedChannel,
            ...runtime,
        });
    }
}

function emptyCounts() {
    return { launches: 0, uniqueUsers: 0, singleMatches: 0, multiMatches: 0, totalMatches: 0 };
}

function aggregateEvents(events, runtime) {
    const now = Date.now();
    const windows = [
        { key: 'today', label: '오늘', since: startOfKstDay(now) },
        { key: 'days7', label: '7일', since: now - 7 * 86400000 },
        { key: 'days30', label: '30일', since: now - 30 * 86400000 },
        { key: 'retention', label: `${runtime.retentionDays}일`, since: now - runtime.retentionDays * 86400000 },
    ];
    const periods = Object.fromEntries(windows.map((window) => [window.key, { ...emptyCounts(), users: new Set() }]));
    const versions = new Map();
    const countries = new Map();
    const userAgents = new Map();
    const daily = new Map();
    const reasons = new Map();
    const pairs = new Map();
    const screenViews = new Map();
    const featureUsage = new Map();

    const incrementVersion = (name, code, field) => {
        const key = `${name || 'unknown'} (${code ?? '-'})`;
        const row = versions.get(key) || { version: key, launches: 0, singleMatches: 0, multiParticipations: 0 };
        row[field] += 1;
        versions.set(key, row);
    };
    const incrementCountry = (country, field) => {
        const key = cleanCountry(country);
        const row = countries.get(key) || { country: key, launches: 0, singleMatches: 0, multiParticipations: 0 };
        row[field] += 1;
        countries.set(key, row);
    };
    const incrementAgent = (agent, field) => {
        const key = cleanText(agent, 200, 'unknown');
        const row = userAgents.get(key) || { userAgent: key, launches: 0, multiConnections: 0 };
        row[field] += 1;
        userAgents.set(key, row);
    };

    for (const event of events) {
        const timestamp = Date.parse(event.occurredAt);
        if (!Number.isFinite(timestamp)) continue;
        const isLaunch = event.eventName === 'app_launch';
        const isSingle = event.eventName === 'single_match_complete';
        const isMulti = event.eventName === 'multi_match_complete';
        for (const window of windows) {
            if (timestamp < window.since) continue;
            const counts = periods[window.key];
            if (isLaunch) counts.launches += 1;
            if (isSingle) counts.singleMatches += 1;
            if (isMulti) counts.multiMatches += 1;
            if (event.playerIdHash) counts.users.add(event.playerIdHash);
            if (isMulti) {
                if (event.properties.hostPlayerHash) counts.users.add(event.properties.hostPlayerHash);
                if (event.properties.guestPlayerHash) counts.users.add(event.properties.guestPlayerHash);
            }
        }

        const dayKey = kstDayKey(timestamp);
        const day = daily.get(dayKey) || {
            date: dayKey,
            launches: 0,
            singleMatches: 0,
            multiMatches: 0,
            users: new Set(),
        };
        if (isLaunch) day.launches += 1;
        if (isSingle) day.singleMatches += 1;
        if (isMulti) day.multiMatches += 1;
        if (event.playerIdHash) day.users.add(event.playerIdHash);
        if (isMulti) {
            if (event.properties.hostPlayerHash) day.users.add(event.properties.hostPlayerHash);
            if (event.properties.guestPlayerHash) day.users.add(event.properties.guestPlayerHash);
        }
        daily.set(dayKey, day);

        if (isLaunch) {
            incrementVersion(event.appVersionName, event.appVersionCode, 'launches');
            incrementCountry(event.countryCode, 'launches');
            incrementAgent(event.userAgent, 'launches');
        } else if (isSingle) {
            incrementVersion(event.appVersionName, event.appVersionCode, 'singleMatches');
            incrementCountry(event.countryCode, 'singleMatches');
        } else if (isMulti) {
            incrementVersion(event.properties.hostVersionName, event.properties.hostVersionCode, 'multiParticipations');
            incrementVersion(event.properties.guestVersionName, event.properties.guestVersionCode, 'multiParticipations');
            incrementCountry(event.properties.hostCountryCode, 'multiParticipations');
            incrementCountry(event.properties.guestCountryCode, 'multiParticipations');
            incrementAgent(event.properties.hostUserAgent, 'multiConnections');
            incrementAgent(event.properties.guestUserAgent, 'multiConnections');

            const reason = cleanText(event.properties.finishReason, 40, 'normal');
            reasons.set(reason, (reasons.get(reason) || 0) + 1);
            const key = cleanText(event.properties.pairHash, 32);
            if (key) {
                const row = pairs.get(key) || {
                    pairHash: key,
                    matches: 0,
                    forfeits: 0,
                    disconnects: 0,
                    shortMatches: 0,
                    winnerCounts: new Map(),
                };
                row.matches += 1;
                if (reason.includes('forfeit')) row.forfeits += 1;
                if (reason.includes('disconnect')) row.disconnects += 1;
                if ((event.properties.durationSec || 0) <= 30) row.shortMatches += 1;
                const winner = cleanText(event.properties.winnerPlayerHash, 32, 'draw');
                row.winnerCounts.set(winner, (row.winnerCounts.get(winner) || 0) + 1);
                pairs.set(key, row);
            }
        } else if (event.eventName === 'screen_view') {
            const screen = cleanText(event.properties.screen, 48);
            if (screen) screenViews.set(screen, (screenViews.get(screen) || 0) + 1);
        } else if (event.eventName === 'feature_use') {
            const feature = cleanText(event.properties.feature, 64);
            if (feature) featureUsage.set(feature, (featureUsage.get(feature) || 0) + 1);
        }
    }

    for (const counts of Object.values(periods)) {
        counts.uniqueUsers = counts.users.size;
        counts.totalMatches = counts.singleMatches + counts.multiMatches;
        delete counts.users;
    }

    const suspiciousPairs = Array.from(pairs.values()).map((row) => {
        const dominantWins = Math.max(...row.winnerCounts.values());
        const dominantRate = row.matches > 0 ? dominantWins / row.matches : 0;
        const score =
            Math.max(0, row.matches - 3) * 2 +
            (dominantRate >= 0.8 ? 4 : 0) +
            row.forfeits * 2 +
            row.disconnects * 2 +
            row.shortMatches;
        return {
            pairHash: row.pairHash,
            matches: row.matches,
            dominantWinRate: Math.round(dominantRate * 100),
            forfeits: row.forfeits,
            disconnects: row.disconnects,
            shortMatches: row.shortMatches,
            riskScore: score,
        };
    }).filter((row) => row.matches >= 2)
        .sort((a, b) => b.riskScore - a.riskScore || b.matches - a.matches)
        .slice(0, 50);
    const dau = periods.today.uniqueUsers;
    const wau = periods.days7.uniqueUsers;
    const mau = periods.days30.uniqueUsers;
    const kpis = {
        dau,
        wau,
        mau,
        dauMauPercent: mau > 0 ? Math.round(dau * 1000 / mau) / 10 : 0,
        matchesPerMau: mau > 0 ? Math.round(periods.days30.totalMatches * 10 / mau) / 10 : 0,
        multiSharePercent: periods.days30.totalMatches > 0
            ? Math.round(periods.days30.multiMatches * 1000 / periods.days30.totalMatches) / 10
            : 0,
    };

    return {
        generatedAt: new Date(now).toISOString(),
        retentionDays: runtime.retentionDays,
        storage: runtime.storage || 'memory',
        uptimeSec: runtime.uptimeSec || 0,
        live: runtime.live || {},
        operations: runtime.operations || {},
        selectedChannel: runtime.selectedChannel || 'all',
        periods,
        kpis,
        versions: Array.from(versions.values()).sort((a, b) => b.launches + b.singleMatches + b.multiParticipations - (a.launches + a.singleMatches + a.multiParticipations)),
        countries: Array.from(countries.values()).sort((a, b) => b.launches + b.singleMatches + b.multiParticipations - (a.launches + a.singleMatches + a.multiParticipations)),
        userAgents: Array.from(userAgents.values()).sort((a, b) => b.launches + b.multiConnections - (a.launches + a.multiConnections)).slice(0, 50),
        daily: filledDailyRows(daily, now, 30),
        finishReasons: Array.from(reasons.entries()).map(([reason, count]) => ({ reason, count })).sort((a, b) => b.count - a.count),
        screenViews: Array.from(screenViews.entries()).map(([screen, count]) => ({ screen, count })).sort((a, b) => b.count - a.count),
        featureUsage: Array.from(featureUsage.entries()).map(([feature, count]) => ({ feature, count })).sort((a, b) => b.count - a.count),
        suspiciousPairs,
    };
}

function startOfKstDay(timestamp) {
    const offsetMs = 9 * 60 * 60 * 1000;
    return Math.floor((timestamp + offsetMs) / 86400000) * 86400000 - offsetMs;
}

function kstDayKey(timestamp) {
    const offsetMs = 9 * 60 * 60 * 1000;
    return new Date(timestamp + offsetMs).toISOString().slice(0, 10);
}

function filledDailyRows(daily, now, count) {
    const rows = [];
    const todayStart = startOfKstDay(now);
    for (let offset = 0; offset < count; offset += 1) {
        const date = kstDayKey(todayStart - offset * 86400000);
        const row = daily.get(date);
        rows.push(row ? {
            date: row.date,
            launches: row.launches,
            singleMatches: row.singleMatches,
            multiMatches: row.multiMatches,
            uniqueUsers: row.users.size,
        } : {
            date,
            launches: 0,
            singleMatches: 0,
            multiMatches: 0,
            uniqueUsers: 0,
        });
    }
    return rows;
}

function escapeHtml(value) {
    return String(value ?? '')
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
}

function table(headers, rows, emptyText = '데이터 없음') {
    const head = headers.map((header) => `<th>${escapeHtml(header)}</th>`).join('');
    const body = rows.length > 0
        ? rows.map((row) => `<tr>${row.map((cell) => `<td>${escapeHtml(cell)}</td>`).join('')}</tr>`).join('')
        : `<tr><td colspan="${headers.length}" class="empty">${escapeHtml(emptyText)}</td></tr>`;
    return `<div class="table-wrap"><table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>`;
}

function lineChart(dailyRows) {
    const rows = [...dailyRows].reverse();
    const width = 960;
    const height = 280;
    const margin = { left: 48, right: 18, top: 18, bottom: 40 };
    const plotWidth = width - margin.left - margin.right;
    const plotHeight = height - margin.top - margin.bottom;
    const maxValue = Math.max(1, ...rows.flatMap((row) => [row.launches, row.singleMatches, row.multiMatches, row.uniqueUsers]));
    const x = (index) => margin.left + (rows.length <= 1 ? 0 : index * plotWidth / (rows.length - 1));
    const y = (value) => margin.top + plotHeight - value * plotHeight / maxValue;
    const points = (key) => rows.map((row, index) => `${x(index).toFixed(1)},${y(row[key]).toFixed(1)}`).join(' ');
    const grid = Array.from({ length: 5 }, (_, index) => {
        const value = Math.round(maxValue * (4 - index) / 4);
        const lineY = margin.top + plotHeight * index / 4;
        return `<line x1="${margin.left}" y1="${lineY}" x2="${width - margin.right}" y2="${lineY}" class="chart-grid"/><text x="${margin.left - 8}" y="${lineY + 4}" class="axis-label" text-anchor="end">${value}</text>`;
    }).join('');
    const labels = rows.map((row, index) => {
        if (index !== 0 && index !== rows.length - 1 && index % 5 !== 0) return '';
        return `<text x="${x(index)}" y="${height - 13}" class="axis-label" text-anchor="middle">${escapeHtml(row.date.slice(5))}</text>`;
    }).join('');
    return `<div class="chart-panel">
        <div class="legend"><span><i class="active"></i>활성 사용자</span><span><i class="launch"></i>실행</span><span><i class="single"></i>싱글</span><span><i class="multi"></i>멀티</span></div>
        <svg class="line-chart" viewBox="0 0 ${width} ${height}" role="img" aria-label="최근 30일 실행, 싱글, 멀티 추이">
            ${grid}${labels}
            <polyline points="${points('uniqueUsers')}" class="series active-line"/>
            <polyline points="${points('launches')}" class="series launch-line"/>
            <polyline points="${points('singleMatches')}" class="series single-line"/>
            <polyline points="${points('multiMatches')}" class="series multi-line"/>
        </svg>
    </div>`;
}

function barChart(rows, labelKey, value, colorClass, emptyText = '데이터 없음') {
    const prepared = rows.map((row) => ({ label: row[labelKey], value: value(row) }))
        .filter((row) => row.value > 0)
        .sort((a, b) => b.value - a.value)
        .slice(0, 12);
    if (prepared.length === 0) return `<div class="chart-panel empty">${escapeHtml(emptyText)}</div>`;
    const maxValue = Math.max(...prepared.map((row) => row.value));
    return `<div class="chart-panel bars">${prepared.map((row) => `
        <div class="bar-row">
            <span class="bar-label" title="${escapeHtml(row.label)}">${escapeHtml(row.label)}</span>
            <span class="bar-track"><i class="${colorClass}" style="width:${Math.max(2, row.value * 100 / maxValue).toFixed(1)}%"></i></span>
            <strong>${escapeHtml(row.value)}</strong>
        </div>`).join('')}</div>`;
}

function renderAdminPage(snapshot) {
    const today = snapshot.periods.today;
    const days7 = snapshot.periods.days7;
    const live = snapshot.live;
    const operations = snapshot.operations || {};
    const relay = operations.relay || {};
    const backpressure = operations.backpressure || {};
    const eventLoopLag = operations.eventLoopLagMs || {};
    const kpis = snapshot.kpis;
    const selectedChannel = normalizeAnalyticsFilter(snapshot.selectedChannel);
    const channelTabs = [
        ['all', '전체'],
        ['beta', '베타'],
        ['production', '운영'],
        ['dev', '개발'],
    ].map(([channel, label]) => {
        const current = channel === selectedChannel;
        return `<a href="/admin?channel=${channel}" class="${current ? 'current' : ''}"${current ? ' aria-current="page"' : ''}>${label}</a>`;
    }).join('');
    const generated = new Date(snapshot.generatedAt).toLocaleString('ko-KR', { timeZone: 'Asia/Seoul' });
    const metric = (label, value, sub) => `
        <article class="metric"><span>${escapeHtml(label)}</span><strong>${escapeHtml(value)}</strong><small>${escapeHtml(sub)}</small></article>`;
    const megabytes = (bytes) => `${(Number(bytes || 0) / 1048576).toFixed(1)} MB`;
    return `<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="robots" content="noindex,nofollow,noarchive">
<title>MiniZeus Admin Stats</title>
<style>
:root{color-scheme:light;--bg:#f4f5f2;--surface:#fff;--line:#d8ddd5;--text:#20231f;--muted:#697067;--green:#19764c;--orange:#c65d1b;--red:#b83b32;--ink:#38424b}*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:14px/1.45 system-ui,-apple-system,"Noto Sans KR",sans-serif;letter-spacing:0}header{background:#20231f;color:#fff;padding:18px 24px;border-bottom:4px solid var(--green)}header h1{font-size:20px;margin:0 0 4px}header p{margin:0;color:#cbd2c8;font-size:12px}main{max-width:1500px;margin:0 auto;padding:20px 24px 48px}.toolbar{display:flex;justify-content:space-between;align-items:center;gap:12px;margin-bottom:14px}.toolbar a{color:#fff;background:var(--green);padding:8px 12px;border-radius:4px;text-decoration:none;font-weight:700}.channel-filter{display:inline-grid;grid-template-columns:repeat(4,minmax(70px,1fr));border:1px solid var(--line);border-radius:6px;overflow:hidden;margin-bottom:22px;background:var(--surface)}.channel-filter a{padding:9px 14px;text-align:center;text-decoration:none;color:var(--text);font-weight:700;border-right:1px solid var(--line)}.channel-filter a:last-child{border-right:0}.channel-filter a.current{background:var(--ink);color:#fff}.metrics{display:grid;grid-template-columns:repeat(5,minmax(130px,1fr));gap:10px;margin-bottom:22px}.metric{background:var(--surface);border:1px solid var(--line);border-radius:6px;padding:12px;min-height:92px}.metric span,.metric small{display:block;color:var(--muted)}.metric strong{display:block;font-size:26px;margin:5px 0;color:var(--green)}section{margin:24px 0}h2{font-size:15px;margin:0 0 9px;padding-left:9px;border-left:4px solid var(--orange)}.grid{display:grid;grid-template-columns:1fr 1fr;gap:20px}.chart-panel{background:var(--surface);border:1px solid var(--line);border-radius:6px;padding:12px;overflow:hidden}.legend{display:flex;justify-content:flex-end;gap:18px;color:var(--muted);font-size:12px}.legend span{display:flex;align-items:center;gap:6px}.legend i{width:18px;height:3px;display:inline-block}.legend .active{background:var(--ink)}.legend .launch,.bar-fill{background:var(--green)}.legend .single,.single-fill{background:var(--orange)}.legend .multi,.reason-fill{background:var(--red)}.line-chart{display:block;width:100%;height:auto;min-height:210px}.chart-grid{stroke:#e3e7e1;stroke-width:1}.axis-label{fill:#737b72;font-size:11px}.series{fill:none;stroke-width:3;stroke-linecap:round;stroke-linejoin:round}.active-line{stroke:var(--ink)}.launch-line{stroke:var(--green)}.single-line{stroke:var(--orange)}.multi-line{stroke:var(--red)}.bars{display:grid;gap:9px;min-height:180px}.bar-row{display:grid;grid-template-columns:minmax(72px,120px) minmax(100px,1fr) 44px;align-items:center;gap:9px}.bar-label{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:#4d554c}.bar-track{height:12px;background:#e9ece7;border-radius:3px;overflow:hidden}.bar-track i{display:block;height:100%;border-radius:3px}.bar-row strong{text-align:right;font-variant-numeric:tabular-nums}.table-wrap{overflow:auto;background:var(--surface);border:1px solid var(--line);border-radius:6px}table{width:100%;border-collapse:collapse;min-width:560px}th,td{text-align:left;padding:9px 11px;border-bottom:1px solid #e7eae5;white-space:nowrap}th{position:sticky;top:0;background:#eef1ec;color:#4d554c;font-size:12px}tbody tr:hover{background:#fafbf9}.empty{text-align:center;color:var(--muted);padding:24px}.warn{color:var(--red);font-weight:700}.foot{color:var(--muted);font-size:12px;margin-top:24px}@media(max-width:900px){main{padding:14px}.metrics{grid-template-columns:repeat(2,1fr)}.grid{grid-template-columns:1fr}header{padding:14px}.legend{justify-content:flex-start;flex-wrap:wrap}.line-chart{min-width:720px}.chart-panel:has(.line-chart){overflow:auto}.channel-filter{display:grid}.channel-filter a{padding:9px 8px}}
</style>
</head>
<body>
<header><h1>MiniZeus 운영 통계</h1><p>관리자 전용 · 원 IP와 실제 기기 식별자는 저장하지 않음</p></header>
<main>
<div class="toolbar"><div>생성 ${escapeHtml(generated)} · 저장소 ${escapeHtml(snapshot.storage)}</div><div><a href="/admin/announcements?channel=${selectedChannel}">공지 관리</a> <a href="/admin/support?channel=${selectedChannel}">문의 관리</a> <a href="/admin?channel=${selectedChannel}">새로고침</a></div></div>
<nav class="channel-filter" aria-label="배포 채널">${channelTabs}</nav>
<div class="metrics">
${metric('오늘 실행', today.launches, `7일 ${days7.launches}`)}
${metric('오늘 싱글', today.singleMatches, `7일 ${days7.singleMatches}`)}
${metric('오늘 멀티', today.multiMatches, `7일 ${days7.multiMatches}`)}
${metric('DAU', kpis.dau, '오늘 익명 사용자')}
${metric('WAU', kpis.wau, '최근 7일')}
${metric('MAU', kpis.mau, '최근 30일')}
${metric('DAU / MAU', `${kpis.dauMauPercent}%`, '활성도')}
${metric('사용자당 매치', kpis.matchesPerMau, '최근 30일')}
${metric('멀티 비중', `${kpis.multiSharePercent}%`, '최근 30일 매치')}
${metric('현재 연결', live.connections || 0, `전체 채널 · 방 ${live.rooms || 0}`)}
${metric('진행 중 대전', live.activeMatches || 0, `전체 채널 · 대기 방 ${live.waitingRooms || 0}`)}
${metric('Relay / P2P', `${live.activeRelayMatches || 0} / ${live.activeP2pMatches || 0}`, '현재 진행 중 대전')}
${metric('Relay 전송', relay.packets || 0, `재시작 후 ${megabytes(relay.bytes)}`)}
${metric('Relay 상태', relay.canStartNewMatch === false ? '제한 중' : '정상', `${relay.code || 'ok'} · 진행 ${relay.activeMatches || 0}/${relay.maxActiveMatches || '∞'}`)}
${metric('Relay 최근 1시간', `${relay.lastHourMb || 0} MB`, `경고 ${relay.warningMbPerHour || '-'} · 차단 ${relay.limitMbPerHour || '-'} MB`)}
${metric('Relay 진입 거부', relay.admissionRejections || 0, `전투 중 fallback ${relay.runtimeFallbacks || 0}`)}
${metric('혼잡 거부', operations.capacityRejections || 0, '재시작 후 누적')}
${metric('이벤트 루프 p95', `${eventLoopLag.p95 || 0} ms`, `최근 60초 · 최대 ${eventLoopLag.max || 0} ms`)}
${metric('송신 지연 보호', backpressure.droppedStatePackets || 0, `연결 종료 ${backpressure.closedConnections || 0}`)}
</div>
<section><h2>최근 30일 활동 추이</h2>${lineChart(snapshot.daily)}</section>
<div class="grid">
<section><h2>국가/지역 활동량</h2>${barChart(snapshot.countries, 'country', (r) => r.launches + r.singleMatches + r.multiParticipations, 'bar-fill')}</section>
<section><h2>멀티 종료 사유 분포</h2>${barChart(snapshot.finishReasons, 'reason', (r) => r.count, 'reason-fill')}</section>
<section><h2>많이 본 화면</h2>${barChart(snapshot.screenViews, 'screen', (r) => r.count, 'bar-fill')}</section>
<section><h2>많이 사용한 기능</h2>${barChart(snapshot.featureUsage, 'feature', (r) => r.count, 'single-fill')}</section>
</div>
<div class="grid">
<section><h2>일자별 추이</h2>${table(['날짜','실행','싱글','멀티'], snapshot.daily.map((r) => [r.date,r.launches,r.singleMatches,r.multiMatches]))}</section>
<section><h2>버전별</h2>${table(['버전','실행','싱글','멀티 참가'], snapshot.versions.map((r) => [r.version,r.launches,r.singleMatches,r.multiParticipations]))}</section>
<section><h2>국가/지역</h2>${table(['국가','실행','싱글','멀티 참가'], snapshot.countries.map((r) => [r.country,r.launches,r.singleMatches,r.multiParticipations]))}</section>
<section><h2>종료 사유</h2>${table(['사유','멀티 매치'], snapshot.finishReasons.map((r) => [r.reason,r.count]))}</section>
</div>
<section><h2>반복 매칭 / 패작 의심 관찰</h2>${table(['익명 상대 조합','매치','우세 승률','기권','이탈','30초 이하','위험 점수'], snapshot.suspiciousPairs.map((r) => [r.pairHash,r.matches,`${r.dominantWinRate}%`,r.forfeits,r.disconnects,r.shortMatches,r.riskScore]))}</section>
<section><h2>User-Agent</h2>${table(['User-Agent','실행','멀티 연결'], snapshot.userAgents.map((r) => [r.userAgent,r.launches,r.multiConnections]))}</section>
<p class="foot">날짜 기준 KST · 통계 보존 ${escapeHtml(snapshot.retentionDays)}일 · 선택 채널 ${escapeHtml(selectedChannel)}</p>
</main>
</body></html>`;
}

module.exports = {
    AnalyticsStore,
    eventFromHttpRequest,
    hashIdentifier,
    normalizeAnalyticsChannel,
    pairHash,
    renderAdminPage,
    requestCountry,
    safeUserAgent,
};
