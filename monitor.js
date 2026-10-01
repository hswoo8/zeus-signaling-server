const crypto = require('crypto');
const https = require('https');
const MINUTE = 60000;
const FIELDS = ['requests', 'failures', 'clientErrors', 'expectedRejections', 'aborted', 'wsClosed', 'wsAbnormal', 'backgroundErrors', 'wsUpgradeRequests', 'wsUpgradeFailures', 'wsUpgradeRejected'];
const EXPECTED = new Set(['server_busy', 'server_maintenance', 'auth_disabled', 'analytics_disabled', 'support_disabled', 'admin_disabled', 'admission_disabled']);

function emptyCounts() { return Object.fromEntries(FIELDS.map((key) => [key, 0])); }
function compareWindows(current, previous) {
    const rate = (v) => v.requests ? v.failures / v.requests : null;
    const currentRate = rate(current), previousRate = rate(previous);
    const enough = current.requests >= 20 && previous.requests >= 20;
    return { current, previous, currentRate, previousRate,
        differencePoints: currentRate === null || previousRate === null ? null : (currentRate - previousRate) * 100,
        trend: !enough ? 'insufficient_samples' : current.failures >= 3 && currentRate >= previousRate + 0.02 && currentRate >= previousRate * 2 ? 'rising' : 'stable' };
}
function readHealth(url) {
    return new Promise((resolve) => {
        const started = Date.now();
        let done = false, raw = '', req;
        const finish = (result) => {
            if (done) return;
            done = true; clearTimeout(deadline);
            resolve({ ...result, checkedAt: new Date().toISOString(), latencyMs: Date.now() - started });
        };
        const deadline = setTimeout(() => { req?.destroy(); finish({ reachable: false, reason: 'timeout' }); }, 4000);
        req = https.get(url, { headers: { 'User-Agent': 'MiniZeus-status-monitor/1' } }, (res) => {
            res.setEncoding('utf8');
            res.on('data', (chunk) => {
                raw += chunk;
                if (Buffer.byteLength(raw) > 128 * 1024) { req.destroy(); finish({ reachable: false, reason: 'response_too_large' }); }
            });
            res.on('error', () => finish({ reachable: false, reason: 'response_error' }));
            res.on('end', () => {
                try {
                    const body = JSON.parse(raw);
                    const reachable = res.statusCode === 200 && body.ok === true;
                    finish({ reachable, reason: reachable ? null : 'http_' + res.statusCode,
                        uptimeSec: typeof body.uptimeSec === 'number' ? body.uptimeSec : null,
                        runtime: body.monitor || null });
                } catch { finish({ reachable: false, reason: 'invalid_health_response' }); }
            });
        });
        req.on('error', () => finish({ reachable: false, reason: 'connection_error' }));
    });
}

class ServerMonitor {
    constructor({ pool, poolId, targets = [], now = Date.now }) {
        Object.assign(this, { pool, poolId, now });
        this.instanceId = crypto.randomUUID();
        this.startedAt = now(); this.buckets = new Map(); this.events = [];
        this.database = { status: pool ? 'pending' : 'not_configured', checkedAt: null, latencyMs: null };
        this.storageStatus = pool ? 'pending' : 'memory_only';
        this.peers = targets.slice(0, 8).map((target) => {
            const url = new URL(target.url);
            if (url.protocol !== 'https:' || url.username || url.password || url.search || url.hash) throw Error('Invalid monitor target');
            return { name: String(target.name).slice(0, 60), url: url.href, failures: 0, state: 'pending' };
        });
        this.probeBusy = false; this.flushBusy = false; this.peersBusy = false; this.storageReady = false;
        this.historyRows = [];
    }
    bucket(at = this.now()) {
        const minute = Math.floor(at / MINUTE) * MINUTE;
        if (!this.buckets.has(minute)) this.buckets.set(minute, { minute, ...emptyCounts() });
        return this.buckets.get(minute);
    }
    record(field, amount = 1) { if (FIELDS.includes(field)) this.bucket()[field] += amount; }
    observeHttp(req, res) {
        let path;
        try { path = new URL(req.url || '/', 'http://localhost').pathname; } catch { return; }
        if (path.startsWith('/admin') || path.startsWith('/health') || path === '/favicon.ico') return;
        let finished = false;
        res.once('finish', () => {
            finished = true;
            if (res.statusCode === 404) return; // unknown scanners are not application traffic
            this.record('requests');
            if (res.statusCode >= 500) this.record(EXPECTED.has(res.monitorErrorCode) ? 'expectedRejections' : 'failures');
            else if (res.statusCode >= 400) this.record('clientErrors');
        });
        res.once('close', () => { if (!finished) this.record('aborted'); });
    }
    event(kind, target) {
        const row = { at: new Date(this.now()).toISOString(), kind, target };
        this.events.push(row); if (this.events.length > 50) this.events.shift();
        console.info('[monitor] ' + JSON.stringify(row)); // no request bodies, IDs, credentials or raw exception text
    }
    counts(from, to) {
        const totals = emptyCounts();
        for (const bucket of this.buckets.values()) if (bucket.minute >= from && bucket.minute < to) {
            for (const field of FIELDS) totals[field] += bucket[field];
        }
        return totals;
    }
    snapshot() {
        // Closed minute buckets give equal-length 5-minute windows.
        const end = Math.floor(this.now() / MINUTE) * MINUTE;
        const comparison = compareWindows(this.counts(end - 5 * MINUTE, end), this.counts(end - 10 * MINUTE, end - 5 * MINUTE));
        if (this.now() - this.startedAt < 10 * MINUTE) comparison.trend = 'warming_up';
        const database = { ...this.database };
        if (this.pool && (!database.checkedAt || this.now() - Date.parse(database.checkedAt) > 90000)) database.status = 'stale';
        return { poolId: this.poolId, instanceId: this.instanceId, startedAt: new Date(this.startedAt).toISOString(),
            observedAt: new Date(this.now()).toISOString(), windowEnd: new Date(end).toISOString(),
            database, storageStatus: this.storageStatus, comparison,
            currentMinute: this.counts(end, end + MINUTE),
            peers: this.peers.map((p) => ({ ...p, state: p.checkedAt && this.now() - Date.parse(p.checkedAt) > 150000 ? 'stale' : p.state })),
            events: [...this.events].reverse(),
            status: ['error', 'stale'].includes(database.status) ? 'database_unavailable'
                : comparison.current.failures || comparison.current.backgroundErrors || comparison.current.wsUpgradeFailures || comparison.current.wsAbnormal ? 'errors_observed' : 'ok' };
    }
    publicSnapshot() {
        const s = this.snapshot();
        return { status: s.status, observedAt: s.observedAt, database: s.database.status,
            http5xx: s.comparison.current.failures, requests: s.comparison.current.requests,
            httpErrorRate: s.comparison.currentRate, trend: s.comparison.trend,
            backgroundErrors: s.comparison.current.backgroundErrors, wsAbnormal: s.comparison.current.wsAbnormal };
    }
    probeDatabase() {
        if (!this.pool || this.probeBusy) return;
        this.probeBusy = true;
        const started = this.now();
        const update = (status) => {
            const previous = this.database.status;
            this.database = { status, checkedAt: new Date(this.now()).toISOString(), latencyMs: this.now() - started };
            if (status !== previous && (status === 'error' || previous === 'error')) this.event(status === 'error' ? 'database_error' : 'database_recovered', this.poolId);
        };
        const deadline = setTimeout(() => update('error'), 2500);
        // Only one probe can be queued even when obtaining a pool connection hangs.
        this.pool.query({ text: 'SELECT 1', query_timeout: 2000 }).then(() => update(this.now() - started > 2500 ? 'error' : 'ok'))
            .catch(() => update('error')).finally(() => { clearTimeout(deadline); this.probeBusy = false; });
    }
    async probePeers() {
        if (this.peersBusy) return;
        this.peersBusy = true;
        try {
            await Promise.allSettled(this.peers.map(async (peer) => {
                const prior = peer.state, uptime = peer.uptimeSec;
                const result = await readHealth(peer.url);
                Object.assign(peer, result);
                peer.failures = result.reachable ? 0 : peer.failures + 1;
                peer.state = !result.reachable ? (peer.failures >= 3 ? 'unreachable' : 'checking_failure')
                    : result.runtime?.status && result.runtime.status !== 'ok' ? 'degraded' : 'ok';
                if (prior !== peer.state && (prior !== 'pending' || peer.state !== 'ok')) this.event('peer_' + peer.state, peer.name);
                if (result.reachable && uptime != null && result.uptimeSec != null && result.uptimeSec < uptime) this.event('peer_restarted', peer.name);
            }));
        } finally { this.peersBusy = false; }
    }
    async flush() {
        if (!this.pool || this.flushBusy) return;
        this.flushBusy = true;
        try {
            if (!this.storageReady) {
                await this.pool.query(`CREATE TABLE IF NOT EXISTS br_monitor_minutes (
                    pool_id TEXT NOT NULL, instance_id TEXT NOT NULL, minute TIMESTAMPTZ NOT NULL,
                    counts JSONB NOT NULL, PRIMARY KEY(pool_id,instance_id,minute))`);
                this.storageReady = true;
            }
            const cutoff = Math.floor(this.now() / MINUTE) * MINUTE;
            for (const bucket of this.buckets.values()) if (!bucket.saved && bucket.minute < cutoff) {
                const { minute, saved, ...counts } = bucket;
                await this.pool.query(`INSERT INTO br_monitor_minutes VALUES($1,$2,$3,$4::jsonb)
                    ON CONFLICT(pool_id,instance_id,minute) DO UPDATE SET counts=EXCLUDED.counts`,
                    [this.poolId, this.instanceId, new Date(minute), JSON.stringify(counts)]);
                bucket.saved = true;
            }
            await this.pool.query("DELETE FROM br_monitor_minutes WHERE pool_id=$1 AND minute<now()-interval '7 days'", [this.poolId]);
            this.storageStatus = 'ok';
        } catch { this.storageStatus = 'error'; }
        finally { this.flushBusy = false; }
    }
    async history() {
        const merged = new Map();
        if (this.pool && this.storageReady && this.database.status === 'ok' && !this.historyPending) {
            this.historyPending = this.pool.query({ text: `SELECT instance_id,minute,counts FROM br_monitor_minutes
                    WHERE pool_id=$1 AND minute>=now()-interval '24 hours' ORDER BY minute DESC LIMIT 5000`, values: [this.poolId], query_timeout: 2000 });
            this.historyPending = this.historyPending.then((result) => { this.historyRows = result.rows; })
                .catch(() => { this.storageStatus = 'error'; }).finally(() => { this.historyPending = null; });
            let deadline;
            await Promise.race([this.historyPending, new Promise((resolve) => { deadline = setTimeout(resolve, 2500); })]);
            clearTimeout(deadline);
        }
        for (const row of this.historyRows) if (new Date(row.minute).getTime() >= this.now() - 24 * 60 * MINUTE) {
            merged.set(row.instance_id + ':' + new Date(row.minute).getTime(), { minute: new Date(row.minute).getTime(), ...row.counts });
        }
        for (const row of this.buckets.values()) merged.set(this.instanceId + ':' + row.minute, row);
        const grouped = new Map();
        for (const row of merged.values()) {
            if (!grouped.has(row.minute)) grouped.set(row.minute, { minute: row.minute, ...emptyCounts() });
            for (const field of FIELDS) grouped.get(row.minute)[field] += Number(row[field]) || 0;
        }
        return [...grouped.values()].sort((a, b) => a.minute - b.minute);
    }
    tick() {
        this.bucket();
        for (const minute of this.buckets.keys()) if (minute < this.now() - 2 * 60 * MINUTE) this.buckets.delete(minute);
        const trend = this.snapshot().comparison.trend;
        if (trend !== this.lastTrend && (trend === 'rising' || this.lastTrend === 'rising')) this.event('error_trend_' + trend, this.poolId);
        this.lastTrend = trend;
        this.probeDatabase(); void this.probePeers(); void this.flush();
    }
    start() { this.tick(); this.timer = setInterval(() => this.tick(), MINUTE); this.timer.unref(); }
    stop() { clearInterval(this.timer); }
}
module.exports = { ServerMonitor, compareWindows };
