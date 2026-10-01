const crypto = require('crypto');

// One controller per single-replica, version-compatible game pool.
// A DB owner fence prevents an old rolling-deployment process from issuing slots.
class AdmissionController {
    constructor({ pool, poolId, hardLimit, share = 1, connected, paused, now = Date.now }) {
        Object.assign(this, { pool, poolId, hardLimit, share, connected, paused, now });
        this.entries = new Map();
        this.tail = Promise.resolve();
        this.settings = { manualLimit: hardLimit, budgetEnabled: false, budgetUsd: 20,
            baselineMonthlyUsd: null, costPer100MonthlyUsd: null, measuredAt: null,
            updatedAt: null };
        this.nextOrder = 0;
        this.ownerId = crypto.randomUUID();
        this.db = pool;
        this.retired = false;
        this.sample = { startedAt: this.now(), lastAt: this.now(), seconds: 0, playerSeconds: 0, peak: 0 };
    }

    serial(work) {
        const result = this.tail.then(async () => {
            if (this.retired) throw new Error('admission_owner_replaced');
            if (!this.pool) return work();
            const client = await this.pool.connect();
            const before = { entries: new Map(this.entries), settings: this.settings, nextOrder: this.nextOrder };
            try {
                await client.query('BEGIN');
                // Held until commit: a replacement process must wait for this operation.
                const owner = await client.query('SELECT owner_id FROM br_admission_settings WHERE pool_id=$1 FOR SHARE', [this.poolId]);
                if (owner.rows[0]?.owner_id !== this.ownerId) {
                    this.retired = true;
                    throw new Error('admission_owner_replaced');
                }
                this.db = client;
                const value = await work();
                await client.query('COMMIT');
                return value;
            } catch (error) {
                await client.query('ROLLBACK').catch(() => {});
                Object.assign(this, before);
                throw error;
            } finally {
                this.db = this.pool;
                client.release();
            }
        });
        this.tail = result.catch(() => {});
        return result;
    }

    async initialize() {
        if (!this.pool) return;
        await this.pool.query(`CREATE TABLE IF NOT EXISTS br_admission_settings (
            pool_id TEXT PRIMARY KEY, settings JSONB NOT NULL, owner_id TEXT)`);
        await this.pool.query('ALTER TABLE br_admission_settings ADD COLUMN IF NOT EXISTS owner_id TEXT');
        await this.pool.query(`CREATE TABLE IF NOT EXISTS br_admission_queue (
            pool_id TEXT NOT NULL, player_id TEXT NOT NULL, entry JSONB NOT NULL,
            PRIMARY KEY(pool_id, player_id))`);
        await this.pool.query(`CREATE TABLE IF NOT EXISTS br_admission_samples (
            pool_id TEXT NOT NULL, started_at TIMESTAMPTZ NOT NULL, covered_seconds DOUBLE PRECISION NOT NULL,
            player_seconds DOUBLE PRECISION NOT NULL, peak INTEGER NOT NULL, PRIMARY KEY(pool_id, started_at))`);
        const client = await this.pool.connect();
        try {
            await client.query('BEGIN');
            await client.query(`INSERT INTO br_admission_settings(pool_id,settings,owner_id) VALUES($1,$2::jsonb,$3)
                ON CONFLICT(pool_id) DO UPDATE SET owner_id=EXCLUDED.owner_id`, [this.poolId, JSON.stringify(this.settings), this.ownerId]);
            const settings = await client.query('SELECT settings FROM br_admission_settings WHERE pool_id=$1', [this.poolId]);
            if (settings.rows[0]) this.settings = this.validatedSettings({ ...settings.rows[0].settings,
                manualLimit: Math.min(this.hardLimit, settings.rows[0].settings.manualLimit) });
            const queued = await client.query('SELECT player_id, entry FROM br_admission_queue WHERE pool_id=$1', [this.poolId]);
            for (const row of queued.rows) {
                const entry = row.entry;
                if (entry && Number.isFinite(entry.order)) this.nextOrder = Math.max(this.nextOrder, entry.order);
                if (entry?.expiresAt > this.now()) this.entries.set(row.player_id, entry);
            }
            this.db = client;
            await this.cleanup();
            await client.query("DELETE FROM br_admission_queue WHERE pool_id=$1 AND (entry->>'expiresAt')::bigint <= $2", [this.poolId, this.now()]);
            await client.query('COMMIT');
        } catch (error) {
            await client.query('ROLLBACK').catch(() => {});
            throw error;
        } finally { this.db = this.pool; client.release(); }
    }

    validatedSettings(input) {
        const settings = { ...this.settings, ...input };
        if (!Number.isInteger(settings.manualLimit) || settings.manualLimit < 0 || settings.manualLimit > this.hardLimit ||
            typeof settings.budgetEnabled !== 'boolean' || !Number.isFinite(settings.budgetUsd) || settings.budgetUsd <= 0 || settings.budgetUsd > 100000) {
            throw new Error('invalid_admission_settings');
        }
        for (const key of ['baselineMonthlyUsd', 'costPer100MonthlyUsd']) {
            if (settings[key] !== null && (!Number.isFinite(settings[key]) || settings[key] < 0 || settings[key] > 100000)) throw new Error('invalid_cost_model');
        }
        if (settings.measuredAt !== null && (!Number.isFinite(Date.parse(settings.measuredAt)) || Date.parse(settings.measuredAt) > this.now() + 60000)) throw new Error('invalid_cost_measurement_date');
        return settings;
    }

    effectiveLimit() {
        const s = this.settings;
        const modelReady = Number.isFinite(s.baselineMonthlyUsd) && s.costPer100MonthlyUsd > 0 && s.measuredAt;
        if (!s.budgetEnabled || !modelReady) return s.manualLimit;
        // The model describes the whole production service, including Beta baseline.
        const globalAffordable = Math.max(0, Math.floor((s.budgetUsd - s.baselineMonthlyUsd) / s.costPer100MonthlyUsd * 100));
        return Math.min(s.manualLimit, Math.floor(globalAffordable * this.share));
    }

    snapshot() {
        const entries = [...this.entries.values()].filter((entry) => entry.expiresAt > this.now());
        const offered = entries.filter((entry) => entry.state !== 'queued').length;
        const modelReady = Number.isFinite(this.settings.baselineMonthlyUsd) && this.settings.costPer100MonthlyUsd > 0 && this.settings.measuredAt;
        return { hardLimit: this.hardLimit, effectiveLimit: this.effectiveLimit(),
            connected: this.connected(), reserved: offered,
            queued: entries.filter((entry) => entry.state === 'queued').length,
            maxQueued: 2000, retryAfterSec: 20, reservationSec: 60, poolShare: this.share,
            budgetStatus: !this.settings.budgetEnabled ? 'manual' : modelReady ? 'measured_model' : 'waiting_for_measurement',
            costModelAgeHours: this.settings.measuredAt ? Math.max(0, (this.now() - Date.parse(this.settings.measuredAt)) / 3600000) : null,
            settings: { ...this.settings } };
    }

    updateSettings(input) {
        return this.serial(async () => {
            const allowed = ['manualLimit', 'budgetEnabled', 'budgetUsd', 'baselineMonthlyUsd', 'costPer100MonthlyUsd', 'measuredAt'];
            if (Object.keys(input).some((key) => !allowed.includes(key))) throw new Error('invalid_admission_settings');
            const next = this.validatedSettings({ ...input, updatedAt: new Date(this.now()).toISOString() });
            if (this.db) await this.db.query('UPDATE br_admission_settings SET settings=$2::jsonb WHERE pool_id=$1', [this.poolId, JSON.stringify(next)]);
            this.settings = next;
            await this.promote();
            return this.snapshot();
        });
    }

    async save(entry) {
        if (this.db) await this.db.query(`INSERT INTO br_admission_queue(pool_id,player_id,entry) VALUES($1,$2,$3::jsonb)
            ON CONFLICT(pool_id,player_id) DO UPDATE SET entry=EXCLUDED.entry`, [this.poolId, entry.playerId, JSON.stringify(entry)]);
        this.entries.set(entry.playerId, entry);
    }

    async remove(playerId) {
        if (this.db) await this.db.query('DELETE FROM br_admission_queue WHERE pool_id=$1 AND player_id=$2', [this.poolId, playerId]);
        this.entries.delete(playerId);
    }

    async cleanup() {
        const now = this.now();
        const expired = [...this.entries.values()].filter((entry) => entry.expiresAt <= now);
        for (const entry of expired) this.entries.delete(entry.playerId);
        if (this.db && expired.length) await this.db.query(`DELETE FROM br_admission_queue WHERE pool_id=$1 AND (entry->>'expiresAt')::bigint <= $2`, [this.poolId, now]);
    }

    async promote() {
        if (this.paused()) return;
        const state = this.snapshot();
        let free = Math.max(0, state.effectiveLimit - state.connected - state.reserved);
        const waiting = [...this.entries.values()].filter((entry) => entry.state === 'queued' && entry.expiresAt > this.now()).sort((a, b) => a.order - b.order);
        for (const entry of waiting) {
            if (free-- <= 0) break;
            await this.save({ ...entry, state: 'offered', reservationId: crypto.randomUUID(), expiresAt: this.now() + 60000 });
        }
    }

    request(playerId, country, channel, waitForSlot, sessionId = null) {
        return this.serial(async () => {
            await this.cleanup();
            let entry = this.entries.get(playerId);
            if (entry && (entry.country !== country || entry.channel !== channel)) {
                await this.remove(playerId);
                entry = null;
            }
            if (entry && sessionId && entry.sessionId !== sessionId && ['queued', 'offered'].includes(entry.state)) {
                entry = { ...entry, sessionId, ...(entry.state === 'offered' ? { reservationId: crypto.randomUUID() } : {}) };
                await this.save(entry);
            }
            if (entry?.state === 'connecting') return { status: 'connecting', retryAfterSec: 2 };
            if (entry?.state === 'reconnect') {
                entry = { ...entry, state: 'offered', reservationId: crypto.randomUUID(), expiresAt: this.now() + 60000 };
                await this.save(entry);
                return { status: 'admitted', reservationId: entry.reservationId, expiresAt: entry.expiresAt };
            }
            if (this.paused()) return { status: 'maintenance', retryAfterSec: 20 };
            await this.promote();
            entry = this.entries.get(playerId);
            if (!entry) {
                const state = this.snapshot();
                if (!waitForSlot && (state.queued > 0 || state.connected + state.reserved >= state.effectiveLimit)) return { status: 'full', retryAfterSec: 20 };
                if (state.queued >= state.maxQueued) return { status: 'queue_full', retryAfterSec: 60 };
                entry = { playerId, country, channel, sessionId, order: ++this.nextOrder, state: 'queued', expiresAt: this.now() + 120000 };
                await this.save(entry);
            } else if (entry.state === 'queued') {
                await this.save({ ...entry, expiresAt: this.now() + 120000 });
            }
            await this.promote();
            entry = this.entries.get(playerId);
            if (entry.state === 'offered') return { status: 'admitted', reservationId: entry.reservationId, expiresAt: entry.expiresAt };
            return { status: 'queued', position: 1 + [...this.entries.values()].filter((other) => other.state === 'queued' && other.order < entry.order && other.expiresAt > this.now()).length, retryAfterSec: 20 };
        });
    }

    claim(playerId, reservationId, country, channel) {
        return this.serial(async () => {
            const entry = this.entries.get(playerId);
            if (!entry || entry.state !== 'offered' || entry.reservationId !== reservationId || entry.country !== country || entry.channel !== channel || entry.expiresAt <= this.now()) return false;
            const claimed = { ...entry, state: 'connecting', expiresAt: this.now() + 10000 };
            if (this.db) {
                const result = await this.db.query(`UPDATE br_admission_queue SET entry=$3::jsonb
                    WHERE pool_id=$1 AND player_id=$2 AND entry->>'state'='offered'
                    AND entry->>'reservationId'=$4 AND (entry->>'expiresAt')::bigint>$5`,
                    [this.poolId, playerId, JSON.stringify(claimed), reservationId, this.now()]);
                if (result.rowCount !== 1) return false;
            }
            this.entries.set(playerId, claimed);
            return true;
        });
    }

    connectedPlayer(playerId, reservationId) {
        return this.serial(async () => {
            if (this.entries.get(playerId)?.reservationId === reservationId) await this.remove(playerId);
        });
    }

    disconnect(playerId, country, channel, retainSlot) {
        return this.serial(async () => {
            if (retainSlot && playerId) await this.save({ playerId, country, channel, order: ++this.nextOrder, state: 'reconnect', expiresAt: this.now() + 45000 });
            await this.promote();
        });
    }

    cancel(playerId, sessionId) {
        return this.serial(async () => {
            const entry = this.entries.get(playerId);
            if (entry && entry.sessionId === sessionId && ['queued', 'offered'].includes(entry.state)) await this.remove(playerId);
            await this.promote();
        });
    }

    async observations() {
        if (!this.pool) return null;
        const result = await this.pool.query(`SELECT min(started_at) AS since, sum(covered_seconds) AS seconds,
            sum(player_seconds) AS player_seconds, max(peak) AS peak FROM br_admission_samples
            WHERE pool_id=$1 AND started_at >= now()-interval '7 days'`, [this.poolId]);
        const row = result.rows[0];
        const seconds = Number(row?.seconds) || 0;
        return { since: row?.since || null, coveredHours: seconds / 3600,
            averageConnections: seconds ? Number(row.player_seconds) / seconds : null,
            peakConnections: Number(row?.peak) || 0, flushIntervalSec: 300 };
    }

    tick() {
        if (this.retired) return Promise.resolve();
        const now = this.now();
        const elapsed = Math.max(0, Math.min(15, (now - this.sample.lastAt) / 1000));
        const count = this.connected();
        this.sample.seconds += elapsed;
        this.sample.playerSeconds += elapsed * count;
        this.sample.peak = Math.max(this.sample.peak, count);
        this.sample.lastAt = now;
        const flush = this.pool && now - this.sample.startedAt >= 300000;
        if (!this.entries.size && !flush) return Promise.resolve();
        return this.serial(async () => {
            await this.cleanup();
            await this.promote();
            if (flush) {
                const sample = { ...this.sample };
                await this.db.query(`INSERT INTO br_admission_samples VALUES($1,$2,$3,$4,$5)
                    ON CONFLICT(pool_id,started_at) DO UPDATE SET covered_seconds=EXCLUDED.covered_seconds,
                    player_seconds=EXCLUDED.player_seconds,peak=EXCLUDED.peak`,
                    [this.poolId, new Date(sample.startedAt), sample.seconds, sample.playerSeconds, sample.peak]);
                await this.db.query("DELETE FROM br_admission_samples WHERE pool_id=$1 AND started_at < now()-interval '30 days'", [this.poolId]);
                // Reset after commit so a DB failure does not silently lose observations.
                return sample;
            }
            return null;
        }).then((flushed) => {
            if (flushed) this.sample = { startedAt: flushed.lastAt, lastAt: this.sample.lastAt,
                seconds: this.sample.seconds - flushed.seconds, playerSeconds: this.sample.playerSeconds - flushed.playerSeconds,
                peak: this.connected() };
        });
    }
}

module.exports = { AdmissionController };
