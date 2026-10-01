// User-run regression cases. Not executed as part of v55 implementation.
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { AdmissionController } = require('./admission');

function fixture() {
    let time = Date.now();
    let connections = 1;
    const controller = new AdmissionController({ pool: null, poolId: 'test', hardLimit: 1,
        connected: () => connections, paused: () => false, now: () => time });
    return { controller, advance: (ms) => { time += ms; }, connected: (count) => { connections = count; },
        request: (id, wait = true, session = id) => controller.request(id, 'KR', 'beta', wait, session) };
}

test('FIFO, player deduplication and no bypass by home/legacy ticket requests', async () => {
    const f = fixture();
    assert.equal((await f.request('first')).position, 1);
    assert.equal((await f.request('second')).position, 2);
    assert.equal((await f.request('first')).position, 1);
    f.connected(0);
    await f.controller.tick();
    assert.equal((await f.request('home', false)).status, 'full');
    assert.equal((await f.request('first')).status, 'admitted');
    assert.equal((await f.request('second')).position, 1);
});

test('reservation binds identity/country/channel and only one concurrent upgrade wins', async () => {
    const f = fixture(); f.connected(0);
    const reservation = await f.request('one');
    assert.equal(await f.controller.claim('other', reservation.reservationId, 'KR', 'beta'), false);
    assert.equal(await f.controller.claim('one', reservation.reservationId, 'US', 'beta'), false);
    assert.equal(await f.controller.claim('one', reservation.reservationId, 'KR', 'production'), false);
    const claims = await Promise.all([0, 1].map(() => f.controller.claim('one', reservation.reservationId, 'KR', 'beta')));
    assert.deepEqual(claims, [true, false]);
});

test('expired offer frees its slot; abandoned queue entries expire', async () => {
    const f = fixture();
    await f.request('first'); await f.request('second');
    f.connected(0); await f.controller.tick();
    f.advance(61000); await f.controller.tick();
    assert.equal((await f.request('second')).status, 'admitted');
    f.advance(121000); await f.controller.tick();
    assert.equal(f.controller.snapshot().reserved, 0);
    assert.equal(f.controller.snapshot().queued, 0);
});

test('stale cancellation does not delete a newer UI session', async () => {
    const f = fixture();
    await f.request('one', true, 'old'); await f.request('one', true, 'new');
    await f.controller.cancel('one', 'old');
    assert.equal(f.controller.snapshot().queued, 1);
    await f.controller.cancel('one', 'new');
    assert.equal(f.controller.snapshot().queued, 0);
});

test('cap reduction keeps existing connections and short authenticated reconnect grace', async () => {
    const f = fixture();
    await f.controller.updateSettings({ manualLimit: 0 });
    assert.equal(f.controller.snapshot().connected, 1);
    assert.equal((await f.request('new')).status, 'queued');
    f.connected(0);
    await f.controller.disconnect('existing', 'KR', 'beta', true);
    assert.equal((await f.request('existing', false)).status, 'admitted');
    assert.equal((await f.request('new')).status, 'queued');
});

test('missing cost measurements retain manual cap; model applies only within hard limit', async () => {
    const c = new AdmissionController({ pool: null, poolId: 'cost', hardLimit: 450, share: 0.9,
        connected: () => 400, paused: () => false });
    await c.updateSettings({ budgetEnabled: true });
    assert.equal(c.effectiveLimit(), 450);
    assert.equal(c.snapshot().budgetStatus, 'waiting_for_measurement');
    await c.updateSettings({ baselineMonthlyUsd: 4, costPer100MonthlyUsd: 8, measuredAt: new Date().toISOString() });
    assert.equal(c.effectiveLimit(), 180);
    assert.equal(c.snapshot().connected, 400);
    await assert.rejects(c.updateSettings({ costPer100MonthlyUsd: -1 }), /invalid_cost_model/);
    await assert.rejects(c.updateSettings({ manualLimit: 451 }), /invalid_admission_settings/);
});

test('Postgres restart preserves FIFO and fences the old process', {
    skip: !process.env.ADMISSION_TEST_DATABASE_URL
}, async () => {
    const { Pool } = require('pg');
    const schema = 'admission_test_' + require('crypto').randomBytes(8).toString('hex');
    const connectionString = process.env.ADMISSION_TEST_DATABASE_URL;
    const admin = new Pool({ connectionString });
    let pool;
    try {
        await admin.query(`CREATE SCHEMA ${schema}`);
        pool = new Pool({ connectionString, options: `-c search_path=${schema}` });
        const make = () => new AdmissionController({ pool, poolId: 'restart', hardLimit: 1,
            connected: () => 1, paused: () => false });
        const first = make(); await first.initialize();
        await first.request('one', 'KR', 'beta', true, 'session1');
        await first.request('two', 'KR', 'beta', true, 'session2');
        const replacement = make(); await replacement.initialize();
        await assert.rejects(first.request('three', 'KR', 'beta', true, 'session3'), /admission_owner_replaced/);
        assert.equal((await replacement.request('two', 'KR', 'beta', true, 'session2')).position, 2);
        await replacement.updateSettings({ manualLimit: 0 });
        const next = make(); await next.initialize();
        assert.equal(next.effectiveLimit(), 0);
        assert.equal((await next.request('one', 'KR', 'beta', true, 'session1')).position, 1);
    } finally {
        if (pool) await pool.end();
        await admin.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
        await admin.end();
    }
});
