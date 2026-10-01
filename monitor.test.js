// User-run checks. Implementation work only performed node --check, not these tests.
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const { ServerMonitor, compareWindows } = require('./monitor');

test('5xx failures are separate from expected admission rejection and client errors', () => {
    const m = new ServerMonitor({ pool: null, poolId: 'test' });
    const reply = (url, code, reason) => {
        const res = new EventEmitter(); res.statusCode = code; res.monitorErrorCode = reason;
        m.observeHttp({ url }, res); res.emit('finish'); res.emit('close');
    };
    reply('/auth/ws-ticket', 503, 'server_busy');
    reply('/auth/ws-ticket', 503, 'server_maintenance');
    reply('/auth/ws-ticket', 503, 'auth_disabled');
    reply('/auth/ws-ticket', 401, 'invalid_access_token');
    reply('/matches/result', 500, 'internal_error');
    reply('/health', 200); reply('/admin/api/monitor', 500); reply('/does-not-exist', 404);
    const b = m.snapshot().currentMinute;
    assert.equal(b.requests, 5); assert.equal(b.failures, 1);
    assert.equal(b.expectedRejections, 3); assert.equal(b.clientErrors, 1); assert.equal(b.aborted, 0);
});

test('incomplete HTTP responses are tracked without pretending to know the final status', () => {
    const m = new ServerMonitor({ pool: null, poolId: 'test' });
    const res = new EventEmitter(); m.observeHttp({ url: '/matches/result' }, res); res.emit('close');
    assert.equal(m.snapshot().currentMinute.aborted, 1);
    assert.equal(m.snapshot().currentMinute.requests, 0);
});

test('low sample rates do not become a rising-error alert', () => {
    assert.equal(compareWindows({ requests: 1, failures: 1 }, { requests: 1, failures: 0 }).trend, 'insufficient_samples');
    assert.equal(compareWindows({ requests: 0, failures: 0 }, { requests: 0, failures: 0 }).currentRate, null);
});

test('sustained rise uses both rate and error count, not traffic growth alone', () => {
    assert.equal(compareWindows({ requests: 100, failures: 8 }, { requests: 100, failures: 1 }).trend, 'rising');
    assert.equal(compareWindows({ requests: 1000, failures: 10 }, { requests: 100, failures: 1 }).trend, 'stable');
    assert.equal(compareWindows({ requests: 100, failures: 2 }, { requests: 100, failures: 0 }).trend, 'stable');
});

test('completed five-minute windows, restart warmup and stale peer state are explicit', () => {
    let time = 0;
    const m = new ServerMonitor({ pool: null, poolId: 'test', now: () => time,
        targets: [{ name: 'peer', url: 'https://example.com/health' }] });
    m.record('requests', 100); m.record('failures', 1);
    assert.equal(m.snapshot().comparison.trend, 'warming_up');
    time = 5 * 60000; m.record('requests', 100); m.record('failures', 10);
    m.peers[0].state = 'ok'; m.peers[0].checkedAt = new Date(time).toISOString();
    time = 10 * 60000;
    const s = m.snapshot();
    assert.equal(s.comparison.current.failures, 10); assert.equal(s.comparison.previous.failures, 1);
    assert.equal(s.comparison.trend, 'rising'); assert.equal(s.peers[0].state, 'stale');
});
