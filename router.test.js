const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const test = require('node:test');

async function waitForServer(baseUrl, child) {
    const deadline = Date.now() + 5000;
    while (Date.now() < deadline) {
        if (child.exitCode !== null) throw new Error(`Router exited with ${child.exitCode}`);
        try {
            const response = await fetch(`${baseUrl}/health`);
            if (response.ok) return;
        } catch (_) {
        }
        await new Promise((resolve) => setTimeout(resolve, 50));
    }
    throw new Error('Router did not become ready');
}

test('router isolates channels and selects an exact ruleset pool', async (t) => {
    const port = 22000 + Math.floor(Math.random() * 1000);
    const baseUrl = `http://127.0.0.1:${port}`;
    const routes = [{
        poolId: 'prod-green',
        webSocketUrl: 'wss://green.example.test',
        statsApiUrl: 'https://green.example.test',
        minAppVersionCode: 10,
        maxAppVersionCode: 20,
        protocolVersion: 3,
        rulesetVersion: 2,
        minBalanceVersion: 7,
        maxBalanceVersion: 9,
    }];
    const child = spawn(process.execPath, ['router.js'], {
        cwd: __dirname,
        env: {
            ...process.env,
            PORT: String(port),
            ROUTER_CHANNEL: 'production',
            ROUTER_ALLOWED_CHANNELS: 'production',
            ROUTER_ROUTES_JSON: JSON.stringify(routes),
            ROUTER_STORE_URL: 'https://play.google.com/store/apps/details?id=test',
        },
        stdio: ['ignore', 'pipe', 'pipe'],
    });
    t.after(() => {
        if (child.exitCode === null) child.kill('SIGTERM');
    });
    await waitForServer(baseUrl, child);

    const available = await fetch(`${baseUrl}/route`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
            channel: 'production',
            appVersionCode: 12,
            protocolVersion: 3,
            rulesetVersion: 2,
            balanceVersion: 8,
        }),
    });
    assert.equal(available.status, 200);
    const route = await available.json();
    assert.equal(route.status, 'available');
    assert.equal(route.poolId, 'prod-green');

    const wrongEnvironment = await fetch(`${baseUrl}/route`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
            channel: 'beta',
            appVersionCode: 12,
            protocolVersion: 3,
            rulesetVersion: 2,
            balanceVersion: 8,
        }),
    });
    assert.equal(wrongEnvironment.status, 409);
    assert.equal((await wrongEnvironment.json()).code, 'wrong_environment');

    const incompatible = await fetch(`${baseUrl}/route`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
            channel: 'production',
            appVersionCode: 12,
            protocolVersion: 4,
            rulesetVersion: 2,
            balanceVersion: 8,
        }),
    });
    assert.equal(incompatible.status, 200);
    assert.equal((await incompatible.json()).status, 'update_required');
});
