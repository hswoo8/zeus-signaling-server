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
            APP_LATEST_VERSION_CODE: '20',
            APP_MIN_SUPPORTED_VERSION_CODE: '10',
            APP_UPDATE_MODE: 'none',
            APP_UPDATE_MESSAGE: '새 버전을 설치해주세요.',
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

test('app policy validates requests and resolves disabled and minimum-version force policies', async (t) => {
    const port = 23000 + Math.floor(Math.random() * 1000);
    const baseUrl = `http://127.0.0.1:${port}`;
    const child = spawn(process.execPath, ['router.js'], {
        cwd: __dirname,
        env: {
            ...process.env,
            PORT: String(port),
            ROUTER_CHANNEL: 'production',
            ROUTER_ALLOWED_CHANNELS: 'production',
            ROUTER_ROUTES_JSON: '[]',
            ROUTER_STORE_URL: 'https://play.google.com/store/apps/details?id=test',
            APP_LATEST_VERSION_CODE: '20',
            APP_MIN_SUPPORTED_VERSION_CODE: '10',
            APP_UPDATE_MODE: 'none',
            APP_UPDATE_MESSAGE: '새 버전을 설치해주세요.',
        },
        stdio: ['ignore', 'pipe', 'pipe'],
    });
    t.after(() => {
        if (child.exitCode === null) child.kill('SIGTERM');
    });
    await waitForServer(baseUrl, child);

    const healthResponse = await fetch(`${baseUrl}/health`);
    assert.equal(healthResponse.headers.get('cache-control'), 'no-store');
    const health = await healthResponse.json();
    assert.deepEqual(health.appPolicy, {
        latestVersionCode: 20,
        minSupportedVersionCode: 10,
        mode: 'none',
    });

    const belowMinimum = await fetch(
        `${baseUrl}/app-policy?channel=production&versionCode=9`
    );
    assert.equal(belowMinimum.status, 200);
    assert.equal(belowMinimum.headers.get('cache-control'), 'no-store');
    assert.deepEqual(await belowMinimum.json(), {
        status: 'ok',
        code: 'ok',
        policy: 'force',
        channel: 'production',
        currentVersionCode: 9,
        latestVersionCode: 20,
        minSupportedVersionCode: 10,
        message: '새 버전을 설치해주세요.',
        storeUrl: 'https://play.google.com/store/apps/details?id=test',
    });

    const updateAvailable = await fetch(
        `${baseUrl}/app-policy?channel=production&versionCode=10`
    );
    assert.equal((await updateAvailable.json()).policy, 'none');

    const current = await fetch(
        `${baseUrl}/app-policy?channel=production&versionCode=20`
    );
    assert.deepEqual(await current.json(), {
        status: 'ok',
        code: 'ok',
        policy: 'none',
        channel: 'production',
        currentVersionCode: 20,
        latestVersionCode: 20,
        minSupportedVersionCode: 10,
        message: null,
        storeUrl: null,
    });

    const wrongEnvironment = await fetch(
        `${baseUrl}/app-policy?channel=beta&versionCode=20`
    );
    assert.equal(wrongEnvironment.status, 409);
    assert.equal(wrongEnvironment.headers.get('cache-control'), 'no-store');
    assert.equal((await wrongEnvironment.json()).code, 'wrong_environment');

    for (const versionCode of ['', '0', '-1', '12abc', '1.5', '9007199254740992']) {
        const invalid = await fetch(
            `${baseUrl}/app-policy?channel=production&versionCode=${encodeURIComponent(versionCode)}`
        );
        assert.equal(invalid.status, 400);
        const body = await invalid.json();
        assert.equal(body.status, 'invalid_request');
        assert.equal(body.code, 'invalid_request');
    }
});

test('app policy honors configured soft and force modes below latest', async (t) => {
    for (const [index, mode] of ['soft', 'force'].entries()) {
        await t.test(mode, async (t) => {
            const port = 24000 + index * 1000 + Math.floor(Math.random() * 1000);
            const baseUrl = `http://127.0.0.1:${port}`;
            const child = spawn(process.execPath, ['router.js'], {
                cwd: __dirname,
                env: {
                    ...process.env,
                    PORT: String(port),
                    ROUTER_CHANNEL: 'beta',
                    ROUTER_ALLOWED_CHANNELS: 'beta',
                    ROUTER_ROUTES_JSON: '[]',
                    APP_LATEST_VERSION_CODE: '30',
                    APP_MIN_SUPPORTED_VERSION_CODE: '20',
                    APP_UPDATE_MODE: mode,
                },
                stdio: ['ignore', 'pipe', 'pipe'],
            });
            t.after(() => {
                if (child.exitCode === null) child.kill('SIGTERM');
            });
            await waitForServer(baseUrl, child);

            const response = await fetch(
                `${baseUrl}/app-policy?channel=beta&versionCode=25`
            );
            assert.equal(response.status, 200);
            assert.equal((await response.json()).policy, mode);
        });
    }
});
