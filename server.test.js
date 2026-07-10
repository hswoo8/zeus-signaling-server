const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const test = require('node:test');
const WebSocket = require('ws');

const versionFields = {
    clientVersionCode: 1,
    protocolVersion: 1,
    balanceVersion: 1,
};

function createInbox(ws) {
    const messages = [];
    const waiters = [];
    ws.on('message', (raw) => {
        const message = JSON.parse(raw.toString());
        const index = waiters.findIndex((waiter) => waiter.predicate(message));
        if (index >= 0) {
            const [waiter] = waiters.splice(index, 1);
            clearTimeout(waiter.timer);
            waiter.resolve(message);
        } else {
            messages.push(message);
        }
    });
    return {
        next(predicate, timeoutMs = 3000) {
            const index = messages.findIndex(predicate);
            if (index >= 0) return Promise.resolve(messages.splice(index, 1)[0]);
            return new Promise((resolve, reject) => {
                const waiter = { predicate, resolve, timer: null };
                waiter.timer = setTimeout(() => {
                    const pendingIndex = waiters.indexOf(waiter);
                    if (pendingIndex >= 0) waiters.splice(pendingIndex, 1);
                    reject(new Error('Timed out waiting for WebSocket message'));
                }, timeoutMs);
                waiters.push(waiter);
            });
        },
        type(type) {
            return this.next((message) => message.type === type);
        },
    };
}

function connect(url) {
    return new Promise((resolve, reject) => {
        const ws = new WebSocket(url);
        ws.once('open', () => resolve(ws));
        ws.once('error', reject);
    });
}

async function waitForServer(baseUrl, child) {
    const deadline = Date.now() + 5000;
    while (Date.now() < deadline) {
        if (child.exitCode !== null) throw new Error(`Server exited with ${child.exitCode}`);
        try {
            const response = await fetch(`${baseUrl}/health`);
            if (response.ok) return;
        } catch (_) {
        }
        await new Promise((resolve) => setTimeout(resolve, 50));
    }
    throw new Error('Server did not become ready');
}

function resultBody(matchId, localPlayer, remotePlayer, outcome) {
    return {
        mode: 'multi',
        serverMatchId: matchId,
        clientMatchId: matchId,
        arenaId: 'CLASSIC_OLYMPUS',
        outcome,
        finishReason: 'normal',
        durationSec: 20,
        completedAt: new Date().toISOString(),
        localPlayer,
        remotePlayer,
    };
}

test('server assigns per-round IDs and accepts only confirmed PvP results', async (t) => {
    const port = 21000 + Math.floor(Math.random() * 1000);
    const baseUrl = `http://127.0.0.1:${port}`;
    const child = spawn(process.execPath, ['server.js'], {
        cwd: __dirname,
        env: {
            ...process.env,
            PORT: String(port),
            DATABASE_URL: '',
            ADMIN_DASHBOARD_USERNAME: 'admin',
            ADMIN_DASHBOARD_PASSWORD: 'test-password',
        },
        stdio: ['ignore', 'pipe', 'pipe'],
    });
    let serverErrors = '';
    child.stderr.on('data', (chunk) => { serverErrors += chunk.toString(); });
    t.after(() => {
        if (child.exitCode === null) child.kill('SIGTERM');
    });
    await waitForServer(baseUrl, child);

    const unauthorizedAdmin = await fetch(`${baseUrl}/admin`);
    assert.equal(unauthorizedAdmin.status, 401);

    const launchEvent = {
        eventId: 'launch:test-1',
        eventName: 'app_launch',
        playerId: 'host-player-id',
        appVersionName: '1.2.3-debug',
        appVersionCode: 12,
        buildType: 'debug',
        platform: 'android',
        countryCode: 'KR',
        properties: { osVersion: '14', deviceModel: 'Test Phone' },
    };
    const launchAccepted = await fetch(`${baseUrl}/analytics/events`, {
        method: 'POST',
        headers: {
            'content-type': 'application/json',
            'user-agent': 'MiniZeus/1.2.3-debug (Android 14; Test Phone)',
            'x-country-code': 'KR',
        },
        body: JSON.stringify(launchEvent),
    });
    assert.equal(launchAccepted.status, 202);
    const launchDuplicate = await fetch(`${baseUrl}/analytics/events`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'x-country-code': 'KR' },
        body: JSON.stringify(launchEvent),
    });
    assert.equal(launchDuplicate.status, 200);

    const singleAccepted = await fetch(`${baseUrl}/analytics/events`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'x-country-code': 'KR' },
        body: JSON.stringify({
            ...launchEvent,
            eventId: 'single:test-1',
            eventName: 'single_match_complete',
            properties: { outcome: 'local_win', durationSec: 45, characterId: 'ZEUS' },
        }),
    });
    assert.equal(singleAccepted.status, 202);

    const host = await connect(`ws://127.0.0.1:${port}`);
    const guest = await connect(`ws://127.0.0.1:${port}`);
    t.after(() => {
        host.close();
        guest.close();
    });
    const hostInbox = createInbox(host);
    const guestInbox = createInbox(guest);
    const hostPlayer = {
        nickname: 'Host Player',
        playerId: 'host-player-id',
        characterId: 'ZEUS',
        passiveId: 'IRON_WILL',
        hp: 125,
    };
    const guestPlayer = {
        nickname: 'Guest Player',
        playerId: 'guest-player-id',
        characterId: 'ZEUS',
        passiveId: 'STORM_MASTERY',
        hp: 0,
    };

    host.send(JSON.stringify({
        type: 'create_room',
        ...versionFields,
        hostCharacterId: hostPlayer.characterId,
        hostPassiveId: hostPlayer.passiveId,
        hostNickname: hostPlayer.nickname,
        hostPlayerId: hostPlayer.playerId,
        arenaId: 'CLASSIC_OLYMPUS',
        battleType: 'short',
    }));
    const created = await hostInbox.type('room_created');
    guest.send(JSON.stringify({
        type: 'join_room',
        ...versionFields,
        code: created.code,
        guestCharacterId: guestPlayer.characterId,
        guestPassiveId: guestPlayer.passiveId,
        guestNickname: guestPlayer.nickname,
        guestPlayerId: guestPlayer.playerId,
        arenaId: 'CLASSIC_OLYMPUS',
    }));
    await Promise.all([hostInbox.type('guest_joined'), guestInbox.type('room_joined')]);

    host.send(JSON.stringify({ type: 'game_start', ...versionFields }));
    const [assigned, started] = await Promise.all([
        hostInbox.type('match_assigned'),
        guestInbox.type('game_start'),
    ]);
    assert.ok(assigned.matchId);
    assert.equal(started.matchId, assigned.matchId);

    host.send(JSON.stringify({
        type: 'game_over',
        roundId: 1,
        hp: 125,
        remoteHp: 0,
        outcome: 'win',
        reason: 'normal',
    }));
    const [hostResult, guestResult] = await Promise.all([
        hostInbox.type('match_result'),
        guestInbox.type('match_result'),
    ]);
    assert.equal(hostResult.outcome, 'win');
    assert.equal(guestResult.outcome, 'loss');
    assert.equal(hostResult.matchId, assigned.matchId);
    assert.equal(guestResult.matchId, assigned.matchId);

    const mismatch = await fetch(`${baseUrl}/matches/pvp-result`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(resultBody(assigned.matchId, guestPlayer, hostPlayer, 'win')),
    });
    assert.equal(mismatch.status, 409);
    assert.equal((await mismatch.json()).error.code, 'result_mismatch');

    const accepted = await fetch(`${baseUrl}/matches/pvp-result`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(resultBody(assigned.matchId, hostPlayer, guestPlayer, 'win')),
    });
    assert.equal(accepted.status, 201, serverErrors);

    const duplicate = await fetch(`${baseUrl}/matches/pvp-result`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(resultBody(assigned.matchId, guestPlayer, hostPlayer, 'loss')),
    });
    assert.equal(duplicate.status, 200, serverErrors);
    assert.equal((await duplicate.json()).duplicate, true);

    const stats = await fetch(`${baseUrl}/players/${hostPlayer.playerId}/stats?mode=multi`);
    assert.equal(stats.status, 200);
    assert.equal((await stats.json()).matches, 1);

    host.send(JSON.stringify({ type: 'game_start', ...versionFields }));
    const [rematchAssigned, rematchStarted] = await Promise.all([
        hostInbox.type('match_assigned'),
        guestInbox.type('game_start'),
    ]);
    assert.ok(rematchAssigned.matchId);
    assert.equal(rematchStarted.matchId, rematchAssigned.matchId);
    assert.notEqual(rematchAssigned.matchId, assigned.matchId);

    guest.send(JSON.stringify({
        type: 'game_over',
        roundId: 2,
        hp: 0,
        remoteHp: 125,
        outcome: 'loss',
        reason: 'local_forfeit',
    }));
    const [hostForfeitResult, guestForfeitResult] = await Promise.all([
        hostInbox.type('match_result'),
        guestInbox.type('match_result'),
    ]);
    assert.equal(hostForfeitResult.finishReason, 'remote_forfeit');
    assert.equal(guestForfeitResult.finishReason, 'local_forfeit');

    const forfeitAccepted = await fetch(`${baseUrl}/matches/pvp-result`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(resultBody(rematchAssigned.matchId, hostPlayer, guestPlayer, 'win')),
    });
    assert.equal(forfeitAccepted.status, 201, serverErrors);
    const forfeitResponse = await forfeitAccepted.json();
    assert.equal(forfeitResponse.players.local.rewardCoins, 20);

    const updatedStats = await fetch(`${baseUrl}/players/${hostPlayer.playerId}/stats?mode=multi`);
    assert.equal(updatedStats.status, 200);
    assert.equal((await updatedStats.json()).matches, 2);

    host.send(JSON.stringify({ type: 'game_start', ...versionFields }));
    const [disconnectAssigned] = await Promise.all([
        hostInbox.type('match_assigned'),
        guestInbox.type('game_start'),
    ]);
    guest.close();
    const disconnectResult = await hostInbox.type('match_result');
    assert.equal(disconnectResult.matchId, disconnectAssigned.matchId);
    assert.equal(disconnectResult.outcome, 'win');
    assert.equal(disconnectResult.finishReason, 'remote_disconnect');

    const disconnectAccepted = await fetch(`${baseUrl}/matches/pvp-result`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(resultBody(disconnectAssigned.matchId, hostPlayer, guestPlayer, 'win')),
    });
    assert.equal(disconnectAccepted.status, 201, serverErrors);
    assert.equal((await disconnectAccepted.json()).players.local.rewardCoins, 20);

    const finalStats = await fetch(`${baseUrl}/players/${hostPlayer.playerId}/stats?mode=multi`);
    assert.equal(finalStats.status, 200);
    assert.equal((await finalStats.json()).matches, 3);

    const adminAuthorization = `Basic ${Buffer.from('admin:test-password').toString('base64')}`;
    const adminStats = await fetch(`${baseUrl}/admin/api/stats`, {
        headers: { authorization: adminAuthorization },
    });
    assert.equal(adminStats.status, 200);
    const snapshot = await adminStats.json();
    assert.equal(snapshot.periods.retention.launches, 1);
    assert.equal(snapshot.periods.retention.singleMatches, 1);
    assert.equal(snapshot.periods.retention.multiMatches, 3);
    assert.equal(snapshot.countries.find((row) => row.country === 'KR').launches, 1);
    assert.equal(snapshot.suspiciousPairs[0].matches, 3);

    const adminPage = await fetch(`${baseUrl}/admin`, {
        headers: { authorization: adminAuthorization },
    });
    assert.equal(adminPage.status, 200);
    assert.match(await adminPage.text(), /MiniZeus 운영 통계/);
});
