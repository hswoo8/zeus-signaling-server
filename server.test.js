const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const test = require('node:test');
const WebSocket = require('ws');

const versionFields = {
    clientVersionCode: 1,
    clientVersionName: '1.2.3-debug',
    analyticsChannel: 'dev',
    protocolVersion: 1,
    rulesetVersion: 1,
    balanceVersion: 1,
};
const appJsonHeaders = { 'content-type': 'application/json', 'x-app-channel': 'dev' };
const appHeaders = { 'x-app-channel': 'dev' };

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

function connect(url, options = {}) {
    return new Promise((resolve, reject) => {
        const ws = new WebSocket(url, options);
        ws.once('open', () => resolve(ws));
        ws.once('error', reject);
        ws.once('unexpected-response', (_request, response) => {
            reject(new Error(`Unexpected WebSocket response: ${response.statusCode}`));
        });
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
            SERVER_CHANNEL: 'dev',
            SERVER_POOL_ID: 'test-pool',
            SERVER_ALLOWED_CHANNELS: 'dev,beta',
            MULTIPLAYER_RULESET_VERSION: '1',
            MULTIPLAYER_MAX_APP_VERSION_CODE: '1',
            MULTIPLAYER_MAX_PROTOCOL_VERSION: '1',
            MULTIPLAYER_MAX_BALANCE_VERSION: '1',
            WS_BACKPRESSURE_SOFT_BYTES: '0',
            WS_BACKPRESSURE_HARD_BYTES: '1048576',
        },
        stdio: ['ignore', 'pipe', 'pipe'],
    });
    let serverErrors = '';
    child.stderr.on('data', (chunk) => { serverErrors += chunk.toString(); });
    t.after(() => {
        if (child.exitCode === null) child.kill('SIGTERM');
    });
    await waitForServer(baseUrl, child);

    const health = await fetch(`${baseUrl}/health`).then((response) => response.json());
    assert.equal(health.channel, 'dev');
    assert.equal(health.poolId, 'test-pool');
    assert.equal(health.rulesetVersion, 1);
    assert.equal(health.operations.capacityRejections, 0);
    assert.equal(health.operations.relay.packets, 0);
    assert.equal(health.operations.relay.canStartNewMatch, true);
    assert.equal(health.operations.relay.maxActiveMatches, null);
    assert.equal(typeof health.operations.eventLoopLagMs.p95, 'number');

    const wrongEnvironmentCapacity = await fetch(
        `${baseUrl}/capacity?clientVersionCode=1&protocolVersion=1&rulesetVersion=1&balanceVersion=1&channel=production`
    );
    assert.equal(wrongEnvironmentCapacity.status, 200);
    const wrongEnvironmentStatus = await wrongEnvironmentCapacity.json();
    assert.equal(wrongEnvironmentStatus.code, 'wrong_environment');
    assert.equal(wrongEnvironmentStatus.canAcceptMatchmaking, false);

    const unauthorizedAdmin = await fetch(`${baseUrl}/admin`);
    assert.equal(unauthorizedAdmin.status, 401);

    const launchEvent = {
        eventId: 'launch:test-1',
        eventName: 'app_launch',
        playerId: 'host-player-id',
        appVersionName: '1.2.3-debug',
        appVersionCode: 12,
        buildType: 'debug',
        analyticsChannel: 'dev',
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

    const betaLaunchAccepted = await fetch(`${baseUrl}/analytics/events`, {
        method: 'POST',
        headers: {
            'content-type': 'application/json',
            'user-agent': 'MiniZeus/1.2.3 (Android 14; Test Phone; release; beta)',
            'x-country-code': 'KR',
        },
        body: JSON.stringify({
            ...launchEvent,
            eventId: 'launch:beta-1',
            playerId: 'beta-player-id',
            appVersionName: '1.2.3',
            buildType: 'release',
            analyticsChannel: 'beta',
        }),
    });
    assert.equal(betaLaunchAccepted.status, 202);

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
    const usageEvents = [
        { eventId: 'screen:home:1', eventName: 'screen_view', properties: { screen: 'home' } },
        { eventId: 'feature:single:1', eventName: 'feature_use', properties: { feature: 'single_open' } },
    ];
    for (const usage of usageEvents) {
        const response = await fetch(`${baseUrl}/analytics/events`, {
            method: 'POST',
            headers: { 'content-type': 'application/json', 'x-country-code': 'KR' },
            body: JSON.stringify({ ...launchEvent, ...usage }),
        });
        assert.equal(response.status, 202);
    }

    const roomListObserver = await connect(`ws://127.0.0.1:${port}`);
    const observerInbox = createInbox(roomListObserver);
    roomListObserver.send(JSON.stringify({ type: 'get_room_list', ...versionFields }));
    await observerInbox.type('room_list');

    const soloHost = await connect(`ws://127.0.0.1:${port}`);
    const soloInbox = createInbox(soloHost);
    soloHost.send(JSON.stringify({
        type: 'create_room',
        ...versionFields,
        hostCharacterId: 'ZEUS',
        hostPassiveId: 'IRON_WILL',
        arenaId: 'CLASSIC_OLYMPUS',
    }));
    const soloCreated = await soloInbox.type('room_created');
    const roomAdded = await observerInbox.type('room_updated');
    assert.equal(roomAdded.room.code, soloCreated.code);
    soloHost.send(JSON.stringify({ type: 'leave_room', ...versionFields }));
    const soloLeft = await soloInbox.type('room_left');
    assert.equal(soloLeft.code, soloCreated.code);
    const roomRemoved = await observerInbox.type('room_removed');
    assert.equal(roomRemoved.code, soloCreated.code);
    soloHost.send(JSON.stringify({ type: 'get_room_list', ...versionFields }));
    const soloRooms = await soloInbox.type('room_list');
    assert.equal(soloRooms.rooms.some((room) => room.code === soloCreated.code), false);
    soloHost.close();
    roomListObserver.close();

    const departingHost = await connect(`ws://127.0.0.1:${port}`);
    const promotedGuest = await connect(`ws://127.0.0.1:${port}`);
    const replacementGuest = await connect(`ws://127.0.0.1:${port}`);
    const departingInbox = createInbox(departingHost);
    const promotedInbox = createInbox(promotedGuest);
    const replacementInbox = createInbox(replacementGuest);
    departingHost.send(JSON.stringify({
        type: 'create_room',
        ...versionFields,
        hostCharacterId: 'ZEUS',
        hostPassiveId: 'IRON_WILL',
        arenaId: 'CLASSIC_OLYMPUS',
    }));
    const migrationRoom = await departingInbox.type('room_created');
    promotedGuest.send(JSON.stringify({
        type: 'join_room',
        ...versionFields,
        code: migrationRoom.code,
        guestCharacterId: 'ZEUS',
        guestPassiveId: 'STORM_MASTERY',
        arenaId: 'CLASSIC_OLYMPUS',
    }));
    await Promise.all([departingInbox.type('guest_joined'), promotedInbox.type('room_joined')]);
    departingHost.send(JSON.stringify({ type: 'leave_room', ...versionFields }));
    const [departingLeft, migrated] = await Promise.all([
        departingInbox.type('room_left'),
        promotedInbox.type('host_migrated'),
    ]);
    assert.equal(departingLeft.code, migrationRoom.code);
    assert.equal(migrated.code, migrationRoom.code);

    replacementGuest.send(JSON.stringify({
        type: 'join_room',
        ...versionFields,
        code: migrationRoom.code,
        guestCharacterId: 'ZEUS',
        guestPassiveId: 'IRON_WILL',
        arenaId: 'CLASSIC_OLYMPUS',
    }));
    await Promise.all([promotedInbox.type('guest_joined'), replacementInbox.type('room_joined')]);
    replacementGuest.send(JSON.stringify({ type: 'leave_room', ...versionFields }));
    const [replacementLeft] = await Promise.all([
        replacementInbox.type('room_left'),
        promotedInbox.type('peer_disconnected'),
    ]);
    assert.equal(replacementLeft.code, migrationRoom.code);
    promotedGuest.send(JSON.stringify({ type: 'leave_room', ...versionFields }));
    await promotedInbox.type('room_left');
    departingHost.close();
    promotedGuest.close();
    replacementGuest.close();

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

    host.send(JSON.stringify({ type: 'game_state', x: 10, y: 20 }));
    await new Promise((resolve) => setTimeout(resolve, 25));
    const backpressureCapacity = await fetch(`${baseUrl}/capacity`).then((response) => response.json());
    assert.equal(backpressureCapacity.backpressure.droppedStatePackets, 1);
    assert.equal(backpressureCapacity.backpressure.closedConnections, 0);
    assert.equal(backpressureCapacity.counts.activeRelayMatches, 1);
    assert.equal(backpressureCapacity.counts.activeP2pMatches, 0);
    assert.equal(backpressureCapacity.operations.relay.packets, 1);
    assert.ok(backpressureCapacity.operations.relay.bytes > 0);
    assert.equal(backpressureCapacity.operations.backpressure.droppedStatePackets, 1);
    assert.equal(backpressureCapacity.relay.canStartNewMatch, true);

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
        headers: appJsonHeaders,
        body: JSON.stringify(resultBody(assigned.matchId, guestPlayer, hostPlayer, 'win')),
    });
    assert.equal(mismatch.status, 409);
    assert.equal((await mismatch.json()).error.code, 'result_mismatch');

    const accepted = await fetch(`${baseUrl}/matches/pvp-result`, {
        method: 'POST',
        headers: appJsonHeaders,
        body: JSON.stringify(resultBody(assigned.matchId, hostPlayer, guestPlayer, 'win')),
    });
    assert.equal(accepted.status, 201, serverErrors);

    const duplicate = await fetch(`${baseUrl}/matches/pvp-result`, {
        method: 'POST',
        headers: appJsonHeaders,
        body: JSON.stringify(resultBody(assigned.matchId, guestPlayer, hostPlayer, 'loss')),
    });
    assert.equal(duplicate.status, 200, serverErrors);
    assert.equal((await duplicate.json()).duplicate, true);

    const stats = await fetch(`${baseUrl}/players/${hostPlayer.playerId}/stats?mode=multi`, { headers: appHeaders });
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
        headers: appJsonHeaders,
        body: JSON.stringify(resultBody(rematchAssigned.matchId, hostPlayer, guestPlayer, 'win')),
    });
    assert.equal(forfeitAccepted.status, 201, serverErrors);
    const forfeitResponse = await forfeitAccepted.json();
    assert.equal(forfeitResponse.players.local.rewardCoins, 20);

    const updatedStats = await fetch(`${baseUrl}/players/${hostPlayer.playerId}/stats?mode=multi`, { headers: appHeaders });
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
        headers: appJsonHeaders,
        body: JSON.stringify(resultBody(disconnectAssigned.matchId, hostPlayer, guestPlayer, 'win')),
    });
    assert.equal(disconnectAccepted.status, 201, serverErrors);
    assert.equal((await disconnectAccepted.json()).players.local.rewardCoins, 20);

    const finalStats = await fetch(`${baseUrl}/players/${hostPlayer.playerId}/stats?mode=multi`, { headers: appHeaders });
    assert.equal(finalStats.status, 200);
    assert.equal((await finalStats.json()).matches, 3);

    const adminAuthorization = `Basic ${Buffer.from('admin:test-password').toString('base64')}`;
    const adminStats = await fetch(`${baseUrl}/admin/api/stats`, {
        headers: { authorization: adminAuthorization },
    });
    assert.equal(adminStats.status, 200);
    const snapshot = await adminStats.json();
    assert.equal(snapshot.daily.length, 30);
    assert.equal(snapshot.periods.retention.launches, 2);
    assert.equal(snapshot.periods.retention.singleMatches, 1);
    assert.equal(snapshot.periods.retention.multiMatches, 3);
    assert.equal(snapshot.kpis.dau, 3);
    assert.equal(snapshot.kpis.wau, 3);
    assert.equal(snapshot.kpis.mau, 3);
    assert.equal(snapshot.screenViews[0].screen, 'home');
    assert.equal(snapshot.featureUsage[0].feature, 'single_open');
    assert.equal(snapshot.countries.find((row) => row.country === 'KR').launches, 2);
    assert.equal(snapshot.suspiciousPairs[0].matches, 3);
    assert.ok(snapshot.operations.relay.packets > 0);
    assert.equal(typeof snapshot.operations.eventLoopLagMs.p95, 'number');

    const betaStats = await fetch(`${baseUrl}/admin/api/stats?channel=beta`, {
        headers: { authorization: adminAuthorization },
    });
    assert.equal(betaStats.status, 200);
    const betaSnapshot = await betaStats.json();
    assert.equal(betaSnapshot.selectedChannel, 'beta');
    assert.equal(betaSnapshot.periods.retention.launches, 1);
    assert.equal(betaSnapshot.periods.retention.multiMatches, 0);
    assert.equal(betaSnapshot.kpis.dau, 1);

    const productionStats = await fetch(`${baseUrl}/admin/api/stats?channel=production`, {
        headers: { authorization: adminAuthorization },
    });
    assert.equal(productionStats.status, 200);
    const productionSnapshot = await productionStats.json();
    assert.equal(productionSnapshot.selectedChannel, 'production');
    assert.equal(productionSnapshot.periods.retention.launches, 0);

    const devStats = await fetch(`${baseUrl}/admin/api/stats?channel=dev`, {
        headers: { authorization: adminAuthorization },
    });
    assert.equal(devStats.status, 200);
    const devSnapshot = await devStats.json();
    assert.equal(devSnapshot.periods.retention.launches, 1);
    assert.equal(devSnapshot.periods.retention.multiMatches, 3);

    const adminPage = await fetch(`${baseUrl}/admin`, {
        headers: { authorization: adminAuthorization },
    });
    assert.equal(adminPage.status, 200);
    const adminHtml = await adminPage.text();
    assert.match(adminHtml, /MiniZeus 운영 통계/);
    assert.match(adminHtml, /최근 30일 활동 추이/);
    assert.match(adminHtml, /class="line-chart"/);
    assert.match(adminHtml, /국가\/지역 활동량/);
    assert.match(adminHtml, /class="channel-filter"/);
    assert.match(adminHtml, /전체/);
    assert.match(adminHtml, /베타/);
    assert.match(adminHtml, /운영/);
    assert.match(adminHtml, /개발/);
    assert.match(adminHtml, /Relay 전송/);
    assert.match(adminHtml, /Relay 최근 1시간/);
    assert.match(adminHtml, /Relay 진입 거부/);
    assert.match(adminHtml, /이벤트 루프 p95/);
    assert.match(adminHtml, /송신 지연 보호/);
});

test('relay admission limits preserve P2P matches and reject new relay starts', async (t) => {
    const port = 22000 + Math.floor(Math.random() * 1000);
    const baseUrl = `http://127.0.0.1:${port}`;
    const child = spawn(process.execPath, ['server.js'], {
        cwd: __dirname,
        env: {
            ...process.env,
            PORT: String(port),
            DATABASE_URL: '',
            SERVER_CHANNEL: 'dev',
            SERVER_ALLOWED_CHANNELS: 'dev',
            MULTIPLAYER_RULESET_VERSION: '1',
            MAX_ACTIVE_RELAY_MATCHES: '1',
            AUTH_TOKEN_SECRET: '',
        },
        stdio: ['ignore', 'pipe', 'pipe'],
    });
    let serverErrors = '';
    child.stderr.on('data', (chunk) => { serverErrors += chunk.toString(); });
    t.after(() => {
        if (child.exitCode === null) child.kill('SIGTERM');
    });
    await waitForServer(baseUrl, child);

    const relayHost = await connect(`ws://127.0.0.1:${port}`);
    const relayGuest = await connect(`ws://127.0.0.1:${port}`);
    const candidateHost = await connect(`ws://127.0.0.1:${port}`);
    const candidateGuest = await connect(`ws://127.0.0.1:${port}`);
    const relayHostInbox = createInbox(relayHost);
    const relayGuestInbox = createInbox(relayGuest);
    const candidateHostInbox = createInbox(candidateHost);
    const candidateGuestInbox = createInbox(candidateGuest);
    t.after(() => {
        relayHost.close();
        relayGuest.close();
        candidateHost.close();
        candidateGuest.close();
    });

    const createPair = async (host, hostInbox, guest, guestInbox, mode) => {
        host.send(JSON.stringify({ type: 'create_room', ...versionFields, networkMode: mode }));
        const created = await hostInbox.type('room_created');
        guest.send(JSON.stringify({ type: 'join_room', ...versionFields, code: created.code }));
        await Promise.all([hostInbox.type('guest_joined'), guestInbox.type('room_joined')]);
        return created.code;
    };

    await createPair(relayHost, relayHostInbox, relayGuest, relayGuestInbox, 'relay');
    relayHost.send(JSON.stringify({ type: 'game_start', ...versionFields, activeTransport: 'relay' }));
    await Promise.all([relayHostInbox.type('match_assigned'), relayGuestInbox.type('game_start')]);

    candidateHost.send(JSON.stringify({ type: 'create_room', ...versionFields, networkMode: 'relay' }));
    const explicitRelayError = await candidateHostInbox.type('error');
    assert.equal(explicitRelayError.code, 'relay_capacity');

    await createPair(candidateHost, candidateHostInbox, candidateGuest, candidateGuestInbox, 'auto');
    candidateHost.send(JSON.stringify({ type: 'game_start', ...versionFields, activeTransport: 'relay' }));
    const [hostFallbackError, guestFallbackError] = await Promise.all([
        candidateHostInbox.type('error'),
        candidateGuestInbox.type('error'),
    ]);
    assert.equal(hostFallbackError.code, 'relay_capacity');
    assert.equal(guestFallbackError.code, 'relay_capacity');

    await createPair(candidateHost, candidateHostInbox, candidateGuest, candidateGuestInbox, 'auto');
    candidateHost.send(JSON.stringify({ type: 'game_start', ...versionFields, activeTransport: 'p2p' }));
    await Promise.all([candidateHostInbox.type('match_assigned'), candidateGuestInbox.type('game_start')]);

    const capacity = await fetch(`${baseUrl}/capacity`).then((response) => response.json());
    assert.equal(capacity.counts.activeRelayMatches, 1, serverErrors);
    assert.equal(capacity.counts.activeP2pMatches, 1, serverErrors);
    assert.equal(capacity.relay.canStartNewMatch, false);
    assert.equal(capacity.relay.code, 'relay_capacity');
    assert.equal(capacity.operations.relay.admissionRejections, 2);

    candidateHost.send(JSON.stringify({ type: 'game_state', seq: 1, x: 10, y: 20 }));
    await candidateGuestInbox.type('game_state');
    const fallbackCapacity = await fetch(`${baseUrl}/capacity`).then((response) => response.json());
    assert.equal(fallbackCapacity.counts.activeRelayMatches, 2);
    assert.equal(fallbackCapacity.counts.activeP2pMatches, 0);
    assert.equal(fallbackCapacity.operations.relay.runtimeFallbacks, 1);
});

test('guest access tokens issue one-time WebSocket tickets', async (t) => {
    const port = 22000 + Math.floor(Math.random() * 1000);
    const baseUrl = `http://127.0.0.1:${port}`;
    const child = spawn(process.execPath, ['server.js'], {
        cwd: __dirname,
        env: {
            ...process.env,
            PORT: String(port),
            DATABASE_URL: '',
            SERVER_CHANNEL: 'dev',
            SERVER_ALLOWED_CHANNELS: 'dev',
            MULTIPLAYER_RULESET_VERSION: '1',
            AUTH_TOKEN_SECRET: 'test-secret-that-is-longer-than-thirty-two-characters',
        },
        stdio: ['ignore', 'pipe', 'pipe'],
    });
    t.after(() => {
        if (child.exitCode === null) child.kill('SIGTERM');
    });
    await waitForServer(baseUrl, child);

    await assert.rejects(connect(`ws://127.0.0.1:${port}`), /401/);

    const registration = await fetch(`${baseUrl}/auth/guest/register`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ playerId: 'auth-test-player', analyticsChannel: 'dev' }),
    });
    assert.equal(registration.status, 201);
    const guest = await registration.json();
    assert.ok(guest.guestToken);

    const tokenResponse = await fetch(`${baseUrl}/auth/token`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ guestToken: guest.guestToken }),
    });
    assert.equal(tokenResponse.status, 200);
    const access = await tokenResponse.json();
    assert.ok(access.accessToken);

    const ticketResponse = await fetch(`${baseUrl}/auth/ws-ticket`, {
        method: 'POST',
        headers: {
            'content-type': 'application/json',
            authorization: `Bearer ${access.accessToken}`,
        },
        body: '{}',
    });
    assert.equal(ticketResponse.status, 201);
    const ticket = await ticketResponse.json();
    assert.ok(ticket.ticket);

    const authenticated = await connect(`ws://127.0.0.1:${port}`, {
        headers: { authorization: `Bearer ${ticket.ticket}` },
    });
    const inbox = createInbox(authenticated);
    authenticated.send(JSON.stringify({ type: 'get_room_list', ...versionFields }));
    assert.equal((await inbox.type('room_list')).type, 'room_list');
    authenticated.close();

    await assert.rejects(connect(`ws://127.0.0.1:${port}`, {
        headers: { authorization: `Bearer ${ticket.ticket}` },
    }), /401/);
});
