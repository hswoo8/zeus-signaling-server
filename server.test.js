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
        async type(type, timeoutMs = 3000) {
            try {
                return await this.next((message) => message.type === type, timeoutMs);
            } catch (error) {
                throw new Error(`Timed out waiting for WebSocket message type=${type}`, {
                    cause: error,
                });
            }
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
            RANK_PLACEMENT_MATCHES: '2',
            RANK_PLACEMENT_K: '48',
            RANK_ESTABLISHED_K: '24',
            QUALITY_REJECT_COOLDOWN_MS: '60000',
            POPULATION_BROADCAST_DEBOUNCE_MS: '50',
            NICKNAME_BLOCKED_TERMS: 'spoilername',
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

    const invalidRoomClient = await connect(`ws://127.0.0.1:${port}`);
    const invalidRoomInbox = createInbox(invalidRoomClient);
    invalidRoomClient.send(JSON.stringify({
        type: 'join_room',
        ...versionFields,
        code: 'bad',
    }));
    const invalidRoom = await invalidRoomInbox.type('error');
    assert.equal(invalidRoom.code, 'invalid_room_code');
    invalidRoomClient.send(JSON.stringify({
        type: 'join_room',
        ...versionFields,
        code: '9999',
    }));
    const missingRoom = await invalidRoomInbox.type('error');
    assert.equal(missingRoom.code, 'room_not_found');
    invalidRoomClient.send(JSON.stringify({
        type: 'selection_update',
        ...versionFields,
        nickname: '관리자',
    }));
    const invalidNickname = await invalidRoomInbox.type('error');
    assert.equal(invalidNickname.code, 'nickname_invalid');
    invalidRoomClient.send(JSON.stringify({
        type: 'selection_update',
        ...versionFields,
        nickname: 'OfficialMiniZeus',
    }));
    const reservedNickname = await invalidRoomInbox.type('error');
    assert.equal(reservedNickname.code, 'nickname_invalid');
    invalidRoomClient.send(JSON.stringify({
        type: 'selection_update',
        ...versionFields,
        nickname: 'boZIna',
    }));
    const obfuscatedInvalidNickname = await invalidRoomInbox.type('error');
    assert.equal(obfuscatedInvalidNickname.code, 'nickname_invalid');
    invalidRoomClient.send(JSON.stringify({
        type: 'selection_update',
        ...versionFields,
        nickname: 'ㅂㅗㅈㅣ골키퍼',
    }));
    const jamoInvalidNickname = await invalidRoomInbox.type('error');
    assert.equal(jamoInvalidNickname.code, 'nickname_invalid');
    invalidRoomClient.send(JSON.stringify({
        type: 'selection_update',
        ...versionFields,
        nickname: 'b o z i na',
    }));
    const spacedInvalidNickname = await invalidRoomInbox.type('error');
    assert.equal(spacedInvalidNickname.code, 'nickname_invalid');
    invalidRoomClient.send(JSON.stringify({
        type: 'selection_update',
        ...versionFields,
        nickname: 'Spoiler_Name',
    }));
    const configuredInvalidNickname = await invalidRoomInbox.type('error');
    assert.equal(configuredInvalidNickname.code, 'nickname_invalid');
    invalidRoomClient.close();
    assert.equal(health.operations.relay.maxActiveMatches, null);
    assert.equal(typeof health.operations.eventLoopLagMs.p95, 'number');
    assert.equal(health.operations.websocketDisconnects.total, 0);
    assert.equal(health.operations.websocketDisconnects.abnormal, 0);
    assert.equal(health.operations.websocketDisconnects.heartbeatTimeouts, 0);
    assert.equal(health.operations.integrityAudits.received, 0);
    assert.equal(health.operations.integrityAudits.flaggedMatches, 0);
    assert.equal(health.operations.integrityAudits.thresholds.invalidReports, 3);
    assert.equal(health.operations.integrityAudits.thresholds.consecutiveHpMismatches, 3);
    assert.equal(health.ready, true);
    assert.equal(health.acceptingConnections, true);
    assert.equal(health.deployment.draining, false);
    assert.equal(health.deployment.activeMatchesDrained, true);

    const wrongEnvironmentCapacity = await fetch(
        `${baseUrl}/capacity?clientVersionCode=1&protocolVersion=1&rulesetVersion=1&balanceVersion=1&channel=production`
    );
    assert.equal(wrongEnvironmentCapacity.status, 200);
    const wrongEnvironmentStatus = await wrongEnvironmentCapacity.json();
    assert.equal(wrongEnvironmentStatus.code, 'wrong_environment');
    assert.equal(wrongEnvironmentStatus.canAcceptMatchmaking, false);

    const unauthorizedAdmin = await fetch(`${baseUrl}/admin`);
    assert.equal(unauthorizedAdmin.status, 401);

    const deploymentAdminAuthorization = `Basic ${Buffer.from('admin:test-password').toString('base64')}`;
    const unauthorizedDrain = await fetch(`${baseUrl}/admin/api/deployment/drain`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ enabled: true }),
    });
    assert.equal(unauthorizedDrain.status, 401);

    const existingDuringDrain = await connect(`ws://127.0.0.1:${port}`);
    const existingDuringDrainInbox = createInbox(existingDuringDrain);
    const drainEnabledResponse = await fetch(`${baseUrl}/admin/api/deployment/drain`, {
        method: 'POST',
        headers: {
            authorization: deploymentAdminAuthorization,
            'content-type': 'application/json',
        },
        body: JSON.stringify({ enabled: true }),
    });
    assert.equal(drainEnabledResponse.status, 200);
    const drainEnabled = await drainEnabledResponse.json();
    assert.equal(drainEnabled.draining, true);
    assert.equal(drainEnabled.acceptingConnections, false);
    assert.equal(drainEnabled.activeMatchesDrained, true);

    const drainedCapacity = await fetch(
        `${baseUrl}/capacity?clientVersionCode=1&protocolVersion=1&rulesetVersion=1&balanceVersion=1&channel=dev`
    ).then((response) => response.json());
    assert.equal(drainedCapacity.code, 'server_maintenance');
    assert.equal(drainedCapacity.canConnect, false);
    assert.equal(drainedCapacity.canAcceptMatchmaking, false);

    existingDuringDrain.send(JSON.stringify({ type: 'get_room_list', ...versionFields }));
    assert.equal((await existingDuringDrainInbox.type('room_list')).type, 'room_list');

    const rejectedDuringDrain = await connect(`ws://127.0.0.1:${port}`);
    const rejectedCloseCode = await new Promise((resolve) => {
        rejectedDuringDrain.once('close', resolve);
    });
    assert.equal(rejectedCloseCode, 1013);

    const drainedHealth = await fetch(`${baseUrl}/health`).then((response) => response.json());
    assert.equal(drainedHealth.ready, true);
    assert.equal(drainedHealth.acceptingConnections, false);
    assert.equal(drainedHealth.deployment.draining, true);

    const drainDisabledResponse = await fetch(`${baseUrl}/admin/api/deployment/drain`, {
        method: 'POST',
        headers: {
            authorization: deploymentAdminAuthorization,
            'content-type': 'application/json',
        },
        body: JSON.stringify({ enabled: false }),
    });
    assert.equal(drainDisabledResponse.status, 200);
    assert.equal((await drainDisabledResponse.json()).acceptingConnections, true);
    existingDuringDrain.close();

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

    const populationViewer = await connect(`ws://127.0.0.1:${port}`, {
        headers: { 'x-country-code': 'KR' },
    });
    const populationBrowser = await connect(`ws://127.0.0.1:${port}`, {
        headers: { 'x-country-code': 'KR' },
    });
    const populationViewerInbox = createInbox(populationViewer);
    const populationBrowserInbox = createInbox(populationBrowser);
    populationViewer.send(JSON.stringify({
        type: 'lobby_presence',
        ...versionFields,
        active: true,
        selectedMode: 'friendly',
    }));
    await populationViewerInbox.next((message) =>
        message.type === 'population_updated' && message.lobbyUsers === 1
    );
    populationBrowser.send(JSON.stringify({
        type: 'lobby_presence',
        ...versionFields,
        active: true,
        selectedMode: 'friendly',
    }));
    await populationBrowserInbox.type('population_updated');
    await populationViewerInbox.next((message) =>
        message.type === 'population_updated' && message.lobbyUsers === 2
    );

    const publicRoomHost = await connect(`ws://127.0.0.1:${port}`, {
        headers: { 'x-country-code': 'KR' },
    });
    const publicRoomHostInbox = createInbox(publicRoomHost);
    publicRoomHost.send(JSON.stringify({
        type: 'lobby_presence',
        ...versionFields,
        active: true,
        selectedMode: 'friendly',
    }));
    await publicRoomHostInbox.type('population_updated');
    publicRoomHost.send(JSON.stringify({
        type: 'create_room',
        ...versionFields,
        matchMode: 'friendly',
        matchmaking: false,
        hostPlayerId: 'population-public-host',
    }));
    await publicRoomHostInbox.type('room_created');
    await populationViewerInbox.next((message) =>
        message.type === 'population_updated' &&
        message.lobbyUsers === 3 &&
        message.availableFriendlyRooms === 1 &&
        message.friendlySearching === 0
    );
    publicRoomHost.send(JSON.stringify({ type: 'leave_room', ...versionFields }));
    await publicRoomHostInbox.type('room_left');
    publicRoomHost.close();

    const friendlySearchHost = await connect(`ws://127.0.0.1:${port}`, {
        headers: { 'x-country-code': 'KR' },
    });
    const friendlySearchHostInbox = createInbox(friendlySearchHost);
    friendlySearchHost.send(JSON.stringify({
        type: 'lobby_presence',
        ...versionFields,
        active: true,
        selectedMode: 'friendly',
    }));
    await friendlySearchHostInbox.type('population_updated');
    friendlySearchHost.send(JSON.stringify({
        type: 'create_room',
        ...versionFields,
        matchMode: 'friendly',
        matchmaking: true,
        hostPlayerId: 'population-friendly-search-host',
    }));
    await friendlySearchHostInbox.type('room_created');
    await populationViewerInbox.next((message) =>
        message.type === 'population_updated' &&
        message.availableFriendlyRooms === 1 &&
        message.friendlySearching === 1
    );
    friendlySearchHost.send(JSON.stringify({ type: 'leave_room', ...versionFields }));
    await friendlySearchHostInbox.type('room_left');
    friendlySearchHost.close();

    const rankedSearchHost = await connect(`ws://127.0.0.1:${port}`, {
        headers: { 'x-country-code': 'KR' },
    });
    const rankedSearchHostInbox = createInbox(rankedSearchHost);
    rankedSearchHost.send(JSON.stringify({
        type: 'lobby_presence',
        ...versionFields,
        active: true,
        selectedMode: 'ranked',
    }));
    await rankedSearchHostInbox.type('population_updated');
    rankedSearchHost.send(JSON.stringify({
        type: 'create_room',
        ...versionFields,
        matchMode: 'ranked',
        hostPlayerId: 'population-ranked-search-host',
    }));
    await rankedSearchHostInbox.type('room_created');
    await populationViewerInbox.next((message) =>
        message.type === 'population_updated' && message.rankedSearching === 1
    );
    rankedSearchHost.send(JSON.stringify({ type: 'leave_room', ...versionFields }));
    await rankedSearchHostInbox.type('room_left');
    rankedSearchHost.close();
    populationViewer.send(JSON.stringify({
        type: 'lobby_presence',
        ...versionFields,
        active: false,
        selectedMode: 'friendly',
    }));
    populationBrowser.send(JSON.stringify({
        type: 'lobby_presence',
        ...versionFields,
        active: false,
        selectedMode: 'friendly',
    }));
    populationViewer.close();
    populationBrowser.close();

    const staleHost = await connect(`ws://127.0.0.1:${port}`);
    const replacementHost = await connect(`ws://127.0.0.1:${port}`);
    const staleHostInbox = createInbox(staleHost);
    const replacementHostInbox = createInbox(replacementHost);
    staleHost.send(JSON.stringify({
        type: 'create_room',
        ...versionFields,
        hostPlayerId: 'duplicate-waiting-host',
        matchMode: 'ranked',
    }));
    const staleRoom = await staleHostInbox.type('room_created');
    replacementHost.send(JSON.stringify({
        type: 'create_room',
        ...versionFields,
        hostPlayerId: 'duplicate-waiting-host',
        matchMode: 'ranked',
    }));
    const [staleRoomLeft, replacementRoom] = await Promise.all([
        staleHostInbox.type('room_left'),
        replacementHostInbox.type('room_created'),
    ]);
    assert.equal(staleRoomLeft.code, staleRoom.code);
    assert.equal(staleRoomLeft.reason, 'replaced_by_new_connection');
    assert.notEqual(replacementRoom.code, staleRoom.code);
    replacementHost.send(JSON.stringify({ type: 'leave_room', ...versionFields }));
    await replacementHostInbox.type('room_left');
    staleHost.close();
    replacementHost.close();

    const rankedObserver = await connect(`ws://127.0.0.1:${port}`);
    const rankedObserverInbox = createInbox(rankedObserver);
    rankedObserver.send(JSON.stringify({
        type: 'get_room_list',
        ...versionFields,
        matchMode: 'ranked',
        playerId: 'ranked-observer',
    }));
    await rankedObserverInbox.type('room_list');
    const rankedHost = await connect(`ws://127.0.0.1:${port}`);
    const rankedHostInbox = createInbox(rankedHost);
    rankedHost.send(JSON.stringify({
        type: 'create_room',
        ...versionFields,
        matchMode: 'ranked',
        hostPlayerId: 'ranked-host',
        hostNickname: 'Ranked Host',
        hostCharacterId: 'ZEUS',
        hostPassiveId: 'IRON_WILL',
        arenaId: 'CLASSIC_OLYMPUS',
    }));
    const rankedCreated = await rankedHostInbox.type('room_created');
    const rankedAdded = await rankedObserverInbox.type('room_updated');
    assert.equal(rankedAdded.room.code, rankedCreated.code);
    assert.equal(rankedCreated.battleType, 'short');
    assert.equal(rankedAdded.room.battleType, 'short');
    assert.ok(['CLASSIC_OLYMPUS', 'SKY_OLYMPUS'].includes(rankedCreated.arenaId));
    assert.equal(rankedAdded.room.arenaId, rankedCreated.arenaId);
    assert.equal(rankedAdded.room.hostNickname, 'Ranked Host');
    assert.equal(rankedAdded.room.hostRating, 1000);
    assert.equal(rankedAdded.room.hostMatches, undefined);
    assert.equal(rankedAdded.room.ratingDifference, 0);
    assert.equal(typeof rankedAdded.room.waitingMs, 'number');

    rankedHost.send(JSON.stringify({
        type: 'get_room_list',
        ...versionFields,
        matchMode: 'ranked',
        playerId: 'ranked-host',
    }));
    const ownRankedRooms = await rankedHostInbox.type('room_list');
    assert.equal(ownRankedRooms.rooms.some((room) => room.code === rankedCreated.code), false);

    const duplicateRankedHost = await connect(`ws://127.0.0.1:${port}`);
    const duplicateRankedHostInbox = createInbox(duplicateRankedHost);
    duplicateRankedHost.send(JSON.stringify({
        type: 'get_room_list',
        ...versionFields,
        matchMode: 'ranked',
        playerId: 'ranked-host',
    }));
    const duplicateOwnRooms = await duplicateRankedHostInbox.type('room_list');
    assert.equal(duplicateOwnRooms.rooms.some((room) => room.code === rankedCreated.code), false);
    duplicateRankedHost.send(JSON.stringify({
        type: 'join_ranked_room',
        ...versionFields,
        code: rankedCreated.code,
        guestPlayerId: 'ranked-host',
    }));
    const selfJoinError = await duplicateRankedHostInbox.type('error');
    assert.equal(selfJoinError.code, 'self_join_not_allowed');
    duplicateRankedHost.close();

    rankedHost.close();
    await rankedObserverInbox.type('room_removed');
    rankedObserver.close();

    const atomicRankedHost = await connect(`ws://127.0.0.1:${port}`, {
        headers: { 'x-country-code': 'KR' },
    });
    const atomicRankedGuest = await connect(`ws://127.0.0.1:${port}`, {
        headers: { 'x-country-code': 'KR' },
    });
    const atomicRankedHostInbox = createInbox(atomicRankedHost);
    const atomicRankedGuestInbox = createInbox(atomicRankedGuest);
    atomicRankedHost.send(JSON.stringify({
        type: 'create_room',
        ...versionFields,
        matchMode: 'ranked',
        hostPlayerId: 'atomic-ranked-host',
        hostCharacterId: 'ZEUS',
        hostPassiveId: 'IRON_WILL',
    }));
    const atomicRankedRoom = await atomicRankedHostInbox.type('room_created');
    atomicRankedGuest.send(JSON.stringify({
        type: 'create_room',
        ...versionFields,
        matchMode: 'ranked',
        hostPlayerId: 'atomic-ranked-guest',
        hostCharacterId: 'TRICKSTER',
        hostPassiveId: 'LUCKY_WITCH',
    }));
    const [atomicGuestJoined, atomicRoomJoined] = await Promise.all([
        atomicRankedHostInbox.type('guest_joined'),
        atomicRankedGuestInbox.type('room_joined'),
    ]);
    assert.equal(atomicRoomJoined.code, atomicRankedRoom.code);
    assert.equal(atomicRoomJoined.matchMode, 'ranked');
    assert.equal(atomicGuestJoined.guestPlayerId, 'atomic-ranked-guest');
    assert.equal(atomicGuestJoined.guestCharacterId, 'TRICKSTER');

    atomicRankedGuest.send(JSON.stringify({ type: 'leave_room', ...versionFields }));
    await atomicRankedGuestInbox.type('room_left');
    atomicRankedHost.send(JSON.stringify({ type: 'leave_room', ...versionFields }));
    await atomicRankedHostInbox.type('room_left');
    atomicRankedHost.close();
    atomicRankedGuest.close();

    const prelaunchRankedHost = await connect(`ws://127.0.0.1:${port}`);
    const promotedRankedGuest = await connect(`ws://127.0.0.1:${port}`);
    const replacementRankedGuest = await connect(`ws://127.0.0.1:${port}`);
    const prelaunchRankedHostInbox = createInbox(prelaunchRankedHost);
    const promotedRankedInbox = createInbox(promotedRankedGuest);
    const replacementRankedInbox = createInbox(replacementRankedGuest);
    prelaunchRankedHost.send(JSON.stringify({
        type: 'create_room',
        ...versionFields,
        matchMode: 'ranked',
        hostPlayerId: 'prelaunch-ranked-host',
        hostCharacterId: 'ZEUS',
        hostPassiveId: 'IRON_WILL',
    }));
    const prelaunchRankedRoom = await prelaunchRankedHostInbox.type('room_created');
    promotedRankedGuest.send(JSON.stringify({
        type: 'join_ranked_room',
        ...versionFields,
        code: prelaunchRankedRoom.code,
        guestPlayerId: 'promoted-ranked-guest',
        guestCharacterId: 'TRICKSTER',
        guestPassiveId: 'LUCKY_WITCH',
    }));
    await Promise.all([
        prelaunchRankedHostInbox.type('guest_joined'),
        promotedRankedInbox.type('room_joined'),
    ]);
    prelaunchRankedHost.send(JSON.stringify({
        type: 'selection_update',
        ...versionFields,
        ready: true,
    }));
    await promotedRankedInbox.type('selection_update');
    promotedRankedGuest.send(JSON.stringify({
        type: 'selection_update',
        ...versionFields,
        ready: true,
    }));
    await prelaunchRankedHostInbox.type('selection_update');
    prelaunchRankedHost.send(JSON.stringify({
        type: 'game_start',
        ...versionFields,
        activeTransport: 'p2p',
    }));
    await Promise.all([
        prelaunchRankedHostInbox.type('match_assigned'),
        promotedRankedInbox.type('game_start'),
    ]);

    prelaunchRankedHost.send(JSON.stringify({ type: 'leave_room', ...versionFields }));
    const [prelaunchRankedLeft, rankedMigrated] = await Promise.all([
        prelaunchRankedHostInbox.type('room_left'),
        promotedRankedInbox.type('host_migrated'),
    ]);
    assert.equal(prelaunchRankedLeft.code, prelaunchRankedRoom.code);
    assert.equal(rankedMigrated.code, prelaunchRankedRoom.code);
    assert.equal(rankedMigrated.matchMode, 'ranked');

    replacementRankedGuest.send(JSON.stringify({
        type: 'join_ranked_room',
        ...versionFields,
        code: prelaunchRankedRoom.code,
        guestPlayerId: 'replacement-ranked-guest',
        guestCharacterId: 'ZEUS',
        guestPassiveId: 'STORM_MASTERY',
    }));
    await Promise.all([
        promotedRankedInbox.type('guest_joined'),
        replacementRankedInbox.type('room_joined'),
    ]);
    replacementRankedGuest.send(JSON.stringify({ type: 'leave_room', ...versionFields }));
    await Promise.all([
        replacementRankedInbox.type('room_left'),
        promotedRankedInbox.type('peer_disconnected'),
    ]);
    promotedRankedGuest.send(JSON.stringify({ type: 'leave_room', ...versionFields }));
    await promotedRankedInbox.type('room_left');
    prelaunchRankedHost.close();
    promotedRankedGuest.close();
    replacementRankedGuest.close();

    const expandedKrPlayer = await connect(`ws://127.0.0.1:${port}`, {
        headers: { 'x-country-code': 'KR' },
    });
    const expandedUsPlayer = await connect(`ws://127.0.0.1:${port}`, {
        headers: { 'x-country-code': 'US' },
    });
    const expandedKrInbox = createInbox(expandedKrPlayer);
    const expandedUsInbox = createInbox(expandedUsPlayer);
    expandedKrPlayer.send(JSON.stringify({
        type: 'create_room',
        ...versionFields,
        matchMode: 'ranked',
        hostPlayerId: 'expanded-ranked-kr',
    }));
    const expandedKrRoom = await expandedKrInbox.type('room_created');
    await new Promise((resolve) => setTimeout(resolve, 200));
    expandedUsPlayer.send(JSON.stringify({
        type: 'create_room',
        ...versionFields,
        matchMode: 'ranked',
        hostPlayerId: 'expanded-ranked-us',
    }));
    const expandedUsRoom = await expandedUsInbox.type('room_created');
    assert.notEqual(expandedUsRoom.code, expandedKrRoom.code);

    await new Promise((resolve) => setTimeout(resolve, 1100));
    await assert.rejects(
        expandedKrInbox.next((message) => message.type === 'guest_joined', 150),
        /Timed out waiting for WebSocket message/
    );

    expandedUsPlayer.send(JSON.stringify({ type: 'leave_room', ...versionFields }));
    await expandedUsInbox.type('room_left');
    expandedKrPlayer.send(JSON.stringify({ type: 'leave_room', ...versionFields }));
    await expandedKrInbox.type('room_left');
    expandedKrPlayer.close();
    expandedUsPlayer.close();

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

    const countryHost = await connect(`ws://127.0.0.1:${port}`, {
        headers: { 'x-country-code': 'KR' },
    });
    const countryKrViewer = await connect(`ws://127.0.0.1:${port}`, {
        headers: { 'x-country-code': 'KR' },
    });
    const countryUsViewer = await connect(`ws://127.0.0.1:${port}`, {
        headers: { 'x-country-code': 'US' },
    });
    const countryHostInbox = createInbox(countryHost);
    const countryKrInbox = createInbox(countryKrViewer);
    const countryUsInbox = createInbox(countryUsViewer);
    countryHost.send(JSON.stringify({
        type: 'create_room',
        ...versionFields,
        hostPlayerId: 'country-host',
        matchMode: 'friendly',
    }));
    const countryRoom = await countryHostInbox.type('room_created');

    countryKrViewer.send(JSON.stringify({
        type: 'get_room_list',
        ...versionFields,
        matchMode: 'friendly',
        playerId: 'country-kr-viewer',
    }));
    const sameCountryRooms = await countryKrInbox.type('room_list');
    const sameCountryEntry = sameCountryRooms.rooms.find((room) => room.code === countryRoom.code);
    assert.equal(sameCountryEntry.sameCountry, true);

    countryUsViewer.send(JSON.stringify({
        type: 'get_room_list',
        ...versionFields,
        matchMode: 'friendly',
        playerId: 'country-us-viewer',
    }));
    const hiddenCrossCountryRooms = await countryUsInbox.type('room_list');
    assert.equal(
        hiddenCrossCountryRooms.rooms.some((room) => room.code === countryRoom.code),
        false
    );
    await new Promise((resolve) => setTimeout(resolve, 1050));
    countryUsViewer.send(JSON.stringify({
        type: 'get_room_list',
        ...versionFields,
        matchMode: 'friendly',
        playerId: 'country-us-viewer',
    }));
    const stillHiddenCrossCountryRooms = await countryUsInbox.type('room_list');
    assert.equal(
        stillHiddenCrossCountryRooms.rooms.some((room) => room.code === countryRoom.code),
        false
    );
    countryUsViewer.send(JSON.stringify({
        type: 'join_room',
        ...versionFields,
        code: countryRoom.code,
        guestPlayerId: 'country-us-viewer',
    }));
    const countryMismatch = await countryUsInbox.type('error');
    assert.equal(countryMismatch.code, 'country_mismatch');

    countryHost.send(JSON.stringify({ type: 'leave_room', ...versionFields }));
    await countryHostInbox.type('room_left');
    countryHost.close();
    countryKrViewer.close();
    countryUsViewer.close();

    const friendlyKrPlayer = await connect(`ws://127.0.0.1:${port}`, {
        headers: { 'x-country-code': 'KR' },
    });
    const friendlyUsPlayer = await connect(`ws://127.0.0.1:${port}`, {
        headers: { 'x-country-code': 'US' },
    });
    const friendlyKrInbox = createInbox(friendlyKrPlayer);
    const friendlyUsInbox = createInbox(friendlyUsPlayer);
    friendlyKrPlayer.send(JSON.stringify({
        type: 'create_room',
        ...versionFields,
        matchMode: 'friendly',
        matchmaking: true,
        hostPlayerId: 'expanded-friendly-kr',
    }));
    const friendlyKrRoom = await friendlyKrInbox.type('room_created');
    await new Promise((resolve) => setTimeout(resolve, 200));
    friendlyUsPlayer.send(JSON.stringify({
        type: 'create_room',
        ...versionFields,
        matchMode: 'friendly',
        matchmaking: true,
        hostPlayerId: 'expanded-friendly-us',
    }));
    const friendlyUsRoom = await friendlyUsInbox.type('room_created');
    assert.notEqual(friendlyUsRoom.code, friendlyKrRoom.code);

    await new Promise((resolve) => setTimeout(resolve, 1100));
    await assert.rejects(
        friendlyKrInbox.next((message) => message.type === 'guest_joined', 150),
        /Timed out waiting for WebSocket message/
    );

    friendlyUsPlayer.send(JSON.stringify({ type: 'leave_room', ...versionFields }));
    await friendlyUsInbox.type('room_left');
    friendlyKrPlayer.send(JSON.stringify({ type: 'leave_room', ...versionFields }));
    await friendlyKrInbox.type('room_left');
    friendlyKrPlayer.close();
    friendlyUsPlayer.close();

    const staleAutoPlayer = await connect(`ws://127.0.0.1:${port}`, {
        headers: { 'x-country-code': 'KR' },
    });
    const lateManualPlayer = await connect(`ws://127.0.0.1:${port}`, {
        headers: { 'x-country-code': 'KR' },
    });
    const staleAutoInbox = createInbox(staleAutoPlayer);
    const lateManualInbox = createInbox(lateManualPlayer);
    staleAutoPlayer.send(JSON.stringify({
        type: 'create_room',
        ...versionFields,
        matchMode: 'friendly',
        matchmaking: true,
        hostPlayerId: 'stale-auto-player',
    }));
    await staleAutoInbox.type('room_created');
    await new Promise((resolve) => setTimeout(resolve, 1100));

    lateManualPlayer.send(JSON.stringify({
        type: 'create_room',
        ...versionFields,
        matchMode: 'friendly',
        matchmaking: false,
        hostPlayerId: 'late-manual-player',
    }));
    const lateManualRoom = await lateManualInbox.type('room_created');
    const [lateManualGuestJoined, staleAutoJoined] = await Promise.all([
        lateManualInbox.type('guest_joined'),
        staleAutoInbox.type('room_joined'),
    ]);
    assert.equal(staleAutoJoined.code, lateManualRoom.code);
    assert.equal(lateManualGuestJoined.guestPlayerId, 'stale-auto-player');

    staleAutoPlayer.send(JSON.stringify({ type: 'leave_room', ...versionFields }));
    await staleAutoInbox.type('room_left');
    lateManualPlayer.send(JSON.stringify({ type: 'leave_room', ...versionFields }));
    await lateManualInbox.type('room_left');
    staleAutoPlayer.close();
    lateManualPlayer.close();

    const failedStartHost = await connect(`ws://127.0.0.1:${port}`);
    const failedStartGuest = await connect(`ws://127.0.0.1:${port}`);
    const failedStartHostInbox = createInbox(failedStartHost);
    const failedStartGuestInbox = createInbox(failedStartGuest);
    failedStartHost.send(JSON.stringify({
        type: 'create_room',
        ...versionFields,
        hostPlayerId: 'failed-start-host',
        networkMode: 'p2p',
    }));
    const failedStartRoom = await failedStartHostInbox.type('room_created');
    failedStartGuest.send(JSON.stringify({
        type: 'join_room',
        ...versionFields,
        code: failedStartRoom.code,
        guestPlayerId: 'failed-start-guest',
    }));
    await Promise.all([
        failedStartHostInbox.type('guest_joined'),
        failedStartGuestInbox.type('room_joined'),
    ]);
    failedStartHost.send(JSON.stringify({
        type: 'game_start_failed',
        ...versionFields,
        code: 'p2p_start_failed',
    }));
    const relayedStartFailure = await failedStartGuestInbox.type('game_start_failed');
    assert.equal(relayedStartFailure.code, 'p2p_start_failed');
    assert.equal(relayedStartFailure.action, 'guest_removed');

    failedStartGuest.send(JSON.stringify({
        type: 'get_room_list',
        ...versionFields,
        matchMode: 'friendly',
        playerId: 'failed-start-guest',
    }));
    const roomsAfterQualityFailure = await failedStartGuestInbox.type('room_list');
    assert.equal(
        roomsAfterQualityFailure.rooms.some((room) => room.code === failedStartRoom.code),
        false
    );
    failedStartGuest.send(JSON.stringify({
        type: 'join_room',
        ...versionFields,
        code: failedStartRoom.code,
        guestPlayerId: 'failed-start-guest',
    }));
    const rejectedFailedPair = await failedStartGuestInbox.type('error');
    assert.equal(rejectedFailedPair.code, 'network_quality_recently_failed');

    failedStartHost.send(JSON.stringify({
        type: 'create_room',
        ...versionFields,
        hostPlayerId: 'failed-start-host',
    }));
    const hostStillInRoom = await failedStartHostInbox.type('error');
    assert.equal(hostStillInRoom.code, 'already_in_room');

    failedStartGuest.send(JSON.stringify({
        type: 'create_room',
        ...versionFields,
        hostPlayerId: 'failed-start-guest',
    }));
    await failedStartGuestInbox.type('room_created');
    failedStartGuest.send(JSON.stringify({ type: 'leave_room', ...versionFields }));
    await failedStartGuestInbox.type('room_left');

    failedStartHost.send(JSON.stringify({ type: 'leave_room', ...versionFields }));
    const retainedHostRoomLeft = await failedStartHostInbox.type('room_left');
    assert.equal(retainedHostRoomLeft.code, failedStartRoom.code);
    failedStartHost.close();
    failedStartGuest.close();

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
        battleType: 'standard',
        matchMode: 'ranked',
        debugNoKo: true,
        debugNoTime: true,
    }));
    const created = await hostInbox.type('room_created');
    assert.equal(created.battleType, 'short');
    assert.ok(['CLASSIC_OLYMPUS', 'SKY_OLYMPUS'].includes(created.arenaId));
    assert.equal(created.hostRating, 1000);
    assert.equal(created.hostMatches, 0);
    assert.equal(created.debugNoKo, true);
    assert.equal(created.debugNoTime, true);
    guest.send(JSON.stringify({
        type: 'join_ranked_room',
        ...versionFields,
        code: created.code,
        guestCharacterId: guestPlayer.characterId,
        guestPassiveId: guestPlayer.passiveId,
        guestNickname: guestPlayer.nickname,
        guestPlayerId: guestPlayer.playerId,
        arenaId: 'CLASSIC_OLYMPUS',
    }));
    const [guestJoined, roomJoined] = await Promise.all([
        hostInbox.type('guest_joined'),
        guestInbox.type('room_joined'),
    ]);
    assert.equal(guestJoined.debugNoKo, true);
    assert.equal(guestJoined.debugNoTime, true);
    assert.equal(roomJoined.debugNoKo, true);
    assert.equal(roomJoined.debugNoTime, true);
    assert.equal(roomJoined.battleType, 'short');
    assert.equal(roomJoined.arenaId, created.arenaId);
    assert.equal(guestJoined.arenaId, created.arenaId);
    assert.equal(roomJoined.hostRating, 1000);
    assert.equal(roomJoined.hostMatches, 0);
    assert.equal(roomJoined.guestRating, 1000);
    assert.equal(roomJoined.guestMatches, 0);
    assert.equal(guestJoined.hostRating, 1000);
    assert.equal(guestJoined.hostMatches, 0);
    assert.equal(guestJoined.guestRating, 1000);
    assert.equal(guestJoined.guestMatches, 0);

    const nonAuthoritativeArena = created.arenaId === 'CLASSIC_OLYMPUS'
        ? 'SKY_OLYMPUS'
        : 'CLASSIC_OLYMPUS';
    guest.send(JSON.stringify({
        type: 'selection_update',
        ...versionFields,
        characterId: guestPlayer.characterId,
        passiveId: guestPlayer.passiveId,
        arenaId: nonAuthoritativeArena,
        battleType: 'long',
        ready: false,
    }));
    const guestSelectionAtHost = await hostInbox.type('selection_update');
    assert.equal(guestSelectionAtHost.arenaId, created.arenaId);
    assert.equal(guestSelectionAtHost.battleType, 'short');

    host.send(JSON.stringify({
        type: 'selection_update',
        ...versionFields,
        characterId: hostPlayer.characterId,
        passiveId: hostPlayer.passiveId,
        arenaId: nonAuthoritativeArena,
        battleType: 'long',
        ready: false,
    }));
    const hostSelectionAtGuest = await guestInbox.type('selection_update');
    assert.equal(hostSelectionAtGuest.arenaId, created.arenaId);
    assert.equal(hostSelectionAtGuest.battleType, 'short');

    host.send(JSON.stringify({
        type: 'game_latency_probe',
        phase: 'setup',
        probeId: 1,
    }));
    const setupLatencyProbe = await guestInbox.type('game_latency_probe');
    assert.equal(setupLatencyProbe.phase, 'setup');
    assert.equal(setupLatencyProbe.probeId, 1);
    guest.send(JSON.stringify({
        type: 'game_latency_ack',
        phase: 'setup',
        probeId: setupLatencyProbe.probeId,
    }));
    const setupLatencyAck = await hostInbox.type('game_latency_ack');
    assert.equal(setupLatencyAck.phase, 'setup');
    assert.equal(setupLatencyAck.probeId, 1);

    host.send(JSON.stringify({
        type: 'game_transport_ready',
        activeTransport: 'relay',
    }));
    const transportReady = await guestInbox.type('game_transport_ready');
    assert.equal(transportReady.activeTransport, 'relay');

    host.send(JSON.stringify({ type: 'selection_update', ...versionFields, ready: true }));
    await guestInbox.type('selection_update');
    guest.send(JSON.stringify({ type: 'selection_update', ...versionFields, ready: true }));
    await hostInbox.type('selection_update');
    host.send(JSON.stringify({ type: 'game_start', ...versionFields }));
    const [assigned, started] = await Promise.all([
        hostInbox.type('match_assigned'),
        guestInbox.type('game_start'),
    ]);
    assert.ok(assigned.matchId);
    assert.equal(started.matchId, assigned.matchId);
    assert.equal(started.matchSequence, assigned.matchSequence);
    assert.equal(assigned.arenaId, created.arenaId);
    assert.equal(started.battleType, 'short');
    assert.equal(started.arenaId, created.arenaId);
    assert.equal(started.debugNoKo, true);
    assert.equal(started.debugNoTime, true);

    guest.send(JSON.stringify({
        type: 'game_damage_confirm',
        roundId: assigned.matchSequence,
        eventId: 'damage-confirm-test',
        sentAtMs: Date.now(),
        amount: 30,
        hp: 95,
        source: 'test_projectile',
    }));
    const relayedDamageConfirm = await hostInbox.type('game_damage_confirm');
    assert.equal(relayedDamageConfirm.eventId, 'damage-confirm-test');
    assert.equal(relayedDamageConfirm.hp, 95);
    assert.equal(relayedDamageConfirm.amount, 30);

    host.send(JSON.stringify({
        type: 'game_hit_claim',
        roundId: assigned.matchSequence,
        eventId: 'h-1-s1',
        skillId: 'lightning_bolt',
        damage: 30,
    }));
    const relayedHitClaim = await guestInbox.type('game_hit_claim');
    assert.equal(relayedHitClaim.eventId, 'h-1-s1');
    assert.equal(relayedHitClaim.skillId, 'lightning_bolt');
    assert.equal(relayedHitClaim.damage, 30);

    host.send(JSON.stringify({
        type: 'game_latency_probe',
        roundId: assigned.matchSequence,
        probeId: 7,
    }));
    const relayedLatencyProbe = await guestInbox.type('game_latency_probe');
    assert.equal(relayedLatencyProbe.probeId, 7);
    guest.send(JSON.stringify({
        type: 'game_latency_ack',
        roundId: assigned.matchSequence,
        probeId: relayedLatencyProbe.probeId,
    }));
    const relayedLatencyAck = await hostInbox.type('game_latency_ack');
    assert.equal(relayedLatencyAck.probeId, 7);

    host.send(JSON.stringify({
        type: 'game_audit',
        matchId: assigned.matchId,
        roundId: 1,
        auditSeq: 1,
        stateSeq: 20,
        elapsedSec: 2,
        localHp: 125,
        remoteHp: 100,
        x: 320,
        y: 360,
    }));
    guest.send(JSON.stringify({
        type: 'game_audit',
        matchId: assigned.matchId,
        roundId: 1,
        auditSeq: 1,
        stateSeq: 20,
        elapsedSec: 2,
        localHp: 100,
        remoteHp: 125,
        x: 960,
        y: 360,
    }));
    await new Promise((resolve) => setTimeout(resolve, 25));
    const integrityHealth = await fetch(`${baseUrl}/health`).then((response) => response.json());
    assert.equal(integrityHealth.operations.integrityAudits.received, 2);
    assert.equal(integrityHealth.operations.integrityAudits.invalid, 0);
    assert.equal(integrityHealth.operations.integrityAudits.hpMismatches, 0);

    for (let auditSeq = 2; auditSeq <= 4; auditSeq += 1) {
        host.send(JSON.stringify({
            type: 'game_audit',
            matchId: assigned.matchId,
            roundId: 1,
            auditSeq,
            stateSeq: auditSeq * 20,
            elapsedSec: auditSeq * 2,
            localHp: 125,
            remoteHp: 90,
            x: 320,
            y: 360,
        }));
        guest.send(JSON.stringify({
            type: 'game_audit',
            matchId: assigned.matchId,
            roundId: 1,
            auditSeq,
            stateSeq: auditSeq * 20,
            elapsedSec: auditSeq * 2,
            localHp: 100,
            remoteHp: 120,
            x: 960,
            y: 360,
        }));
        await new Promise((resolve) => setTimeout(resolve, 10));
    }
    const flaggedIntegrityHealth = await fetch(`${baseUrl}/health`).then((response) => response.json());
    assert.equal(flaggedIntegrityHealth.operations.integrityAudits.hpMismatches, 3);
    assert.equal(flaggedIntegrityHealth.operations.integrityAudits.flaggedMatches, 1);

    host.send(JSON.stringify({ type: 'game_state', x: 10, y: 20 }));
    await new Promise((resolve) => setTimeout(resolve, 25));
    const backpressureCapacity = await fetch(`${baseUrl}/capacity`).then((response) => response.json());
    assert.equal(backpressureCapacity.backpressure.droppedStatePackets, 1);
    assert.equal(backpressureCapacity.backpressure.closedConnections, 0);
    assert.equal(backpressureCapacity.counts.activeRelayMatches, 1);
    assert.equal(backpressureCapacity.counts.activeP2pMatches, 0);
    assert.equal(backpressureCapacity.operations.relay.packets, 9);
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

    host.send(JSON.stringify({
        type: 'rematch_ready',
        ...versionFields,
        ready: true,
        characterId: hostPlayer.characterId,
        passiveId: hostPlayer.passiveId,
    }));
    const hostReadyForRematch = await guestInbox.type('rematch_ready');
    assert.equal(hostReadyForRematch.ready, true);
    host.send(JSON.stringify({
        type: 'rematch_ready',
        ...versionFields,
        ready: false,
        characterId: hostPlayer.characterId,
        passiveId: hostPlayer.passiveId,
    }));
    const hostCancelledRematchReady = await guestInbox.type('rematch_ready');
    assert.equal(hostCancelledRematchReady.ready, false);
    host.send(JSON.stringify({
        type: 'rematch_ready',
        ...versionFields,
        ready: true,
        characterId: hostPlayer.characterId,
        passiveId: hostPlayer.passiveId,
    }));
    assert.equal((await guestInbox.type('rematch_ready')).ready, true);

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
    const acceptedBody = await accepted.json();
    assert.equal(acceptedBody.players.local.ratingBefore, 1000);
    assert.equal(acceptedBody.players.local.ratingDelta, 24);
    assert.equal(acceptedBody.players.local.rating, 1024);
    assert.equal(acceptedBody.players.local.rewardCoins, 20);
    assert.equal(acceptedBody.players.remote.ratingDelta, -24);
    assert.equal(acceptedBody.players.remote.rating, 976);
    assert.equal(acceptedBody.players.remote.rewardCoins, 0);

    const duplicate = await fetch(`${baseUrl}/matches/pvp-result`, {
        method: 'POST',
        headers: appJsonHeaders,
        body: JSON.stringify(resultBody(assigned.matchId, guestPlayer, hostPlayer, 'loss')),
    });
    assert.equal(duplicate.status, 200, serverErrors);
    const duplicateBody = await duplicate.json();
    assert.equal(duplicateBody.duplicate, true);
    assert.equal(duplicateBody.players.local.rating, 976);
    assert.equal(duplicateBody.players.remote.rating, 1024);

    const stats = await fetch(`${baseUrl}/players/${hostPlayer.playerId}/stats?mode=multi`, { headers: appHeaders });
    assert.equal(stats.status, 200);
    const firstStats = await stats.json();
    assert.equal(firstStats.matches, 1);
    assert.equal(firstStats.rating, 1024);

    host.send(JSON.stringify({ type: 'game_start', ...versionFields }));
    const [rematchAssigned, rematchStarted] = await Promise.all([
        hostInbox.type('match_assigned'),
        guestInbox.type('game_start'),
    ]);
    assert.ok(rematchAssigned.matchId);
    assert.equal(rematchStarted.matchId, rematchAssigned.matchId);
    assert.notEqual(rematchAssigned.matchId, assigned.matchId);
    assert.equal(rematchAssigned.arenaId, rematchStarted.arenaId);
    assert.notEqual(rematchStarted.arenaId, started.arenaId);

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
    assert.equal(forfeitResponse.players.local.ratingBefore, 1024);
    assert.equal(forfeitResponse.players.local.ratingDelta, 21);
    assert.equal(forfeitResponse.players.local.rating, 1045);

    const updatedStats = await fetch(`${baseUrl}/players/${hostPlayer.playerId}/stats?mode=multi`, { headers: appHeaders });
    assert.equal(updatedStats.status, 200);
    assert.equal((await updatedStats.json()).matches, 2);

    host.send(JSON.stringify({ type: 'game_start', ...versionFields }));
    const [disconnectAssigned, disconnectStarted] = await Promise.all([
        hostInbox.type('match_assigned'),
        guestInbox.type('game_start'),
    ]);
    assert.equal(disconnectAssigned.arenaId, disconnectStarted.arenaId);
    assert.notEqual(disconnectStarted.arenaId, rematchStarted.arenaId);
    guest.send(JSON.stringify({ type: 'game_ready', ...versionFields }));
    await Promise.all([
        hostInbox.type('game_countdown_sync'),
        guestInbox.type('game_countdown_sync'),
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
    const disconnectAcceptedBody = await disconnectAccepted.json();
    assert.equal(disconnectAcceptedBody.players.local.rewardCoins, 20);
    assert.equal(disconnectAcceptedBody.players.local.ratingBefore, 1045);
    assert.equal(disconnectAcceptedBody.players.local.ratingDelta, 9);
    assert.equal(disconnectAcceptedBody.players.local.rating, 1054);

    const finalStats = await fetch(`${baseUrl}/players/${hostPlayer.playerId}/stats?mode=multi`, { headers: appHeaders });
    assert.equal(finalStats.status, 200);
    assert.equal((await finalStats.json()).matches, 3);

    host.send(JSON.stringify({
        type: 'leave_room',
        ...versionFields,
        leaveReason: 'return_to_lobby',
    }));
    const completedRoomLeft = await hostInbox.type('room_left');
    assert.ok(completedRoomLeft.code === created.code || completedRoomLeft.code === null);
    host.send(JSON.stringify({
        type: 'create_room',
        ...versionFields,
        matchMode: 'friendly',
        hostPlayerId: hostPlayer.playerId,
    }));
    const roomAfterCompletedMatch = await hostInbox.type('room_created');
    assert.ok(roomAfterCompletedMatch.code);
    host.send(JSON.stringify({ type: 'leave_room', ...versionFields }));
    await hostInbox.type('room_left');

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
    assert.equal(snapshot.deployment.draining, false);
    assert.equal(snapshot.deployment.acceptingConnections, true);

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
    assert.match(adminHtml, /배포 드레인 시작/);
});

test('players can leave a completed rematch room and create another room', async (t) => {
    const port = 24500 + Math.floor(Math.random() * 200);
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
        },
        stdio: ['ignore', 'pipe', 'pipe'],
    });
    t.after(() => {
        if (child.exitCode === null) child.kill('SIGTERM');
    });
    await waitForServer(baseUrl, child);

    const host = await connect(`ws://127.0.0.1:${port}`);
    const guest = await connect(`ws://127.0.0.1:${port}`);
    const hostInbox = createInbox(host);
    const guestInbox = createInbox(guest);
    t.after(() => {
        host.close();
        guest.close();
    });

    host.send(JSON.stringify({
        type: 'create_room',
        ...versionFields,
        matchMode: 'friendly',
        networkMode: 'relay',
        hostPlayerId: 'completed-room-host',
    }));
    const created = await hostInbox.type('room_created');
    guest.send(JSON.stringify({
        type: 'join_room',
        ...versionFields,
        code: created.code,
        guestPlayerId: 'completed-room-guest',
    }));
    await Promise.all([hostInbox.type('guest_joined'), guestInbox.type('room_joined')]);

    host.send(JSON.stringify({
        type: 'game_start',
        ...versionFields,
        activeTransport: 'relay',
    }));
    await Promise.all([hostInbox.type('match_assigned'), guestInbox.type('game_start')]);
    guest.send(JSON.stringify({ type: 'game_ready', ...versionFields }));
    await Promise.all([
        hostInbox.type('game_countdown_sync'),
        guestInbox.type('game_countdown_sync'),
    ]);
    host.send(JSON.stringify({
        type: 'game_over',
        roundId: 1,
        hp: 125,
        remoteHp: 0,
        outcome: 'win',
        reason: 'normal',
    }));
    await Promise.all([hostInbox.type('match_result'), guestInbox.type('match_result')]);

    host.send(JSON.stringify({
        type: 'leave_room',
        ...versionFields,
        leaveReason: 'return_to_lobby',
    }));
    const [hostLeft, guestMigrated] = await Promise.all([
        hostInbox.type('room_left'),
        guestInbox.type('host_migrated'),
    ]);
    assert.equal(hostLeft.code, created.code);
    assert.equal(guestMigrated.code, created.code);

    host.send(JSON.stringify({
        type: 'create_room',
        ...versionFields,
        matchMode: 'friendly',
        networkMode: 'relay',
        hostPlayerId: 'completed-room-host',
    }));
    const nextRoom = await hostInbox.type('room_created');
    assert.notEqual(nextRoom.code, created.code);

    host.send(JSON.stringify({ type: 'leave_room', ...versionFields }));
    guest.send(JSON.stringify({ type: 'leave_room', ...versionFields }));
    await Promise.all([hostInbox.type('room_left'), guestInbox.type('room_left')]);
});

test('ranked setup ready checks time out inactive players and expose setup outcomes', async (t) => {
    const port = 21500 + Math.floor(Math.random() * 400);
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
            RANKED_READY_CHECK_TIMEOUT_MS: '120',
            RANKED_SETUP_IDLE_TIMEOUT_MS: '250',
        },
        stdio: ['ignore', 'pipe', 'pipe'],
    });
    let serverErrors = '';
    child.stderr.on('data', (chunk) => { serverErrors += chunk.toString(); });
    t.after(() => {
        if (child.exitCode === null) child.kill('SIGTERM');
    });
    await waitForServer(baseUrl, child);

    const inactiveHost = await connect(`ws://127.0.0.1:${port}`);
    const readyGuest = await connect(`ws://127.0.0.1:${port}`);
    const replacementGuest = await connect(`ws://127.0.0.1:${port}`);
    const inactiveHostInbox = createInbox(inactiveHost);
    const readyGuestInbox = createInbox(readyGuest);
    const replacementInbox = createInbox(replacementGuest);
    t.after(() => {
        inactiveHost.close();
        readyGuest.close();
        replacementGuest.close();
    });

    inactiveHost.send(JSON.stringify({
        type: 'create_room',
        ...versionFields,
        matchMode: 'ranked',
        networkMode: 'relay',
        hostPlayerId: 'inactive-ready-host',
    }));
    const created = await inactiveHostInbox.type('room_created');
    readyGuest.send(JSON.stringify({
        type: 'join_ranked_room',
        ...versionFields,
        code: created.code,
        guestPlayerId: 'ready-ranked-guest',
    }));
    await Promise.all([
        inactiveHostInbox.type('guest_joined'),
        readyGuestInbox.type('room_joined'),
    ]);
    const [hostSetupDeadline, guestSetupDeadline] = await Promise.all([
        inactiveHostInbox.type('setup_deadline'),
        readyGuestInbox.type('setup_deadline'),
    ]);
    assert.equal(hostSetupDeadline.deadlineAtMs, guestSetupDeadline.deadlineAtMs);
    readyGuest.send(JSON.stringify({ type: 'selection_update', ...versionFields, ready: true }));
    await inactiveHostInbox.type('selection_update');
    const [hostReadyCheck, guestReadyCheck] = await Promise.all([
        inactiveHostInbox.type('setup_ready_check'),
        readyGuestInbox.type('setup_ready_check'),
    ]);
    assert.equal(hostReadyCheck.active, true);
    assert.equal(guestReadyCheck.active, true);
    assert.ok(hostReadyCheck.countdownMs <= 120 && hostReadyCheck.countdownMs > 0);
    assert.ok(hostReadyCheck.deadlineAtMs <= hostSetupDeadline.deadlineAtMs);

    readyGuest.send(JSON.stringify({ type: 'selection_update', ...versionFields, ready: false }));
    await inactiveHostInbox.type('selection_update');
    const [hostReadyCancelled, guestReadyCancelled] = await Promise.all([
        inactiveHostInbox.next((message) => message.type === 'setup_ready_check' && message.active === false),
        readyGuestInbox.next((message) => message.type === 'setup_ready_check' && message.active === false),
    ]);
    assert.equal(hostReadyCancelled.active, false);
    assert.equal(guestReadyCancelled.active, false);
    await new Promise((resolve) => setTimeout(resolve, 20));
    readyGuest.send(JSON.stringify({ type: 'selection_update', ...versionFields, ready: true }));
    await inactiveHostInbox.type('selection_update');
    const [hostResetReadyCheck, guestResetReadyCheck] = await Promise.all([
        inactiveHostInbox.next((message) => message.type === 'setup_ready_check' && message.active === true),
        readyGuestInbox.next((message) => message.type === 'setup_ready_check' && message.active === true),
    ]);
    assert.ok(hostResetReadyCheck.deadlineAtMs > hostReadyCheck.deadlineAtMs);
    assert.ok(hostResetReadyCheck.deadlineAtMs <= hostSetupDeadline.deadlineAtMs);
    assert.equal(hostResetReadyCheck.deadlineAtMs, guestResetReadyCheck.deadlineAtMs);

    const [hostTimeout, guestTimeout, migrated] = await Promise.all([
        inactiveHostInbox.type('setup_ready_timeout'),
        readyGuestInbox.type('setup_ready_timeout'),
        readyGuestInbox.type('host_migrated'),
    ]);
    assert.equal(hostTimeout.localTimedOut, true);
    assert.equal(guestTimeout.localTimedOut, false);
    assert.equal(hostTimeout.timedOutRole, 'host');
    assert.equal(migrated.code, created.code);
    assert.equal(migrated.reason, 'setup_ready_timeout');

    replacementGuest.send(JSON.stringify({
        type: 'join_ranked_room',
        ...versionFields,
        code: created.code,
        guestPlayerId: 'replacement-ready-guest',
    }));
    await Promise.all([
        readyGuestInbox.type('guest_joined'),
        replacementInbox.type('room_joined'),
    ]);
    replacementGuest.send(JSON.stringify({
        type: 'leave_room',
        ...versionFields,
        leaveReason: 'user_back',
    }));
    const [replacementLeft, peerSawUserLeave] = await Promise.all([
        replacementInbox.type('room_left'),
        readyGuestInbox.type('peer_disconnected'),
    ]);
    assert.equal(replacementLeft.reason, 'user_left');
    assert.equal(replacementLeft.detail, 'user_back');
    assert.equal(peerSawUserLeave.reason, 'user_left');
    assert.equal(peerSawUserLeave.detail, 'user_back');
    readyGuest.send(JSON.stringify({ type: 'leave_room', ...versionFields }));
    await readyGuestInbox.type('room_left');

    const disconnectHost = await connect(`ws://127.0.0.1:${port}`);
    const disconnectGuest = await connect(`ws://127.0.0.1:${port}`);
    const disconnectHostInbox = createInbox(disconnectHost);
    const disconnectGuestInbox = createInbox(disconnectGuest);
    t.after(() => {
        disconnectHost.close();
        disconnectGuest.close();
    });
    disconnectHost.send(JSON.stringify({
        type: 'create_room',
        ...versionFields,
        matchMode: 'ranked',
        networkMode: 'relay',
        hostPlayerId: 'disconnect-ranked-host',
    }));
    const disconnectCreated = await disconnectHostInbox.type('room_created');
    disconnectGuest.send(JSON.stringify({
        type: 'join_ranked_room',
        ...versionFields,
        code: disconnectCreated.code,
        guestPlayerId: 'disconnect-ranked-guest',
    }));
    await Promise.all([
        disconnectHostInbox.type('guest_joined'),
        disconnectGuestInbox.type('room_joined'),
    ]);
    disconnectGuest.terminate();
    const peerSawDisconnect = await disconnectHostInbox.type('peer_disconnected');
    assert.equal(peerSawDisconnect.reason, 'disconnect');
    assert.equal(peerSawDisconnect.detail, 'transport');
    disconnectHost.send(JSON.stringify({ type: 'leave_room', ...versionFields }));
    await disconnectHostInbox.type('room_left');

    const idleHost = await connect(`ws://127.0.0.1:${port}`);
    const idleGuest = await connect(`ws://127.0.0.1:${port}`);
    const idleHostInbox = createInbox(idleHost);
    const idleGuestInbox = createInbox(idleGuest);
    t.after(() => {
        idleHost.close();
        idleGuest.close();
    });
    idleHost.send(JSON.stringify({
        type: 'create_room',
        ...versionFields,
        matchMode: 'ranked',
        networkMode: 'relay',
        hostPlayerId: 'idle-ranked-host',
    }));
    const idleCreated = await idleHostInbox.type('room_created');
    idleGuest.send(JSON.stringify({
        type: 'join_ranked_room',
        ...versionFields,
        code: idleCreated.code,
        guestPlayerId: 'idle-ranked-guest',
    }));
    await Promise.all([idleHostInbox.type('guest_joined'), idleGuestInbox.type('room_joined')]);
    const [idleHostTimeout, idleGuestTimeout] = await Promise.all([
        idleHostInbox.type('setup_idle_timeout'),
        idleGuestInbox.type('setup_idle_timeout'),
    ]);
    assert.equal(idleHostTimeout.localTimedOut, true);
    assert.equal(idleGuestTimeout.localTimedOut, true);

    const capacity = await fetch(`${baseUrl}/capacity`).then((response) => response.json());
    assert.equal(capacity.operations.rankedSetup.sessions, 4, serverErrors);
    assert.equal(capacity.operations.rankedSetup.launched, 0);
    assert.equal(capacity.operations.rankedSetup.notStarted, 4);
    assert.equal(capacity.operations.rankedSetup.failureReasons.ready_timeout, 1);
    assert.equal(capacity.operations.rankedSetup.failureReasons.idle_timeout, 1);
    assert.equal(capacity.operations.rankedSetup.failureReasons.user_left, 1);
    assert.equal(capacity.operations.rankedSetup.failureReasons.disconnect, 1);
    assert.equal(capacity.operations.rankedSetup.failureDetails['user_left.user_back'], 1);
    assert.equal(capacity.operations.rankedSetup.failureDetails['disconnect.transport'], 1);
    assert.equal(capacity.operations.rankedSetup.readyTimeoutSec, 1);
    assert.equal(capacity.operations.rankedSetup.idleTimeoutSec, 1);

});

test('relay admission limits preserve P2P matches and drop P2P gameplay sent over WebSocket', async (t) => {
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
    assert.equal(hostFallbackError.code, 'p2p_required');
    assert.equal(guestFallbackError.code, 'p2p_required');

    await createPair(candidateHost, candidateHostInbox, candidateGuest, candidateGuestInbox, 'auto');
    candidateHost.send(JSON.stringify({ type: 'game_start', ...versionFields, activeTransport: 'p2p' }));
    await Promise.all([candidateHostInbox.type('match_assigned'), candidateGuestInbox.type('game_start')]);

    const capacity = await fetch(`${baseUrl}/capacity`).then((response) => response.json());
    assert.equal(capacity.counts.activeRelayMatches, 1, serverErrors);
    assert.equal(capacity.counts.activeP2pMatches, 1, serverErrors);
    assert.equal(capacity.relay.canStartNewMatch, false);
    assert.equal(capacity.relay.code, 'relay_capacity');
    assert.equal(capacity.operations.relay.admissionRejections, 1);

    candidateHost.send(JSON.stringify({ type: 'game_state', seq: 1, x: 10, y: 20 }));
    await assert.rejects(
        candidateGuestInbox.next((message) => message.type === 'game_state', 150),
        /Timed out waiting for WebSocket message/
    );
    const fallbackCapacity = await fetch(`${baseUrl}/capacity`).then((response) => response.json());
    assert.equal(fallbackCapacity.counts.activeRelayMatches, 1);
    assert.equal(fallbackCapacity.counts.activeP2pMatches, 1);
    assert.equal(fallbackCapacity.operations.relay.runtimeFallbacks, 0);
});

test('P2P failure checks assign a missing peer and invalidate ambiguous failures', async (t) => {
    const port = 23000 + Math.floor(Math.random() * 1000);
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
            P2P_FAILURE_CHECK_TIMEOUT_MS: '250',
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

    const createRankedPair = async (suffix) => {
        const host = await connect(`ws://127.0.0.1:${port}`, { headers: { 'x-country-code': 'KR' } });
        const guest = await connect(`ws://127.0.0.1:${port}`, { headers: { 'x-country-code': 'KR' } });
        const hostInbox = createInbox(host);
        const guestInbox = createInbox(guest);
        const hostPlayer = {
            playerId: `failure-host-${suffix}`,
            nickname: `Host${suffix}`,
            characterId: 'ZEUS',
            passiveId: 'IRON_WILL',
            hp: 125,
        };
        const guestPlayer = {
            playerId: `failure-guest-${suffix}`,
            nickname: `Guest${suffix}`,
            characterId: 'TRICKSTER',
            passiveId: 'LUCKY_WITCH',
            hp: 100,
        };
        host.send(JSON.stringify({
            type: 'create_room',
            ...versionFields,
            matchMode: 'ranked',
            networkMode: 'auto',
            hostPlayerId: hostPlayer.playerId,
            hostNickname: hostPlayer.nickname,
            hostCharacterId: hostPlayer.characterId,
            hostPassiveId: hostPlayer.passiveId,
        }));
        const created = await hostInbox.next((message) =>
            message.type === 'room_created' ||
            message.type === 'room_joined' ||
            message.type === 'error'
        );
        assert.equal(created.type, 'room_created', JSON.stringify(created));
        guest.send(JSON.stringify({
            type: 'join_ranked_room',
            ...versionFields,
            code: created.code,
            guestPlayerId: guestPlayer.playerId,
            guestNickname: guestPlayer.nickname,
            guestCharacterId: guestPlayer.characterId,
            guestPassiveId: guestPlayer.passiveId,
        }));
        await Promise.all([hostInbox.type('guest_joined'), guestInbox.type('room_joined')]);
        host.send(JSON.stringify({ type: 'selection_update', ...versionFields, ready: true }));
        await guestInbox.type('selection_update');
        guest.send(JSON.stringify({ type: 'selection_update', ...versionFields, ready: true }));
        await hostInbox.type('selection_update');
        host.send(JSON.stringify({ type: 'game_start', ...versionFields, activeTransport: 'p2p' }));
        const [assigned] = await Promise.all([
            hostInbox.type('match_assigned'),
            guestInbox.type('game_start'),
        ]);
        guest.send(JSON.stringify({ type: 'game_ready', ...versionFields }));
        await Promise.all([
            hostInbox.type('game_countdown_sync'),
            guestInbox.type('game_countdown_sync'),
        ]);
        return {
            host,
            guest,
            hostInbox,
            guestInbox,
            hostPlayer,
            guestPlayer,
            assigned,
            code: created.code,
        };
    };

    const missingPeer = await createRankedPair('missing');
    missingPeer.host.send(JSON.stringify({
        type: 'p2p_failure_report',
        ...versionFields,
        roundId: missingPeer.assigned.matchSequence,
    }));
    const [hostMissingCheck, guestMissingCheck] = await Promise.all([
        missingPeer.hostInbox.type('p2p_failure_check'),
        missingPeer.guestInbox.type('p2p_failure_check'),
    ]);
    assert.equal(hostMissingCheck.checkId, guestMissingCheck.checkId);
    missingPeer.host.send(JSON.stringify({
        type: 'p2p_failure_ack',
        checkId: hostMissingCheck.checkId,
    }));
    const [hostMissingResult, guestMissingResult] = await Promise.all([
        missingPeer.hostInbox.type('match_result'),
        missingPeer.guestInbox.type('match_result'),
    ]);
    assert.equal(hostMissingResult.outcome, 'win');
    assert.equal(hostMissingResult.finishReason, 'remote_disconnect');
    assert.equal(guestMissingResult.outcome, 'loss');
    assert.equal(guestMissingResult.finishReason, 'local_disconnect');
    await Promise.all([
        missingPeer.hostInbox.type('p2p_failure_decided'),
        missingPeer.guestInbox.type('p2p_failure_decided'),
    ]);
    const lateGuest = await connect(`ws://127.0.0.1:${port}`, {
        headers: { 'x-country-code': 'KR' },
    });
    const lateGuestInbox = createInbox(lateGuest);
    lateGuest.send(JSON.stringify({
        type: 'resume_match',
        ...versionFields,
        code: missingPeer.code,
        matchId: missingPeer.assigned.matchId,
        playerId: missingPeer.guestPlayer.playerId,
    }));
    const lateGuestResult = await lateGuestInbox.type('match_result');
    assert.equal(lateGuestResult.outcome, 'loss');
    assert.equal(lateGuestResult.finishReason, 'local_disconnect');
    lateGuest.close();
    missingPeer.host.close();
    missingPeer.guest.close();

    const socketDisconnect = await createRankedPair('socket');
    socketDisconnect.guest.close();
    const socketCheck = await socketDisconnect.hostInbox.type('p2p_failure_check');
    socketDisconnect.host.send(JSON.stringify({
        type: 'p2p_failure_ack',
        checkId: socketCheck.checkId,
    }));
    const socketHostResult = await socketDisconnect.hostInbox.type('match_result');
    assert.equal(socketHostResult.outcome, 'win');
    assert.equal(socketHostResult.finishReason, 'remote_disconnect');
    const disconnectedGuest = await connect(`ws://127.0.0.1:${port}`, {
        headers: { 'x-country-code': 'KR' },
    });
    const disconnectedGuestInbox = createInbox(disconnectedGuest);
    disconnectedGuest.send(JSON.stringify({
        type: 'resume_match',
        ...versionFields,
        code: socketDisconnect.code,
        matchId: socketDisconnect.assigned.matchId,
        playerId: socketDisconnect.guestPlayer.playerId,
    }));
    const disconnectedGuestResult = await disconnectedGuestInbox.type('match_result');
    assert.equal(disconnectedGuestResult.outcome, 'loss');
    assert.equal(disconnectedGuestResult.finishReason, 'local_disconnect');
    socketDisconnect.host.close();
    disconnectedGuest.close();

    const unresolved = await createRankedPair('invalid');
    unresolved.host.send(JSON.stringify({
        type: 'p2p_failure_report',
        ...versionFields,
        roundId: unresolved.assigned.matchSequence,
    }));
    const [hostInvalidCheck, guestInvalidCheck] = await Promise.all([
        unresolved.hostInbox.type('p2p_failure_check'),
        unresolved.guestInbox.type('p2p_failure_check'),
    ]);
    assert.equal(hostInvalidCheck.checkId, guestInvalidCheck.checkId);
    for (const socket of [unresolved.host, unresolved.guest]) {
        socket.send(JSON.stringify({
            type: 'p2p_failure_ack',
            checkId: hostInvalidCheck.checkId,
        }));
    }
    const [hostInvalidResult, guestInvalidResult] = await Promise.all([
        unresolved.hostInbox.type('match_result'),
        unresolved.guestInbox.type('match_result'),
    ]);
    assert.equal(hostInvalidResult.outcome, 'draw');
    assert.equal(guestInvalidResult.outcome, 'draw');
    assert.equal(hostInvalidResult.finishReason, 'network_unresolved');
    const invalidSubmission = await fetch(`${baseUrl}/matches/pvp-result`, {
        method: 'POST',
        headers: appJsonHeaders,
        body: JSON.stringify(resultBody(
            unresolved.assigned.matchId,
            unresolved.hostPlayer,
            unresolved.guestPlayer,
            'draw'
        )),
    });
    assert.equal(invalidSubmission.status, 409, serverErrors);
    assert.equal((await invalidSubmission.json()).error.code, 'match_invalidated');
    unresolved.host.close();
    unresolved.guest.close();
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

test('support inquiries retain support metadata and expose player context to admins', async (t) => {
    const port = 23000 + Math.floor(Math.random() * 1000);
    const baseUrl = `http://127.0.0.1:${port}`;
    const child = spawn(process.execPath, ['server.js'], {
        cwd: __dirname,
        env: {
            ...process.env,
            PORT: String(port),
            DATABASE_URL: '',
            SERVER_CHANNEL: 'dev',
            SERVER_ALLOWED_CHANNELS: 'dev',
            ADMIN_DASHBOARD_USERNAME: 'admin',
            ADMIN_DASHBOARD_PASSWORD: 'test-password',
            SUPPORT_RATE_LIMIT_PER_HOUR: '1',
        },
        stdio: ['ignore', 'pipe', 'pipe'],
    });
    let serverErrors = '';
    child.stderr.on('data', (chunk) => { serverErrors += chunk.toString(); });
    t.after(() => {
        if (child.exitCode === null) child.kill('SIGTERM');
    });
    await waitForServer(baseUrl, child);

    const playerId = 'support-player-id';
    const matchResponse = await fetch(`${baseUrl}/matches/result`, {
        method: 'POST',
        headers: appJsonHeaders,
        body: JSON.stringify({
            mode: 'single',
            clientMatchId: 'support-single-match',
            outcome: 'win',
            finishReason: 'normal',
            durationSec: 40,
            completedAt: new Date().toISOString(),
            nickname: 'Support Player',
            playerId,
            characterId: 'ZEUS',
            passiveId: 'STORM_MASTERY',
            arenaId: 'CLASSIC_OLYMPUS',
            localHp: 120,
            remoteHp: 0,
        }),
    });
    assert.equal(matchResponse.status, 201, serverErrors);

    const supportHeaders = {
        'content-type': 'application/json',
        'x-app-channel': 'dev',
        'x-player-id': playerId,
        'x-app-version-name': '1.2.3-debug',
        'x-app-version-code': '123',
        'x-build-type': 'debug',
        'x-country-code': 'KR',
        'user-agent': 'MiniZeus/1.2.3-debug (Android 14; Test Phone; debug; dev)',
    };
    const submitted = await fetch(`${baseUrl}/support/inquiries`, {
        method: 'POST',
        headers: supportHeaders,
        body: JSON.stringify({
            category: 'bug',
            message: '전투 결과 화면에서 버튼이 눌리지 않습니다.',
            replyEmail: 'tester@example.com',
        }),
    });
    assert.equal(submitted.status, 201, serverErrors);
    const receipt = await submitted.json();
    assert.match(receipt.inquiryId, /^sup_/);
    assert.equal(receipt.status, 'open');

    const rateLimited = await fetch(`${baseUrl}/support/inquiries`, {
        method: 'POST',
        headers: supportHeaders,
        body: JSON.stringify({ category: 'other', message: '두 번째 문의입니다.' }),
    });
    assert.equal(rateLimited.status, 429);

    const adminAuthorization = `Basic ${Buffer.from('admin:test-password').toString('base64')}`;
    const listed = await fetch(`${baseUrl}/admin/api/support?channel=dev`, {
        headers: { authorization: adminAuthorization },
    });
    assert.equal(listed.status, 200);
    const supportSnapshot = await listed.json();
    assert.equal(supportSnapshot.retentionDays, 180);
    assert.equal(supportSnapshot.inquiries.length, 1);
    assert.equal(supportSnapshot.inquiries[0].countryCode, 'KR');
    assert.equal(supportSnapshot.inquiries[0].replyEmail, 'tester@example.com');
    assert.equal(supportSnapshot.inquiries[0].playerStats.single.rating, 1016);
    assert.match(supportSnapshot.inquiries[0].userAgent, /MiniZeus/);

    const updated = await fetch(`${baseUrl}/admin/api/support/${receipt.inquiryId}/status`, {
        method: 'POST',
        headers: {
            authorization: adminAuthorization,
            'content-type': 'application/json',
        },
        body: JSON.stringify({ status: 'review' }),
    });
    assert.equal(updated.status, 200);
    assert.equal((await updated.json()).status, 'review');

    const adminPage = await fetch(`${baseUrl}/admin/support?channel=dev`, {
        headers: { authorization: adminAuthorization },
    });
    assert.equal(adminPage.status, 200);
    const adminHtml = await adminPage.text();
    assert.match(adminHtml, /MiniZeus 문의 관리/);
    assert.match(adminHtml, /전투 결과 화면/);
    assert.match(adminHtml, /MMR 1016/);
});
