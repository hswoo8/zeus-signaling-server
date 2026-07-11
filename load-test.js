const WebSocket = require('ws');

const args = parseArgs(process.argv.slice(2));
const url = String(args.url || 'ws://127.0.0.1:8080');
const clientsRequested = positiveInt(args.clients, 100);
const durationSec = positiveInt(args.duration, 15);
const mode = ['lobby', 'matched', 'relay'].includes(args.mode) ? args.mode : 'lobby';
const allowProduction = args['allow-production'] === true;

if (!allowProduction && /(^|\.)api\.minizeusgame\.com/i.test(new URL(url).hostname)) {
    throw new Error('Production load tests require --allow-production');
}
if (mode !== 'lobby' && clientsRequested % 2 !== 0) {
    throw new Error('Matched and relay modes require an even --clients value');
}

const versionFields = {
    clientVersionCode: 1,
    clientVersionName: 'load-test',
    analyticsChannel: 'dev',
    protocolVersion: 1,
    rulesetVersion: 1,
    balanceVersion: 1,
};

class VirtualClient {
    constructor(index) {
        this.index = index;
        this.ws = null;
        this.messages = [];
        this.waiters = [];
        this.errors = [];
        this.receivedGameStates = 0;
        this.unexpectedClose = false;
    }

    connect() {
        const startedAt = performance.now();
        return new Promise((resolve, reject) => {
            const ws = new WebSocket(url, {
                headers: {
                    'user-agent': `MiniZeusLoadTest/${this.index}`,
                    'x-country-code': 'KR',
                },
            });
            this.ws = ws;
            ws.once('open', () => resolve(performance.now() - startedAt));
            ws.once('error', reject);
            ws.on('message', (raw) => this.onMessage(JSON.parse(raw.toString())));
            ws.on('close', () => {
                if (!this.closing) this.unexpectedClose = true;
            });
        });
    }

    onMessage(message) {
        if (message.type === 'game_state') this.receivedGameStates += 1;
        if (message.type === 'error') this.errors.push(message);
        const index = this.waiters.findIndex((waiter) => waiter.type === message.type);
        if (index >= 0) {
            const [waiter] = this.waiters.splice(index, 1);
            clearTimeout(waiter.timer);
            waiter.resolve(message);
        } else {
            this.messages.push(message);
        }
    }

    send(message) {
        return this.ws?.readyState === WebSocket.OPEN && this.ws.send(JSON.stringify(message)) === undefined;
    }

    waitFor(type, timeoutMs = 10000) {
        const index = this.messages.findIndex((message) => message.type === type);
        if (index >= 0) return Promise.resolve(this.messages.splice(index, 1)[0]);
        return new Promise((resolve, reject) => {
            const waiter = { type, resolve, timer: null };
            waiter.timer = setTimeout(() => {
                const index = this.waiters.indexOf(waiter);
                if (index >= 0) this.waiters.splice(index, 1);
                reject(new Error(`client ${this.index} timed out waiting for ${type}`));
            }, timeoutMs);
            this.waiters.push(waiter);
        });
    }

    close() {
        this.closing = true;
        if (this.ws?.readyState === WebSocket.OPEN) this.ws.close(1000, 'load test complete');
    }
}

async function connectClients(clients) {
    const latencies = [];
    for (let offset = 0; offset < clients.length; offset += 25) {
        const batch = clients.slice(offset, offset + 25);
        latencies.push(...await Promise.all(batch.map((client) => client.connect())));
        await sleep(50);
    }
    return latencies;
}

async function setupMatches(clients) {
    const pairs = [];
    for (let index = 0; index < clients.length; index += 2) {
        pairs.push({ host: clients[index], guest: clients[index + 1], code: null });
    }
    for (let offset = 0; offset < pairs.length; offset += 20) {
        await Promise.all(pairs.slice(offset, offset + 20).map(async (pair) => {
            pair.host.send({
                type: 'create_room',
                ...versionFields,
                hostCharacterId: 'ZEUS',
                hostPassiveId: 'IRON_WILL',
                arenaId: 'CLASSIC_OLYMPUS',
                battleType: 'short',
                networkMode: mode === 'relay' ? 'relay' : 'p2p',
                hostNickname: `LoadHost${pair.host.index}`,
                hostPlayerId: `load-host-${pair.host.index}`,
                region: 'asia',
            });
            pair.code = (await pair.host.waitFor('room_created')).code;
            pair.guest.send({
                type: 'join_room',
                ...versionFields,
                code: pair.code,
                guestCharacterId: 'ZEUS',
                guestPassiveId: 'STORM_MASTERY',
                arenaId: 'CLASSIC_OLYMPUS',
                guestNickname: `LoadGuest${pair.guest.index}`,
                guestPlayerId: `load-guest-${pair.guest.index}`,
                region: 'asia',
            });
            await Promise.all([pair.host.waitFor('guest_joined'), pair.guest.waitFor('room_joined')]);
        }));
    }
    if (mode === 'relay') {
        await Promise.all(pairs.map(async (pair) => {
            pair.host.send({ type: 'game_start', ...versionFields, networkMode: 'relay' });
            await Promise.all([pair.host.waitFor('match_assigned'), pair.guest.waitFor('game_start')]);
        }));
    }
    return pairs;
}

async function runTraffic(clients, pairs) {
    let sentPackets = 0;
    let tick = 0;
    const startedAt = Date.now();
    while (Date.now() - startedAt < durationSec * 1000) {
        if (mode === 'lobby') {
            clients.forEach((client) => {
                client.send({ type: 'ping_check', ...versionFields, clientTime: Date.now(), rttMs: 40 });
                if (tick === 0) client.send({ type: 'get_room_list', ...versionFields, region: 'asia' });
            });
        } else if (mode === 'relay') {
            clients.forEach((client) => {
                client.send({ type: 'game_state', seq: tick, x: tick % 100, y: client.index, hp: 100 });
                sentPackets += 1;
            });
        }
        tick += 1;
        await sleep(mode === 'relay' ? 50 : 2000);
    }
    if (mode === 'relay') {
        pairs.forEach((pair) => pair.host.send({
            type: 'game_over',
            roundId: 1,
            hp: 100,
            remoteHp: 100,
            outcome: 'draw',
            reason: 'normal',
        }));
        await sleep(250);
    }
    return sentPackets;
}

async function capacity() {
    const parsed = new URL(url);
    parsed.protocol = parsed.protocol === 'wss:' ? 'https:' : 'http:';
    parsed.pathname = '/capacity';
    parsed.search = 'clientVersionCode=1&channel=dev&protocolVersion=1&rulesetVersion=1&balanceVersion=1';
    return fetch(parsed).then((response) => response.json());
}

function percentile(values, fraction) {
    if (!values.length) return null;
    const sorted = [...values].sort((a, b) => a - b);
    return Math.round(sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * fraction))]);
}

function parseArgs(values) {
    const result = {};
    for (let index = 0; index < values.length; index += 1) {
        const value = values[index];
        if (!value.startsWith('--')) continue;
        const key = value.slice(2);
        const next = values[index + 1];
        if (!next || next.startsWith('--')) {
            result[key] = true;
        } else {
            result[key] = next;
            index += 1;
        }
    }
    return result;
}

function positiveInt(value, fallback) {
    const parsed = Number.parseInt(String(value || ''), 10);
    return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

(async () => {
    const clients = Array.from({ length: clientsRequested }, (_, index) => new VirtualClient(index));
    const startedAt = performance.now();
    let pairs = [];
    try {
        const connectionLatencies = await connectClients(clients);
        if (mode !== 'lobby') pairs = await setupMatches(clients);
        const before = await capacity();
        const sentPackets = await runTraffic(clients, pairs);
        const during = await capacity();
        const summary = {
            target: url,
            mode,
            clients: clientsRequested,
            matches: pairs.length,
            durationSec,
            elapsedSec: Number(((performance.now() - startedAt) / 1000).toFixed(2)),
            connectLatencyMs: {
                p50: percentile(connectionLatencies, 0.5),
                p95: percentile(connectionLatencies, 0.95),
                max: percentile(connectionLatencies, 1),
            },
            sentPackets,
            receivedGameStates: clients.reduce((sum, client) => sum + client.receivedGameStates, 0),
            serverErrors: clients.flatMap((client) => client.errors).reduce((counts, error) => {
                const code = error.code || 'unknown';
                counts[code] = (counts[code] || 0) + 1;
                return counts;
            }, {}),
            unexpectedCloses: clients.filter((client) => client.unexpectedClose).length,
            capacityBefore: before.counts,
            capacityDuring: during.counts,
            backpressure: during.backpressure,
        };
        console.log(JSON.stringify(summary, null, 2));
        if (summary.unexpectedCloses > 0 || Object.keys(summary.serverErrors).length > 0) process.exitCode = 1;
    } finally {
        clients.forEach((client) => client.close());
        await sleep(250);
    }
})().catch((error) => {
    console.error(error.stack || error);
    process.exitCode = 1;
});
