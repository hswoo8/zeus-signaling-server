const http = require('http');
const packageJson = require('./package.json');

const PORT = Number(process.env.PORT || 8080);
const ROUTER_CHANNEL = cleanToken(process.env.ROUTER_CHANNEL || 'production', 24);
const ROUTER_ALLOWED_CHANNELS = new Set(
    String(process.env.ROUTER_ALLOWED_CHANNELS || ROUTER_CHANNEL)
        .split(',')
        .map((value) => cleanToken(value, 24))
        .filter(Boolean)
);
const ROUTER_CACHE_TTL_SEC = positiveInt(process.env.ROUTER_CACHE_TTL_SEC, 60);
const ROUTER_MAINTENANCE = envBool('ROUTER_MAINTENANCE', false);
const ROUTER_MAINTENANCE_MESSAGE = String(
    process.env.ROUTER_MAINTENANCE_MESSAGE || '대전 서버 점검 중입니다. 잠시 후 다시 시도해주세요.'
).trim();
const ROUTER_STORE_URL = String(process.env.ROUTER_STORE_URL || '').trim();
const APP_LATEST_VERSION_CODE = positiveInt(process.env.APP_LATEST_VERSION_CODE, 1);
const APP_MIN_SUPPORTED_VERSION_CODE = positiveInt(process.env.APP_MIN_SUPPORTED_VERSION_CODE, 1);
const APP_UPDATE_MODE = parseUpdateMode(process.env.APP_UPDATE_MODE);
const APP_UPDATE_MESSAGE = String(process.env.APP_UPDATE_MESSAGE || '').trim();
const ROUTER_ALLOW_INSECURE_LOCAL = envBool('ROUTER_ALLOW_INSECURE_LOCAL', false);
const ROUTES = parseRoutes(process.env.ROUTER_ROUTES_JSON || '[]');

if (APP_MIN_SUPPORTED_VERSION_CODE > APP_LATEST_VERSION_CODE) {
    throw new Error('APP_MIN_SUPPORTED_VERSION_CODE must not exceed APP_LATEST_VERSION_CODE');
}

function cleanToken(value, maxLength) {
    return typeof value === 'string'
        ? value.trim().toLowerCase().replace(/[^a-z0-9._-]/g, '').slice(0, maxLength)
        : '';
}

function positiveInt(value, fallback = null) {
    const parsed = Number.parseInt(String(value ?? ''), 10);
    return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function optionalMax(value) {
    return positiveInt(value, Number.MAX_SAFE_INTEGER);
}

function parseUpdateMode(value) {
    const mode = String(value || 'none').trim().toLowerCase();
    if (!['none', 'soft', 'force'].includes(mode)) {
        throw new Error('APP_UPDATE_MODE must be one of none, soft, or force');
    }
    return mode;
}

function parseVersionCode(value) {
    const raw = String(value ?? '').trim();
    if (!/^[1-9]\d*$/.test(raw)) return null;
    const parsed = Number(raw);
    return Number.isSafeInteger(parsed) ? parsed : null;
}

function envBool(name, fallback) {
    const value = String(process.env[name] || '').trim().toLowerCase();
    if (!value) return fallback;
    if (['1', 'true', 'yes', 'on'].includes(value)) return true;
    if (['0', 'false', 'no', 'off'].includes(value)) return false;
    return fallback;
}

function safeUrl(value, protocols) {
    const raw = String(value || '').trim();
    try {
        const parsed = new URL(raw);
        if (!protocols.includes(parsed.protocol)) return '';
        const secure = parsed.protocol === 'https:' || parsed.protocol === 'wss:';
        const local = ['localhost', '127.0.0.1', '10.0.2.2'].includes(parsed.hostname);
        return secure || (ROUTER_ALLOW_INSECURE_LOCAL && local)
            ? parsed.toString().replace(/\/$/, '')
            : '';
    } catch {
        return '';
    }
}

function parseRoutes(raw) {
    let parsed;
    try {
        parsed = JSON.parse(raw);
    } catch (error) {
        throw new Error(`ROUTER_ROUTES_JSON is invalid JSON: ${error.message}`);
    }
    const rows = Array.isArray(parsed) ? parsed : parsed?.routes;
    if (!Array.isArray(rows)) throw new Error('ROUTER_ROUTES_JSON must be an array');
    return rows.map((route, index) => {
        const poolId = cleanToken(route?.poolId, 48);
        const webSocketUrl = safeUrl(route?.webSocketUrl, ['wss:', 'ws:']);
        const statsApiUrl = safeUrl(route?.statsApiUrl, ['https:', 'http:']);
        const protocolVersion = positiveInt(route?.protocolVersion);
        const rulesetVersion = positiveInt(route?.rulesetVersion);
        if (!poolId || !webSocketUrl || !statsApiUrl || !protocolVersion || !rulesetVersion) {
            throw new Error(`Route ${index} is missing a valid poolId, URL, protocolVersion, or rulesetVersion`);
        }
        return {
            poolId,
            webSocketUrl,
            statsApiUrl,
            minAppVersionCode: positiveInt(route.minAppVersionCode, 1),
            maxAppVersionCode: optionalMax(route.maxAppVersionCode),
            protocolVersion,
            rulesetVersion,
            minBalanceVersion: positiveInt(route.minBalanceVersion, 1),
            maxBalanceVersion: optionalMax(route.maxBalanceVersion),
        };
    });
}

function readJson(req) {
    return new Promise((resolve, reject) => {
        let body = '';
        req.setEncoding('utf8');
        req.on('data', (chunk) => {
            body += chunk;
            if (Buffer.byteLength(body, 'utf8') > 16384) {
                reject(Object.assign(new Error('Request too large'), { statusCode: 413 }));
                req.destroy();
            }
        });
        req.on('end', () => {
            try {
                resolve(body.trim() ? JSON.parse(body) : {});
            } catch {
                reject(Object.assign(new Error('Malformed JSON'), { statusCode: 400 }));
            }
        });
        req.on('error', reject);
    });
}

function sendJson(res, statusCode, body) {
    const raw = JSON.stringify(body);
    res.writeHead(statusCode, {
        'content-type': 'application/json; charset=utf-8',
        'content-length': Buffer.byteLength(raw),
        'cache-control': 'no-store',
    });
    res.end(raw);
}

function routeFor(body) {
    const channel = cleanToken(body?.channel, 24);
    if (!ROUTER_ALLOWED_CHANNELS.has(channel)) {
        return {
            httpStatus: 409,
            body: {
                status: 'wrong_environment',
                code: 'wrong_environment',
                message: `${channel || 'unknown'} 앱은 ${ROUTER_CHANNEL} 라우터를 사용할 수 없습니다.`,
            },
        };
    }
    if (ROUTER_MAINTENANCE) {
        return {
            httpStatus: 200,
            body: {
                status: 'maintenance',
                code: 'server_maintenance',
                message: ROUTER_MAINTENANCE_MESSAGE,
                retryAfterSec: 30,
            },
        };
    }

    const appVersionCode = positiveInt(body?.appVersionCode);
    const protocolVersion = positiveInt(body?.protocolVersion);
    const rulesetVersion = positiveInt(body?.rulesetVersion);
    const balanceVersion = positiveInt(body?.balanceVersion);
    if (!appVersionCode || !protocolVersion || !rulesetVersion || !balanceVersion) {
        return {
            httpStatus: 400,
            body: {
                status: 'invalid_request',
                code: 'missing_version_fields',
                message: '앱의 대전 버전 정보를 확인할 수 없습니다.',
            },
        };
    }

    const route = ROUTES.find((candidate) =>
        appVersionCode >= candidate.minAppVersionCode &&
        appVersionCode <= candidate.maxAppVersionCode &&
        protocolVersion === candidate.protocolVersion &&
        rulesetVersion === candidate.rulesetVersion &&
        balanceVersion >= candidate.minBalanceVersion &&
        balanceVersion <= candidate.maxBalanceVersion
    );
    if (!route) {
        const requiredVersionCode = ROUTES.length > 0
            ? Math.min(...ROUTES.map((candidate) => candidate.minAppVersionCode))
            : null;
        return {
            httpStatus: 200,
            body: {
                status: 'update_required',
                code: 'no_compatible_pool',
                message: '현재 앱과 호환되는 대전 서버가 없습니다. 최신 버전으로 업데이트해주세요.',
                requiredVersionCode,
                storeUrl: ROUTER_STORE_URL || null,
            },
        };
    }

    return {
        httpStatus: 200,
        body: {
            status: 'available',
            code: 'ok',
            channel: ROUTER_CHANNEL,
            poolId: route.poolId,
            webSocketUrl: route.webSocketUrl,
            statsApiUrl: route.statsApiUrl,
            protocolVersion: route.protocolVersion,
            rulesetVersion: route.rulesetVersion,
            minBalanceVersion: route.minBalanceVersion,
            maxBalanceVersion: route.maxBalanceVersion,
            cacheTtlSec: ROUTER_CACHE_TTL_SEC,
            expiresAtMs: Date.now() + ROUTER_CACHE_TTL_SEC * 1000,
        },
    };
}

function appPolicyFor(channelValue, versionCodeValue) {
    const channel = cleanToken(channelValue, 24);
    if (!ROUTER_ALLOWED_CHANNELS.has(channel)) {
        return {
            httpStatus: 409,
            body: {
                status: 'wrong_environment',
                code: 'wrong_environment',
                message: `${channel || 'unknown'} 앱은 ${ROUTER_CHANNEL} 라우터를 사용할 수 없습니다.`,
            },
        };
    }

    const currentVersionCode = parseVersionCode(versionCodeValue);
    if (!currentVersionCode) {
        return {
            httpStatus: 400,
            body: {
                status: 'invalid_request',
                code: 'invalid_request',
                message: '유효한 versionCode가 필요합니다.',
            },
        };
    }

    let policy = 'none';
    if (currentVersionCode < APP_MIN_SUPPORTED_VERSION_CODE) {
        policy = 'force';
    } else if (currentVersionCode < APP_LATEST_VERSION_CODE) {
        policy = APP_UPDATE_MODE;
    }

    return {
        httpStatus: 200,
        body: {
            status: 'ok',
            code: 'ok',
            policy,
            channel: ROUTER_CHANNEL,
            currentVersionCode,
            latestVersionCode: APP_LATEST_VERSION_CODE,
            minSupportedVersionCode: APP_MIN_SUPPORTED_VERSION_CODE,
            message: policy === 'none' ? null : APP_UPDATE_MESSAGE || null,
            storeUrl: policy === 'none' ? null : ROUTER_STORE_URL || null,
        },
    };
}

function createRouterServer() {
    return http.createServer(async (req, res) => {
        const url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
        if (req.method === 'GET' && url.pathname === '/health') {
            sendJson(res, 200, {
                ok: true,
                service: 'beerock-match-router',
                version: packageJson.version || '1.0.0',
                channel: ROUTER_CHANNEL,
                routeCount: ROUTES.length,
                pools: ROUTES.map((route) => route.poolId),
                appPolicy: {
                    latestVersionCode: APP_LATEST_VERSION_CODE,
                    minSupportedVersionCode: APP_MIN_SUPPORTED_VERSION_CODE,
                    mode: APP_UPDATE_MODE,
                },
            });
            return;
        }
        if (req.method === 'GET' && url.pathname === '/app-policy') {
            const result = appPolicyFor(
                url.searchParams.get('channel'),
                url.searchParams.get('versionCode')
            );
            sendJson(res, result.httpStatus, result.body);
            return;
        }
        if (req.method === 'POST' && url.pathname === '/route') {
            try {
                const result = routeFor(await readJson(req));
                sendJson(res, result.httpStatus, result.body);
            } catch (error) {
                sendJson(res, error.statusCode || 500, {
                    status: 'error',
                    code: error.statusCode === 400 ? 'invalid_json' : 'internal_error',
                    message: error.statusCode ? error.message : 'Internal server error',
                });
            }
            return;
        }
        sendJson(res, 404, { error: { code: 'not_found', message: 'Not found' } });
    });
}

if (require.main === module) {
    createRouterServer().listen(PORT, '0.0.0.0', () => {
        console.log(`Match router running on port ${PORT} (${ROUTER_CHANNEL}, ${ROUTES.length} routes)`);
    });
}

module.exports = { appPolicyFor, createRouterServer, routeFor };
