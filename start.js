const role = String(process.env.SERVICE_ROLE || 'game').trim().toLowerCase();

if (role === 'router') {
    const { createRouterServer } = require('./router');
    const port = Number(process.env.PORT || 8080);
    const channel = String(process.env.ROUTER_CHANNEL || 'production').trim().toLowerCase();
    createRouterServer().listen(port, '0.0.0.0', () => {
        console.log(`Match router running on port ${port} (${channel})`);
    });
} else if (role === 'game') {
    require('./server');
} else {
    throw new Error(`Unsupported SERVICE_ROLE: ${role}`);
}
