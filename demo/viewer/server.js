const http = require('http');
const fs = require('fs');
const path = require('path');
const net = require('net');

const WEB_PORT = Number(process.env.PORT || 8085);
const PUBLIC_DIR = path.join(__dirname, 'public');
const sseClients = new Set();
const connections = new Map();

function createTcpState(screenId, extra = {}) {
  return {
    screenId,
    connected: false,
    host: null,
    port: null,
    lastError: null,
    recordsReceived: 0,
    startedAt: null,
    info: '',
    ...extra,
  };
}

function getConnection(screenId) {
  let connection = connections.get(screenId);
  if (!connection) {
    connection = {
      screenId,
      socket: null,
      lineBuffer: '',
      state: createTcpState(screenId),
    };
    connections.set(screenId, connection);
  }
  return connection;
}

function listStatuses() {
  return Array.from(connections.values()).map((connection) => ({ ...connection.state }));
}

function sendSse(res, event, payload) {
  res.write(`event: ${event}\n`);
  res.write(`data: ${JSON.stringify(payload)}\n\n`);
}

function broadcast(event, payload) {
  for (const client of sseClients) {
    sendSse(client, event, payload);
  }
}

function sendSnapshot(res) {
  sendSse(res, 'snapshot', { screens: listStatuses() });
}

function sendStatus(screenId, extra = {}) {
  const connection = getConnection(screenId);
  connection.state = {
    ...connection.state,
    ...extra,
    screenId,
  };
  broadcast('status', connection.state);
}

function removeConnectionIfIdle(screenId) {
  const connection = connections.get(screenId);
  if (!connection) return;
  if (connection.socket || connection.state.connected) return;
  if (connection.state.host || connection.state.port || connection.state.recordsReceived > 0) return;
  connections.delete(screenId);
}

function disconnectTcp(screenId, reason = 'Disconnected', options = {}) {
  const connection = connections.get(screenId);
  if (!connection) return;

  if (connection.socket) {
    const socket = connection.socket;
    connection.socket = null;
    socket.removeAllListeners();
    socket.destroy();
  }

  connection.lineBuffer = '';
  connection.state = {
    ...connection.state,
    connected: false,
    info: reason,
  };

  if (options.reset) {
    connection.state = createTcpState(screenId, { info: reason });
  }

  broadcast('status', connection.state);
  if (options.reset) {
    removeConnectionIfIdle(screenId);
  }
}

function safeJson(res, code, payload) {
  res.writeHead(code, {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-cache',
  });
  res.end(JSON.stringify(payload));
}

function serveStatic(req, res) {
  const requestPath = req.url === '/' ? '/index.html' : req.url;
  const cleanPath = path.normalize(requestPath).replace(/^\/+/, '');
  const filePath = path.join(PUBLIC_DIR, cleanPath);

  if (!filePath.startsWith(PUBLIC_DIR)) {
    res.writeHead(403);
    res.end('Forbidden');
    return;
  }

  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404);
      res.end('Not found');
      return;
    }

    const ext = path.extname(filePath).toLowerCase();
    const contentType = {
      '.html': 'text/html; charset=utf-8',
      '.css': 'text/css; charset=utf-8',
      '.js': 'application/javascript; charset=utf-8',
      '.json': 'application/json; charset=utf-8',
      '.svg': 'image/svg+xml',
      '.png': 'image/png',
    }[ext] || 'application/octet-stream';

    res.writeHead(200, { 'Content-Type': contentType });
    res.end(data);
  });
}

function parseBody(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', (chunk) => {
      body += chunk.toString('utf8');
      if (body.length > 1_000_000) {
        reject(new Error('Request body too large'));
        req.destroy();
      }
    });
    req.on('end', () => {
      try {
        resolve(body ? JSON.parse(body) : {});
      } catch (err) {
        reject(new Error('Invalid JSON body'));
      }
    });
    req.on('error', reject);
  });
}

function firstFourCommaIndices(line) {
  const indices = [];
  for (let i = 0; i < line.length; i += 1) {
    if (line[i] === ',') {
      indices.push(i);
      if (indices.length === 4) {
        return indices;
      }
    }
  }
  return null;
}

function stripQuotes(value) {
  const trimmed = value.trim();
  if (trimmed.startsWith('"') && trimmed.endsWith('"')) {
    return trimmed.slice(1, -1);
  }
  return trimmed;
}

function parseCsvRecord(line) {
  const trimmed = line.trim();
  if (!trimmed) return null;

  const commas = firstFourCommaIndices(trimmed);
  if (!commas) {
    throw new Error(`Invalid CSV record (expected 5 columns): ${trimmed.slice(0, 120)}`);
  }

  const [c1, c2, c3, c4] = commas;
  const id = stripQuotes(trimmed.slice(0, c1));
  const timestamp = stripQuotes(trimmed.slice(c1 + 1, c2));
  const width = Number(stripQuotes(trimmed.slice(c2 + 1, c3)));
  const height = Number(stripQuotes(trimmed.slice(c3 + 1, c4)));
  const data = stripQuotes(trimmed.slice(c4 + 1));

  if (!Number.isFinite(width) || !Number.isFinite(height)) {
    throw new Error(`Invalid width/height in CSV record: ${trimmed.slice(0, 120)}`);
  }

  return { id, timestamp, width, height, data };
}

function processTcpChunk(screenId, chunk) {
  const connection = getConnection(screenId);
  connection.lineBuffer += chunk.toString('utf8');

  while (true) {
    const newlineIndex = connection.lineBuffer.indexOf('\n');
    if (newlineIndex === -1) break;

    const line = connection.lineBuffer.slice(0, newlineIndex).replace(/\r$/, '');
    connection.lineBuffer = connection.lineBuffer.slice(newlineIndex + 1);

    if (!line.trim()) continue;

    try {
      const frame = parseCsvRecord(line);
      if (!frame) continue;

      connection.state.recordsReceived += 1;
      broadcast('frame', { screenId, frame });

      if (connection.state.recordsReceived % 25 === 0) {
        sendStatus(screenId);
      }
    } catch (err) {
      connection.state.lastError = err.message;
      sendStatus(screenId);
      broadcast('warning', {
        screenId,
        message: err.message,
        linePreview: line.slice(0, 200),
      });
    }
  }
}

function connectTcp(screenId, host, port) {
  return new Promise((resolve, reject) => {
    const cleanScreenId = String(screenId || '').trim();
    const cleanHost = String(host || '').trim();
    const numericPort = Number(port);

    if (!cleanScreenId) {
      reject(new Error('screenId is required'));
      return;
    }

    if (!cleanHost || !numericPort) {
      reject(new Error('Both host and port are required'));
      return;
    }

    const connection = getConnection(cleanScreenId);
    if (connection.socket) {
      disconnectTcp(cleanScreenId, 'Reconnecting');
    }

    connection.lineBuffer = '';
    connection.state = createTcpState(cleanScreenId, {
      host: cleanHost,
      port: numericPort,
      startedAt: new Date().toISOString(),
    });

    const socket = net.createConnection({ host: cleanHost, port: numericPort });
    connection.socket = socket;
    let settled = false;

    socket.on('connect', () => {
      connection.state.connected = true;
      connection.state.info = 'TCP connection established';
      sendStatus(cleanScreenId);
      if (!settled) {
        settled = true;
        resolve(connection.state);
      }
    });

    socket.on('data', (chunk) => processTcpChunk(cleanScreenId, chunk));

    socket.on('error', (err) => {
      connection.state.lastError = err.message;
      connection.state.connected = false;
      connection.state.info = 'TCP connection error';
      sendStatus(cleanScreenId);
      if (!settled) {
        settled = true;
        reject(err);
      }
    });

    socket.on('close', () => {
      const current = connections.get(cleanScreenId);
      if (!current || current.socket !== socket) {
        return;
      }
      current.socket = null;
      current.state.connected = false;
      current.state.info = 'TCP connection closed';
      sendStatus(cleanScreenId);
    });
  });
}

const server = http.createServer(async (req, res) => {
  if (req.method === 'GET' && req.url === '/events') {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache, no-transform',
      Connection: 'keep-alive',
      'Access-Control-Allow-Origin': '*',
    });
    res.write('\n');

    sseClients.add(res);
    sendSnapshot(res);

    req.on('close', () => {
      sseClients.delete(res);
    });
    return;
  }

  if (req.method === 'GET' && (req.url === '/' || req.url.startsWith('/index.html'))) {
    serveStatic(req, res);
    return;
  }

  if (req.method === 'GET' && req.url.startsWith('/api/status')) {
    safeJson(res, 200, { screens: listStatuses() });
    return;
  }

  if (req.method === 'POST' && req.url === '/api/connect') {
    try {
      const body = await parseBody(req);
      const status = await connectTcp(body.screenId, body.host, body.port);
      safeJson(res, 200, { ok: true, status });
    } catch (err) {
      safeJson(res, 400, { ok: false, error: err.message });
    }
    return;
  }

  if (req.method === 'POST' && req.url === '/api/disconnect') {
    try {
      const body = await parseBody(req);
      const screenId = String(body.screenId || '').trim();
      const shouldRemove = Boolean(body.remove);
      if (!screenId) {
        throw new Error('screenId is required');
      }
      disconnectTcp(
        screenId,
        shouldRemove ? 'Screen removed' : 'Disconnected',
        shouldRemove ? { reset: true } : {}
      );
      const connection = connections.get(screenId);
      safeJson(res, 200, {
        ok: true,
        status: connection ? connection.state : createTcpState(screenId, {
          info: shouldRemove ? 'Screen removed' : 'Disconnected',
        }),
      });
    } catch (err) {
      safeJson(res, 400, { ok: false, error: err.message });
    }
    return;
  }

  if (req.method === 'GET') {
    serveStatic(req, res);
    return;
  }

  res.writeHead(404);
  res.end('Not found');
});

server.listen(WEB_PORT, () => {
  console.log(`TCP video viewer available at http://localhost:${WEB_PORT}`);
});

function shutdown() {
  for (const screenId of connections.keys()) {
    disconnectTcp(screenId, 'Server shutting down');
  }
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
