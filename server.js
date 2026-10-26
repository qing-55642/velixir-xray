// Velixir free plan supervisor.
// - Listens on process.env.PORT (Velixir edge terminates HTTPS, forwards plain HTTP/WS here).
// - On startup downloads Xray-linux-64.zip from GitHub releases and extracts it with
//   the pure-JS `unzipper` package (the container has no unzip / python3).
// - Xray itself listens on 127.0.0.1:XRAY_PORT (default 18000) with VLESS + WS, no TLS.
// - This Node process proxies WebSocket upgrades on WS_PATH to Xray, and answers /health
//   so the Velixir health probe can tell when the app is actually up.
const http = require('http');
const https = require('https');
const net = require('net');
const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');

const PORT = parseInt(process.env.PORT || '3000', 10);
const XRAY_PORT = parseInt(process.env.XRAY_PORT || '18000', 10);
const WS_PATH = process.env.WS_PATH || '/velixir-vless';
const UUID = process.env.UUID;
const XRAY_URL = 'https://github.com/XTLS/Xray-core/releases/latest/download/Xray-linux-64.zip';
const XRAY_BIN = path.join(__dirname, 'xray');
const CONFIG_PATH = path.join(__dirname, 'config.json');

if (!UUID) {
  console.error('Missing UUID env var, refusing to start');
  process.exit(1);
}

function download(url, dest, redirects = 0) {
  return new Promise((resolve, reject) => {
    if (redirects > 5) return reject(new Error('too many redirects'));
    https.get(url, { headers: { 'User-Agent': 'velixir-xray/1.0' } }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        res.resume();
        return download(res.headers.location, dest, redirects + 1).then(resolve, reject);
      }
      if (res.statusCode !== 200) return reject(new Error('download HTTP ' + res.statusCode));
      const file = fs.createWriteStream(dest);
      res.pipe(file);
      file.on('finish', () => file.close(resolve));
      file.on('error', reject);
    }).on('error', reject);
  });
}

async function ensureXray() {
  if (fs.existsSync(XRAY_BIN)) {
    console.log('xray binary already present');
    return;
  }
  console.log('downloading Xray from', XRAY_URL);
  const zipPath = path.join(__dirname, 'xray.zip');
  await download(XRAY_URL, zipPath);
  const unzipper = require('unzipper');
  await fs.createReadStream(zipPath).pipe(unzipper.Extract({ path: __dirname })).promise();
  fs.unlinkSync(zipPath);
  fs.chmodSync(XRAY_BIN, 0o755);
  console.log('xray extracted');
}

function writeConfig() {
  const config = {
    log: { loglevel: 'warning' },
    inbounds: [{
      port: XRAY_PORT,
      listen: '127.0.0.1',
      protocol: 'vless',
      settings: { clients: [{ id: UUID, level: 0 }], decryption: 'none' },
      streamSettings: { network: 'ws', wsSettings: { path: WS_PATH } },
    }],
    outbounds: [
      { protocol: 'freedom', tag: 'direct' },
      { protocol: 'blackhole', tag: 'block' },
    ],
  };
  fs.writeFileSync(CONFIG_PATH, JSON.stringify(config, null, 2));
}

let xrayReady = false;
function startXray() {
  writeConfig();
  const child = spawn(XRAY_BIN, ['run', '-c', CONFIG_PATH], { stdio: ['ignore', 'pipe', 'pipe'] });
  child.stdout.on('data', (d) => { process.stdout.write('[xray] ' + d); if (!xrayReady) xrayReady = true; });
  child.stderr.on('data', (d) => { process.stderr.write('[xray] ' + d); if (!xrayReady) xrayReady = true; });
  child.on('exit', (code) => {
    console.error('xray exited with code', code, '- restarting in 3s');
    xrayReady = false;
    setTimeout(startXray, 3000);
  });
  child.on('error', (e) => console.error('failed to spawn xray', e));
}

function proxyUpgrade(req, socket, head) {
  let attempts = 0;
  const tryConnect = () => {
    const upstream = net.connect(XRAY_PORT, '127.0.0.1', () => {
      upstream.write(`${req.method} ${req.url} HTTP/1.1\r\n`);
      for (const [k, v] of Object.entries(req.headers)) upstream.write(`${k}: ${v}\r\n`);
      upstream.write('\r\n');
      if (head && head.length) upstream.write(head);
      socket.pipe(upstream);
      upstream.pipe(socket);
    });
    upstream.on('error', (e) => {
      attempts += 1;
      if (attempts <= 10) { setTimeout(tryConnect, 500); return; }
      console.error('upstream connect failed', e.message);
      try { socket.destroy(); } catch (_) {}
    });
    socket.on('error', () => { try { upstream.destroy(); } catch (_) {} });
    upstream.on('error', () => { try { socket.destroy(); } catch (_) {} });
  };
  tryConnect();
}

const server = http.createServer((req, res) => {
  if (req.url === '/health' || req.url === '/') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('ok');
    return;
  }
  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('not found');
});

server.on('upgrade', (req, socket, head) => {
  const urlPath = req.url.split('?')[0];
  if (urlPath !== WS_PATH) { try { socket.destroy(); } catch (_) {} return; }
  proxyUpgrade(req, socket, head);
});

(async () => {
  try {
    await ensureXray();
  } catch (e) {
    console.error('Xray download/extract failed:', e.message);
    console.error('Will retry on next restart. Keeping HTTP server up for logs/health.');
  }
  startXray();
  server.listen(PORT, () => console.log(`supervisor listening on ${PORT}, WS_PATH=${WS_PATH}, XRAY_PORT=${XRAY_PORT}`));
})();
