// Speed Strike — oyun dosyalarını sunar ve aynı odadaki oyuncuların mesajlarını birbirine iletir.
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
// Oyun dosyaları 'public' klasöründeyse oradan, yoksa bu dosyanın yanından sunulur.
const PUB = fs.existsSync(path.join(__dirname, 'public', 'index.html')) ? path.join(__dirname, 'public') : __dirname;
const ALLOWED = new Set(['/index.html', '/three.min.js']); // yalnızca bu iki dosya dışarı verilir
const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8' };
const MAX_ROOMS = 300;        // aynı anda en fazla oda
const MAX_PER_ROOM = 12;      // bir odada en fazla oyuncu
const MAX_MSG_PER_SEC = 120;  // bir oyuncunun saniyede gönderebileceği mesaj
const ROOM_RE = /^[a-z0-9][a-z0-9_.-]{0,47}$/;
const TOPIC_RE = /^[a-z][a-z0-9_.-]{0,47}$/;

// --- Dünya sıralaması: gerçek oyuncuların seviyesi (en iyi 100 gösterilir) ---
// Not: ücretsiz sunucuda disk kalıcı değildir; sunucu yeniden başlarsa liste sıfırlanır,
// oyuncular oyunu açtıkça yeniden dolar.
const LB_FILE = path.join(require('os').tmpdir(), 'speedstrike-lb.json');
const LB_KEEP = 1000;
const lb = new Map(); // oyuncu kimliği -> {n, xp, rp, ts}
try { for (const [k, e] of JSON.parse(fs.readFileSync(LB_FILE, 'utf8'))) lb.set(k, e); } catch (e) {}
let lbTimer = null;
function lbSave() { if (lbTimer) return; lbTimer = setTimeout(() => { lbTimer = null; fs.writeFile(LB_FILE, JSON.stringify([...lb]), () => {}); }, 5000); }
function lbTop() { return [...lb].map(([id, e]) => ({ id, n: e.n, xp: e.xp, rp: e.rp })).sort((a, b) => b.xp - a.xp).slice(0, 100); }
const lbHits = new Map(); // adres -> son yazma zamanı
function lbPost(req, res) {
  const ip = String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
  const now = Date.now();
  if (now - (lbHits.get(ip) || 0) < 1500) { res.writeHead(429); return res.end(); }
  lbHits.set(ip, now); if (lbHits.size > 5000) lbHits.clear();
  let body = '', bad = false;
  req.on('data', c => { body += c; if (body.length > 600) { bad = true; req.destroy(); } });
  req.on('end', () => {
    if (bad) return;
    let m; try { m = JSON.parse(body); } catch (e) { res.writeHead(400); return res.end(); }
    const id = m && typeof m.id === 'string' ? m.id : '';
    const xp = Math.floor(Number(m && m.xp)), rp = Math.floor(Number(m && m.rp) || 0);
    if (!/^[a-z0-9]{8,40}$/i.test(id) || !(xp > 0) || xp > 5e6 || !(rp >= 0) || rp > 1e6) { res.writeHead(400); return res.end(); }
    const n = String(m.n || 'Oyuncu').replace(/[\u0000-\u001f<>&"']/g, '').trim().slice(0, 14) || 'Oyuncu';
    lb.set(id, { n, xp, rp, ts: now });
    if (lb.size > LB_KEEP) { const low = [...lb].sort((a, b) => a[1].xp - b[1].xp).slice(0, lb.size - LB_KEEP); for (const [k] of low) lb.delete(k); }
    lbSave(); res.writeHead(204); res.end();
  });
  req.on('error', () => {});
}

const server = http.createServer((req, res) => {
  let u;
  try { u = decodeURIComponent((req.url || '/').split('?')[0]); } catch (e) { u = '/'; }
  if (u === '/healthz') { res.writeHead(200); return res.end('ok'); }
  if (u === '/lb') {
    if (req.method === 'POST') return lbPost(req, res);
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' }); return res.end(JSON.stringify(lbTop()));
  }
  if (u === '/') u = '/index.html';
  const f = path.normalize(path.join(PUB, u));
  if (!ALLOWED.has(u) || !f.startsWith(PUB + path.sep) || !TYPES[path.extname(f)]) { res.writeHead(404); return res.end('Bulunamadi'); }
  fs.readFile(f, (err, buf) => {
    if (err) { res.writeHead(404); return res.end('Bulunamadi'); }
    res.writeHead(200, { 'Content-Type': TYPES[path.extname(f)], 'Cache-Control': 'no-cache', 'X-Content-Type-Options': 'nosniff' });
    res.end(buf);
  });
});

const rooms = new Map(); // oda adı -> Map(oyuncu kimliği -> bağlantı)
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 8192 });
let nextId = 1;

function send(ws, obj) { if (ws.readyState === 1) ws.send(JSON.stringify(obj)); }
function others(room, me, obj) { const s = JSON.stringify(obj); for (const [id, c] of room) if (id !== me && c.readyState === 1) c.send(s); }

wss.on('connection', (ws, req) => {
  let name = '';
  try { name = new URL(req.url, 'http://x').searchParams.get('room') || ''; } catch (e) {}
  if (!ROOM_RE.test(name)) return ws.close(1008, 'oda adi gecersiz');
  let room = rooms.get(name);
  if (!room) {
    if (rooms.size >= MAX_ROOMS) return ws.close(1013, 'sunucu dolu');
    room = new Map(); rooms.set(name, room);
  }
  if (room.size >= MAX_PER_ROOM) return ws.close(1013, 'oda dolu');

  const id = 'p' + (nextId++).toString(36) + Math.random().toString(36).slice(2, 8);
  ws.pr = {}; ws.alive = true; ws.tokens = MAX_MSG_PER_SEC; ws.last = Date.now();
  send(ws, { k: 'hi', id, peers: [...room].map(([pid, c]) => [pid, c.pr]) });
  room.set(id, ws);

  ws.on('pong', () => { ws.alive = true; });
  ws.on('message', (raw, isBinary) => {
    if (isBinary) return;
    const now = Date.now();
    ws.tokens = Math.min(MAX_MSG_PER_SEC, ws.tokens + (now - ws.last) / 1000 * MAX_MSG_PER_SEC); ws.last = now;
    if (ws.tokens < 1) return; ws.tokens -= 1;
    let m; try { m = JSON.parse(raw.toString()); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;
    if (m.k === 'p' && m.pr && typeof m.pr === 'object' && !Array.isArray(m.pr)) { ws.pr = m.pr; others(room, id, { k: 'p', peer: id, pr: m.pr }); }
    else if (m.k === 'e' && typeof m.t === 'string' && TOPIC_RE.test(m.t)) others(room, id, { k: 'e', t: m.t, d: m.d, peer: id });
  });
  ws.on('close', () => {
    room.delete(id); others(room, id, { k: 'bye', peer: id });
    if (!room.size) rooms.delete(name);
  });
  ws.on('error', () => {});
});

// Kopmuş bağlantıları temizle
setInterval(() => {
  for (const ws of wss.clients) { if (!ws.alive) { ws.terminate(); continue; } ws.alive = false; try { ws.ping(); } catch (e) {} }
}, 30000);

server.listen(PORT, () => console.log('Speed Strike calisiyor: http://localhost:' + PORT));
