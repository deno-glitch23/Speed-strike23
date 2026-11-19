// Speed Strike — oyun dosyalarını sunar, aynı odadaki oyuncuların mesajlarını birbirine iletir
// ve oyuncu hesaplarını (kullanıcı adı + şifre + ilerleme) saklar.
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
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

/* ---------------- hesap deposu ----------------
   Render'ın ücretsiz sunucusu uyuyunca kendi dosyalarını siler. Bu yüzden hesaplar Upstash
   veritabanında tutulur. Render'da şu iki ayar (Environment) girilmiş olmalıdır:
     UPSTASH_REDIS_REST_URL   ve   UPSTASH_REDIS_REST_TOKEN
   Ayarlar yoksa hesaplar geçici bir dosyada tutulur (yalnızca deneme içindir, kalıcı değildir). */
const DB_URL = (process.env.UPSTASH_REDIS_REST_URL || '').trim().replace(/\/+$/, '');
const DB_TOKEN = (process.env.UPSTASH_REDIS_REST_TOKEN || '').trim();
const KALICI = /^(https:\/\/|http:\/\/127\.0\.0\.1[:\/])/.test(DB_URL) && DB_TOKEN.length > 0;
const SESSION_SEC = 180 * 24 * 3600; // giriş 180 gün geçerli

async function redis(cmd) {
  const ac = new AbortController(); const t = setTimeout(() => ac.abort(), 8000);
  try {
    const r = await fetch(DB_URL, { method: 'POST', signal: ac.signal, headers: { Authorization: 'Bearer ' + DB_TOKEN, 'Content-Type': 'application/json' }, body: JSON.stringify(cmd) });
    const j = await r.json();
    if (!r.ok || j.error) throw new Error('veritabani: ' + (j.error || r.status));
    return j.result;
  } finally { clearTimeout(t); }
}
const FILE = path.join(require('os').tmpdir(), 'speed-strike-hesaplar.json');
let mem = new Map(), memTimer = null;
if (!KALICI) { try { mem = new Map(Object.entries(JSON.parse(fs.readFileSync(FILE, 'utf8')))); } catch (e) {} }
function memFlush() { if (memTimer) return; memTimer = setTimeout(() => { memTimer = null; fs.writeFile(FILE, JSON.stringify(Object.fromEntries(mem)), () => {}); }, 500); }
const store = KALICI ? {
  get: k => redis(['GET', k]),
  set: (k, v, ex) => redis(ex ? ['SET', k, v, 'EX', ex] : ['SET', k, v]),
  setnx: async (k, v) => (await redis(['SET', k, v, 'NX'])) === 'OK',
  del: k => redis(['DEL', k]),
} : {
  get: async k => (mem.has(k) ? mem.get(k) : null),
  set: async (k, v) => { mem.set(k, v); memFlush(); },
  setnx: async (k, v) => { if (mem.has(k)) return false; mem.set(k, v); memFlush(); return true; },
  del: async k => { mem.delete(k); memFlush(); },
};

const USER_RE = /^[\p{L}\p{N}_]{3,14}$/u;
const userKey = name => String(name).normalize('NFKC').toLocaleLowerCase('tr');
const sha = s => crypto.createHash('sha256').update(s).digest('hex');
const scrypt = (pw, salt) => new Promise((ok, no) => crypto.scrypt(pw, salt, 32, (e, b) => (e ? no(e) : ok(b))));

// Basit hız sınırı: anahtar başına belirli sürede en fazla n deneme
const hits = new Map();
function limited(key, n, ms) {
  const now = Date.now(); let h = hits.get(key);
  if (!h || now > h.t) { h = { c: 0, t: now + ms }; hits.set(key, h); }
  h.c++; return h.c > n;
}
setInterval(() => { const now = Date.now(); for (const [k, h] of hits) if (now > h.t) hits.delete(k); }, 60000).unref();

const sessions = new Map(); // oturum özeti -> kullanıcı (veritabanına daha az gitmek için)
async function whoIs(req) {
  const m = /^Bearer ([a-f0-9]{48})$/.exec(req.headers.authorization || '');
  if (!m) return null;
  const h = sha(m[1]);
  if (sessions.has(h)) return { key: sessions.get(h), h };
  const key = await store.get('t:' + h);
  if (!key) return null;
  if (sessions.size > 5000) sessions.clear();
  sessions.set(h, key);
  return { key, h };
}
async function newSession(key) {
  const tok = crypto.randomBytes(24).toString('hex'), h = sha(tok);
  await store.set('t:' + h, key, SESSION_SEC);
  sessions.set(h, key);
  return tok;
}
function readBody(req) {
  return new Promise((ok, no) => {
    let n = 0; const parts = [];
    req.on('data', c => { n += c.length; if (n > 16384) { no(new Error('buyuk')); req.destroy(); } else parts.push(c); });
    req.on('end', () => { try { const j = JSON.parse(Buffer.concat(parts).toString('utf8') || '{}'); ok(j && typeof j === 'object' ? j : {}); } catch (e) { no(e); } });
    req.on('error', no);
  });
}
function json(res, code, obj) {
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff' });
  res.end(JSON.stringify(obj));
}
function cleanSaveStr(v) { // ilerleme: düz bir nesne olmalı ve küçük kalmalı
  if (!v || typeof v !== 'object' || Array.isArray(v)) return null;
  const s = JSON.stringify(v);
  return s.length <= 6000 ? s : null;
}
const parse = s => { try { return s ? JSON.parse(s) : null; } catch (e) { return null; } };

async function api(req, res, u) {
  const ip = String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
  const M = req.method;
  if (u === '/api/status' && M === 'GET') return json(res, 200, { ok: true, kalici: KALICI });
  if (limited('ip:' + ip, 240, 60000)) return json(res, 429, { err: 'yavas' });

  if ((u === '/api/register' || u === '/api/login') && M === 'POST') {
    if (limited('auth:' + ip, 12, 5 * 60000)) return json(res, 429, { err: 'cok-deneme' });
    const b = await readBody(req);
    const name = typeof b.user === 'string' ? b.user.trim().normalize('NFKC') : '', pass = typeof b.pass === 'string' ? b.pass : '';
    if (!USER_RE.test(name)) return json(res, 400, { err: 'ad' });
    if (pass.length < 6 || pass.length > 64) return json(res, 400, { err: 'sifre' });
    const key = userKey(name);

    if (u === '/api/register') {
      if (limited('reg:' + ip, 5, 3600000)) return json(res, 429, { err: 'cok-deneme' });
      const salt = crypto.randomBytes(16);
      const hash = await scrypt(pass, salt);
      const rec = JSON.stringify({ name, salt: salt.toString('hex'), hash: hash.toString('hex'), at: Date.now() });
      if (!(await store.setnx('u:' + key, rec))) return json(res, 409, { err: 'alinmis' });
      const sv = cleanSaveStr(b.save);
      if (sv) await store.set('s:' + key, sv);
      return json(res, 200, { user: name, token: await newSession(key), save: parse(sv) });
    }
    if (limited('fail:' + key, 10, 10 * 60000)) return json(res, 429, { err: 'cok-deneme' });
    const rec = parse(await store.get('u:' + key));
    const salt = Buffer.from(rec ? rec.salt : '00', 'hex');
    const hash = await scrypt(pass, salt); // hesap yoksa da aynı süre harcansın
    if (!rec || !crypto.timingSafeEqual(hash, Buffer.from(rec.hash, 'hex'))) return json(res, 401, { err: 'yanlis' });
    hits.delete('fail:' + key);
    return json(res, 200, { user: rec.name, token: await newSession(key), save: parse(await store.get('s:' + key)) });
  }

  const who = await whoIs(req);
  if (!who) return json(res, 401, { err: 'oturum' });
  if (u === '/api/save' && M === 'GET') {
    const rec = parse(await store.get('u:' + who.key));
    if (!rec) return json(res, 401, { err: 'oturum' });
    return json(res, 200, { user: rec.name, save: parse(await store.get('s:' + who.key)) });
  }
  if (u === '/api/save' && M === 'PUT') {
    if (limited('save:' + who.h, 40, 60000)) return json(res, 429, { err: 'yavas' });
    const sv = cleanSaveStr((await readBody(req)).save);
    if (!sv) return json(res, 400, { err: 'kayit' });
    await store.set('s:' + who.key, sv);
    return json(res, 200, { ok: true });
  }
  if (u === '/api/logout' && M === 'POST') {
    sessions.delete(who.h); await store.del('t:' + who.h);
    return json(res, 200, { ok: true });
  }
  return json(res, 404, { err: 'yok' });
}

const server = http.createServer((req, res) => {
  let u;
  try { u = decodeURIComponent((req.url || '/').split('?')[0]); } catch (e) { u = '/'; }
  if (u === '/healthz') { res.writeHead(200); return res.end('ok'); }
  if (u.startsWith('/api/')) {
    return void api(req, res, u).catch(e => { console.error('api hatasi:', e && e.message); if (!res.headersSent) json(res, 500, { err: 'sunucu' }); });
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

server.listen(PORT, () => console.log('Speed Strike calisiyor: http://localhost:' + PORT + (KALICI ? ' (hesaplar: kalici veritabani)' : ' (UYARI: veritabani ayari yok, hesaplar kalici degil)')));
