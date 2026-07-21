// punch_test · сервер кімнат для «Мамо, це не драма»
// Модель: ведучий-авторитет. Сервер = релей стану + присутність + серверний час.
const path = require("path");
const http = require("http");
const express = require("express");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const app = express();
app.use((req, res, next) => { res.set("Cache-Control", "no-store"); next(); });
// самопошук файлів гри: корінь / public / punch_cloud — де лежить index.html, звідти й роздаємо
const fs = require("fs");
const candidates = [__dirname, path.join(__dirname, "public"), path.join(__dirname, "punch_cloud")];
const staticRoot = candidates.find(d => { try { return fs.existsSync(path.join(d, "index.html")); } catch { return false; } }) || __dirname;
console.log("static root:", staticRoot);
app.use(express.static(staticRoot));
app.get("/health", (_q, res) => res.json({ ok: true, rooms: rooms.size }));

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: "/ws" });

const CODE_CHARS = "ABCDEFHKMNPRSTUWXYZ23456789";
const genCode = () => Array.from({ length: 4 }, () => CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)]).join("");
const genId = () => Math.random().toString(36).slice(2, 10);

// code -> { host: ws|null, state: obj|null, clients: Map(pid -> {ws, role, name, joinedAt}), emptySince }
const rooms = new Map();

const send = (ws, obj) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj)); };

function roster(room) {
  return [...room.clients.entries()]
    .filter(([, c]) => c.role === "player")
    .map(([pid, c]) => ({ pid, name: c.name, joinedAt: c.joinedAt, online: c.ws && c.ws.readyState === 1 }));
}
const pushPresence = (room) => send(room.host, { t: "presence", players: roster(room) });
const broadcast = (room, obj) => { for (const [, c] of room.clients) send(c.ws, obj); };

wss.on("connection", (ws) => {
  ws.meta = { code: null, pid: null, role: null };
  ws.isAlive = true;
  ws.on("pong", () => { ws.isAlive = true; });

  ws.on("message", (raw) => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    const room = ws.meta.code ? rooms.get(ws.meta.code) : (m.code ? rooms.get(String(m.code).toUpperCase()) : null);

    switch (m.t) {
      case "host_create": {
        let code; do { code = genCode(); } while (rooms.has(code));
        rooms.set(code, { host: ws, state: null, clients: new Map(), emptySince: null });
        ws.meta = { code, pid: "host", role: "host" };
        send(ws, { t: "room", code, state: null });
        break;
      }
      case "host_resume": {
        const code = String(m.code || "").toUpperCase();
        const r = rooms.get(code);
        if (!r) { send(ws, { t: "resume_fail" }); break; }
        r.host = ws;
        ws.meta = { code, pid: "host", role: "host" };
        send(ws, { t: "room", code, state: r.state });
        pushPresence(r);
        break;
      }
      case "join": { // {code, role: player|viewer, name?, pid?}
        const code = String(m.code || "").toUpperCase();
        const r = rooms.get(code);
        if (!r) { send(ws, { t: "err", err: "room_not_found" }); break; }
        const role = m.role === "viewer" ? "viewer" : "player";
        const pid = (m.pid && r.clients.has(m.pid)) ? m.pid : genId();
        const prev = r.clients.get(pid);
        r.clients.set(pid, {
          ws, role,
          name: (m.name || prev?.name || (role === "viewer" ? "Глядач" : "Гравець")).slice(0, 16),
          joinedAt: prev?.joinedAt || Date.now(),
        });
        ws.meta = { code, pid, role };
        send(ws, { t: "joined", pid, state: r.state });
        if (role === "player") pushPresence(r);
        break;
      }
      case "state": { // від ведучого → всім
        if (!room || room.host !== ws) break;
        room.state = m.state;
        broadcast(room, { t: "state", state: m.state });
        break;
      }
      case "pupdate": { // від гравця чи глядача → ведучому, з серверним часом
        if (!room || (ws.meta.role !== "player" && ws.meta.role !== "viewer")) break;
        const c = room.clients.get(ws.meta.pid);
        send(room.host, { t: "pupdate", pid: ws.meta.pid, name: c?.name, data: m.data, ts: Date.now() });
        break;
      }
      case "ping": send(ws, { t: "pong", ts: Date.now() }); break;
    }
  });

  ws.on("close", () => {
    const { code, pid, role } = ws.meta;
    const r = code && rooms.get(code);
    if (!r) return;
    if (role === "host") {
      if (r.host === ws) r.host = null; // кімната живе — ведучий може повернутись
    } else if (pid && r.clients.get(pid)?.ws === ws) {
      r.clients.get(pid).ws = null;    // гравець може повернутись зі своїм pid
      if (role === "player") pushPresence(r);
    }
    if (!r.host && ![...r.clients.values()].some(c => c.ws)) r.emptySince = Date.now();
  });
});

// heartbeat + прибирання порожніх кімнат (30 хв)
setInterval(() => {
  wss.clients.forEach(ws => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false; ws.ping();
  });
  const now = Date.now();
  for (const [code, r] of rooms) {
    const anyone = r.host || [...r.clients.values()].some(c => c.ws);
    if (anyone) { r.emptySince = null; continue; }
    if (!r.emptySince) r.emptySince = now;
    if (now - r.emptySince > 30 * 60 * 1000) rooms.delete(code);
  }
}, 30 * 1000);

server.listen(PORT, () => console.log(`punch_test up on :${PORT}`));
