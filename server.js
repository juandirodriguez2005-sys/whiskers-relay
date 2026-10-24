// Whiskers Post Office — servidor de salas (sin dependencias, solo Node.js).
// Empareja jugadores con códigos de sala y reenvía sus mensajes.
// No guarda partidas ni ejecuta lógica: el anfitrión de cada sala manda.
//
// Protocolo sobre WebSocket:
//   texto JSON  -> {op:"create", name, mode} | {op:"join", code, name} | {op:"leave"} | {op:"ping"}
//   respuestas  -> {op:"created", code} | {op:"joined", code, host, mode}
//                  {op:"peer_joined", name} | {op:"peer_left"} | {op:"error", msg} | {op:"pong"}
//   binario     -> se reenvía tal cual al otro jugador de la sala

"use strict";
const http = require("http");
const crypto = require("crypto");

const PORT = Number(process.env.PORT) || 8787;
const MAX_PLAYERS = 2;
const MAX_MSG_BYTES = 1024 * 1024;
const ROOM_IDLE_MS = 6 * 60 * 60 * 1000;
const WORDS = ["GATO", "MIAU", "BIGOTE", "OVILLO", "SARDINA", "COLA", "ZARPA", "SIESTA", "PELUSA", "SELLO", "CARTA", "FARO"];
const GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

const rooms = new Map(); // code -> {host, guests:Set, mode, hostName, touched}
const clients = new Set();

// ------------------------------------------------------------- WebSocket mínimo (RFC 6455)

class Client {
  constructor(socket) {
    this.socket = socket;
    this.buf = Buffer.alloc(0);
    this.frag = null; // {opcode, parts}
    this.alive = true;
    this.open = true;
    this.room = null;
    this.name = "";
    socket.setNoDelay(true);
    socket.on("data", (d) => this.onData(d));
    socket.on("close", () => this.onClose());
    socket.on("error", () => this.onClose());
  }

  onData(d) {
    this.buf = Buffer.concat([this.buf, d]);
    while (true) {
      if (this.buf.length < 2) return;
      const b0 = this.buf[0], b1 = this.buf[1];
      const fin = (b0 & 0x80) !== 0;
      const opcode = b0 & 0x0f;
      const masked = (b1 & 0x80) !== 0;
      let len = b1 & 0x7f;
      let off = 2;
      if (len === 126) {
        if (this.buf.length < 4) return;
        len = this.buf.readUInt16BE(2);
        off = 4;
      } else if (len === 127) {
        if (this.buf.length < 10) return;
        const big = this.buf.readBigUInt64BE(2);
        if (big > BigInt(MAX_MSG_BYTES)) return this.close(1009);
        len = Number(big);
        off = 10;
      }
      if (len > MAX_MSG_BYTES) return this.close(1009);
      const need = off + (masked ? 4 : 0) + len;
      if (this.buf.length < need) return;
      let payload = this.buf.subarray(off + (masked ? 4 : 0), need);
      if (masked) {
        const mask = this.buf.subarray(off, off + 4);
        const out = Buffer.allocUnsafe(len);
        for (let i = 0; i < len; i++) out[i] = payload[i] ^ mask[i & 3];
        payload = out;
      } else {
        payload = Buffer.from(payload);
      }
      this.buf = this.buf.subarray(need);
      this.onFrame(fin, opcode, payload);
      if (!this.open) return;
    }
  }

  onFrame(fin, opcode, payload) {
    if (opcode === 0x8) return this.close(1000);
    if (opcode === 0x9) return this.sendFrame(0xA, payload);
    if (opcode === 0xA) { this.alive = true; return; }
    if (opcode === 0x0) {
      if (!this.frag) return;
      this.frag.parts.push(payload);
      if (fin) {
        const all = Buffer.concat(this.frag.parts);
        const op = this.frag.opcode;
        this.frag = null;
        onMessage(this, all, op === 0x2);
      }
      return;
    }
    if (opcode === 0x1 || opcode === 0x2) {
      if (!fin) { this.frag = { opcode, parts: [payload] }; return; }
      onMessage(this, payload, opcode === 0x2);
    }
  }

  sendFrame(opcode, payload) {
    if (!this.open) return;
    const len = payload.length;
    let header;
    if (len < 126) {
      header = Buffer.from([0x80 | opcode, len]);
    } else if (len < 65536) {
      header = Buffer.alloc(4);
      header[0] = 0x80 | opcode; header[1] = 126; header.writeUInt16BE(len, 2);
    } else {
      header = Buffer.alloc(10);
      header[0] = 0x80 | opcode; header[1] = 127; header.writeBigUInt64BE(BigInt(len), 2);
    }
    this.socket.write(Buffer.concat([header, payload]));
  }

  sendJSON(obj) { this.sendFrame(0x1, Buffer.from(JSON.stringify(obj), "utf8")); }
  sendBinary(buf) { this.sendFrame(0x2, buf); }

  close(code = 1000) {
    if (!this.open) return;
    const b = Buffer.alloc(2);
    b.writeUInt16BE(code, 0);
    try { this.sendFrame(0x8, b); } catch {}
    this.open = false;
    this.socket.end();
    this.onClose();
  }

  onClose() {
    if (this.closedHandled) return;
    this.closedHandled = true;
    this.open = false;
    clients.delete(this);
    leave(this);
    this.socket.destroy();
  }
}

// ------------------------------------------------------------- salas

function makeCode() {
  for (let i = 0; i < 100; i++) {
    const code = `${WORDS[Math.floor(Math.random() * WORDS.length)]}-${Math.floor(1000 + Math.random() * 9000)}`;
    if (!rooms.has(code)) return code;
  }
  return "SALA-" + Date.now().toString().slice(-6);
}

function normalize(code) {
  return String(code || "").trim().toUpperCase().replace(/\s+/g, "");
}

function onMessage(c, data, isBinary) {
  if (isBinary) {
    const room = rooms.get(c.room);
    if (!room) return;
    room.touched = Date.now();
    const targets = c === room.host ? [...room.guests] : [room.host];
    for (const t of targets) t.sendBinary(data);
    return;
  }
  let msg;
  try { msg = JSON.parse(data.toString("utf8")); } catch { return c.sendJSON({ op: "error", msg: "Mensaje no válido" }); }
  switch (msg.op) {
    case "create": {
      if (c.room) return c.sendJSON({ op: "error", msg: "Ya estás en una sala" });
      const code = makeCode();
      rooms.set(code, { host: c, guests: new Set(), mode: msg.mode === "visit" ? "visit" : "shared",
        hostName: String(msg.name || "Anfitrión").slice(0, 24), touched: Date.now() });
      c.room = code;
      c.name = String(msg.name || "Anfitrión").slice(0, 24);
      c.sendJSON({ op: "created", code });
      log(`sala ${code} creada (${rooms.get(code).mode})`);
      break;
    }
    case "join": {
      const code = normalize(msg.code);
      const room = rooms.get(code);
      if (!room) return c.sendJSON({ op: "error", msg: "No existe ninguna sala con ese código" });
      if (1 + room.guests.size >= MAX_PLAYERS) return c.sendJSON({ op: "error", msg: "La sala está llena" });
      if (c.room) return c.sendJSON({ op: "error", msg: "Ya estás en una sala" });
      room.guests.add(c);
      room.touched = Date.now();
      c.room = code;
      c.name = String(msg.name || "Invitado").slice(0, 24);
      c.sendJSON({ op: "joined", code, host: room.hostName, mode: room.mode });
      room.host.sendJSON({ op: "peer_joined", name: c.name });
      log(`${c.name} entra en ${code}`);
      break;
    }
    case "leave":
      leave(c);
      break;
    case "ping":
      c.sendJSON({ op: "pong", t: msg.t });
      break;
  }
}

function leave(c) {
  const code = c.room;
  if (!code) return;
  c.room = null;
  const room = rooms.get(code);
  if (!room) return;
  if (c === room.host) {
    for (const g of room.guests) {
      g.sendJSON({ op: "error", msg: "El anfitrión ha cerrado la sala" });
      g.room = null;
    }
    rooms.delete(code);
    log(`sala ${code} cerrada`);
  } else {
    room.guests.delete(c);
    room.host.sendJSON({ op: "peer_left" });
  }
}

function log(s) {
  console.log(new Date().toISOString(), s);
}

// ------------------------------------------------------------- HTTP + upgrade

const server = http.createServer((req, res) => {
  res.writeHead(200, { "content-type": "text/plain; charset=utf-8" });
  res.end(`Whiskers Post Office · servidor de salas OK · salas activas: ${rooms.size}\n`);
});

server.on("upgrade", (req, socket) => {
  const key = req.headers["sec-websocket-key"];
  if (!key || String(req.headers.upgrade).toLowerCase() !== "websocket") {
    socket.end("HTTP/1.1 400 Bad Request\r\n\r\n");
    return;
  }
  const accept = crypto.createHash("sha1").update(key + GUID).digest("base64");
  socket.write(
    "HTTP/1.1 101 Switching Protocols\r\n" +
    "Upgrade: websocket\r\nConnection: Upgrade\r\n" +
    `Sec-WebSocket-Accept: ${accept}\r\n\r\n`
  );
  clients.add(new Client(socket));
});

setInterval(() => {
  for (const c of clients) {
    if (!c.alive) { c.close(1001); continue; }
    c.alive = false;
    c.sendFrame(0x9, Buffer.alloc(0));
  }
  const now = Date.now();
  for (const [code, room] of rooms) {
    if (now - room.touched > ROOM_IDLE_MS || !room.host.open) rooms.delete(code);
  }
}, 25000);

server.listen(PORT, () => log(`Whiskers relay escuchando en el puerto ${PORT}`));
