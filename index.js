const http = require("http");
const net  = require("net");
const { WebSocketServer } = require("ws");

const PORT       = process.env.PORT       || 10000;
const AUTH_TOKEN = process.env.PROXY_TOKEN || null;

// ── HTTP server ───────────────────────────────────────────────────────────────
const server = http.createServer((req, res) => {
  if (req.url === "/" || req.url === "/__proxy_stats") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "ok", uptime: Math.floor(process.uptime()) }));
    return;
  }
  res.writeHead(404);
  res.end("Not found");
});

// ── WebSocket tunnel ──────────────────────────────────────────────────────────
const wss = new WebSocketServer({
  server,
  // Allow large binary frames (e.g. MP4 chunks).
  // The ws library defaults to 100 MB; being explicit here is safer.
  maxPayload: 128 * 1024 * 1024, // 128 MB
});

wss.on("connection", (ws) => {
  let tcpSocket     = null;
  let handshakeDone = false;
  let buffer        = [];       // frames that arrive before TCP is ready
  let tcpDrained    = true;     // backpressure gate: TCP → WS direction
  let wsDrained     = true;     // backpressure gate: WS  → TCP direction

  // ── WS → TCP ───────────────────────────────────────────────────────────────
  ws.on("message", (data, isBinary) => {

    // First message: JSON handshake
    if (!handshakeDone) {
      let handshake;
      try   { handshake = JSON.parse(data.toString()); }
      catch { ws.close(1008, "Bad handshake"); return; }

      if (AUTH_TOKEN && handshake.token !== AUTH_TOKEN) {
        ws.close(1008, "Unauthorized");
        return;
      }
      const { host, port } = handshake;
      if (!host || !port) { ws.close(1008, "Missing host/port"); return; }

      console.log(`[WS] Tunnel → ${host}:${port}`);
      handshakeDone = true;

      tcpSocket = net.connect(port, host, () => {
        ws.send(JSON.stringify({ status: "connected" }));
        // Flush buffered pre-connect frames
        for (const b of buffer) _tcpWrite(b);
        buffer = [];
      });

      // ── TCP → WS (with backpressure) ────────────────────────────────────
      tcpSocket.on("data", (chunk) => {
        if (ws.readyState !== ws.OPEN) return;

        const ok = ws.send(chunk, { binary: true }, (err) => {
          if (err) { tcpSocket && tcpSocket.destroy(); }
        });

        // ws.send() returns false when the WS send-buffer is full.
        // Pause TCP reads until the WS buffer drains to avoid OOM.
        if (ok === false && tcpDrained) {
          tcpDrained = false;
          tcpSocket.pause();
        }
      });

      tcpSocket.on("drain", () => {
        // TCP write-buffer drained → resume WS reads
        if (!wsDrained) {
          wsDrained = true;
          ws.resume();
        }
      });

      tcpSocket.on("close", ()      => ws.close());
      tcpSocket.on("error", (err)   => {
        console.error(`[TCP] ${err.message}`);
        ws.close(1011, err.message);
      });

      return;
    }

    // Subsequent messages: raw bytes → TCP
    if (tcpSocket && tcpSocket.writable) {
      _tcpWrite(data);
    } else {
      buffer.push(data);
    }
  });

  // Resume TCP reads once WS buffer has drained
  ws.on("drain", () => {
    if (!tcpDrained) {
      tcpDrained = true;
      tcpSocket && tcpSocket.resume();
    }
  });

  ws.on("close", () => tcpSocket && tcpSocket.destroy());
  ws.on("error", () => tcpSocket && tcpSocket.destroy());

  // ── Helpers ─────────────────────────────────────────────────────────────
  function _tcpWrite(data) {
    const ok = tcpSocket.write(data);
    // TCP write-buffer full → pause WS reads until it drains
    if (!ok && wsDrained) {
      wsDrained = false;
      ws.pause();
    }
  }
});

// ── WS "drain" event note ─────────────────────────────────────────────────────
// The built-in ws library emits "drain" on the socket underneath.
// Attach it after the server starts so it is always available.
wss.on("connection", () => {}); // no-op, real handler above

server.listen(PORT, () => {
  console.log(`🔀 WS proxy server on port ${PORT}`);
  console.log(AUTH_TOKEN ? "🔒 Auth enabled" : "⚠️  No PROXY_TOKEN set — open proxy");
});
