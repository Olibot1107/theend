const http = require("http");
const net = require("net");
const url = require("url");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 10000;
const AUTH_TOKEN = process.env.PROXY_TOKEN || null;

// ── HTTP server (handles stats + WS upgrade) ─────────────────────────────────
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
// Client connects via WS, sends a JSON handshake:
//   { host: "example.com", port: 443, token: "optional" }
// Then raw bytes flow both ways over the WS frames.

const wss = new WebSocketServer({ server });

wss.on("connection", (ws, req) => {
  let tcpSocket = null;
  let handshakeDone = false;
  let buffer = [];

  ws.on("message", (data) => {
    // First message is always the handshake JSON
    if (!handshakeDone) {
      let handshake;
      try {
        handshake = JSON.parse(data.toString());
      } catch {
        ws.close(1008, "Bad handshake");
        return;
      }

      if (AUTH_TOKEN && handshake.token !== AUTH_TOKEN) {
        ws.close(1008, "Unauthorized");
        return;
      }

      const { host, port } = handshake;
      if (!host || !port) {
        ws.close(1008, "Missing host/port");
        return;
      }

      console.log(`[WS] Tunnel → ${host}:${port}`);
      handshakeDone = true;

      tcpSocket = net.connect(port, host, () => {
        ws.send(JSON.stringify({ status: "connected" }));
        // Flush any buffered frames that arrived before socket was ready
        buffer.forEach((b) => tcpSocket.write(b));
        buffer = [];
      });

      tcpSocket.on("data", (chunk) => {
        if (ws.readyState === ws.OPEN) ws.send(chunk);
      });

      tcpSocket.on("close", () => ws.close());
      tcpSocket.on("error", (err) => {
        console.error(`[WS] TCP error: ${err.message}`);
        ws.close(1011, err.message);
      });

      return;
    }

    // Subsequent messages are raw bytes → forward to TCP
    if (tcpSocket && tcpSocket.writable) {
      tcpSocket.write(data);
    } else {
      buffer.push(data);
    }
  });

  ws.on("close", () => tcpSocket && tcpSocket.destroy());
  ws.on("error", () => tcpSocket && tcpSocket.destroy());
});

server.listen(PORT, () => {
  console.log(`🔀 WS proxy server on port ${PORT}`);
  console.log(AUTH_TOKEN ? "🔒 Auth enabled" : "⚠️  No PROXY_TOKEN set — open proxy");
});
