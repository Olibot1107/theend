const http = require("http");
const https = require("https");
const net = require("net");
const url = require("url");

const PORT = process.env.PORT || 8888;
const AUTH_TOKEN = process.env.PROXY_TOKEN || null; // optional: set this env var to require auth

// Very small in-memory request log (last 200)
const requestLog = [];
function logRequest(entry) {
  requestLog.unshift({ ...entry, time: new Date().toISOString() });
  if (requestLog.length > 200) requestLog.pop();
}

function checkAuth(req) {
  if (!AUTH_TOKEN) return true;
  const header = req.headers["proxy-authorization"] || "";
  // Accepts: Proxy-Authorization: Bearer <token>
  const [scheme, token] = header.split(" ");
  return scheme === "Bearer" && token === AUTH_TOKEN;
}

function sendAuthError(socket) {
  socket.write(
    "HTTP/1.1 407 Proxy Authentication Required\r\n" +
    "Proxy-Authenticate: Bearer realm=\"proxy\"\r\n" +
    "Content-Length: 0\r\n\r\n"
  );
  socket.destroy();
}

const server = http.createServer((req, res) => {
  // Health check / stats endpoint (not a proxy request)
  if (req.url === "/__proxy_stats" || req.url === "/") {
    const stats = {
      status: "ok",
      uptime_seconds: Math.floor(process.uptime()),
      recent_requests: requestLog.slice(0, 20),
      total_logged: requestLog.length,
    };
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(stats, null, 2));
    return;
  }

  if (!checkAuth(req)) {
    res.writeHead(407, { "Proxy-Authenticate": 'Bearer realm="proxy"' });
    res.end("Proxy authentication required");
    return;
  }

  // Plain HTTP proxying
  const parsed = url.parse(req.url);
  const options = {
    hostname: parsed.hostname,
    port: parsed.port || 80,
    path: parsed.path,
    method: req.method,
    headers: { ...req.headers, host: parsed.hostname },
  };

  // Strip hop-by-hop headers
  delete options.headers["proxy-authorization"];
  delete options.headers["proxy-connection"];

  logRequest({ type: "HTTP", method: req.method, host: parsed.hostname, path: parsed.path });

  const proxyReq = http.request(options, (proxyRes) => {
    res.writeHead(proxyRes.statusCode, proxyRes.headers);
    proxyRes.pipe(res);
  });

  proxyReq.on("error", (err) => {
    console.error(`[HTTP] Error proxying ${req.url}:`, err.message);
    res.writeHead(502);
    res.end(`Proxy error: ${err.message}`);
  });

  req.pipe(proxyReq);
});

// HTTPS CONNECT tunneling
server.on("connect", (req, clientSocket, head) => {
  if (!checkAuth(req)) {
    sendAuthError(clientSocket);
    return;
  }

  const [hostname, portStr] = req.url.split(":");
  const port = parseInt(portStr) || 443;

  logRequest({ type: "CONNECT", host: hostname, port });

  const serverSocket = net.connect(port, hostname, () => {
    clientSocket.write("HTTP/1.1 200 Connection Established\r\n\r\n");
    serverSocket.write(head);
    serverSocket.pipe(clientSocket);
    clientSocket.pipe(serverSocket);
  });

  serverSocket.on("error", (err) => {
    console.error(`[CONNECT] Error connecting to ${hostname}:${port}:`, err.message);
    clientSocket.write("HTTP/1.1 502 Bad Gateway\r\n\r\n");
    clientSocket.destroy();
  });

  clientSocket.on("error", () => serverSocket.destroy());
});

server.listen(PORT, () => {
  console.log(`🔀 Proxy server running on port ${PORT}`);
  if (AUTH_TOKEN) {
    console.log(`🔒 Auth enabled — Bearer token required`);
  } else {
    console.log(`⚠️  No PROXY_TOKEN set — proxy is open (fine for testing, lock it down for production)`);
  }
});
