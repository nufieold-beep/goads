import http.server, os

# Maximum payload size (10 MB) to prevent memory exhaustion.
_MAX_BODY = 10 * 1024 * 1024
# Shared secret — must match the sender.  Set via RECV_SECRET env var.
_SECRET = os.environ.get('RECV_SECRET', '')

if not _SECRET:
    raise SystemExit('RECV_SECRET environment variable must be set')

class H(http.server.BaseHTTPRequestHandler):
    def do_PUT(self):
        if self.headers.get('X-Auth-Token') != _SECRET:
            self.send_response(403)
            self.end_headers()
            return
        n = int(self.headers.get('Content-Length', 0))
        if n <= 0 or n > _MAX_BODY:
            self.send_response(400)
            self.end_headers()
            return
        d = self.rfile.read(n)
        open('/tmp/pbs_new','wb').write(d)
        self.send_response(200)
        self.end_headers()
    def log_message(self, *a):
        pass

# Bind to loopback only — not exposed to the network.
s = http.server.HTTPServer(('127.0.0.1', 8765), H)
s.handle_request()