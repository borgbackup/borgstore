# borgstore systemd + nginx setup

Runs one borgstore REST server per repository, started **on demand** by systemd
socket activation, with nginx terminating TLS and routing clients to the right
instance by URL path.

## How it works

```
borg client
  → http://backup.example.com/repos/myrepo/   (nginx)
    → strips /repos/myrepo prefix
    → http://unix:/run/borgstore/myrepo.sock:/  (borgstore process)
```

Note: The example nginx configuration uses HTTP for simplicity and CI
compatibility. For production use, you **must** add SSL/TLS configuration.

- **`borgstore@.socket`** — systemd creates `/run/borgstore/<name>.sock` and
  starts the matching service on the first incoming connection.
- **`borgstore@.service`** — runs `borgstore.server.rest --socket-activation`,
  adopting the pre-bound socket from systemd.
- **`nginx-borgstore.conf`** — a single wildcard `location` block routes any
  `/repos/<name>/` URL to the matching Unix socket; nginx strips the path
  prefix so borgstore always sees requests rooted at `/`.
- **`borgstore-proxy.conf`** — shared nginx snippet (proxy headers, buffering
  off, timeouts) included by the wildcard location.

## Files

| File | Install to |
|------|------------|
| `borgstore@.service` | `/etc/systemd/system/` |
| `borgstore@.socket` | `/etc/systemd/system/` |
| `nginx-borgstore.conf` | `/etc/nginx/sites-available/` |
| `borgstore-proxy.conf` | `/etc/nginx/snippets/` |
| `repo1.env.example` | `/etc/borgstore/<name>.env` (one per repo) |

## Adding a repository

**1. Create the env file** (`chmod 600`, owned by `borgstore`):

```ini
# /etc/borgstore/myrepo.env
BORGSTORE_BACKEND=file:///srv/borgstore/myrepo
BORGSTORE_USERNAME=myuser
BORGSTORE_PASSWORD=secret
```

**2. Enable the socket unit:**

```bash
systemctl enable --now borgstore@myrepo.socket
```

That's it. The wildcard nginx location picks up the new repo automatically
— no nginx reload needed.

**3. Use with borg:**

```bash
borg -r http://myuser:secret@backup.example.com/repos/myrepo/ repo-create ...
```

## Initial deployment

```bash
# Install units
cp borgstore@.service borgstore@.socket /etc/systemd/system/
systemctl daemon-reload

# Install nginx config
cp nginx-borgstore.conf /etc/nginx/sites-available/borgstore
ln -s /etc/nginx/sites-available/borgstore /etc/nginx/sites-enabled/
cp borgstore-proxy.conf /etc/nginx/snippets/

# Create the borgstore system user (if not already present)
useradd --system --home /srv/borgstore --shell /usr/sbin/nologin borgstore

# Add repos as above, then test nginx config
nginx -t && nginx -s reload
```

## Notes

- The borgstore process is started on the first connection and stays running
  while connections are open. Add `TimeoutStopSec=` to the service unit to
  shut it down after a period of inactivity.
- The socket file at `/run/borgstore/<name>.sock` is recreated automatically
  after a reboot by systemd (`RuntimeDirectory=borgstore` in the service unit).
- TLS is handled entirely by nginx; the borgstore process never sees HTTPS.
