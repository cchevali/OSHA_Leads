# Deployment Notes

## One-Click Unsubscribe (Lightsail + Caddy)

**Caddyfile (proxy all paths to the app):**
```
unsub.microflowops.com {
  reverse_proxy 127.0.0.1:8088
}
```

**Validation commands (run on the Lightsail instance):**
```
sudo caddy validate --config /etc/caddy/Caddyfile
sudo systemctl reload caddy
sudo systemctl status caddy --no-pager -l | sed -n '1,20p'
sudo journalctl -u caddy --no-pager -n 30
```

**Expected app routes:**
- `POST /unsubscribe/register`
- `POST /unsubscribe/check`
- `GET /unsubscribe?token=...`
