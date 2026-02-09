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
- `GET /prefs/enable_lows?token=<signed_token>&subscriber_key=...&territory_code=...` (accepts `token=` or `t=`)
- `GET /prefs/disable_lows?token=<signed_token>&subscriber_key=...&territory_code=...` (accepts `token=` or `t=`)

### Deploy / Restart (Lightsail)

This service typically runs under `systemd` on the Lightsail instance and is fronted by Caddy.

1. SSH into the instance and update the repo (use the real path for your deploy checkout):
```bash
cd ~/OSHA_Leads
git fetch origin
git pull --ff-only
```

2. Restart the unsub service.

If you know the unit name:
```bash
sudo systemctl restart unsub
sudo systemctl status unsub --no-pager -l
```

If you do not know the unit name, discover it first:
```bash
sudo systemctl list-units --type=service --all | grep -i unsub
sudo systemctl list-units --type=service --all | grep -i microflow
ps aux | grep -i unsubscribe_server.py | grep -v grep
```
Then restart the matching unit and check logs:
```bash
sudo journalctl -u <UNIT_NAME> --no-pager -n 80
```

3. Verify routes via `curl` (invalid token should be a branded error page, not a 404):
```bash
curl -I https://unsub.microflowops.com/prefs/enable_lows
curl -I https://unsub.microflowops.com/prefs/disable_lows

curl -i 'https://unsub.microflowops.com/prefs/enable_lows?token=invalid.invalid&territory_code=TX_TRIANGLE_V1&subscriber_key=sub_tx_triangle_v1_0000000000'
curl -i 'https://unsub.microflowops.com/prefs/disable_lows?token=invalid.invalid&territory_code=TX_TRIANGLE_V1&subscriber_key=sub_tx_triangle_v1_0000000000'
```

Expected:
- `curl -I` on the prefs paths returns `HTTP/1.1 200` (Caddy + app reachability).
- `GET ...?t=invalid.invalid` returns `HTTP/1.1 400` with an HTML body containing an "Invalid link" message.
