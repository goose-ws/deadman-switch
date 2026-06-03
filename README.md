![alt text](https://github.com/goose-ws/deadman-switch/blob/main/logo.png?raw=true "Deadman's Switch")

# Distributed Deadman's Switch

A highly-available, distributed deadman's switch written in Go, managed via Telegram's Bot API, and backed by a distributed consensus protocol (HashiCorp Raft).

## About the Project

The original inspiration for this project comes from a decade spent working in public safety as a firefighter and paramedic. In that line of work, you quickly realize how unpredictable life can be, and how many people wake up not knowing that their daily routine will suddenly be interrupted by an unforeseen emergency. While we all hope to live long, fulfilling lives, the reality is that none of us truly know what the future holds.

This project was built as a way to find peace of mind with that unpredictability. It serves as an automated, highly-available contingency plan for your digital life. Powered by a Telegram bot API and HashiCorp's Raft consensus algorithm, it can span across multiple cloud providers to ensure maximum uptime.

On a configurable schedule, the system "arms" itself and sends a check-in query. You then have a set period to respond with the expected disarm code. If the switch is not disarmed within the required timeframe, the system assumes you are no longer able to respond and triggers a predefined external script.

This payload script can be configured to execute any automated task -- from sending crucial "just in case" messages to loved ones and trusted contacts, to granting emergency server access, or even gracefully shutting down infrastructure. It ensures that your digital footprint and final communications are handled exactly as you intend, even if you are suddenly unavailable.

In full disclosure, I did not author all of this code. I drew the skeleton of the blueprints for this project on my own, with very limited knowledge of Go. Once I had fleshed out the general idea, commands, and functions, I used the aid of LLM AI to flesh it out the rest of the way.

## How It Works

1. **High Availability (Raft Consensus):** The application runs on separate servers (nodes) that constantly communicate securely using mutual TLS. Because they use a consensus algorithm, if a node goes offline, the remaining nodes will automatically elect a new "Leader" and continue functioning seamlessly without losing track of your timer.
2. **Telegram Interface:** The Leader node acts as your interface. It sends you messages via a Telegram Bot, prompting you to disarm the timer periodically.
3. **The Payload:** If the timer reaches zero, the Leader executes a bash script (`payload.sh`). This script can do anything you configure it to -- send emails, post webhooks, delete databases, or decrypt and deliver files.
4. **Monthly Automated Tests:** To ensure your payload actually works when the time comes, the cluster orchestrates a monthly dry run of your script sequentially across all nodes and sends you a detailed results report.

## Prerequisites

If you know your way around a Linux command line, you can set this up. You will need:

**Multiple Virtual Private Servers:** While the app *can* technically run on fewer nodes, true fault tolerance using the Raft consensus algorithm requires a "quorum" (a strict majority). Here is how the math breaks down:

| Nodes | Can tolerate | Notes |
| :---- | :---- | :---- |
| 1 | 0 failures | Runs fine, but offers zero redundancy. If this node goes down, your switch goes with it. |
| 2 | 0 failures | The *worst* configuration. Both nodes must be online at all times. It can actually be *less* reliable than a single node. |
| 3 | 1 failure | **The recommended minimum.** You can safely lose 1 node entirely without interrupting the countdown. |
| 5 | 2 failures | If you want stronger guarantees, go straight to 5 -- 4 nodes can still only tolerate 1 failure, due to the quorum requirement. |

**Provider Diversity (Multi-Cloud):** Ideally, you should spread your nodes across different cloud providers and geographic locations. This way, if one provider suffers a regional outage, your cluster stays alive. My personal setup uses:

1. A **Linode** in Atlanta, GA.
2. A **Google Cloud Compute** e2-micro instance (Always Free Tier) in Charleston, SC.
3. An **Oracle Cloud Compute** ARM instance (Always Free Tier) in Ashburn, VA.

If you would like to use [Linode](https://linode.com) as one of your hosts, you could support this project by using [my referral link](https://www.linode.com/lp/refer/?r=bc22b5da1ec4a6cee9540caad17b2b10b2fdcdc0). New accounts receive a $100 credit (valid for 60 days) once a valid payment method is added.

Because compiled Go binaries are incredibly lightweight, this multi-cloud architecture gives you enterprise-grade redundancy at minimal cost.

> **A Note on Docker:** While Docker is incredibly useful, I strongly advise against running this tool as a container. For a system designed for maximum reliability over years, you want this running directly on the OS. An unexpected Docker daemon failure, package update, or networking bridge issue should not be able to bring your deadman switch cluster down. Keep it simple and run the binary natively.

**Other requirements:**

- **A Domain Name** (e.g., `example.com`). You need DNS records pointing to your nodes (e.g., `node1.example.com`, `node2.example.com`) to obtain valid TLS certificates through [Certbot](https://certbot.eff.org/).
- **A Telegram Account** to interact with the bot.

> **A Note on Telegram:** While I don't hold a strong opinion one way or the other about Telegram and their ethics, I acknowledge that others do for what are very valid reasons. I chose the Telegram Bot API as the communication backbone because of the simplicity and ease of use its API offers for two way communication between a user and a bot. While similar results could be achieved with e-mail, a Discord bot, or other services, this was the easiest method available to me. I'd welcome any pull requests enabling additional platforms for those who do not wish to use Telegram.*

## Step 1: Telegram Bot Setup

1. Open Telegram and search for **@BotFather**.
2. Send `/newbot` and follow the prompts to name your bot and choose a username.
3. BotFather will give you a **Telegram Token** (e.g., `123456789:ABCDefgh...`). Save this for your config file.
4. **Create the Command Menu:** Send `/setcommands` to BotFather, select your bot, and paste the following list. This creates a quick-menu inside the chat:

```
init_arm - Initialize the system with your one-time arm code
arm - Skip snooze and re-arm the timer immediately
disarm - Disarm the timer (Requires offset code)
disable - Disarm (if active) and prevent automatic re-arming (Requires passphrase)
extend - Push the deadline forward without a full disarm cycle (Requires offset code)
status - Show the current state and node health
history - Show the last 10 significant system events
test - Run a manual payload test across all nodes
```

5. **Adjust Privacy Settings:** If you plan to use this bot in a group or supergroup, send `/setprivacy` to BotFather, select your bot, and choose **Disable**. This allows the bot to read your commands in the group without you needing to explicitly tag it every time.

6. **Admin Permissions:** If you are adding the bot to a group or supergroup, go to Group Settings → Administrators → Add your bot and grant it **Delete Messages** permission. This allows the bot to immediately redact your disarm codes after a successful disarm, preventing anyone with access to chat history from reverse-engineering your secret offset.

## Step 2: Server Prep & Certificates

Do this on **all nodes**.

The nodes communicate over port `8443` (derived from your `address` config) using mutual TLS, so they need valid certificates to authenticate each other.

**1. Open Ports**

Ensure the port specified in your `address` config (default `8443`) is open on your server's firewall for TCP.

**2. Get Let's Encrypt Certificates**

The easiest approach is [Certbot](https://certbot.eff.org/). Assuming you've pointed a DNS A record (e.g., `node1.yourdomain.com`) to your server's IP:

```bash
apt update && apt install certbot -y
certbot certonly --standalone -d node1.yourdomain.com
```

Your certificates will be generated at `/etc/letsencrypt/live/node1.yourdomain.com/`. You'll need the paths to `fullchain.pem` and `privkey.pem` for the config.

> **⚠️ Certificate Validity is Critical:** The cluster nodes authenticate each other using mutual TLS on every connection. If a node's certificate expires, all other nodes will refuse to connect to it, the cluster will lose quorum, and your switch will be unable to operate. **Do not let certificates expire.** The daemon logs a warning and sends a Telegram alert on startup if the certificate is expired or within 10 days of expiry.

It is recommended you add permissions and a post-renewal hook to configure your certificates in a way that the daemon can read them, and to allow the daemon to be restarted after certificate renewal to load the new ones.

**3. Create a Dedicated Service User**

Run the service under its own user. For the purposes of this README, it is assumed you are using the user `deadman`. Config files contain secrets (SMTP passwords, API tokens) so permissions must be locked down.

```bash
useradd --system --no-create-home deadman
mkdir -p /var/lib/deadman/raft /etc/deadman-switch/certs
chown -R deadman:deadman /var/lib/deadman /etc/deadman-switch
```

Rather than granting the `deadman` user access to Let's Encrypt's live directory directly, copy the certificates into a dedicated path owned by `deadman` and lock them down:

```bash
cp /etc/letsencrypt/live/node1.yourdomain.com/fullchain.pem /etc/deadman-switch/certs/fullchain.pem
cp /etc/letsencrypt/live/node1.yourdomain.com/privkey.pem /etc/deadman-switch/certs/privkey.pem
chown deadman:deadman /etc/deadman-switch/certs/*.pem
chmod 400 /etc/deadman-switch/certs/*.pem
```

Point `cert_file` and `key_file` in your config at these paths (`/etc/deadman-switch/certs/fullchain.pem`, etc.) rather than directly at the Let's Encrypt live directory.

**4. Automate Certificate Renewals**

Let's Encrypt certificates expire every 90 days. Because the daemon loads certificates on startup, it will not pick up a renewed cert until it is restarted. You must automate both the copy and the restart.

Create a renewal script (e.g. `/home/your-user/bin/scripts/update-ssl-certs.sh`) that:
1. Records checksums of the current certs before running certbot
2. Runs `certbot renew`
3. Compares checksums after renewal — if a cert changed, copies the new files into `/etc/deadman-switch/certs/`, sets ownership and permissions, then restarts the service

Because the script runs as a non-root user but needs to copy files into `/etc/deadman-switch/certs/` and restart the service, add the following passwordless sudo entries (adjust the domain name per host):

```
your-user ALL=(ALL) NOPASSWD: /usr/bin/cp /path/to/certbot/live/node1.yourdomain.com/fullchain.pem /etc/deadman-switch/certs/fullchain.pem
your-user ALL=(ALL) NOPASSWD: /usr/bin/cp /path/to/certbot/live/node1.yourdomain.com/privkey.pem /etc/deadman-switch/certs/privkey.pem
your-user ALL=(ALL) NOPASSWD: /usr/bin/chown deadman\:deadman /etc/deadman-switch/certs/fullchain.pem
your-user ALL=(ALL) NOPASSWD: /usr/bin/chown deadman\:deadman /etc/deadman-switch/certs/privkey.pem
your-user ALL=(ALL) NOPASSWD: /usr/bin/chmod 400 /etc/deadman-switch/certs/fullchain.pem
your-user ALL=(ALL) NOPASSWD: /usr/bin/chmod 400 /etc/deadman-switch/certs/privkey.pem
your-user ALL=(ALL) NOPASSWD: /usr/bin/systemctl restart deadman
```

Schedule the renewal script monthly via cron or a systemd timer. A missed or failed renewal will produce a Telegram alert the next time the service restarts, but by then peers may already be rejecting connections — renew proactively, well before the 90-day expiry.

## Step 3: Compiling the Binary

You only need to compile once -- on your own machine or any one of the servers -- then copy the resulting binary to all nodes.

1. Install Go **1.24 or later** (`apt install golang` may give an older version — check with `go version`, or download the latest from [go.dev](https://go.dev)).
2. Save `main.go` to a working directory.
3. In that directory, run:

```bash
go mod init deadmanswitch
go mod tidy
```

4. Compile for your servers:

```bash
# Standard 64-bit Linux (AMD/Intel):
GOOS=linux GOARCH=amd64 go build -o deadman-switch main.go

# ARM servers (Raspberry Pi, Oracle ARM instances, etc.):
GOOS=linux GOARCH=arm64 go build -o deadman-switch main.go
```

5. Copy the binary to `/usr/local/bin/` on all servers and make it executable:

```bash
scp deadman-switch user@node1.yourdomain.com:/usr/local/bin/deadman-switch
ssh user@node1.yourdomain.com "chmod +x /usr/local/bin/deadman-switch"
```

> **⚠️ Updating the Binary:** If the service is running, stop it before overwriting the binary. Attempting to copy over a running executable will fail with `Text file busy`.
> ```bash
> systemctl stop deadman
> cp deadman-switch /usr/local/bin/deadman-switch
> systemctl start deadman
> ```

## Step 4: Configuration

Create `/etc/deadman-switch/config.json` on each server. The `node_id`, `address`, `cert_file`, and `key_file` fields must reflect each individual node -- everything else is identical across nodes.

```json
{
  "node_id": "node1.yourdomain.com",
  "address": "node1.yourdomain.com:8443",
  "peers": ["node2.yourdomain.com:8443", "node3.yourdomain.com:8443"],
  "data_dir": "/var/lib/deadman/raft",
  "cert_file": "/etc/deadman-switch/certs/fullchain.pem",
  "key_file": "/etc/deadman-switch/certs/privkey.pem",
  "telegram_token": "YOUR_TELEGRAM_BOT_TOKEN",
  "payload_path": "/var/lib/deadman/payload.sh",
  "authorized_user_ids": [],
  "duration": "21d",
  "answer_offset": 3,
  "communication_time": 120,
  "announce_topology": true,
  "telegram_chat_id": 0,
  "telegram_thread_id": 0,
  "disable_passphrase": "a strong secret phrase",
  "smtp_host": "",
  "smtp_port": 465,
  "smtp_tls": true,
  "smtp_user": "",
  "smtp_password": "",
  "smtp_sender": "",
  "smtp_recipient": ""
}
```

### Securing the Config

This file contains your bot token and any SMTP credentials in plain text. Lock it down immediately:

```bash
chown deadman:deadman /etc/deadman-switch/config.json
chmod 600 /etc/deadman-switch/config.json
```

### Config Reference

**Core / Cluster**

| Field | Default | Description |
| :---- | :---- | :---- |
| `node_id` | *(required)* | The unique name of this node. Should match the hostname portion of `address` (e.g., `node1.yourdomain.com`). |
| `address` | *(required)* | The publicly reachable address and port of this node (e.g., `node1.yourdomain.com:8443`). |
| `peers` | *(required)* | A JSON array of the *other* nodes' addresses (e.g., `["node2.yourdomain.com:8443", "node3.yourdomain.com:8443"]`). Do not include this node itself. |
| `data_dir` | *(required)* | Directory where the Raft consensus database is stored (e.g., `/var/lib/deadman/raft`). Must be writable by the service user. |
| `cert_file` | *(required)* | Path to the TLS certificate (`fullchain.pem`) for this node. Recommended: copy certs to `/etc/deadman-switch/certs/` (owned `deadman:deadman`, mode `400`) rather than pointing directly at the Let's Encrypt live directory. |
| `key_file` | *(required)* | Path to the TLS private key (`privkey.pem`) for this node. Must be readable by the service user. See `cert_file` note above. |
| `communication_time` | `120` | Heartbeat timeout in **seconds**. The cluster will start a leader election if a node goes silent for this long, and the actual heartbeat interval is `communication_time / 10`. Increase this value to reduce inter-node network traffic; decrease it for faster failover detection. At the default of 120s, heartbeats are sent every 12 seconds and failover happens within roughly 2 minutes. |

**Timer**

| Field | Default | Description |
| :---- | :---- | :---- |
| `duration` | `504h` | How long the timer runs before executing the payload. Supports `d` for days (e.g., `21d`), or standard Go duration strings (e.g., `504h`, `72h`). |
| `answer_offset` | *(required)* | The secret math offset used to generate your disarm code. When the bot asks `Query: 150` and your offset is `3`, you must reply `/disarm 153`. Can be negative (e.g., `-5` means you'd reply `/disarm 145`). Because the bot immediately deletes your response on a successful disarm, observers cannot see the answer -- but keeping this offset secret adds a second layer of protection. |
| `disable_passphrase` | *(empty)* | A secret passphrase that enables the `/disable` command. When set, sending `/disable <passphrase>` will disarm the switch (if active) and prevent it from automatically re-arming until an `/arm` command is sent. If omitted, the `/disable` command is unavailable. Like the config file itself, treat this value as a secret. |

**Telegram**

| Field | Default | Description |
| :---- | :---- | :---- |
| `telegram_token` | *(required)* | Your Telegram Bot API token from BotFather. |
| `telegram_chat_id` | `0` | See [Telegram Routing](#telegram-routing) below. Omit or set to `0` for DM mode. |
| `telegram_thread_id` | `0` | See [Telegram Routing](#telegram-routing) below. Omit or set to `0` if not using a supergroup topic. |
| `announce_topology` | `false` | Set to `true` to receive a Telegram notification when leadership changes or a peer node connects/disconnects. |
| `authorized_user_ids` | *(empty)* | An optional JSON array of additional Telegram User IDs permitted to operate the switch (e.g., `[123456789, 987654321]`). The primary user registered via `/init_arm` is always authorized. Use this to grant trusted contacts the ability to disarm, extend, or disable the switch without requiring them to know the one-time arm code. Find a Telegram user ID by forwarding their message to [@userinfobot](https://t.me/userinfobot). |

**SMTP (Optional)**

All SMTP fields are optional. When `smtp_host` is empty, email is disabled entirely. When configured, warnings (at 50%, 25%, and 10% time remaining) and the final deadline notification are sent via email *in addition to* Telegram, so you still get notified if either channel is unavailable.

| Field | Default | Description |
| :---- | :---- | :---- |
| `smtp_host` | *(empty)* | SMTP server hostname (e.g., `smtp.gmail.com`). Leave empty to disable email entirely. |
| `smtp_port` | `465` | SMTP server port. Typically `465` for implicit TLS or `587` for STARTTLS. |
| `smtp_tls` | `false` | Set to `true` for implicit TLS (port 465 style). Set to `false` for STARTTLS (port 587 style). |
| `smtp_user` | *(empty)* | SMTP login username (usually your full email address). |
| `smtp_password` | *(empty)* | SMTP password or app-specific password. |
| `smtp_sender` | *(empty)* | The `From:` address on outgoing warning emails. If omitted, falls back to `smtp_user`. |
| `smtp_recipient` | *(empty)* | The address that receives warning emails. |

### Telegram Routing

You can route the bot's messages to three different destinations by combining `telegram_chat_id` and `telegram_thread_id`:

- **Direct Message (DM):** Omit both fields (or set both to `0`). Start a DM with your bot. The system will automatically capture your chat ID the first time you run `/init_arm`.
- **Supergroup main channel:** Set `telegram_chat_id` to your supergroup's ID (a negative number with a `-100` prefix, e.g., `-1001234567890`). Leave `telegram_thread_id` as `0`.
- **Supergroup topic thread:** Set both `telegram_chat_id` to your supergroup ID *and* `telegram_thread_id` to the integer topic ID (e.g., `4`). You can find this ID in the URL of a message posted in that topic: `t.me/c/{chat_id}/{thread_id}/{message_id}`.

## Step 5: The Payload

Create your payload script at the path specified in `payload_path` (e.g., `/var/lib/deadman/payload.sh`) and make it executable.

```bash
chmod +x /var/lib/deadman/payload.sh
```

If you use supplementary config files, secure those too:

```bash
chmod 600 /var/lib/deadman/payload.conf
chown deadman:deadman /var/lib/deadman/payload.conf
```

### The `--test` Flag (Required)

The cluster runs your script with `--test` on the first of every month, sequentially on each node, to verify the payload still works. **Your script must recognize this flag** and perform a non-destructive dry run, printing results to stdout. Output is forwarded to Telegram.

The test framework looks for structured `STEP:` and `SUMMARY:` lines in stdout to generate the Telegram report. At minimum, emit `SUMMARY:PASS` or `SUMMARY:FAIL` at the end of your test run.

```bash
#!/bin/bash

if [[ "${1}" == "--test" ]]; then
    echo "Running monthly systems check..."

    # Check your dependencies, ping your servers, verify your API keys, etc.
    # Each check should emit a STEP line:
    # STEP:<phase>:<name>:PASS
    # STEP:<phase>:<name>:FAIL:<optional detail>

    echo "STEP:Checks:ExampleCheck:PASS"

    # Emit the final summary
    echo "SUMMARY:PASS"
    exit 0
fi

# ==========================================
# ACTUAL PAYLOAD -- only runs when the timer expires
# ==========================================
echo "Deadman switch triggered."
# Your real logic here
```

## Step 6: Running as a Service

Create `/etc/systemd/system/deadman.service`:

```ini
[Unit]
Description=Deadman Switch (Raft Cluster)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=deadman
Group=deadman
# Ensure the deadman user can read the certs (see Step 2 in README: copy to /etc/deadman-switch/certs/, chown deadman:deadman, chmod 400)
ExecStart=/usr/local/bin/deadman-switch -config /etc/deadman-switch/config.json
Restart=always
RestartSec=10s
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

Enable and start the service on all nodes:

```bash
systemctl daemon-reload
systemctl enable deadman
systemctl start deadman
```

## Step 7: First Boot & Arming

Once all nodes are running, they will communicate and elect a Leader. This takes up to `communication_time` seconds, so be patient -- at the default of 120 seconds, the cluster should be ready within about two minutes.

**1. Find your one-time arm code.**

On the Leader node, check the service logs:

```bash
journalctl -u deadman -f
```

Look for a line like:

```
🔑 NEW ONE-TIME ARM CODE GENERATED: 123456  (only required on first-ever arm)
```

If you're not sure which node is the leader, just send `/init_arm` (with no code) to the bot -- it will tell you which node is the current Leader and where to look.

**2. Send the arm command from your Telegram chat:**

```
/init_arm 123456
```

The cluster is now locked to your Telegram User ID and the timer is running. You will never need the one-time code again unless you deliberately wipe the Raft database.

## Telegram Commands

| Command | Description |
| :---- | :---- |
| `/init_arm <code>` | First-time setup only. Registers your Telegram user as the authorized user and arms the timer. Requires the one-time code from the Leader's logs. On first arm, a test email is sent immediately to verify your SMTP settings. After first use, call `/arm` (no code) to re-arm manually. |
| `/disarm <code>` | Disarms the running timer. The code is the current Query number plus your `answer_offset`. For example, if the bot shows `Query: 150` and your offset is `3`, send `/disarm 153`. The bot immediately deletes your message upon success. |
| `/arm` | Manually re-arms the timer immediately, skipping the remaining snooze period. Also clears a `/disable` — use this to bring the switch back online after disabling it. |
| `/extend <duration> <code>` | Pushes the current deadline forward without a full disarm/re-arm cycle. Requires the current disarm code as authorization. Duration uses the same format as `duration` in the config (e.g., `/extend 2d 153`, `/extend 12h 153`). The bot deletes your message on success and resets warning thresholds against the new total duration. |
| `/disable <passphrase>` | Disarms the timer if it is currently running, then puts the switch into a **disabled** state. It will not automatically re-arm until you send `/arm`. Requires the `disable_passphrase` value from your config. The bot immediately deletes your message on success. Incorrect attempts are subject to the same exponential backoff lockout as `/disarm`. |
| `/status` | Shows the current timer state (armed, snoozed, disabled, or triggered), time remaining or time until re-arm (with percentage of total duration remaining when armed), and the live connectivity status of all cluster nodes. |
| `/history` | Shows the last 10 significant events in reverse-chronological order — arms, disarms, extensions, disables, warnings fired, and payload triggers — along with timestamps and the originating node. |
| `/test` | Manually triggers the cluster-wide payload test across all nodes sequentially. Results are posted to your chat when complete. |

## Troubleshooting & Resetting

**The cluster won't elect a Leader.**
Check that port 8443 (or whichever port is in your `address` config) is open on all nodes and that they can reach each other. Run `journalctl -u deadman -f` and look for connection errors.

**Nodes show peers as DOWN with a TLS certificate error.**
A node's certificate has expired or has not been loaded after renewal. On the affected node, check the cert expiry:
```bash
openssl x509 -enddate -noout -in /etc/deadman-switch/certs/fullchain.pem
```
If it is expired, renew with certbot, copy the new files into `/etc/deadman-switch/certs/`, re-apply `chown deadman:deadman` and `chmod 400`, then restart the service. The daemon will log `🚨 CERT EXPIRED` and send a Telegram alert on startup whenever it detects an expired or near-expiry certificate.

**The bot isn't responding to commands in a group.**
Privacy mode is likely enabled. Go to BotFather → `/setprivacy` → select your bot → Disable. Restart the service afterwards.

**I need to wipe the cluster and start fresh** (e.g., to change the authorized user, or after replacing too many nodes at once and breaking quorum).

Run the following on **all nodes before proceeding to the next step**, to prevent nodes from restoring state from each other:

```bash
# Step 1 -- Stop the service on ALL nodes first
systemctl stop deadman

# Step 2 -- Wipe the Raft database on ALL nodes
rm -rf /var/lib/deadman/raft/*

# Step 3 -- Restart the service
systemctl start deadman
```

The nodes will boot blank, form a new cluster, and generate a fresh one-time arm code in the Leader's logs.
