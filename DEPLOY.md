# VPS Deployment Guide

## 1. Upload bot files to VPS
```bash
scp -r otpbot-final/ root@YOUR_VPS_IP:/root/otpbot/
```

## 2. Connect to VPS
```bash
ssh root@YOUR_VPS_IP
```

## 3. Install Python & dependencies
```bash
apt update && apt install -y python3 python3-pip
cd /root/otpbot
pip3 install -r requirements.txt
```

## 3b. MongoDB + environment variables
Free MongoDB Atlas cluster is enough (Database Access -> user, Network Access -> allow your VPS IP).
Set these before starting (or add them under `[Service]` in otpbot.service as `Environment=KEY=value`):
```bash
export API_ID=...
export API_HASH=...
export BOT_TOKEN=...
export MONGO_URI="mongodb+srv://USER:PASS@cluster0.xxxxx.mongodb.net/?retryWrites=true&w=majority"
export MONGO_DB_NAME=otpbot
# Log channels (bot dono me admin ho):
export PAYMENT_LOG_FULL_ID=-100xxxxxxxxxx     # full details (admin only)
export PAYMENT_LOG_MASKED_ID=-100yyyyyyyyyy   # masked details (public/proof)
```
Old PostgreSQL data? Run `python3 migrate_postgres_to_mongo.py` once (see the file header).

## 4. Test the bot runs
```bash
python3 bot.py
# Press Ctrl+C once confirmed working
```

## 5. Setup systemd (runs 24/7, auto-restarts)
```bash
cp otpbot.service /etc/systemd/system/otpbot.service
systemctl daemon-reload
systemctl enable otpbot
systemctl start otpbot
```

## 6. Useful commands
```bash
systemctl status otpbot      # Check if running
systemctl restart otpbot     # Restart bot
systemctl stop otpbot        # Stop bot
journalctl -u otpbot -f      # Live logs
journalctl -u otpbot -n 50   # Last 50 log lines
```

## 7. Update bot code later
```bash
systemctl stop otpbot
# upload new files
systemctl start otpbot
```

## Sessions Backup (run daily via cron)
```bash
crontab -e
# Add this line:
0 2 * * * zip -r /root/sessions_backup_$(date +\%F).zip /root/otpbot/sessions/
```
