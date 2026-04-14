# UPI Access Hub — Railway Deployment Guide

## 📁 Folder Structure
```
upi_access_hub_railway/
├── bot.py              ← Main bot file
├── requirements.txt    ← Dependencies
├── railway.toml        ← Railway config
├── .env.example        ← Environment variables template
└── README.md
```

---

## 🚂 Deploy to Railway — Step by Step

### Step 1: Push to GitHub
```bash
git init
git add .
git commit -m "UPI Access Hub v6"
git remote add origin https://github.com/YOUR_USERNAME/upi-access-hub
git push -u origin main
```

### Step 2: Create Railway Project
1. Go to [railway.app](https://railway.app)
2. Click **New Project** → **Deploy from GitHub repo**
3. Select your repo

### Step 3: Add Volume (CRITICAL — saves data permanently)
1. In Railway dashboard → your service → **Settings**
2. Scroll to **Volumes** → **Add Volume**
3. Mount path: `/app/data`
4. Click **Create Volume**

### Step 4: Set Environment Variables
In Railway dashboard → your service → **Variables** → add each:

```
BOT_TOKEN          = your_bot_token
SUPER_ADMIN_ID     = 5695957392
BOT_USERNAME       = UPIAccessbot
PLATFORM_UPI       = Ankiii@upi
DATA_FILE          = /app/data/hub_data.json
CFG_FILE           = /app/data/hub_config.json
```

### Step 5: Deploy
Railway auto-deploys on every GitHub push. Click **Deploy** in dashboard.

---

## ✅ Verify It's Working
Check Railway logs — you should see:
```
🚀 UPI Access Hub v6.0 — Railway Production — LIVE!
💳 Platform UPI: Ankiii@upi
📁 Data: /app/data/hub_data.json
```

---

## 🔄 Update Bot
Just push to GitHub:
```bash
git add .
git commit -m "update"
git push
```
Railway auto-redeploys in ~30 seconds.

---

## 💾 Data Backup
- Bot auto-backs up data to your Telegram every day at 2 AM IST
- Manual backup: `/adminpanel` → 💾 Backup

---

## 🆕 Features in v6.0
- ⚡ Flash Sales (time-limited discounts with countdown)
- 🖼️ Product Thumbnails (photos in product cards)
- ❤️ Student Wishlist
- 💸 Creator Withdrawal System
- 📈 Analytics Dashboard (7-day revenue chart)
- 💾 Daily Auto-Backup to admin
- ❓ /help command
- 🔒 Rate Limiting (prevents spam)
- 🔄 Full data migration from old bot formats
