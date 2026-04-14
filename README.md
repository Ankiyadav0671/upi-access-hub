# 💳 UPI Access Hub

**Buy & sell premium courses, mock tests & digital products via Telegram. Pay with UPI, get instant access.**

---

## 🔗 Open the Bot

**Telegram:** https://t.me/UPIACCESSBOT

Scan QR → or click the link above → press **Start**. Done. No signup needed.

---

## 👤 New User? Start Here

**Step 1** → Open https://t.me/UPIACCESSBOT  
**Step 2** → Press **Start**  
**Step 3** → Browse products, buy with UPI, get instant access  

Want to sell your own courses?  
**Step 3** → Tap **🚀 Become a Creator** → Fill form → Admin approves → Start selling

---

## ✅ What You Can Do

**As a Student**
- Browse & buy courses from verified creators
- Pay via UPI — access delivered instantly
- Use coupon codes for discounts
- Refer friends → earn 30% of their purchases
- Save products to wishlist
- Rate & review products

**As a Creator**
- Sell courses, PDFs, mock tests, anything digital
- Payments go directly to your UPI ID
- 3-day free trial — zero upfront cost
- Boost products, run flash sales
- Broadcast messages to all your students
- Schedule live class reminders
- Withdraw your earnings anytime
- Analytics dashboard with revenue chart

---

## 🛠️ Deploy to Railway

### 1. Push to GitHub
```bash
git init
git add .
git commit -m "UPI Access Hub v6"
git remote add origin https://github.com/YOUR_USERNAME/upi-access-hub
git push -u origin main
```

### 2. Create Railway Project
1. Go to https://railway.app
2. **New Project** → **Deploy from GitHub** → select your repo

### 3. Add Volume — REQUIRED ⚠️
> Railway Settings → Volumes → Add Volume → Mount path: `/app/data`
>
> Without this every restart wipes all your data.

### 4. Add Environment Variables
```
BOT_TOKEN        = paste your token from @BotFather
SUPER_ADMIN_ID   = your Telegram user ID
BOT_USERNAME     = UPIACCESSBOT
PLATFORM_UPI     = Ankiii@upi
DATA_FILE        = /app/data/hub_data.json
CFG_FILE         = /app/data/hub_config.json
```

### 5. Deploy
Railway auto-deploys. Check logs for:
```
🚀 UPI Access Hub v6.0 — Railway Production — LIVE!
```

### Updating Later
```bash
git add . && git commit -m "update" && git push
```
Redeploys in ~30 seconds.

---

## 📋 Commands

| Command | Who | What it does |
|---------|-----|--------------|
| `/start` | Everyone | Main menu |
| `/help` | Everyone | Full help guide |
| `/myproducts` | Students | View purchases |
| `/wallet` | Students | Wallet & referrals |
| `/refer` | Students | Get referral link |
| `/search` | Everyone | Search products |
| `/topcreators` | Everyone | Creator leaderboard |
| `/profile CODE` | Everyone | View a creator store |
| `/dashboard` | Creators | Creator panel |
| `/addproduct` | Creators | Add new product |
| `/editproduct` | Creators | Edit existing product |
| `/renewpanel` | Creators | Renew subscription |
| `/createcoupon` | Creators | Create discount code |
| `/broadcast` | Creators | Message all students |
| `/boostproduct` | Creators | Boost product visibility |
| `/scheduleclass` | Creators | Set live class reminder |
| `/adminpanel` | Admin | Admin dashboard |
| `/approve_creator ID` | Admin | Approve creator |
| `/verifycreator ID` | Admin | Give verified badge |
| `/exportdata` | Admin | Download data backup |
| `/cancel` | Everyone | Cancel current action |

---

## 💾 Backup
Data is auto-backed up to admin Telegram every day at 2:00 AM.
Manual backup available in Admin Panel → 💾 Backup.

---

**Bot:** https://t.me/UPIACCESSBOT
