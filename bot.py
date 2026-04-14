"""
╔══════════════════════════════════════════════════════════════════╗
║  UPI ACCESS HUB  ·  v6.0  ·  RAILWAY PRODUCTION EDITION        ║
║  Features: All creator+student flows · Flash Sales · Analytics  ║
║  Withdrawals · Wishlist · Help · Backup · Rate Limit · Thumbnails║
╚══════════════════════════════════════════════════════════════════╝
"""

import os, json, logging, re, string, random, asyncio, time
from datetime import datetime, timedelta
from collections import defaultdict
from dotenv import load_dotenv
load_dotenv()

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    BotCommand, BotCommandScopeChat, BotCommandScopeDefault,
    InputMediaPhoto,
)
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    MessageHandler, ConversationHandler, ContextTypes, filters,
)
from telegram.constants import ParseMode

# ═══════════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════════
BOT_TOKEN  = os.getenv("BOT_TOKEN", "")
ADMIN_ID   = int(os.getenv("SUPER_ADMIN_ID", "5695957392"))
BOT_USER   = os.getenv("BOT_USERNAME", "UPIAccessbot")
PAY_UPI    = os.getenv("PLATFORM_UPI", "Ankiii@upi")
DATA_F     = os.getenv("DATA_FILE", "data/hub_data.json")
CFG_F      = os.getenv("CFG_FILE",  "data/hub_config.json")
MD         = ParseMode.MARKDOWN

os.makedirs(os.path.dirname(DATA_F), exist_ok=True)

DEFAULT_CFG = {
    "boost_cost": 29, "boost_days": 3,
    "ref_pct": 30,    "trial_days": 3,
    "flash_enabled": False,
    "plans": {
        "basic":   {"price": 199, "limit": 10,     "name": "Basic 🥉"},
        "pro":     {"price": 399, "limit": 25,     "name": "Pro 🥈"},
        "premium": {"price": 699, "limit": 999999, "name": "Premium 🥇"},
    },
    "coupons": {},
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
#  RATE LIMITER  (prevent spam)
# ═══════════════════════════════════════════════════════════════
_rate: dict = defaultdict(list)

def rate_ok(uid: str, limit: int = 20, window: int = 10) -> bool:
    now = time.time()
    _rate[uid] = [t for t in _rate[uid] if now - t < window]
    if len(_rate[uid]) >= limit:
        return False
    _rate[uid].append(now)
    return True

# ═══════════════════════════════════════════════════════════════
#  IN-MEMORY STORE
# ═══════════════════════════════════════════════════════════════
_D: dict = {}
_C: dict = {}

def _load():
    global _D, _C
    _D = json.load(open(DATA_F)) if os.path.exists(DATA_F) else _blank()
    _migrate()
    _C = json.load(open(CFG_F)) if os.path.exists(CFG_F) else {}
    for k, v in DEFAULT_CFG.items():
        _C.setdefault(k, v)
    log.info("Data loaded: %d users, %d creators, %d products",
             len(_D["users"]), len(_D["creators"]), len(_D["products"]))

def _save():
    with open(DATA_F, "w") as f:
        json.dump(_D, f, separators=(",", ":"))

def _savec():
    with open(CFG_F, "w") as f:
        json.dump(_C, f, indent=2)

def _blank() -> dict:
    return {
        "users":    {},  # uid → {name,username,role,wallet,joined,ref}
        "creators": {},  # uid → creator_doc
        "products": {},  # pid → product_doc
        "purchases":{},  # oid → purchase_doc
        "refs":     {},  # new_uid → ref_uid
        "crefs":    {},  # new_cid → ref_cid
        "sales":    [],
        "utr_log":  [],
        "boosts":   {},  # pid → expiry
        "ratings":  {},  # "uid|pid" → star
        "broadcasts":[],
        "pan_pend": {},  # uid → {plan,utr,ts}
        "classes":  [],
        "cpn_used": {},  # "uid|code" → True
        "wcoupons": {},
        "wishlist":  {},  # uid → [pid, ...]
        "withdrawals": [], # {uid, cid, amount, upi, ts, status}
        "flash_sales": {},  # pid → {disc_pct, ends_at}
        "backups": [],   # ts of last backup
    }

def _migrate():
    for k, v in _blank().items():
        _D.setdefault(k, v)
    sa = str(ADMIN_ID)
    if sa in _D["users"]:
        _D["users"][sa]["role"] = "super_admin"
    # Migrate old key formats from previous bot versions
    for uid, c in _D["creators"].items():
        _rk(c, "panel_status",    "ps")
        _rk(c, "panel_expiry",    "panel_exp")
        _rk(c, "trial_expiry",    "trial_exp")
        _rk(c, "creator_name",    "name")
        _rk(c, "creator_code",    "code")
        _rk(c, "creator_category","cat")
        _rk(c, "creator_bio",     "bio")
        _rk(c, "creator_upi_id",  "upi")
        _rk(c, "creator_upi_qr",  "qr")
        _rk(c, "approval_mode",   "mode")
        _rk(c, "total_sales",     "sales")
        _rk(c, "verified_badge",  "ver")
        _rk(c, "panel_plan",      "plan")
        _rk(c, "wallet_balance",  "wallet")
        if uid not in _D["users"]:
            _D["users"][uid] = {"name":c.get("name",""),"username":"",
                "role":"creator","wallet":0.0,"joined":now(),"ref":None}
        else:
            _D["users"][uid]["role"] = "creator"
    for pid, p in _D["products"].items():
        _rk(p, "creator_id",       "cid")
        _rk(p, "product_name",     "name")
        _rk(p, "delivery_type",    "dtype")
        _rk(p, "delivery_link",    "link")
        _rk(p, "access_duration",  "dur")
        _rk(p, "subscription_flag","sub")
        _rk(p, "students_count",   "students")
        if "status" in p and "active" not in p:
            p["active"] = p.pop("status") == "active"
    for oid, pur in _D["purchases"].items():
        if "user_id"    in pur: pur["uid"] = str(pur.pop("user_id"))
        if "product_id" in pur: pur["pid"] = str(pur.pop("product_id"))
        if "expiry_date" in pur: pur["exp"] = pur.pop("expiry_date")
        if "status" in pur and "ok" not in pur:
            pur["ok"] = pur.pop("status") == "approved"

def _rk(d, old, new):
    if old in d and new not in d:
        d[new] = d.pop(old)

# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════
_ESC = re.compile(r"([_*`\[\\])")
def esc(t) -> str: return _ESC.sub(r"\\\1", str(t)) if t else ""

def now() -> str:
    return datetime.now().strftime("%d %b %Y %I:%M %p")

def exp_str(days: int) -> str:
    return (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d %H:%M")

def days_left(s: str) -> int:
    if not s: return 0
    try: return max(0, (datetime.strptime(s, "%Y-%m-%d %H:%M") - datetime.now()).days)
    except: return 0

def is_expired(s: str) -> bool:
    if not s: return False
    try: return datetime.now() > datetime.strptime(s, "%Y-%m-%d %H:%M")
    except: return False

def rand_pid() -> str: return "P" + str(random.randint(10**7, 10**8-1))
def rand_oid() -> str: return "O" + str(random.randint(10**5, 10**6-1))
def rand_cpn() -> str: return "".join(random.choices(string.ascii_uppercase+string.digits, k=8))
def rand_code(n="") -> str:
    p = re.sub(r"[^A-Z]", "", n.upper())[:5] or "CRTR"
    return p + str(random.randint(100, 999))

def valid_utr(u: str) -> bool:
    u = u.replace(" ", "")
    return u.isalnum() and 10 <= len(u) <= 25

def utr_used(utr: str) -> bool:
    return utr.upper() in [x.upper() for x in _D.get("utr_log", [])]

# ═══════════════════════════════════════════════════════════════
#  ROLE HELPERS
# ═══════════════════════════════════════════════════════════════
def get_role(uid: str) -> str:
    if int(uid) == ADMIN_ID: return "super_admin"
    return _D["users"].get(uid, {}).get("role", "student")

def is_admin(uid: str) -> bool: return int(uid) == ADMIN_ID
def is_creator(uid: str) -> bool: return get_role(uid) in ("creator", "super_admin")

def panel_active(uid: str) -> bool:
    if int(uid) == ADMIN_ID: return True
    c = _D["creators"].get(uid, {})
    ps = c.get("ps", "")
    if ps == "trial":  return not is_expired(c.get("trial_exp", ""))
    if ps == "active": return not is_expired(c.get("panel_exp", ""))
    return False

def prod_limit(uid: str) -> int:
    plan = _D["creators"].get(uid, {}).get("plan", "basic")
    return _C["plans"].get(plan, {}).get("limit", 10)

def prod_count(uid: str) -> int:
    return sum(1 for p in _D["products"].values()
               if p.get("cid") == uid and p.get("active"))

def has_bought(uid: str, pid: str) -> bool:
    return any(p.get("uid") == uid and p.get("pid") == pid and p.get("ok")
               for p in _D["purchases"].values())

def track_user(user):
    uid = str(user.id)
    if uid not in _D["users"]:
        _D["users"][uid] = {
            "name": user.full_name or "", "username": user.username or "",
            "role": "super_admin" if user.id == ADMIN_ID else "student",
            "wallet": 0.0, "joined": now(), "ref": None,
        }
    if user.id == ADMIN_ID:
        _D["users"][uid]["role"] = "super_admin"

def get_effective_price(pid: str) -> float:
    p = _D["products"].get(pid, {})
    base = p.get("price", 0)
    fs = _D["flash_sales"].get(pid)
    if fs and not is_expired(fs.get("ends_at", "")):
        disc = round(base * fs["disc_pct"] / 100, 2)
        return max(0.0, base - disc)
    return base

def grant_purchase(oid: str):
    pur = _D["purchases"].get(oid)
    if not pur: return
    pid, uid = pur["pid"], pur["uid"]
    prod = _D["products"].get(pid, {})
    dur = prod.get("dur", "lifetime")
    pur["ok"]  = True
    pur["exp"] = None if dur == "lifetime" else (
        exp_str(int(dur)) if str(dur).isdigit() else None)
    _D["products"].setdefault(pid, {})
    _D["products"][pid]["students"] = _D["products"][pid].get("students", 0) + 1
    _D["sales"].append({"uid": uid, "pid": pid, "amt": prod.get("price", 0), "ts": now()})
    cid = prod.get("cid")
    if cid and cid in _D["creators"]:
        _D["creators"][cid]["sales"] = _D["creators"][cid].get("sales", 0) + prod.get("price", 0)

def credit_ref(uid: str, amount: float):
    ref = _D["refs"].get(uid)
    if not ref or ref == uid: return
    pct = _C.get("ref_pct", 30)
    _D["users"].setdefault(ref, {})
    _D["users"][ref]["wallet"] = _D["users"][ref].get("wallet", 0) + round(amount * pct / 100, 2)

def apply_coupon(code: str, price: float, uid: str):
    code = code.upper().strip()
    c = _C["coupons"].get(code)
    if not c: return False, price, 0, "❌ Invalid coupon code."
    if c.get("exp") and is_expired(c["exp"]): return False, price, 0, "❌ Coupon expired."
    if c.get("used", 0) >= c.get("max", 1): return False, price, 0, "❌ Usage limit reached."
    if _D["cpn_used"].get(f"{uid}|{code}"): return False, price, 0, "❌ Already used this coupon."
    disc = (round(price * c.get("pct", 0) / 100, 2)
            if c.get("type") == "pct" else min(c.get("flat", 0), price))
    return True, max(0.0, price - disc), disc, f"✅ ₹{disc:.0f} off applied!"

def extend_panel(uid: str, days: int):
    c = _D["creators"].get(uid)
    if not c: return
    base = c.get("panel_exp")
    try:    dt = datetime.strptime(base, "%Y-%m-%d %H:%M") if base else datetime.now()
    except: dt = datetime.now()
    _D["creators"][uid]["panel_exp"] = (dt + timedelta(days=days)).strftime("%Y-%m-%d %H:%M")

# ═══════════════════════════════════════════════════════════════
#  FSM STATES
# ═══════════════════════════════════════════════════════════════
(
    S_RN, S_RC, S_RB, S_RU, S_RI, S_RQ,   # register
    S_PN, S_PP, S_PD, S_PL, S_PV, S_PS,   # add product
    S_PI,                                   # product image (optional)
    S_EP, S_EF, S_EV,                      # edit product
    S_CPN, S_UTR,                          # purchase
    S_RUTR,                                # renew utr
    S_VN, S_VD, S_VX, S_VM,               # coupon creation
    S_BT, S_BM,                            # broadcast
    S_SQ,                                  # search
    S_LT, S_LD,                            # live class
    S_AB,                                  # admin broadcast
    S_BU,                                  # boost utr
    S_WD_AMT,                              # withdrawal amount
    S_FS_DISC, S_FS_DUR,                   # flash sale
) = range(33)

# ═══════════════════════════════════════════════════════════════
#  KEYBOARDS
# ═══════════════════════════════════════════════════════════════
def ib(text, cbd=None, url=None):
    if url: return InlineKeyboardButton(text, url=url)
    return InlineKeyboardButton(text, callback_data=cbd)

def kb(*rows): return InlineKeyboardMarkup(list(rows))
def back(to="home"): return kb([ib("🔙 Back", to)])

KB_ADMIN = kb(
    [ib("👑 Admin Panel",   "adm:home"),   ib("📊 Dashboard",    "cr:dash")],
    [ib("➕ Add Product",   "cr:addp"),    ib("📦 Products",     "cr:prods")],
    [ib("✏️ Edit Product", "cr:editp"),   ib("🎟 Coupons",      "cr:mkcp")],
    [ib("📣 Broadcast",    "cr:bc"),       ib("💰 Wallet",       "cr:wlt")],
    [ib("⚡ Boost",        "cr:boost"),   ib("⚡ Flash Sale",    "cr:flash")],
    [ib("🔔 Live Class",   "cr:lcls"),    ib("🔄 Renew Panel",  "cr:renew")],
    [ib("⚙️ Settings",    "cr:set"),     ib("🏪 Marketplace",  "mkt:home")],
)
KB_CREATOR = kb(
    [ib("📊 Dashboard",   "cr:dash"),     ib("➕ Add Product",  "cr:addp")],
    [ib("📦 Products",    "cr:prods"),    ib("✏️ Edit",        "cr:editp")],
    [ib("🎟 Coupons",     "cr:mkcp"),     ib("📣 Broadcast",   "cr:bc")],
    [ib("💰 Wallet",      "cr:wlt"),      ib("⚡ Boost",       "cr:boost")],
    [ib("⚡ Flash Sale",  "cr:flash"),    ib("🔔 Live Class",  "cr:lcls")],
    [ib("🔄 Renew Panel", "cr:renew"),    ib("⚙️ Settings",   "cr:set")],
    [ib("🏪 Marketplace", "mkt:home")],
)
KB_STUDENT = kb(
    [ib("🔥 Trending",     "mkt:trend"),  ib("🆕 New Products", "mkt:new")],
    [ib("🏪 Categories",   "mkt:cats"),   ib("🎁 Free",         "mkt:free")],
    [ib("🔍 Search",       "mkt:srch"),   ib("⭐ Top Creators", "mkt:topc")],
    [ib("📦 My Products",  "st:prods"),   ib("❤️ Wishlist",    "st:wish")],
    [ib("👛 Wallet",       "st:wlt"),     ib("🔗 Refer & Earn","st:ref")],
    [ib("🏆 Leaderboard",  "mkt:top"),    ib("❓ Help",         "st:help")],
    [ib("🚀 Become a Creator", "beco")],
)
KB_MKT = kb(
    [ib("🔥 Trending",    "mkt:trend"),   ib("🆕 New",          "mkt:new")],
    [ib("🏪 Categories",  "mkt:cats"),    ib("🎁 Free",         "mkt:free")],
    [ib("🔍 Search",      "mkt:srch"),    ib("⭐ Top Creators", "mkt:topc")],
    [ib("🔙 Back to Panel","home")],
)

def home_kb(uid: str):
    r = get_role(uid)
    if r == "super_admin": return KB_ADMIN
    if r == "creator":     return KB_CREATOR
    return KB_STUDENT

CATS = ["📚 Education","💻 Tech & Coding","💰 Finance","🎨 Design",
        "📈 Business","🎵 Music","💪 Fitness","🌐 Marketing","🎯 Other"]
DTYPE_MAP = {
    "tg_ch":"📢 TG Channel","tg_gr":"👥 TG Group","discord":"💬 Discord",
    "drive":"📁 Drive","pdf":"📄 PDF","web":"🌐 Website/Pass",
}
DURS = [("7 Days","7"),("30 Days","30"),("90 Days","90"),("1 Year","365"),("Lifetime","lif")]

def cats_kb():
    rows = []
    for i in range(0, len(CATS), 2):
        row = [ib(CATS[i], f"cat|{CATS[i]}")]
        if i+1 < len(CATS): row.append(ib(CATS[i+1], f"cat|{CATS[i+1]}"))
        rows.append(row)
    rows.append([ib("🔙 Back","home")]); return kb(*rows)

def dtype_kb():
    items = list(DTYPE_MAP.items()); rows = []
    for i in range(0, len(items), 2):
        row = [ib(items[i][1], f"dt|{items[i][0]}")]
        if i+1 < len(items): row.append(ib(items[i+1][1], f"dt|{items[i+1][0]}"))
        rows.append(row)
    return kb(*rows)

def dur_kb(): return kb(*[[ib(l, f"dur|{v}")] for l, v in DURS])

def star_kb(pid: str):
    return kb(
        [ib("⭐ 1",f"rate|{pid}|1"),ib("⭐⭐ 2",f"rate|{pid}|2"),ib("⭐⭐⭐ 3",f"rate|{pid}|3")],
        [ib("⭐⭐⭐⭐ 4",f"rate|{pid}|4"),ib("⭐⭐⭐⭐⭐ 5",f"rate|{pid}|5")],
    )

def plans_kb():
    rows = [[ib(f"{v['name']} — ₹{v['price']}/mo ({v['limit']} prods)", f"plan|{k}")]
            for k, v in _C["plans"].items()]
    rows.append([ib("💰 Use Creator Wallet","cr:wlt_renew"),ib("🔙 Back","home")])
    return kb(*rows)

# ═══════════════════════════════════════════════════════════════
#  SEND HELPERS
# ═══════════════════════════════════════════════════════════════
async def send(update: Update, text: str, markup=None):
    kw = {"text": text, "parse_mode": MD, "reply_markup": markup}
    q = update.callback_query
    if q:
        t = asyncio.create_task(q.answer())
        try:    await q.edit_message_text(**kw)
        except: await q.message.reply_text(**kw)
        try: await t
        except: pass
    else:
        await update.message.reply_text(**kw)

async def alert(update: Update, text: str):
    if q := update.callback_query:
        try: await q.answer(text, show_alert=True)
        except: pass

async def send_invoice(bot, oid: str, pur: dict, prod: dict):
    c = _D["creators"].get(prod.get("cid",""), {})
    try:
        await bot.send_message(int(pur["uid"]),
            f"🧾 *Invoice — UPI Access Hub*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"Receipt: `#{oid}`\n"
            f"📦 {esc(prod.get('name',''))}\n"
            f"👤 Creator: {esc(c.get('name',''))}\n"
            f"💰 Amount: ₹{prod.get('price',0):.0f}\n"
            f"UTR: `{pur.get('utr','')}`\n"
            f"📅 {now()}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"_Thank you for your purchase!_ 🙏",
            parse_mode=MD)
    except Exception as e:
        log.warning("Invoice send failed: %s", e)

# ═══════════════════════════════════════════════════════════════
#  /start
# ═══════════════════════════════════════════════════════════════
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    uid  = str(user.id)
    if not rate_ok(uid): return
    track_user(user)
    arg  = ctx.args[0] if ctx.args else ""

    if arg.startswith("ref") and arg[3:].isdigit():
        ref = arg[3:]
        if ref != uid and uid not in _D["refs"]:
            _D["refs"][uid] = ref; _D["users"][uid]["ref"] = ref
    elif arg.startswith("cref") and arg[4:].isdigit():
        ref = arg[4:]
        if ref != uid and uid not in _D["crefs"]: _D["crefs"][uid] = ref
    elif arg:
        for cid, c in _D["creators"].items():
            if c.get("code","").upper() == arg.upper() and c.get("ps") in ("active","trial"):
                _save()
                await _show_store(update, cid); return
    _save()
    asyncio.create_task(_set_cmds(ctx.bot, user.id, get_role(uid)))
    await _show_home(update, uid)

async def _show_home(update: Update, uid: str):
    role = get_role(uid)
    if role == "super_admin":
        tc   = sum(1 for c in _D["creators"].values() if c.get("ps") in ("active","trial"))
        pend = sum(1 for c in _D["creators"].values() if c.get("ps") == "pending")
        rev  = sum(s.get("amt", 0) for s in _D["sales"])
        wd_pend = sum(1 for w in _D["withdrawals"] if w.get("status") == "pending")
        await send(update,
            f"👑 *UPI Access Hub — Super Admin*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👥 {len(_D['users'])} users  •  🎨 {tc} creators\n"
            f"⏳ {pend} approvals  •  💸 {wd_pend} withdrawals\n"
            f"💰 Total Revenue: ₹{rev:,.0f}", KB_ADMIN)
    elif role == "creator":
        c = _D["creators"].get(uid, {})
        if not c:
            _D["users"][uid]["role"] = "student"; _save()
            await _show_home(update, uid); return
        if c.get("ps") == "pending":
            await send(update,
                "⏳ *Application Under Review*\n\n"
                "Your account is being reviewed by admin.\n"
                "You'll receive your store link once approved! 🎉\n\n"
                "_Usually approved within a few hours._",
                kb([ib("🏪 Browse Marketplace","mkt:home"),ib("❓ Help","st:help")]))
            return
        if c.get("ps") in ("expired","trial_expired") or (
                c.get("ps") not in ("active","trial")):
            await send(update,
                f"🔴 *Panel Expired — {esc(c.get('name',''))}*\n\n"
                f"Renew to access dashboard & earn.\n"
                f"Wallet: ₹{c.get('wallet',0):,.0f}  •  Sales: ₹{c.get('sales',0):,.0f}",
                kb([ib("🔄 Renew Panel Now","cr:renew")],
                   [ib("🏪 Browse","mkt:home"),ib("👛 Wallet","cr:wlt")]))
            return
        exp = c.get("trial_exp") if c.get("ps") == "trial" else c.get("panel_exp")
        dl  = days_left(exp) if exp else 0
        tag = "🔔 Trial" if c.get("ps") == "trial" else ("✅ Active" if dl > 0 else "🔴 Expired")
        cnt, lim = prod_count(uid), prod_limit(uid)
        await send(update,
            f"🎨 *Welcome back, {esc(c.get('name',''))}!*{'  ✅' if c.get('ver') else ''}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"Panel: {tag}  |  *{dl}d* remaining\n"
            f"📦 {cnt}/{lim} products  •  💰 ₹{c.get('wallet',0):,.0f}\n"
            f"💰 Total Sales: ₹{c.get('sales',0):,.0f}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"_Tap any option below_ 👇", KB_CREATOR)
    else:
        await send(update,
            f"👋 *Welcome to UPI Access Hub!*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🏆 *India's #1 Creator Marketplace*\n\n"
            f"✅ Buy premium courses from top creators\n"
            f"✅ Pay via UPI — *instant access*\n"
            f"✅ Refer friends → earn *30% commission*\n"
            f"✅ Wallet → convert rewards to coupons\n"
            f"✅ Rate & review products\n"
            f"✅ Save products to wishlist\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🎨 *Want to sell your courses?*\n"
            f"Tap *Become a Creator* below!\n\n"
            f"_Browse 👇_", KB_STUDENT)

# ═══════════════════════════════════════════════════════════════
#  MAIN CALLBACK ROUTER
# ═══════════════════════════════════════════════════════════════
async def cb_router(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    uid = str(q.from_user.id)
    d   = q.data
    if not rate_ok(uid): await q.answer("⏳ Slow down!", show_alert=True); return

    # ── HOME ──────────────────────────────────────────
    if d in ("home","start"):
        await _show_home(update, uid)

    elif d == "beco":
        if uid in _D["creators"]:
            await send(update, f"ℹ️ Creator account exists. Status: *{_D['creators'][uid].get('ps','?')}*", back())
        else:
            await send(update,
                "🚀 *Become a Creator!*\n━━━━━━━━━━━━━━━━\n"
                "✅ Sell courses & digital products\n"
                "✅ Get paid via UPI — instant\n"
                "✅ 3-day FREE trial\n"
                "✅ Up to 10 products on Basic plan\n"
                "✅ Auto or manual payment approval\n"
                "✅ Boost products for more visibility\n"
                "✅ Flash sales to boost revenue\n"
                "✅ Live class reminders for students\n"
                "✅ Wallet + withdrawal system\n"
                "━━━━━━━━━━━━━━━━",
                kb([ib("🎉 Register Now","reg:start")],[ib("🔙 Back","home")]))

    # ── CREATOR PANEL ─────────────────────────────────
    elif d == "cr:dash":
        await _creator_dashboard(update, uid)

    elif d == "cr:prods":
        await _creator_products(update, uid)

    elif d.startswith("cr:mgp|"):
        await _manage_product(update, uid, d[7:])

    elif d.startswith("cr:delp|"):
        pid = d[8:]
        p = _D["products"].get(pid)
        if p and (p.get("cid") == uid or is_admin(uid)):
            _D["products"][pid]["active"] = False; _save()
            await send(update, f"🗑 *{esc(p['name'])}* deleted.", back("cr:prods"))
        else: await alert(update, "Not authorized!")

    elif d.startswith("cr:pstat|"):
        await _product_stats(update, d[9:])

    elif d == "cr:wlt":
        await _creator_wallet(update, uid)

    elif d == "cr:wlt_renew":
        c    = _D["creators"].get(uid, {})
        plan = c.get("plan","basic")
        cost = _C["plans"].get(plan,{}).get("price",199)
        if c.get("wallet",0) < cost:
            await alert(update, f"Need ₹{cost}. Wallet: ₹{c.get('wallet',0):.0f}"); return
        _D["creators"][uid]["wallet"] -= cost
        extend_panel(uid, 30)
        _D["creators"][uid]["ps"] = "active"
        _D["creators"][uid]["trial_status"] = False
        _save()
        await send(update, f"✅ *Panel renewed via wallet!*\n₹{cost} deducted. +30 days.", back("cr:dash"))

    elif d == "cr:wlt_boost":
        c    = _D["creators"].get(uid,{})
        cost = _C.get("boost_cost",29)
        if c.get("wallet",0) < cost:
            await alert(update, f"Need ₹{cost}. Wallet: ₹{c.get('wallet',0):.0f}"); return
        prods = [(pid,p) for pid,p in _D["products"].items() if p.get("cid")==uid and p.get("active")]
        if not prods: await alert(update,"No products!"); return
        rows = [[ib(p["name"][:35],f"bstw|{pid}")] for pid,p in prods]
        rows.append([ib("🔙 Back","cr:wlt")])
        await send(update,f"⚡ Select product to boost (₹{cost} from wallet):",kb(*rows))

    elif d.startswith("bstw|"):
        pid  = d[5:]
        cost = _C.get("boost_cost",29); bd = _C.get("boost_days",3)
        if _D["creators"].get(uid,{}).get("wallet",0) < cost:
            await alert(update,f"Need ₹{cost}!"); return
        _D["creators"][uid]["wallet"] -= cost
        _D["boosts"][pid] = exp_str(bd); _save()
        await send(update,f"⚡ *Boosted for {bd} days!*", back("cr:dash"))

    elif d == "cr:boost":
        if not is_creator(uid): await alert(update,"Not a creator!"); return
        prods = [(pid,p) for pid,p in _D["products"].items() if p.get("cid")==uid and p.get("active")]
        if not prods: await send(update,"No products to boost!",back("cr:dash")); return
        cost = _C.get("boost_cost",29); bd = _C.get("boost_days",3)
        rows = [[ib(p["name"][:35],f"bstp|{pid}")] for pid,p in prods]
        rows.append([ib("🔙 Dashboard","cr:dash")])
        await send(update,
            f"⚡ *Boost a Product*\nCost: ₹{cost} | Duration: {bd} days\n"
            f"💳 Pay to: `{PAY_UPI}`\n\nSelect product:", kb(*rows))

    elif d.startswith("bstp|"):
        pid = d[5:]
        p   = _D["products"].get(pid)
        if not p or (p.get("cid")!=uid and not is_admin(uid)):
            await alert(update,"Not yours!"); return
        cost = _C.get("boost_cost",29); bd = _C.get("boost_days",3)
        bal  = _D["creators"].get(uid,{}).get("wallet",0)
        ctx.user_data["boost_pid"] = pid
        await send(update,
            f"⚡ *Boost: {esc(p['name'])}*\nCost: ₹{cost} | {bd} days\nPay to: `{PAY_UPI}`",
            kb([ib(f"💰 Use Wallet (₹{bal:.0f})",f"bstw|{pid}")],
               [ib("💳 Pay UPI → Send UTR","bst:upi")],
               [ib("🔙 Cancel","cr:boost")]))

    elif d == "cr:flash":
        if not is_creator(uid): await alert(update,"Not a creator!"); return
        prods = [(pid,p) for pid,p in _D["products"].items() if p.get("cid")==uid and p.get("active")]
        if not prods: await send(update,"No products!",back("cr:dash")); return
        rows = [[ib(f"⚡ {p['name'][:30]}  ₹{p['price']:.0f}",f"flash|{pid}")] for pid,p in prods]
        rows.append([ib("🔙 Dashboard","cr:dash")])
        await send(update,
            "⚡ *Flash Sale*\n\nSet a time-limited discount on a product.\nStudents see it as SALE!\n\nSelect product:",
            kb(*rows))

    elif d.startswith("flash|"):
        pid = d[6:]
        p   = _D["products"].get(pid)
        if not p or (p.get("cid")!=uid and not is_admin(uid)):
            await alert(update,"Not yours!"); return
        fs = _D["flash_sales"].get(pid)
        if fs and not is_expired(fs.get("ends_at","")):
            # Show current flash sale with option to cancel
            await send(update,
                f"⚡ *{esc(p['name'])}* — Flash Sale Active\n"
                f"Discount: *{fs['disc_pct']}%*\nEnds: {fs['ends_at']}",
                kb([ib("🗑 Cancel Flash Sale",f"flash:cancel|{pid}")],
                   [ib("🔙 Back","cr:flash")]))
        else:
            ctx.user_data["flash_pid"] = pid
            await send(update,
                f"⚡ *Set Flash Sale for {esc(p['name'])}*\n"
                f"Original price: ₹{p['price']:.0f}\n\nEnter discount % (e.g. 20 for 20% off):")
            return S_FS_DISC

    elif d.startswith("flash:cancel|"):
        pid = d[13:]
        if pid in _D["flash_sales"]: del _D["flash_sales"][pid]; _save()
        await send(update,"✅ Flash sale cancelled.",back("cr:flash"))

    elif d == "cr:set":
        c    = _D["creators"].get(uid,{})
        code = c.get("code","")
        link = f"https://t.me/{BOT_USER}?start={code}" if code else "Not activated"
        await send(update,
            f"⚙️ *Creator Settings*\n━━━━━━━━━━━━━━\n"
            f"Approval: *{c.get('mode','manual').title()}*\n"
            f"Code: `{code}`\nLink: {link}",
            kb([ib("🔄 Toggle Approval","cr:amode")],
               [ib("🔗 My Store Link","cr:mystore")],
               [ib("🔙 Back","home")]))

    elif d == "cr:amode":
        await send(update,"⚙️ *Approval Mode:*",
            kb([ib("🤖 Auto Approve","cr:am|auto"),ib("👤 Manual","cr:am|manual")],
               [ib("🔙 Back","cr:set")]))

    elif d.startswith("cr:am|"):
        mode = d[6:]
        if uid in _D["creators"]: _D["creators"][uid]["mode"] = mode; _save()
        await send(update,f"✅ Mode: *{mode.title()}*",back("cr:set"))

    elif d == "cr:mystore":
        c    = _D["creators"].get(uid,{})
        code = c.get("code","")
        if not code: await alert(update,"Not activated yet!"); return
        link = f"https://t.me/{BOT_USER}?start={code}"
        await send(update,f"🏪 *Your Store*\nCode: `{code}`\nLink: {link}",
            kb([ib("📢 Share",url=f"https://t.me/share/url?url={link}&text=Check+out+my+courses!")],
               [ib("🔙 Back","cr:set")]))

    elif d == "cr:renew":
        if not is_creator(uid) and not is_admin(uid):
            await alert(update,"Not a creator!"); return
        c   = _D["creators"].get(uid,{})
        bal = c.get("wallet",0.0)
        await send(update,
            f"🔄 *Renew Creator Panel*\n━━━━━━━━━━━━━━\n"
            f"Current: *{c.get('plan','basic').title()}*  |  Wallet: *₹{bal:,.0f}*\n\n"
            f"💳 Pay to: `{PAY_UPI}`\n"
            f"Then select plan & enter UTR:", plans_kb())

    elif d == "cr:lcls":
        if not is_creator(uid) and not is_admin(uid):
            await alert(update,"Not a creator!"); return
        await send(update,"🔔 *Schedule Live Class — Step 1/2*\n\nEnter class title:")
        return S_LT

    elif d == "cr:bc":
        if not is_creator(uid) and not is_admin(uid):
            await alert(update,"Not a creator!"); return
        if not panel_active(uid):
            await send(update,"❌ Panel not active.",kb([ib("🔄 Renew","cr:renew")])); return
        await send(update,"📣 *Broadcast*\n\nWho to message?",
            kb([ib("👥 All My Students","bct|all")],[ib("❌ Cancel","cr:dash")]))
        return S_BT

    elif d == "cr:mkcp":
        if not is_creator(uid) and not is_admin(uid):
            await alert(update,"Not a creator!"); return
        await send(update,"🎟 *Create Coupon — Step 1/4*\n\nEnter code (blank = auto):")
        return S_VN

    # ── WITHDRAWAL ────────────────────────────────────
    elif d == "cr:withdraw":
        c   = _D["creators"].get(uid,{})
        bal = c.get("wallet",0.0)
        if bal < 100:
            await alert(update,f"Min ₹100 required. Balance: ₹{bal:.0f}"); return
        await send(update,
            f"💸 *Withdrawal Request*\n\nWallet Balance: *₹{bal:,.2f}*\n"
            f"Your UPI: `{esc(c.get('upi',''))}`\n\n"
            f"Enter amount to withdraw (min ₹100):")
        return S_WD_AMT

    # ── MARKETPLACE ───────────────────────────────────
    elif d == "mkt:home":
        mkb = KB_MKT if is_creator(uid) else KB_STUDENT
        await send(update,"🏪 *Marketplace*",mkb)

    elif d == "mkt:trend":
        await _trending(update)

    elif d == "mkt:new":
        prods = sorted(
            [(pid,p) for pid,p in _D["products"].items() if p.get("active")],
            key=lambda x:x[1].get("ts",""),reverse=True)[:12]
        if not prods: await send(update,"No products yet!",back("mkt:home")); return
        rows = [[ib(f"🆕 {p['name'][:30]}  ₹{get_effective_price(pid):.0f}",f"vp|{pid}")] for pid,p in prods]
        rows.append([ib("🔙 Back","mkt:home")])
        await send(update,"🆕 *New Products*",kb(*rows))

    elif d == "mkt:free":
        prods = [(pid,p) for pid,p in _D["products"].items()
                 if p.get("active") and p.get("price",1)==0]
        if not prods: await send(update,"No free products!",back("mkt:home")); return
        rows = [[ib(f"🎁 {p['name'][:30]}",f"vp|{pid}")] for pid,p in prods[:12]]
        rows.append([ib("🔙 Back","mkt:home")])
        await send(update,"🎁 *Free Products*",kb(*rows))

    elif d == "mkt:cats":
        cats = list({c.get("cat") for c in _D["creators"].values()
                     if c.get("ps") in ("active","trial") and c.get("cat")})
        if not cats: await send(update,"No categories yet!",back("mkt:home")); return
        await send(update,"📂 *Browse by Category*",cats_kb())

    elif d.startswith("cat|"):
        cat   = d[4:]
        cids  = {cid for cid,c in _D["creators"].items() if c.get("cat")==cat}
        prods = [(pid,p) for pid,p in _D["products"].items()
                 if p.get("cid") in cids and p.get("active")]
        if not prods: await send(update,f"No products in *{esc(cat)}*!",back("mkt:cats")); return
        rows  = [[ib(f"🛒 {p['name'][:28]}  ₹{get_effective_price(pid):.0f}",f"vp|{pid}")] for pid,p in prods[:15]]
        rows.append([ib("🔙 Back","mkt:cats")])
        await send(update,f"📂 *{esc(cat)}* — {len(prods)} products",kb(*rows))

    elif d == "mkt:topc":
        top = sorted([(cid,c) for cid,c in _D["creators"].items() if c.get("ps") in ("active","trial")],
                     key=lambda x:-x[1].get("sales",0))[:10]
        if not top: await send(update,"No creators yet!",back("mkt:home")); return
        txt = "⭐ *Top Creators*\n\n"; rows = []
        for i,(cid,c) in enumerate(top,1):
            txt += f"{i}. *{esc(c['name'])}* {'✅' if c.get('ver') else ''}  ₹{c.get('sales',0):,.0f}\n"
            rows.append([ib(f"🏪 {c['name'][:28]}",f"cpr|{cid}")])
        rows.append([ib("🔙 Back","mkt:home")]); await send(update,txt,kb(*rows))

    elif d.startswith("cpr|"):
        await _creator_profile(update,d[4:])

    elif d == "mkt:top":
        top = sorted([(pid,p) for pid,p in _D["products"].items() if p.get("active")],
                     key=lambda x:(-x[1].get("students",0),-x[1].get("rating",0)))[:10]
        if not top: await send(update,"No products yet!",back("mkt:home")); return
        txt = "🏆 *Top Products*\n\n"; rows = []
        for i,(pid,p) in enumerate(top,1):
            txt += f"{i}. *{esc(p['name'])}*  ₹{p['price']:.0f}  👥{p.get('students',0)} ⭐{p.get('rating',0):.1f}\n"
            rows.append([ib(f"🛒 {p['name'][:28]}",f"vp|{pid}")])
        rows.append([ib("🔙 Back","mkt:home")]); await send(update,txt,kb(*rows))

    elif d == "mkt:srch":
        await send(update,"🔍 *Search Products*\n\nType your keyword:")
        return S_SQ

    # ── PRODUCT VIEW ──────────────────────────────────
    elif d.startswith("dorate|"):
        pid = d[7:]
        await send(update,"⭐ *Rate this product:*",star_kb(pid))

    elif d.startswith("vp|"):
        await _view_product(update,uid,d[3:])

    elif d.startswith("acc|"):
        parts = d[4:].split("|",1)
        if len(parts) < 2: return
        owner, pid = parts[0], parts[1]
        if owner != uid: await alert(update,"Not yours!"); return
        await _access_product(update,uid,pid)

    elif d.startswith("rate|"):
        parts = d.split("|"); pid, star = parts[1], int(parts[2])
        if not has_bought(uid,pid): await alert(update,"Buy first!"); return
        _D["ratings"][f"{uid}|{pid}"] = star
        rats = [v for k,v in _D["ratings"].items() if k.endswith(f"|{pid}")]
        if pid in _D["products"]:
            _D["products"][pid]["rating"] = round(sum(rats)/len(rats),1)
        _save()
        await send(update,f"{'⭐'*star} Rating saved! Thanks 🙏",back("st:prods"))

    elif d.startswith("apv|"):
        await _approve_purchase(update,ctx,uid,d[4:])

    elif d.startswith("rjt|"):
        oid = d[4:]; pur = _D["purchases"].get(oid)
        if pur:
            _D["purchases"][oid]["ok"] = False
            _D["purchases"][oid]["rejected"] = True; _save()
            try: await ctx.bot.send_message(int(pur["uid"]),
                "❌ Payment not verified. Please contact the creator.")
            except: pass
        await send(update,"❌ Rejected.",back("home"))

    # ── WISHLIST ──────────────────────────────────────
    elif d.startswith("wish|add|"):
        pid = d[9:]
        wl  = _D["wishlist"].setdefault(uid, [])
        if pid not in wl:
            wl.append(pid); _save()
            await alert(update,"❤️ Added to wishlist!")
        else:
            await alert(update,"Already in wishlist!")

    elif d.startswith("wish|rem|"):
        pid = d[9:]
        wl  = _D["wishlist"].get(uid, [])
        if pid in wl: wl.remove(pid); _save()
        await send(update,"💔 Removed from wishlist.",back("st:wish"))

    # ── STUDENT PANEL ─────────────────────────────────
    elif d == "st:prods":
        await _my_products(update,uid)

    elif d == "st:wlt":
        await _wallet(update,uid)

    elif d == "st:ref":
        await _refer(update,uid)

    elif d == "st:wish":
        await _wishlist(update,uid)

    elif d == "st:help":
        await _help(update,uid)

    elif d == "st:wlt_conv":
        bal = _D["users"].get(uid,{}).get("wallet",0.0)
        if bal < 10: await alert(update,"Minimum ₹10 required!"); return
        code = rand_cpn()
        _D["wcoupons"][code] = {"uid":uid,"val":bal,"ts":now()}
        _C["coupons"][code]  = {"type":"flat","flat":bal,"pct":0,"max":1,"used":0,"exp":None}
        _D["users"][uid]["wallet"] = 0.0; _save(); _savec()
        await send(update,
            f"✅ *Wallet Converted!*\nCode: `{code}`\nValue: ₹{bal:,.2f}",
            back("st:wlt"))

    # ── ADMIN ─────────────────────────────────────────
    elif d == "adm:home":
        if not is_admin(uid): await alert(update,"Not authorized!"); return
        await _admin_home(update)

    elif d == "adm:pend":
        if not is_admin(uid): return
        await _admin_pending(update)

    elif d == "adm:crts":
        if not is_admin(uid): return
        await _admin_all_creators(update)

    elif d.startswith("adm:cdt|"):
        if not is_admin(uid): return
        await _admin_creator_detail(update,d[8:])

    elif d == "adm:sales":
        if not is_admin(uid): return
        await _admin_sales(update)

    elif d == "adm:prods":
        if not is_admin(uid): return
        prods = [(pid,p) for pid,p in _D["products"].items() if p.get("active")]
        rows  = [[ib(f"🗑 {p['name'][:25]}",f"adm:dp|{pid}")] for pid,p in prods[:20]]
        rows.append([ib("🔙 Back","adm:home")])
        await send(update,f"📦 *Products ({len(prods)})*",kb(*rows))

    elif d.startswith("adm:dp|"):
        if not is_admin(uid): return
        pid = d[7:]
        if pid in _D["products"]: _D["products"][pid]["active"] = False; _save()
        await send(update,"🗑 Removed.",back("adm:prods"))

    elif d.startswith("adm:apv|"):
        if not is_admin(uid): return
        await _approve_creator(update,ctx,d[8:])

    elif d.startswith("adm:rjt|"):
        if not is_admin(uid): return
        cid = d[8:]
        if cid in _D["creators"]: _D["creators"][cid]["ps"] = "rejected"; _save()
        await send(update,"❌ Rejected.",back("adm:pend"))
        try: await ctx.bot.send_message(int(cid),"❌ Application not approved.")
        except: pass

    elif d.startswith("adm:vfy|"):
        if not is_admin(uid): return
        cid = d[8:]
        if cid not in _D["creators"]: return
        new = not _D["creators"][cid].get("ver",False)
        _D["creators"][cid]["ver"] = new; _save()
        await send(update,f"{'✅ Badge granted!' if new else '❌ Badge removed.'}",back(f"adm:cdt|{cid}"))

    elif d.startswith("adm:ext|"):
        if not is_admin(uid): return
        cid = d[8:]
        if cid not in _D["creators"]: return
        extend_panel(cid,7); _save()
        await send(update,f"✅ +7 days for `{cid}`",back(f"adm:cdt|{cid}"))

    elif d.startswith("adm:rnw|"):
        if not is_admin(uid): return
        parts = d[8:].split("|"); cid, plan = parts[0], parts[1]
        extend_panel(cid,30)
        _D["creators"][cid].update({"plan":plan,"ps":"active","trial_status":False})
        ref = _D["crefs"].get(cid)
        if ref and ref in _D["creators"]:
            extend_panel(ref,7); _D["crefs"][cid] = None
        _save()
        await send(update,f"✅ Renewed `{cid}` — {plan.title()}",back("adm:home"))
        try:
            lim = _C["plans"].get(plan,{}).get("limit",10)
            await ctx.bot.send_message(int(cid),
                f"✅ *Panel Renewed!*\nPlan: *{plan.title()}* | Products: *{lim}* | +30 days\n/dashboard",
                parse_mode=MD,reply_markup=KB_CREATOR)
        except: pass

    elif d == "adm:bc":
        if not is_admin(uid): return
        await send(update,"📣 *Admin Broadcast*\n\nType message to ALL users:")
        return S_AB

    elif d == "adm:ver":
        if not is_admin(uid): return
        await send(update,"Use: `/verifycreator CREATOR_ID`",back("adm:home"))

    elif d == "adm:wd":
        if not is_admin(uid): return
        await _admin_withdrawals(update)

    elif d.startswith("adm:wd_apv|"):
        if not is_admin(uid): return
        idx = int(d[11:])
        if idx < len(_D["withdrawals"]):
            wd = _D["withdrawals"][idx]
            wd["status"] = "approved"
            # Deduct from creator wallet
            cid = wd.get("cid","")
            if cid in _D["creators"]:
                _D["creators"][cid]["wallet"] = max(0,_D["creators"][cid].get("wallet",0)-wd["amount"])
            _save()
            await send(update,f"✅ Withdrawal approved for ₹{wd['amount']:.0f}",back("adm:wd"))
            try:
                await ctx.bot.send_message(int(wd["cid"]),
                    f"✅ *Withdrawal Approved!*\nAmount: ₹{wd['amount']:.0f}\nPaid to: {esc(wd['upi'])}",
                    parse_mode=MD)
            except: pass

    elif d.startswith("adm:bst|"):
        if not is_admin(uid): return
        parts = d[8:].split("|"); cid, pid = parts[0], parts[1]
        bd = _C.get("boost_days",3)
        _D["boosts"][pid] = exp_str(bd); _save()
        await send(update,f"✅ Boost approved — {bd} days!",back("adm:home"))
        try: await ctx.bot.send_message(int(cid),f"⚡ Product boosted *{bd} days*! 🚀",parse_mode=MD)
        except: pass

    elif d.startswith("adm:analytics"):
        if not is_admin(uid): return
        await _admin_analytics(update)

    elif d.startswith("adm:backup"):
        if not is_admin(uid): return
        await _do_backup(ctx.bot)
        await send(update,"✅ Backup sent to your DM!",back("adm:home"))

    else:
        try: await q.answer()
        except: pass

# ═══════════════════════════════════════════════════════════════
#  CREATOR DASHBOARD
# ═══════════════════════════════════════════════════════════════
async def _creator_dashboard(update: Update, uid: str):
    if not is_creator(uid):
        await send(update,"❌ Not a creator. Tap *Become a Creator*!",KB_STUDENT); return
    c = _D["creators"].get(uid,{})
    if not c:
        _D["users"][uid]["role"] = "student"; _save()
        await _show_home(update,uid); return
    if c.get("ps") == "pending":
        await send(update,"⏳ *Pending Approval*\n\nAdmin will approve you soon! 🎉",back()); return
    today  = datetime.now().strftime("%d %b %Y")
    ts     = sum(1 for x in _D["purchases"].values()
                 if _D["products"].get(x.get("pid",""),{}).get("cid")==uid
                 and x.get("ok") and today in x.get("ts",""))
    total  = sum(1 for x in _D["purchases"].values()
                 if _D["products"].get(x.get("pid",""),{}).get("cid")==uid and x.get("ok"))
    cnt, lim = prod_count(uid), prod_limit(uid)
    exp    = c.get("trial_exp") if c.get("ps")=="trial" else c.get("panel_exp")
    dl     = days_left(exp) if exp else 0
    tag    = "🔔 Trial" if c.get("ps")=="trial" else ("✅ Active" if dl>0 else "🔴 Expired")
    bar    = "▓"*min(cnt,10)+"░"*max(0,min(10,lim)-cnt)
    # Weekly revenue
    week_ago = (datetime.now()-timedelta(days=7)).strftime("%d %b")
    week_rev = sum(s.get("amt",0) for s in _D["sales"]
                   if s.get("pid","") in [pid for pid,p in _D["products"].items() if p.get("cid")==uid])
    await send(update,
        f"📊 *Dashboard* {'✅' if c.get('ver') else ''}\n"
        f"👤 *{esc(c.get('name',''))}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👥 Today: *{ts}*  |  Total: *{total}*\n"
        f"💰 Revenue: *₹{c.get('sales',0):,.0f}*\n"
        f"📦 [{bar}] *{cnt}/{lim}* ({c.get('plan','basic').title()})\n"
        f"👛 Wallet: *₹{c.get('wallet',0):,.0f}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Panel: {tag}  |  *{dl}d* left  |  {c.get('mode','manual').title()} mode",
        kb(
            [ib("📦 Products","cr:prods"),    ib("✏️ Edit","cr:editp")],
            [ib("🎟 Coupons","cr:mkcp"),      ib("📣 Broadcast","cr:bc")],
            [ib("💰 Wallet","cr:wlt"),        ib("⚡ Boost","cr:boost")],
            [ib("⚡ Flash Sale","cr:flash"),  ib("🔔 Live Class","cr:lcls")],
            [ib("🔄 Renew Panel","cr:renew"), ib("⚙️ Settings","cr:set")],
            [ib("🏪 Marketplace","mkt:home")],
        ))

async def _creator_products(update: Update, uid: str):
    if not is_creator(uid): await send(update,"❌ Not a creator.",back()); return
    prods = [(pid,p) for pid,p in _D["products"].items() if p.get("cid")==uid and p.get("active")]
    lim   = prod_limit(uid)
    if not prods:
        await send(update,f"📦 *My Products (0/{lim})*\n\nNo products yet!",
            kb([ib("➕ Add Product","cr:addp")],[ib("🔙 Dashboard","cr:dash")])); return
    txt  = f"📦 *My Products ({len(prods)}/{lim})*\n\n"
    rows = []
    for pid, p in prods:
        bst  = "⚡" if pid in _D["boosts"] and not is_expired(_D["boosts"][pid]) else ""
        fs   = _D["flash_sales"].get(pid)
        sale = "🔥" if fs and not is_expired(fs.get("ends_at","")) else ""
        txt += f"{bst}{sale}*{esc(p['name'])}*  ₹{p['price']:.0f}  👥{p.get('students',0)}\n"
        rows.append([ib(f"⚙️ {p['name'][:28]}",f"cr:mgp|{pid}")])
    rows.append([ib("➕ Add","cr:addp"),ib("✏️ Edit","cr:editp"),ib("🔙 Back","cr:dash")])
    await send(update,txt,kb(*rows))

async def _manage_product(update: Update, uid: str, pid: str):
    p = _D["products"].get(pid)
    if not p or (p.get("cid")!=uid and not is_admin(uid)):
        await alert(update,"Not yours!"); return
    bst  = _D["boosts"].get(pid)
    bstr = f"⚡ Until {bst}" if bst and not is_expired(bst) else "Not boosted"
    fs   = _D["flash_sales"].get(pid)
    fstr = f"🔥 {fs['disc_pct']}% off until {fs['ends_at']}" if fs and not is_expired(fs.get("ends_at","")) else "No flash sale"
    price_eff = get_effective_price(pid)
    await send(update,
        f"⚙️ *{esc(p['name'])}*\n━━━━━━━━━━━━━━\n"
        f"💰 ₹{p['price']:.0f}{f' → ₹{price_eff:.0f}' if price_eff!=p['price'] else ''}\n"
        f"📂 {DTYPE_MAP.get(p.get('dtype',''),'?')}\n"
        f"⏳ {p.get('dur','lifetime')}  |  👥 {p.get('students',0)}\n"
        f"⭐ {p.get('rating',0):.1f}  |  {bstr}\n"
        f"{fstr}",
        kb(
            [ib("📝 Name",f"ep:name|{pid}"),  ib("💰 Price",f"ep:price|{pid}")],
            [ib("🔗 Link",f"ep:link|{pid}"),   ib("⏳ Duration",f"ep:dur|{pid}")],
            [ib("🗑 Delete",f"cr:delp|{pid}"), ib("⚡ Boost",f"bstp|{pid}")],
            [ib("🔥 Flash Sale",f"flash|{pid}"),ib("📊 Stats",f"cr:pstat|{pid}")],
            [ib("🔙 Products","cr:prods")],
        ))

async def _product_stats(update: Update, pid: str):
    p   = _D["products"].get(pid,{})
    rev = sum(s.get("amt",0) for s in _D["sales"] if s.get("pid")==pid)
    today = datetime.now().strftime("%d %b %Y")
    tn  = sum(1 for x in _D["purchases"].values()
              if x.get("pid")==pid and x.get("ok") and today in x.get("ts",""))
    wl_count = sum(1 for wl in _D["wishlist"].values() if pid in wl)
    await send(update,
        f"📊 *{esc(p.get('name',''))} Stats*\n━━━━━━━━━━━━━━\n"
        f"👥 Total: *{p.get('students',0)}*  |  📅 Today: *{tn}*\n"
        f"💰 Revenue: *₹{rev:,.0f}*  |  ⭐ {p.get('rating',0):.1f}/5\n"
        f"❤️ Wishlisted by: *{wl_count}* users",
        back(f"cr:mgp|{pid}"))

async def _creator_wallet(update: Update, uid: str):
    c    = _D["creators"].get(uid,{})
    bal  = c.get("wallet",0.0)
    plan = c.get("plan","basic")
    cost = _C["plans"].get(plan,{}).get("price",199)
    bc   = _C.get("boost_cost",29)
    pend_wd = sum(1 for w in _D["withdrawals"] if w.get("cid")==uid and w.get("status")=="pending")
    await send(update,
        f"💰 *Creator Wallet*\n━━━━━━━━━━━━━━\n"
        f"Balance: *₹{bal:,.2f}*\n"
        f"Pending withdrawals: {pend_wd}\n\n"
        f"Use for: 🔄 Renew ₹{cost}  |  ⚡ Boost ₹{bc}",
        kb([ib(f"🔄 Renew (₹{cost})","cr:wlt_renew")],
           [ib(f"⚡ Boost (₹{bc})","cr:wlt_boost")],
           [ib("💸 Request Withdrawal","cr:withdraw")],
           [ib("🔙 Dashboard","cr:dash")]))

# ═══════════════════════════════════════════════════════════════
#  MARKETPLACE
# ═══════════════════════════════════════════════════════════════
async def _trending(update: Update):
    prods = [(pid,p) for pid,p in _D["products"].items() if p.get("active")]
    prods.sort(key=lambda x:(-(x[0] in _D["boosts"] and not is_expired(_D["boosts"].get(x[0],""))),
                              -x[1].get("students",0)))
    if not prods: await send(update,"No products yet! Check back soon.",back("mkt:home")); return
    txt = "🔥 *Trending Products*\n\n"; rows = []
    for pid, p in prods[:10]:
        bst  = "⚡" if pid in _D["boosts"] and not is_expired(_D["boosts"][pid]) else ""
        fs   = _D["flash_sales"].get(pid)
        sale = f"  🔥{fs['disc_pct']}% OFF" if fs and not is_expired(fs.get("ends_at","")) else ""
        ep   = get_effective_price(pid)
        c    = _D["creators"].get(p.get("cid",""),{})
        price_str = f"~~₹{p['price']:.0f}~~ ₹{ep:.0f}" if ep!=p["price"] else f"₹{ep:.0f}"
        txt += f"{bst}*{esc(p['name'])}*{sale}\n  {price_str}  ⭐{p.get('rating',0):.1f}  👥{p.get('students',0)}\n\n"
        rows.append([ib(f"{'⚡' if bst else '🛒'} {p['name'][:28]}",f"vp|{pid}")])
    rows.append([ib("🔙 Back","mkt:home")])
    await send(update,txt,kb(*rows))

async def _view_product(update: Update, uid: str, pid: str):
    p = _D["products"].get(pid)
    if not p or not p.get("active"):
        await send(update,"❌ Product not available.",back()); return
    is_b  = has_bought(uid,pid)
    rated = f"{uid}|{pid}" in _D["ratings"]
    c     = _D["creators"].get(p.get("cid",""),{})
    bst   = "⚡ *BOOSTED* | " if pid in _D["boosts"] and not is_expired(_D["boosts"][pid]) else ""
    fs    = _D["flash_sales"].get(pid)
    sale  = f"🔥 *FLASH SALE — {fs['disc_pct']}% OFF!* | " if fs and not is_expired(fs.get("ends_at","")) else ""
    ep    = get_effective_price(pid)
    dur   = f"{p['dur']} days" if str(p.get("dur","")) not in ("lif","lifetime") else "♾ Lifetime"
    wl    = _D["wishlist"].get(uid,[])
    in_wl = pid in wl
    rl    = f"https://t.me/{BOT_USER}?start={c.get('code','')}"
    price_txt = f"~~₹{p['price']:.0f}~~ *₹{ep:.0f}*" if ep!=p["price"] else f"*₹{ep:.0f}*"
    rows  = []
    if is_b:
        rows.append([ib("📂 Access Product",f"acc|{uid}|{pid}")])
        if not rated: rows.append([ib("⭐ Rate Product",f"dorate|{pid}")])
    else:
        rows.append([ib(f"🛒 Buy Now — ₹{ep:.0f}",f"buy|{pid}")])
        rows.append([ib("❤️ Save to Wishlist" if not in_wl else "💔 Remove Wishlist",
                        f"wish|{'add' if not in_wl else 'rem'}|{pid}")])
    rows.append([ib("📢 Share & Earn",url=f"https://t.me/share/url?url={rl}&text=Check+this+out!")])
    if c.get("code"): rows.append([ib("🏪 Creator Store",url=rl)])
    rows.append([ib("🔙 Back","mkt:trend")])
    thumb = p.get("thumb")
    text  = (
        f"{sale}{bst}📦 *{esc(p['name'])}*{'  🔄' if p.get('sub') else ''}\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"👤 *{esc(c.get('name','?'))}* {'✅' if c.get('ver') else ''}\n"
        f"💰 Price: {price_txt}  |  ⏳ {dur}\n"
        f"📂 {DTYPE_MAP.get(p.get('dtype',''),'?')}\n"
        f"👥 {p.get('students',0)} students  |  ⭐ {p.get('rating',0):.1f}/5\n"
        f"{'✅ _You own this_' if is_b else '👆 _Tap Buy for instant access!_'}"
    )
    if thumb:
        try:
            m = update.effective_message
            await m.reply_photo(thumb, caption=text, parse_mode=MD, reply_markup=kb(*rows))
            if q := update.callback_query:
                try: await q.answer()
                except: pass
            return
        except: pass
    await send(update, text, kb(*rows))

async def _access_product(update: Update, uid: str, pid: str):
    pur = next((v for v in _D["purchases"].values()
                if v.get("uid")==uid and v.get("pid")==pid and v.get("ok")), None)
    if not pur: await send(update,"❌ Access not found!",back("st:prods")); return
    p   = _D["products"].get(pid,{})
    exp = pur.get("exp")
    rl  = f"https://t.me/{BOT_USER}?start=ref{uid}"
    await send(update,
        f"📂 *{esc(p.get('name',''))}*\n━━━━━━━━━━━━━━\n"
        f"⏳ {f'Expires: {exp} ({days_left(exp)}d left)' if exp else '♾ Lifetime Access'}\n\n"
        f"🔗 *Access Link:*\n{p.get('link','Contact creator for access')}\n\n"
        f"💡 Share & earn 30%:\n`{rl}`",
        kb([ib("📢 Share & Earn",url=f"https://t.me/share/url?url={rl}")],
           [ib("⭐ Rate",f"dorate|{pid}"),ib("🔙 My Products","st:prods")]))

async def _creator_profile(update: Update, cid: str):
    c = _D["creators"].get(cid)
    if not c: await send(update,"Not found!",back("mkt:topc")); return
    prods = [(pid,p) for pid,p in _D["products"].items() if p.get("cid")==cid and p.get("active")]
    stud  = sum(p.get("students",0) for _,p in prods)
    avg   = round(sum(p.get("rating",0) for _,p in prods)/len(prods),1) if prods else 0
    rl    = f"https://t.me/{BOT_USER}?start={c.get('code','')}"
    rows  = [[ib(f"🛒 {p['name'][:28]}  ₹{get_effective_price(pid):.0f}",f"vp|{pid}")] for pid,p in prods[:5]]
    if c.get("code"): rows.append([ib("🏪 Full Store",url=rl)])
    rows.append([ib("🔙 Back","mkt:topc")])
    await send(update,
        f"🏪 *{esc(c['name'])}* {'✅' if c.get('ver') else ''}\n━━━━━━━━━━━━━━━\n"
        f"📂 {esc(c.get('cat',''))}  |  👥 {stud} students\n"
        f"📝 _{esc(c.get('bio',''))}_\n"
        f"📦 {len(prods)} products  |  ⭐ {avg}  |  💰 ₹{c.get('sales',0):,.0f}",
        kb(*rows))

async def _show_store(update: Update, cid: str):
    c     = _D["creators"].get(cid,{})
    prods = [(pid,p) for pid,p in _D["products"].items() if p.get("cid")==cid and p.get("active")]
    stud  = sum(p.get("students",0) for _,p in prods)
    rl    = f"https://t.me/{BOT_USER}?start={c.get('code','')}"
    rows  = [[ib(f"🛒 {p['name'][:28]}  ₹{get_effective_price(pid):.0f}",f"vp|{pid}")] for pid,p in prods[:8]]
    rows.append([ib("📢 Share Store",url=f"https://t.me/share/url?url={rl}")])
    rows.append([ib("🏠 Main Menu","home")])
    await send(update,
        f"🏪 *{esc(c.get('name',''))}* {'✅' if c.get('ver') else ''}\n"
        f"📂 {esc(c.get('cat',''))}  |  👥 {stud}\n"
        f"📝 _{esc(c.get('bio',''))}_\n📦 *{len(prods)} products*",
        kb(*rows))

# ═══════════════════════════════════════════════════════════════
#  STUDENT PANEL
# ═══════════════════════════════════════════════════════════════
async def _my_products(update: Update, uid: str):
    my = {oid:p for oid,p in _D["purchases"].items() if p.get("uid")==uid and p.get("ok")}
    if not my:
        await send(update,
            "📦 *My Products*\n\nNo purchases yet!\nBrowse the marketplace to find great courses.",
            kb([ib("🏪 Browse","mkt:home")],[ib("🔗 Refer & Earn","st:ref")])); return
    txt  = f"📦 *My Products ({len(my)})*\n\n"; rows = []
    for oid, pur in list(my.items())[:15]:
        pid = pur.get("pid",""); p = _D["products"].get(pid,{})
        exp = pur.get("exp")
        el  = f"⏳{days_left(exp)}d" if exp and days_left(exp)>0 else ("🔴 Exp" if exp else "♾")
        txt += f"• *{esc(p.get('name','?'))}* {el}\n"
        row = [ib(f"📂 {p.get('name','?')[:22]} [{el}]",f"acc|{uid}|{pid}")]
        if p.get("sub") or (exp and days_left(exp)<=7): row.append(ib("🔄",f"buy|{pid}"))
        rows.append(row)
    rows.append([ib("🔙 Home","home")]); await send(update,txt,kb(*rows))

async def _wallet(update: Update, uid: str):
    bal  = _D["users"].get(uid,{}).get("wallet",0.0)
    refs = sum(1 for v in _D["refs"].values() if v==uid)
    rl   = f"https://t.me/{BOT_USER}?start=ref{uid}"
    pct  = _C.get("ref_pct",30)
    await send(update,
        f"👛 *Your Wallet*\n━━━━━━━━━━━━━━\n"
        f"💰 Balance: *₹{bal:,.2f}*\n"
        f"👥 Referrals: *{refs}*\n\n"
        f"Earn *{pct}%* of every referral purchase!\n`{rl}`",
        kb([ib("🎟 Convert to Coupon","st:wlt_conv")],
           [ib("📢 Share Link",url=f"https://t.me/share/url?url={rl}&text=Join+UPI+Access+Hub!")],
           [ib("🔙 Home","home")]))

async def _refer(update: Update, uid: str):
    rl   = f"https://t.me/{BOT_USER}?start=ref{uid}"
    refs = sum(1 for v in _D["refs"].values() if v==uid)
    bal  = _D["users"].get(uid,{}).get("wallet",0.0)
    pct  = _C.get("ref_pct",30)
    await send(update,
        f"🔗 *Refer & Earn*\n━━━━━━━━━━━━━━\n"
        f"Link: `{rl}`\n━━━━━━━━━━━━━━\n"
        f"👥 {refs} referrals  |  💰 ₹{bal:,.2f} earned\n\n"
        f"*Earn {pct}% of every purchase your friends make!*\n"
        f"Credits go directly to your wallet 💳",
        kb([ib("📢 Share Now",url=f"https://t.me/share/url?url={rl}&text=Join+UPI+Access+Hub+and+learn!")],
           [ib("👛 Wallet","st:wlt"),ib("🔙 Home","home")]))

async def _wishlist(update: Update, uid: str):
    wl = _D["wishlist"].get(uid, [])
    if not wl:
        await send(update,
            "❤️ *Wishlist*\n\nNo saved products yet!\nTap ❤️ on any product to save it.",
            kb([ib("🏪 Browse","mkt:home")],[ib("🔙 Home","home")])); return
    txt  = f"❤️ *Wishlist ({len(wl)} items)*\n\n"; rows = []
    for pid in wl[:12]:
        p  = _D["products"].get(pid,{})
        ep = get_effective_price(pid)
        if not p.get("active"): continue
        fs = _D["flash_sales"].get(pid)
        sale = " 🔥SALE" if fs and not is_expired(fs.get("ends_at","")) else ""
        txt += f"• *{esc(p.get('name','?'))}*  ₹{ep:.0f}{sale}\n"
        rows.append([ib(f"🛒 {p.get('name','?')[:25]}",f"vp|{pid}"),
                     ib("💔",f"wish|rem|{pid}")])
    rows.append([ib("🔙 Home","home")])
    await send(update,txt,kb(*rows))

async def _help(update: Update, uid: str):
    role = get_role(uid)
    base = (
        "❓ *UPI Access Hub — Help*\n━━━━━━━━━━━━━━━━━━━━\n\n"
        "*Student Commands:*\n"
        "/start — 🏠 Main menu\n"
        "/myproducts — 📦 My purchases\n"
        "/wallet — 👛 Wallet & rewards\n"
        "/refer — 🔗 Referral link\n"
        "/search — 🔍 Search products\n"
        "/topcreators — ⭐ Top creators\n"
        "/topproducts — 🏆 Top products\n"
        "/profile CODE — 🏪 View creator store\n"
        "/cancel — ❌ Cancel current action\n\n"
        "*How to Buy:*\n"
        "1. Browse or search products\n"
        "2. Tap Buy → Apply coupon (optional)\n"
        "3. Pay to UPI → Send UTR\n"
        "4. Get instant access!\n\n"
        "*Refer & Earn:*\n"
        "Share your link → Friend buys → You earn 30%!"
    )
    creator_extra = ""
    if is_creator(uid):
        creator_extra = (
            "\n\n*Creator Commands:*\n"
            "/dashboard — 📊 Creator panel\n"
            "/addproduct — ➕ Add product\n"
            "/editproduct — ✏️ Edit product\n"
            "/renewpanel — 🔄 Renew panel\n"
            "/createcoupon — 🎟 Create coupon\n"
            "/broadcast — 📣 Message students\n"
            "/boostproduct — ⚡ Boost product\n"
            "/scheduleclass — 🔔 Live class\n"
        )
    await send(update, base + creator_extra, back())

# ═══════════════════════════════════════════════════════════════
#  PURCHASE FLOW
# ═══════════════════════════════════════════════════════════════
async def cb_buy_entry(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    pid = update.callback_query.data[4:]
    p   = _D["products"].get(pid)
    if not p or not p.get("active"):
        await send(update,"❌ Product unavailable.",back()); return ConversationHandler.END
    if has_bought(uid,pid):
        await alert(update,"✅ You already own this!"); return ConversationHandler.END
    ep = get_effective_price(pid)
    if ep == 0:
        oid = rand_oid()
        _D["purchases"][oid] = {"uid":uid,"pid":pid,"utr":"FREE","ok":False,"exp":None,"ts":now()}
        grant_purchase(oid); _save()
        await send(update,
            f"🎁 *Free Access Granted!*\n\n*{esc(p['name'])}*\n🔗 {p.get('link','Contact creator')}",
            kb([ib("📦 My Products","st:prods")],[ib("🏠 Home","home")]))
        return ConversationHandler.END
    ctx.user_data.update({"bpid":pid,"bprice":ep,"bdisc":0,"bcpn":None})
    sale_note = f"\n🔥 *Flash Sale price applied!*" if ep!=p["price"] else ""
    await send(update,
        f"🛒 *{esc(p['name'])}*  ₹{ep:.0f}{sale_note}\n\n🎟 Have a coupon? Type it or skip:",
        kb([ib("⏭ Skip — Pay Full",f"skp|{pid}")]))
    return S_CPN

async def cb_skip_coupon(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    pid = update.callback_query.data[4:]
    ctx.user_data.update({"bpid":pid,"bprice":get_effective_price(pid),"bdisc":0,"bcpn":None})
    await _show_payment(update,ctx,pid,ctx.user_data["bprice"]); return S_UTR

async def fsm_coupon(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid   = str(update.effective_user.id)
    code  = update.message.text.strip()
    pid   = ctx.user_data.get("bpid","")
    price = ctx.user_data.get("bprice",0)
    ok, final, disc, msg = apply_coupon(code,price,uid)
    await update.message.reply_text(msg,parse_mode=MD)
    if ok: ctx.user_data.update({"bprice":final,"bdisc":disc,"bcpn":code.upper()})
    await _show_payment(update,ctx,pid,ctx.user_data["bprice"]); return S_UTR

async def _show_payment(update: Update, ctx, pid: str, final_price: float):
    p   = _D["products"].get(pid,{})
    c   = _D["creators"].get(p.get("cid",""),{})
    upi = c.get("upi","") or PAY_UPI
    txt = (f"💳 *Payment Details*\n━━━━━━━━━━━━━━\n"
           f"📦 *{esc(p.get('name',''))}*\n"
           f"💰 *₹{final_price:.0f}*\n━━━━━━━━━━━━━━\n"
           f"📲 Pay to UPI:\n`{esc(upi)}`\n\n"
           f"✅ Send *UTR / Transaction ID* after payment:")
    if c.get("qr"):
        try:
            m = update.message or update.callback_query.message
            await m.reply_photo(c["qr"],caption=txt,parse_mode=MD); return
        except: pass
    m = update.message or update.callback_query.message
    await m.reply_text(txt,parse_mode=MD)

async def fsm_utr(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    utr = update.message.text.strip().replace(" ","")
    if not valid_utr(utr): await update.message.reply_text("❌ Invalid UTR (10–25 chars). Try again:"); return S_UTR
    if utr_used(utr): await update.message.reply_text("❌ UTR already used! Contact support."); return S_UTR
    pid  = ctx.user_data.get("bpid","")
    cpn  = ctx.user_data.get("bcpn")
    p    = _D["products"].get(pid,{})
    cid  = p.get("cid","")
    c    = _D["creators"].get(cid,{})
    _D["utr_log"].append(utr.upper())
    if cpn:
        if cpn in _C["coupons"]: _C["coupons"][cpn]["used"] = _C["coupons"][cpn].get("used",0)+1
        _D["cpn_used"][f"{uid}|{cpn}"] = True; _savec()
    oid  = rand_oid()
    _D["purchases"][oid] = {"uid":uid,"pid":pid,"utr":utr.upper(),"ok":False,"exp":None,"ts":now()}
    rl   = f"https://t.me/{BOT_USER}?start=ref{uid}"
    if c.get("mode","manual")=="auto":
        grant_purchase(oid); credit_ref(uid,p.get("price",0)); _save()
        await update.message.reply_text(
            f"✅ *Access Granted Instantly!*\n\n*{esc(p.get('name',''))}*\n"
            f"🔗 {p.get('link','Contact creator')}\n\n💡 Share & earn 30%:\n`{rl}`",
            parse_mode=MD,
            reply_markup=kb([ib("📢 Share",url=f"https://t.me/share/url?url={rl}")],
                            [ib("📦 My Products","st:prods"),ib("⭐ Rate",f"dorate|{pid}")]))
        asyncio.create_task(send_invoice(ctx.bot,oid,_D["purchases"][oid],p))
    else:
        _save()
        await update.message.reply_text(
            "✅ *UTR Submitted!*\n\nWaiting for creator approval. 🎉",parse_mode=MD)
        try:
            await ctx.bot.send_message(int(cid),
                f"💳 *New Payment!*\n👤 {esc(update.effective_user.full_name)} (`{uid}`)\n"
                f"📦 *{esc(p.get('name',''))}*  ₹{p.get('price',0):.0f}\nUTR: `{utr}`",
                parse_mode=MD,
                reply_markup=kb([ib("✅ Approve",f"apv|{oid}"),ib("❌ Reject",f"rjt|{oid}")]))
        except: pass
    return ConversationHandler.END

async def _approve_purchase(update: Update, ctx, uid: str, oid: str):
    pur  = _D["purchases"].get(oid)
    if not pur or pur.get("ok"):
        await send(update,"Already processed.",back()); return
    prod = _D["products"].get(pur.get("pid",""),{})
    if not is_admin(uid) and prod.get("cid")!=uid:
        await alert(update,"Not authorized!"); return
    grant_purchase(oid); credit_ref(pur["uid"],prod.get("price",0)); _save()
    await send(update,"✅ *Purchase approved!*",back("home"))
    rl = f"https://t.me/{BOT_USER}?start=ref{pur['uid']}"
    try:
        await ctx.bot.send_message(int(pur["uid"]),
            f"✅ *Access Granted!*\n*{esc(prod.get('name',''))}*\n"
            f"🔗 {prod.get('link','Contact creator')}\n\n💡 Share:\n`{rl}`",
            parse_mode=MD,
            reply_markup=kb([ib("📢 Share",url=f"https://t.me/share/url?url={rl}")],
                            [ib("📦 My Products","st:prods"),ib("⭐ Rate",f"dorate|{pur['pid']}")]))
        asyncio.create_task(send_invoice(ctx.bot,oid,_D["purchases"][oid],prod))
    except: pass

# ═══════════════════════════════════════════════════════════════
#  ADMIN
# ═══════════════════════════════════════════════════════════════
async def _admin_home(update: Update):
    tc   = sum(1 for c in _D["creators"].values() if c.get("ps") in ("active","trial"))
    pend = sum(1 for c in _D["creators"].values() if c.get("ps")=="pending")
    rev  = sum(s.get("amt",0) for s in _D["sales"])
    today = datetime.now().strftime("%d %b %Y")
    tr   = sum(s.get("amt",0) for s in _D["sales"] if today in s.get("ts",""))
    wd   = sum(1 for w in _D["withdrawals"] if w.get("status")=="pending")
    await send(update,
        f"👑 *Admin Dashboard*\n━━━━━━━━━━━━━━━━━━\n"
        f"👥 {len(_D['users'])} users  •  🎨 {tc} creators\n"
        f"⏳ {pend} pending  •  💸 {wd} withdrawals\n"
        f"💰 Total: *₹{rev:,.0f}*  •  Today: *₹{tr:,.0f}*",
        kb(
            [ib("⏳ Pending","adm:pend"),      ib("👥 All Creators","adm:crts")],
            [ib("📊 Sales","adm:sales"),        ib("📦 Products","adm:prods")],
            [ib("💸 Withdrawals","adm:wd"),     ib("📈 Analytics","adm:analytics")],
            [ib("📣 Broadcast","adm:bc"),       ib("💾 Backup","adm:backup")],
            [ib("🏅 Verify Creator","adm:ver"), ib("🔙 Home","home")],
        ))

async def _admin_pending(update: Update):
    pend = [(cid,c) for cid,c in _D["creators"].items() if c.get("ps")=="pending"]
    if not pend: await send(update,"✅ No pending applications!",back("adm:home")); return
    txt = f"⏳ *Pending ({len(pend)})*\n\n"; rows = []
    for cid,c in pend:
        txt += f"• *{esc(c['name'])}* `{cid}`\nUPI: {esc(c.get('upi','?'))}\n\n"
        rows.append([ib(f"✅ {c['name'][:15]}",f"adm:apv|{cid}"),ib("❌ Reject",f"adm:rjt|{cid}")])
    rows.append([ib("🔙 Back","adm:home")]); await send(update,txt,kb(*rows))

async def _admin_all_creators(update: Update):
    rows = []
    for cid,c in list(_D["creators"].items())[:20]:
        ico = {"active":"🟢","trial":"🔵","pending":"🟡","expired":"🔴"}.get(c.get("ps",""),"⚪")
        vb  = "✅" if c.get("ver") else "⬜"
        rows.append([ib(f"{vb}{ico} {c['name'][:20]}  ₹{c.get('sales',0):,.0f}",f"adm:cdt|{cid}")])
    rows.append([ib("🔙 Back","adm:home")]); await send(update,f"🎨 *Creators ({len(_D['creators'])})*",kb(*rows))

async def _admin_creator_detail(update: Update, cid: str):
    c = _D["creators"].get(cid)
    if not c: await send(update,"Not found!",back("adm:home")); return
    exp = c.get("trial_exp") if c.get("ps")=="trial" else c.get("panel_exp")
    await send(update,
        f"👤 *{esc(c['name'])}* {'✅' if c.get('ver') else ''}\n━━━━━━━━━━━━━━━━━\n"
        f"ID: `{cid}`  •  {esc(c.get('cat',''))}\n"
        f"Status: *{c.get('ps','?')}*  •  Plan: *{c.get('plan','basic')}*\n"
        f"Exp: {exp} ({days_left(exp)}d)  •  UPI: {esc(c.get('upi',''))}\n"
        f"Products: {prod_count(cid)}  •  💰 ₹{c.get('sales',0):,.0f}  •  👛 ₹{c.get('wallet',0):.0f}",
        kb([ib("✅ Approve",f"adm:apv|{cid}"),  ib("❌ Reject",f"adm:rjt|{cid}")],
           [ib("🏅 Verify",f"adm:vfy|{cid}"),   ib("➕ +7 Days",f"adm:ext|{cid}")],
           [ib("🔙 Back","adm:crts")]))

async def _admin_sales(update: Update):
    rev = sum(s.get("amt",0) for s in _D["sales"])
    today = datetime.now().strftime("%d %b %Y")
    tr  = sum(s.get("amt",0) for s in _D["sales"] if today in s.get("ts",""))
    # Top creators
    cr_rev = {}
    for s in _D["sales"]:
        cid = _D["products"].get(s.get("pid",""),{}).get("cid","")
        cr_rev[cid] = cr_rev.get(cid,0)+s.get("amt",0)
    txt = (f"📊 *Sales Report*\n━━━━━━━━━━━━━━\n"
           f"💰 Total: *₹{rev:,.0f}*\n"
           f"📅 Today: *₹{tr:,.0f}*\n"
           f"🛒 Orders: *{len(_D['sales'])}*\n\n"
           f"*Top Creators:*\n")
    for cid,amt in sorted(cr_rev.items(),key=lambda x:-x[1])[:5]:
        c = _D["creators"].get(cid,{})
        txt += f"• {esc(c.get('name','?'))}  ₹{amt:,.0f}\n"
    await send(update,txt,back("adm:home"))

async def _admin_analytics(update: Update):
    # Last 7 days revenue
    days_data = {}
    for i in range(7):
        day = (datetime.now()-timedelta(days=i)).strftime("%d %b")
        days_data[day] = sum(s.get("amt",0) for s in _D["sales"] if day in s.get("ts",""))
    txt = "📈 *Analytics — Last 7 Days*\n━━━━━━━━━━━━━━━━━━\n"
    max_val = max(days_data.values()) if days_data else 1
    for day, amt in list(days_data.items())[:7]:
        bars = int((amt/max_val)*10) if max_val>0 else 0
        txt += f"{day}: {'▓'*bars}{'░'*(10-bars)} ₹{amt:,.0f}\n"
    txt += f"\n*Total Users:* {len(_D['users'])}\n"
    txt += f"*Active Creators:* {sum(1 for c in _D['creators'].values() if c.get('ps') in ('active','trial'))}\n"
    txt += f"*Total Products:* {sum(1 for p in _D['products'].values() if p.get('active'))}\n"
    txt += f"*Total Purchases:* {sum(1 for p in _D['purchases'].values() if p.get('ok'))}"
    await send(update,txt,back("adm:home"))

async def _admin_withdrawals(update: Update):
    pend = [(i,w) for i,w in enumerate(_D["withdrawals"]) if w.get("status")=="pending"]
    if not pend: await send(update,"✅ No pending withdrawals!",back("adm:home")); return
    txt = f"💸 *Pending Withdrawals ({len(pend)})*\n\n"; rows = []
    for idx,w in pend:
        c = _D["creators"].get(w.get("cid",""),{})
        txt += f"• *{esc(c.get('name','?'))}* — ₹{w['amount']:.0f}\nUPI: {esc(w['upi'])}\n\n"
        rows.append([ib(f"✅ Approve ₹{w['amount']:.0f}",f"adm:wd_apv|{idx}"),
                     ib("❌ Reject",f"adm:wd_rjt|{idx}")])
    rows.append([ib("🔙 Back","adm:home")]); await send(update,txt,kb(*rows))

async def _approve_creator(update: Update, ctx, cid: str):
    c = _D["creators"].get(cid)
    if not c: await send(update,"Not found!",back("adm:pend")); return
    code = rand_code(c.get("name","")); td = _C.get("trial_days",3)
    _D["creators"][cid].update({"code":code,"ps":"trial","trial_status":True,"trial_exp":exp_str(td)})
    _save()
    link = f"https://t.me/{BOT_USER}?start={code}"
    await send(update,f"✅ *Approved!*\nCode: `{code}`\nLink: {link}",back("adm:pend"))
    try:
        await ctx.bot.send_message(int(cid),
            f"🎉 *You're Approved!*\n━━━━━━━━━━━━━━\n"
            f"Code: `{code}`\nStore: {link}\n\n"
            f"🔥 *{td}-day FREE trial* starts now!\nUse /dashboard 🚀",
            parse_mode=MD,reply_markup=KB_CREATOR)
        asyncio.create_task(_set_cmds(ctx.bot,int(cid),"creator"))
    except: pass

async def _do_backup(bot):
    try:
        await bot.send_document(ADMIN_ID,
            open(DATA_F,"rb"),filename=f"backup_{datetime.now().strftime('%Y%m%d_%H%M')}.json",
            caption=f"💾 *Auto Backup*\n{now()}")
        _D["backups"].append(now()); _save()
    except Exception as e:
        log.error("Backup failed: %s", e)

# ═══════════════════════════════════════════════════════════════
#  COMMAND MENUS
# ═══════════════════════════════════════════════════════════════
_SC = [BotCommand("start","🏠 Home"),BotCommand("myproducts","📦 My Products"),
       BotCommand("wallet","👛 Wallet"),BotCommand("refer","🔗 Refer & Earn"),
       BotCommand("search","🔍 Search"),BotCommand("topcreators","⭐ Top Creators"),
       BotCommand("topproducts","🏆 Top Products"),BotCommand("profile","🏪 Profile"),
       BotCommand("help","❓ Help"),BotCommand("cancel","❌ Cancel")]
_CC = [BotCommand("start","🏠 Home"),BotCommand("dashboard","📊 Dashboard"),
       BotCommand("addproduct","➕ Add Product"),BotCommand("editproduct","✏️ Edit"),
       BotCommand("myproducts","📦 Products"),BotCommand("renewpanel","🔄 Renew"),
       BotCommand("createcoupon","🎟 Coupon"),BotCommand("broadcast","📣 Broadcast"),
       BotCommand("boostproduct","⚡ Boost"),BotCommand("scheduleclass","🔔 Live Class"),
       BotCommand("wallet","👛 Wallet"),BotCommand("help","❓ Help"),BotCommand("cancel","❌ Cancel")]
_AC = [BotCommand("start","🏠 Home"),BotCommand("adminpanel","👑 Admin"),
       BotCommand("approve_creator","✅ Approve"),BotCommand("verifycreator","🏅 Verify"),
       BotCommand("dashboard","📊 Dashboard"),BotCommand("addproduct","➕ Add"),
       BotCommand("editproduct","✏️ Edit"),BotCommand("renewpanel","🔄 Renew"),
       BotCommand("createcoupon","🎟 Coupon"),BotCommand("broadcast","📣 Broadcast"),
       BotCommand("exportdata","📤 Export"),BotCommand("cancel","❌ Cancel")]

async def _set_cmds(bot, uid: int, role: str):
    try:
        cmds = _AC if role=="super_admin" else _CC if role=="creator" else _SC
        await bot.set_my_commands(cmds,scope=BotCommandScopeChat(chat_id=uid))
    except: pass

# ═══════════════════════════════════════════════════════════════
#  FSM HANDLERS
# ═══════════════════════════════════════════════════════════════

# REGISTRATION
async def fsm_reg_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if update.callback_query: await update.callback_query.answer()
    if uid in _D["creators"]:
        st = _D["creators"][uid].get("ps","?")
        await send(update,f"Account exists. Status: *{st}*",back()); return ConversationHandler.END
    await send(update,"🎉 *Register — Step 1/4*\n\nEnter your *creator/brand name*:")
    return S_RN

async def fsm_rn(u, ctx):
    n = u.message.text.strip()
    if not 2<=len(n)<=50: await u.message.reply_text("❌ 2–50 chars:"); return S_RN
    ctx.user_data["rn"]=n
    await u.message.reply_text("📂 *Step 2/4 — Category:*",parse_mode=MD,reply_markup=cats_kb())
    return S_RC

async def fsm_rc(u, ctx):
    await u.callback_query.answer()
    ctx.user_data["rc"]=u.callback_query.data[4:]
    await u.callback_query.edit_message_text("📝 *Step 3/4 — Short bio* (max 200 chars):",parse_mode=MD)
    return S_RB

async def fsm_rb(u, ctx):
    ctx.user_data["rb"]=u.message.text.strip()[:200]
    await u.message.reply_text("💳 *Step 4/4 — Payment Method:*",parse_mode=MD,
        reply_markup=kb([ib("🔤 Enter UPI ID","rupi:txt")],[ib("📷 Upload QR","rupi:qr")]))
    return S_RU

async def fsm_ru(u, ctx):
    await u.callback_query.answer()
    if u.callback_query.data=="rupi:txt":
        await u.callback_query.edit_message_text("Enter UPI ID (e.g. name@upi):")
        return S_RI
    await u.callback_query.edit_message_text("📷 Send UPI QR image:")
    return S_RQ

async def fsm_ri(u, ctx):
    upi=u.message.text.strip()
    if "@" not in upi: await u.message.reply_text("❌ Invalid UPI:"); return S_RI
    ctx.user_data.update({"rupi":upi,"rqr":None}); return await _save_reg(u,ctx)

async def fsm_rq(u, ctx):
    if not u.message.photo: await u.message.reply_text("❌ Send image!"); return S_RQ
    ctx.user_data.update({"rqr":u.message.photo[-1].file_id,"rupi":None}); return await _save_reg(u,ctx)

async def _save_reg(u, ctx):
    uid=str(u.effective_user.id); track_user(u.effective_user)
    _D["creators"][uid]={"name":ctx.user_data["rn"],"code":None,"cat":ctx.user_data.get("rc",""),
        "bio":ctx.user_data.get("rb",""),"upi":ctx.user_data.get("rupi",""),"qr":ctx.user_data.get("rqr"),
        "ps":"pending","plan":"basic","panel_exp":None,"trial_exp":None,"trial_status":False,
        "mode":"manual","wallet":0.0,"sales":0.0,"ver":False,"thumb":None}
    _D["users"][uid]["role"]="creator"; _save()
    await u.message.reply_text(
        "✅ *Submitted!* Admin reviews within 24 hours. 🎉\nYou'll get your store link once approved!",
        parse_mode=MD,reply_markup=kb([ib("🏪 Browse Marketplace","mkt:home")]))
    try:
        await ctx.bot.send_message(ADMIN_ID,
            f"🆕 *New Creator Application!*\n*{esc(ctx.user_data['rn'])}* `{uid}`\n"
            f"📂 {esc(ctx.user_data.get('rc',''))}  UPI: {esc(ctx.user_data.get('rupi','QR'))}\n"
            f"📝 _{esc(ctx.user_data.get('rb',''))}_",parse_mode=MD,
            reply_markup=kb([ib("✅ Approve",f"adm:apv|{uid}"),ib("❌ Reject",f"adm:rjt|{uid}")]))
    except: pass
    return ConversationHandler.END

# ADD PRODUCT
async def fsm_addp_start(u, ctx):
    uid=str(u.effective_user.id)
    if u.callback_query: await u.callback_query.answer()
    if not is_creator(uid): await send(u,"❌ Not a creator.",back()); return ConversationHandler.END
    if not panel_active(uid): await send(u,"❌ Panel not active.",kb([ib("🔄 Renew","cr:renew")])); return ConversationHandler.END
    cnt,lim=prod_count(uid),prod_limit(uid)
    if cnt>=lim and not is_admin(uid):
        await send(u,f"❌ Limit {cnt}/{lim}. Upgrade plan.",kb([ib("⬆️ Upgrade","cr:renew")],[ib("🔙 Back","cr:dash")])); return ConversationHandler.END
    await send(u,f"➕ *Add Product* ({cnt}/{lim})\n\nEnter *product name*:")
    return S_PN

async def fsm_pn(u,ctx):
    ctx.user_data["pn"]=u.message.text.strip()
    await u.message.reply_text("💰 Price in ₹ (0 = free):",parse_mode=MD); return S_PP

async def fsm_pp(u,ctx):
    try: ctx.user_data["pp"]=float(u.message.text.strip())
    except: await u.message.reply_text("❌ Enter number:"); return S_PP
    await u.message.reply_text("📂 Delivery type:",parse_mode=MD,reply_markup=dtype_kb()); return S_PD

async def fsm_pd(u,ctx):
    await u.callback_query.answer(); ctx.user_data["pd"]=u.callback_query.data[3:]
    await u.callback_query.edit_message_text("🔗 Enter access link or value:"); return S_PL

async def fsm_pl(u,ctx):
    ctx.user_data["pl"]=u.message.text.strip()
    await u.message.reply_text("⏳ Access duration:",parse_mode=MD,reply_markup=dur_kb()); return S_PV

async def fsm_pv(u,ctx):
    await u.callback_query.answer(); ctx.user_data["pv"]=u.callback_query.data[4:]
    await u.callback_query.edit_message_text(
        "🔄 *Subscription product?*",parse_mode=MD,
        reply_markup=kb([ib("✅ Yes — Monthly","psub|y"),ib("❌ No — One-Time","psub|n")])); return S_PS

async def fsm_ps(u,ctx):
    await u.callback_query.answer(); sub=u.callback_query.data=="psub|y"
    ctx.user_data["ps_sub"]=sub
    await u.callback_query.edit_message_text(
        "🖼️ *Add a product thumbnail?* (Helps with sales!)",
        reply_markup=kb([ib("📷 Upload Image","pthumb|y"),ib("⏭ Skip","pthumb|n")])); return S_PI

async def fsm_pi_choice(u,ctx):
    await u.callback_query.answer()
    if u.callback_query.data=="pthumb|y":
        await u.callback_query.edit_message_text("📷 Send product image:"); return S_PI
    return await _save_product(u.callback_query,ctx,None)

async def fsm_pi_photo(u,ctx):
    thumb=u.message.photo[-1].file_id if u.message.photo else None
    return await _save_product(u.message,ctx,thumb)

async def _save_product(msg,ctx,thumb):
    uid=str(msg.from_user.id) if hasattr(msg,"from_user") else str(msg.chat.id)
    pid=rand_pid()
    _D["products"][pid]={"cid":uid,"name":ctx.user_data["pn"],"price":ctx.user_data["pp"],
        "dtype":ctx.user_data["pd"],"link":ctx.user_data["pl"],"dur":ctx.user_data["pv"],
        "sub":ctx.user_data.get("ps_sub",False),"rating":0.0,"students":0,
        "ts":now(),"active":True,"thumb":thumb}
    _save()
    text=f"✅ *Product Added!*\n*{esc(ctx.user_data['pn'])}*  ₹{ctx.user_data['pp']:.0f}"
    mkp=kb([ib("📦 Products","cr:prods"),ib("➕ Add Another","cr:addp")],[ib("📊 Dashboard","cr:dash")])
    if hasattr(msg,"edit_message_text"):
        await msg.edit_message_text(text,parse_mode=MD,reply_markup=mkp)
    else:
        await msg.reply_text(text,parse_mode=MD,reply_markup=mkp)
    return ConversationHandler.END

# EDIT PRODUCT
async def fsm_editp_start(u,ctx):
    uid=str(u.effective_user.id)
    if u.callback_query: await u.callback_query.answer()
    if not is_creator(uid): await send(u,"❌ Not a creator.",back()); return ConversationHandler.END
    prods=[(pid,p) for pid,p in _D["products"].items() if p.get("cid")==uid and p.get("active")]
    if not prods: await send(u,"No products!",back("cr:dash")); return ConversationHandler.END
    rows=[[ib(f"✏️ {p['name'][:28]}  ₹{p['price']:.0f}",f"epc|{pid}")] for pid,p in prods]
    rows.append([ib("🔙 Dashboard","cr:dash")]); await send(u,"✏️ *Select product:*",kb(*rows)); return S_EP

async def fsm_ep(u,ctx):
    await u.callback_query.answer(); pid=u.callback_query.data[4:]
    ctx.user_data["edit_pid"]=pid; p=_D["products"].get(pid,{})
    await send(u,f"✏️ *{esc(p.get('name',''))}* — What to change?",
        kb([ib("📝 Name",f"ef|name"),ib("💰 Price",f"ef|price")],
           [ib("🔗 Link",f"ef|link"),ib("⏳ Duration",f"ef|dur")],
           [ib("🖼️ Thumbnail",f"ef|thumb"),ib("🔙 Cancel","cr:prods")])); return S_EF

async def fsm_ef(u,ctx):
    await u.callback_query.answer(); field=u.callback_query.data[3:]
    ctx.user_data["edit_field"]=field
    if field=="thumb":
        await u.callback_query.edit_message_text("📷 Send new product thumbnail image:"); return S_EV
    labels={"name":"product name","price":"new price ₹","link":"new access link","dur":"duration"}
    await u.callback_query.edit_message_text(f"✏️ Enter new *{labels.get(field,field)}*:",parse_mode=MD); return S_EV

async def fsm_ev(u,ctx):
    pid=ctx.user_data.get("edit_pid",""); field=ctx.user_data.get("edit_field","")
    if pid not in _D["products"]: await u.message.reply_text("Not found!"); return ConversationHandler.END
    if field=="thumb":
        if u.message.photo:
            _D["products"][pid]["thumb"]=u.message.photo[-1].file_id
        else: await u.message.reply_text("❌ Send image!"); return S_EV
    else:
        val=u.message.text.strip()
        if field=="price":
            try: val=float(val)
            except: await u.message.reply_text("❌ Enter number:"); return S_EV
            _D["products"][pid]["price"]=val
        elif field=="name": _D["products"][pid]["name"]=val
        elif field=="link": _D["products"][pid]["link"]=val
        elif field=="dur":  _D["products"][pid]["dur"]=val
    _save()
    await u.message.reply_text("✅ *Updated!*",parse_mode=MD,reply_markup=back(f"cr:mgp|{pid}"))
    return ConversationHandler.END

# RENEW PANEL
async def fsm_plan_pick(u,ctx):
    await u.callback_query.answer(); plan=u.callback_query.data[5:]
    if plan not in _C["plans"]: await alert(u,"Invalid!"); return ConversationHandler.END
    price=_C["plans"][plan]["price"]; lim=_C["plans"][plan]["limit"]
    ctx.user_data["renew_plan"]=plan
    await u.callback_query.edit_message_text(
        f"💳 *{plan.title()} Plan — ₹{price}/mo*\nProducts: *{lim}* | +30 days\n\n"
        f"Pay to: `{PAY_UPI}`\n\nSend *UTR* after paying:",parse_mode=MD)
    return S_RUTR

async def fsm_rutr(u,ctx):
    uid=str(u.effective_user.id); utr=u.message.text.strip().replace(" ","")
    if not valid_utr(utr): await u.message.reply_text("❌ Invalid UTR:"); return S_RUTR
    if utr_used(utr): await u.message.reply_text("❌ UTR already used!"); return S_RUTR
    plan=ctx.user_data.get("renew_plan","basic")
    _D["utr_log"].append(utr.upper()); _D["pan_pend"][uid]={"plan":plan,"utr":utr.upper(),"ts":now()}; _save()
    await u.message.reply_text("✅ *UTR Submitted!* Admin will verify & activate shortly. 🎉",parse_mode=MD)
    try:
        price=_C["plans"].get(plan,{}).get("price",199)
        await u.get_bot().send_message(ADMIN_ID,
            f"💳 *Panel Renewal*\nCreator: `{uid}`\nPlan: *{plan.title()}*\n"
            f"₹{price}  UTR: `{utr}`\nUPI: {PAY_UPI}",parse_mode=MD,
            reply_markup=kb([ib(f"✅ Approve {plan.title()}",f"adm:rnw|{uid}|{plan}"),ib("❌ Reject",f"adm:rjt|{uid}")]))
    except: pass
    return ConversationHandler.END

# COUPON
async def fsm_cpn_start(u,ctx):
    uid=str(u.effective_user.id)
    if u.callback_query: await u.callback_query.answer()
    if not is_creator(uid) and not is_admin(uid): await send(u,"❌ Not a creator.",back()); return ConversationHandler.END
    await send(u,"🎟 *Create Coupon — Step 1/4*\n\nEnter code (blank = auto):"); return S_VN

async def fsm_vn(u,ctx):
    ctx.user_data["vn"]=u.message.text.strip().upper() or rand_cpn()
    await u.message.reply_text(f"Code: `{ctx.user_data['vn']}`\n\n💰 Discount:\n`20` = 20% off  |  `F50` = ₹50 flat",parse_mode=MD); return S_VD

async def fsm_vd(u,ctx):
    t=u.message.text.strip()
    try:
        if t.upper().startswith("F"): ctx.user_data.update({"vt":"flat","vfl":float(t[1:]),"vpc":0})
        else: ctx.user_data.update({"vt":"pct","vpc":float(t),"vfl":0})
    except: await u.message.reply_text("❌ Try `20` or `F50`:"); return S_VD
    await u.message.reply_text("📅 Expiry in days (0=never):"); return S_VX

async def fsm_vx(u,ctx):
    try: d=int(u.message.text.strip())
    except: await u.message.reply_text("Enter a number:"); return S_VX
    ctx.user_data["vx"]=exp_str(d) if d>0 else None
    await u.message.reply_text("🔢 Max uses:"); return S_VM

async def fsm_vm(u,ctx):
    try: mx=int(u.message.text.strip())
    except: await u.message.reply_text("Enter a number:"); return S_VM
    code=ctx.user_data["vn"]
    _C["coupons"][code]={"type":ctx.user_data["vt"],"pct":ctx.user_data["vpc"],"flat":ctx.user_data["vfl"],"max":mx,"used":0,"exp":ctx.user_data.get("vx")}
    _savec()
    dl=f"{ctx.user_data['vpc']}%" if ctx.user_data["vt"]=="pct" else f"₹{ctx.user_data['vfl']:.0f}"
    await u.message.reply_text(f"✅ *Coupon Created!*\nCode: `{code}`\nDiscount: *{dl}*\nMax: *{mx}*",parse_mode=MD,reply_markup=back("cr:dash"))
    return ConversationHandler.END

# BROADCAST
async def fsm_bc_start(u,ctx):
    uid=str(u.effective_user.id)
    if u.callback_query: await u.callback_query.answer()
    if not is_creator(uid) and not is_admin(uid): await send(u,"❌ Not a creator.",back()); return ConversationHandler.END
    await send(u,"📣 *Broadcast*\n\nWho to message?",kb([ib("👥 All My Students","bct|all")],[ib("❌ Cancel","cr:dash")])); return S_BT

async def fsm_bt(u,ctx):
    await u.callback_query.answer(); ctx.user_data["bt"]=u.callback_query.data[4:]
    await u.callback_query.edit_message_text("✍️ Type your message:"); return S_BM

async def fsm_bm(u,ctx):
    uid=str(u.effective_user.id); msg=u.message.text
    sids=list({x.get("uid") for x in _D["purchases"].values()
               if _D["products"].get(x.get("pid",""),{}).get("cid")==uid and x.get("ok")})
    sent=0
    for sid in sids:
        try: await u.get_bot().send_message(int(sid),f"📣 *Message from your creator:*\n\n{msg}",parse_mode=MD); sent+=1
        except: pass
    _D["broadcasts"].append({"cid":uid,"msg":msg,"ts":now()}); _save()
    await u.message.reply_text(f"✅ *Sent to {sent}/{len(sids)} students!*",parse_mode=MD,reply_markup=back("cr:dash"))
    return ConversationHandler.END

# SEARCH
async def fsm_srch_start(u,ctx):
    if u.callback_query: await u.callback_query.answer()
    await send(u,"🔍 *Search Products*\n\nType keyword:"); return S_SQ

async def fsm_sq(u,ctx):
    q=u.message.text.strip().lower()
    prods=[(pid,p) for pid,p in _D["products"].items()
           if p.get("active") and q in p.get("name","").lower()]
    if not prods: await u.message.reply_text(f"❌ No results for *'{esc(q)}'*",parse_mode=MD,reply_markup=back("mkt:home")); return ConversationHandler.END
    rows=[[ib(f"🛒 {p['name'][:28]}  ₹{get_effective_price(pid):.0f}",f"vp|{pid}")] for pid,p in prods[:10]]
    rows.append([ib("🔙 Back","mkt:home")]); await u.message.reply_text(f"🔍 *{len(prods)} result(s)*",parse_mode=MD,reply_markup=kb(*rows))
    return ConversationHandler.END

# LIVE CLASS
async def fsm_lcls_start(u,ctx):
    uid=str(u.effective_user.id)
    if u.callback_query: await u.callback_query.answer()
    if not is_creator(uid) and not is_admin(uid): await send(u,"❌ Not a creator.",back()); return ConversationHandler.END
    await send(u,"🔔 *Schedule Live Class — Step 1/2*\n\nEnter class title:"); return S_LT

async def fsm_lt(u,ctx):
    ctx.user_data["lt"]=u.message.text.strip()
    await u.message.reply_text("📅 *Date & time:*\n`YYYY-MM-DD HH:MM`",parse_mode=MD); return S_LD

async def fsm_ld(u,ctx):
    txt=u.message.text.strip()
    try: datetime.strptime(txt,"%Y-%m-%d %H:%M")
    except: await u.message.reply_text("❌ Format: `YYYY-MM-DD HH:MM`"); return S_LD
    uid=str(u.effective_user.id)
    _D["classes"].append({"id":rand_pid(),"cid":uid,"title":ctx.user_data["lt"],"at":txt,"r15":False,"r5":False}); _save()
    await u.message.reply_text(f"✅ *Scheduled!*\n📚 {esc(ctx.user_data['lt'])}\n⏰ {txt}\nStudents get 15min & 5min reminders!",parse_mode=MD,reply_markup=back("cr:dash"))
    return ConversationHandler.END

# ADMIN BROADCAST
async def fsm_ab(u,ctx):
    msg=u.message.text; uids=list(_D["users"].keys()); sent=0
    for uid in uids:
        try: await u.get_bot().send_message(int(uid),f"📣 *Platform Announcement:*\n\n{msg}",parse_mode=MD); sent+=1
        except: pass
    _D["broadcasts"].append({"cid":"ADMIN","msg":msg,"ts":now()}); _save()
    await u.message.reply_text(f"✅ Sent to {sent}/{len(uids)} users.")
    return ConversationHandler.END

# BOOST UTR
async def fsm_bst_upi(u,ctx):
    await u.callback_query.answer()
    cost=_C.get("boost_cost",29)
    await u.callback_query.edit_message_text(f"💳 Pay *₹{cost}* to:\n`{PAY_UPI}`\n\nSend UTR after payment:",parse_mode=MD); return S_BU

async def fsm_bu(u,ctx):
    utr=u.message.text.strip().replace(" ",""); uid=str(u.effective_user.id)
    if not valid_utr(utr): await u.message.reply_text("❌ Invalid UTR:"); return S_BU
    if utr_used(utr): await u.message.reply_text("❌ Already used!"); return S_BU
    pid=ctx.user_data.get("boost_pid",""); _D["utr_log"].append(utr.upper()); _save()
    try:
        await u.get_bot().send_message(ADMIN_ID,
            f"⚡ *Boost Payment*\nCreator: `{uid}`\nProduct: `{pid}`\nUTR: `{utr}`",parse_mode=MD,
            reply_markup=kb([ib("✅ Approve",f"adm:bst|{uid}|{pid}"),ib("❌ Reject","home")]))
    except: pass
    await u.message.reply_text("✅ UTR submitted! Boost activates after admin approval.")
    return ConversationHandler.END

# WITHDRAWAL
async def fsm_wd_amt(u,ctx):
    uid=str(u.effective_user.id)
    try: amt=float(u.message.text.strip())
    except: await u.message.reply_text("❌ Enter amount:"); return S_WD_AMT
    if amt < 100: await u.message.reply_text("❌ Minimum ₹100!"); return S_WD_AMT
    bal=_D["creators"].get(uid,{}).get("wallet",0.0)
    if amt>bal: await u.message.reply_text(f"❌ Insufficient. Balance: ₹{bal:.0f}"); return S_WD_AMT
    upi=_D["creators"].get(uid,{}).get("upi","")
    _D["withdrawals"].append({"cid":uid,"amount":amt,"upi":upi,"ts":now(),"status":"pending"}); _save()
    await u.message.reply_text(
        f"✅ *Withdrawal Request Submitted!*\nAmount: ₹{amt:.0f}\nUPI: {esc(upi)}\n\nAdmin will process within 24 hours.",
        parse_mode=MD,reply_markup=back("cr:wlt"))
    try:
        c=_D["creators"].get(uid,{})
        await u.get_bot().send_message(ADMIN_ID,
            f"💸 *Withdrawal Request*\nCreator: {esc(c.get('name','?'))} `{uid}`\nAmount: ₹{amt:.0f}\nUPI: {esc(upi)}",
            parse_mode=MD,
            reply_markup=kb([ib(f"✅ Approve ₹{amt:.0f}",f"adm:wd_apv|{len(_D['withdrawals'])-1}"),ib("❌ Reject","home")]))
    except: pass
    return ConversationHandler.END

# FLASH SALE
async def fsm_fs_disc(u,ctx):
    try: pct=float(u.message.text.strip())
    except: await u.message.reply_text("❌ Enter %:"); return S_FS_DISC
    if not 1<=pct<=90: await u.message.reply_text("❌ 1–90%:"); return S_FS_DISC
    ctx.user_data["fs_disc"]=pct
    await u.message.reply_text("⏰ Duration in hours (e.g. 24 for 24 hours):")
    return S_FS_DUR

async def fsm_fs_dur(u,ctx):
    try: hrs=float(u.message.text.strip())
    except: await u.message.reply_text("❌ Enter hours:"); return S_FS_DUR
    pid=ctx.user_data.get("flash_pid",""); pct=ctx.user_data.get("fs_disc",10)
    p=_D["products"].get(pid,{})
    ends=(datetime.now()+timedelta(hours=hrs)).strftime("%Y-%m-%d %H:%M")
    _D["flash_sales"][pid]={"disc_pct":pct,"ends_at":ends}; _save()
    ep=get_effective_price(pid)
    await u.message.reply_text(
        f"✅ *Flash Sale Active!*\n📦 {esc(p.get('name',''))}\n"
        f"🔥 {pct}% OFF  |  ₹{p.get('price',0):.0f} → ₹{ep:.0f}\n"
        f"⏰ Ends: {ends}",parse_mode=MD,reply_markup=back("cr:flash"))
    return ConversationHandler.END

async def do_cancel(u,ctx):
    if u.callback_query: await u.callback_query.answer()
    await send(u,"❌ Cancelled.",back()); return ConversationHandler.END

# ═══════════════════════════════════════════════════════════════
#  COMMANDS
# ═══════════════════════════════════════════════════════════════
async def cmd_dashboard(u,c):   track_user(u.effective_user); await _creator_dashboard(u,str(u.effective_user.id))
async def cmd_myproducts(u,c):  track_user(u.effective_user); await _my_products(u,str(u.effective_user.id))
async def cmd_wallet(u,c):      track_user(u.effective_user); await _wallet(u,str(u.effective_user.id))
async def cmd_refer(u,c):       track_user(u.effective_user); await _refer(u,str(u.effective_user.id))
async def cmd_help(u,c):        track_user(u.effective_user); await _help(u,str(u.effective_user.id))

async def cmd_topcreators(u,c):
    track_user(u.effective_user)
    top=sorted([(cid,cr) for cid,cr in _D["creators"].items() if cr.get("ps") in ("active","trial")],key=lambda x:-x[1].get("sales",0))[:10]
    if not top: await u.message.reply_text("No creators yet!"); return
    txt="⭐ *Top Creators*\n\n"
    for i,(cid,cr) in enumerate(top,1):
        txt+=f"{i}. *{esc(cr['name'])}* {'✅' if cr.get('ver') else ''}  ₹{cr.get('sales',0):,.0f}\n"
    await u.message.reply_text(txt,parse_mode=MD)

async def cmd_topproducts(u,c):
    track_user(u.effective_user)
    top=sorted([(pid,p) for pid,p in _D["products"].items() if p.get("active")],key=lambda x:(-x[1].get("students",0),-x[1].get("rating",0)))[:10]
    if not top: await u.message.reply_text("No products yet!"); return
    txt="🏆 *Top Products*\n\n"
    for i,(pid,p) in enumerate(top,1):
        txt+=f"{i}. *{esc(p['name'])}*  ₹{p['price']:.0f}  👥{p.get('students',0)}\n"
    await u.message.reply_text(txt,parse_mode=MD)

async def cmd_profile(u,c):
    track_user(u.effective_user)
    code=c.args[0].upper() if c.args else ""
    if not code: await u.message.reply_text("Usage: /profile CREATORCODE"); return
    match=next(((cid,cr) for cid,cr in _D["creators"].items() if cr.get("code","").upper()==code),None)
    if not match: await u.message.reply_text("❌ Not found!"); return
    await _show_store(u,match[0])

async def cmd_adminpanel(u,c):
    if not is_admin(str(u.effective_user.id)): await u.message.reply_text("⛔"); return
    await _admin_home(u)

async def cmd_approve_creator(u,c):
    if not is_admin(str(u.effective_user.id)): return
    if not c.args: await u.message.reply_text("Usage: /approve\\_creator ID",parse_mode=MD); return
    await _approve_creator(u,c,c.args[0])

async def cmd_verifycreator(u,c):
    if not is_admin(str(u.effective_user.id)): return
    if not c.args: await u.message.reply_text("Usage: /verifycreator ID"); return
    cid=c.args[0]
    if cid not in _D["creators"]: await u.message.reply_text("Not found!"); return
    new=not _D["creators"][cid].get("ver",False)
    _D["creators"][cid]["ver"]=new; _save()
    await u.message.reply_text(f"{'✅ Badge granted' if new else '❌ Badge removed'}")

async def cmd_renewpanel(u,c):
    uid=str(u.effective_user.id); track_user(u.effective_user)
    if not is_creator(uid): await u.message.reply_text("❌ Not a creator."); return
    cr=_D["creators"].get(uid,{})
    await u.message.reply_text(
        f"🔄 *Renew Panel*\nPlan: *{cr.get('plan','basic').title()}*  Wallet: *₹{cr.get('wallet',0):,.0f}*\n\n💳 Pay to: `{PAY_UPI}`\nSelect plan:",
        parse_mode=MD,reply_markup=plans_kb())

async def cmd_approvalmode(u,c):
    uid=str(u.effective_user.id)
    if not is_creator(uid): await u.message.reply_text("Not a creator!"); return
    await u.message.reply_text("⚙️ Mode:",reply_markup=kb([ib("🤖 Auto","cr:am|auto"),ib("👤 Manual","cr:am|manual")]))

async def cmd_exportdata(u,c):
    if not is_admin(str(u.effective_user.id)): return
    if os.path.exists(DATA_F): await u.message.reply_document(open(DATA_F,"rb"),filename="hub_data.json",caption="📤 Data")

async def cmd_showconfig(u,c):
    if not is_admin(str(u.effective_user.id)): return
    if os.path.exists(CFG_F): await u.message.reply_document(open(CFG_F,"rb"),filename="hub_config.json",caption="⚙️ Config")

# ═══════════════════════════════════════════════════════════════
#  SCHEDULER
# ═══════════════════════════════════════════════════════════════
async def tick(ctx: ContextTypes.DEFAULT_TYPE):
    now_dt  = datetime.now()
    changed = False

    for oid, pur in _D["purchases"].items():
        if not pur.get("ok") or not pur.get("exp"): continue
        dl = days_left(pur["exp"])
        if dl in (3,1):
            prod=_D["products"].get(pur.get("pid",""),{})
            try:
                await ctx.bot.send_message(int(pur["uid"]),
                    f"⚠️ *Access expiring in {dl} day(s)!*\n*{esc(prod.get('name',''))}*",
                    parse_mode=MD,reply_markup=kb([ib("🔄 Renew",f"buy|{pur['pid']}")]))
            except: pass
        if is_expired(pur["exp"]): _D["purchases"][oid]["ok"]=False; _D["purchases"][oid]["expired"]=True; changed=True

    for cid, c in _D["creators"].items():
        if c.get("trial_status") and c.get("trial_exp") and is_expired(c["trial_exp"]):
            _D["creators"][cid]["trial_status"]=False; _D["creators"][cid]["ps"]="trial_expired"; changed=True
            try: await ctx.bot.send_message(int(cid),"⚠️ *Trial ended!* Use /renewpanel.",parse_mode=MD,reply_markup=kb([ib("🔄 Renew","cr:renew")]))
            except: pass
        if not c.get("trial_status") and c.get("panel_exp"):
            dl=days_left(c["panel_exp"])
            if dl in (7,3,1):
                try: await ctx.bot.send_message(int(cid),f"⚠️ Panel expiring in *{dl}d!*",parse_mode=MD,reply_markup=kb([ib("🔄 Renew","cr:renew")]))
                except: pass
            if is_expired(c["panel_exp"]): _D["creators"][cid]["ps"]="expired"; changed=True

    for i, cls in enumerate(_D["classes"]):
        try: cdt=datetime.strptime(cls["at"],"%Y-%m-%d %H:%M")
        except: continue
        mins=(cdt-now_dt).total_seconds()/60
        sids=list({x.get("uid") for x in _D["purchases"].values()
                   if _D["products"].get(x.get("pid",""),{}).get("cid")==cls["cid"] and x.get("ok")})
        if 14<=mins<=15.5 and not cls.get("r15"):
            for sid in sids:
                try: await ctx.bot.send_message(int(sid),f"🔔 *Live in 15 min!*\n📚 {esc(cls['title'])}\n⏰ {cls['at']}",parse_mode=MD)
                except: pass
            _D["classes"][i]["r15"]=True; changed=True
        elif 4<=mins<=5.5 and not cls.get("r5"):
            for sid in sids:
                try: await ctx.bot.send_message(int(sid),f"🚨 *Starting in 5 min!*\n{esc(cls['title'])}",parse_mode=MD)
                except: pass
            _D["classes"][i]["r5"]=True; changed=True

    # Cleanup expired boosts
    for pid in [p for p,e in _D["boosts"].items() if is_expired(e)]:
        del _D["boosts"][pid]; changed=True
    # Cleanup expired flash sales
    for pid in [p for p,fs in _D["flash_sales"].items() if is_expired(fs.get("ends_at",""))]:
        del _D["flash_sales"][pid]; changed=True

    if changed: _save()

# Daily backup at 02:00
async def daily_backup(ctx: ContextTypes.DEFAULT_TYPE):
    await _do_backup(ctx.bot)
    log.info("Daily backup sent to admin")

# ═══════════════════════════════════════════════════════════════
#  POST INIT
# ═══════════════════════════════════════════════════════════════
async def post_init(app):
    await app.bot.set_my_commands(_SC,scope=BotCommandScopeDefault())
    try: await app.bot.set_my_commands(_AC,scope=BotCommandScopeChat(chat_id=ADMIN_ID))
    except: pass
    tasks=[_set_cmds(app.bot,int(cid),"creator") for cid,c in _D["creators"].items()
           if c.get("ps") in ("active","trial")]
    if tasks: await asyncio.gather(*tasks,return_exceptions=True)
    log.info("✅ Bot initialized — commands set for all roles")

# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    _load()

    app=(ApplicationBuilder()
         .token(BOT_TOKEN)
         .concurrent_updates(True)
         .connection_pool_size(16)
         .connect_timeout(10).read_timeout(10).write_timeout(10)
         .get_updates_connect_timeout(10).get_updates_read_timeout(10).get_updates_write_timeout(10)
         .post_init(post_init)
         .build())

    def CH(entries,states):
        return ConversationHandler(entry_points=entries,states=states,
            fallbacks=[CommandHandler("cancel",do_cancel)],
            allow_reentry=True,per_message=False,per_chat=True)

    app.add_handler(CH(
        [CommandHandler("register_creator",fsm_reg_start),CallbackQueryHandler(fsm_reg_start,"^reg:start$")],
        {S_RN:[MessageHandler(filters.TEXT&~filters.COMMAND,fsm_rn)],
         S_RC:[CallbackQueryHandler(fsm_rc,"^cat\\|")],
         S_RB:[MessageHandler(filters.TEXT&~filters.COMMAND,fsm_rb)],
         S_RU:[CallbackQueryHandler(fsm_ru,"^rupi:")],
         S_RI:[MessageHandler(filters.TEXT&~filters.COMMAND,fsm_ri)],
         S_RQ:[MessageHandler(filters.PHOTO,fsm_rq)]}))

    app.add_handler(CH(
        [CommandHandler("addproduct",fsm_addp_start),CallbackQueryHandler(fsm_addp_start,"^cr:addp$")],
        {S_PN:[MessageHandler(filters.TEXT&~filters.COMMAND,fsm_pn)],
         S_PP:[MessageHandler(filters.TEXT&~filters.COMMAND,fsm_pp)],
         S_PD:[CallbackQueryHandler(fsm_pd,"^dt\\|")],
         S_PL:[MessageHandler(filters.TEXT&~filters.COMMAND,fsm_pl)],
         S_PV:[CallbackQueryHandler(fsm_pv,"^dur\\|")],
         S_PS:[CallbackQueryHandler(fsm_ps,"^psub\\|")],
         S_PI:[CallbackQueryHandler(fsm_pi_choice,"^pthumb\\|"),MessageHandler(filters.PHOTO,fsm_pi_photo)]}))

    app.add_handler(CH(
        [CommandHandler("editproduct",fsm_editp_start),CallbackQueryHandler(fsm_editp_start,"^cr:editp$")],
        {S_EP:[CallbackQueryHandler(fsm_ep,"^epc\\|")],
         S_EF:[CallbackQueryHandler(fsm_ef,"^ef\\|")],
         S_EV:[MessageHandler(filters.TEXT&~filters.COMMAND,fsm_ev),MessageHandler(filters.PHOTO,fsm_ev)]}))

    app.add_handler(CH(
        [CallbackQueryHandler(cb_buy_entry,"^buy\\|")],
        {S_CPN:[MessageHandler(filters.TEXT&~filters.COMMAND,fsm_coupon),CallbackQueryHandler(cb_skip_coupon,"^skp\\|")],
         S_UTR:[MessageHandler(filters.TEXT&~filters.COMMAND,fsm_utr)]}))

    app.add_handler(CH(
        [CallbackQueryHandler(fsm_plan_pick,"^plan\\|")],
        {S_RUTR:[MessageHandler(filters.TEXT&~filters.COMMAND,fsm_rutr)]}))

    app.add_handler(CH(
        [CommandHandler("createcoupon",fsm_cpn_start),CallbackQueryHandler(fsm_cpn_start,"^cr:mkcp$")],
        {S_VN:[MessageHandler(filters.TEXT&~filters.COMMAND,fsm_vn)],
         S_VD:[MessageHandler(filters.TEXT&~filters.COMMAND,fsm_vd)],
         S_VX:[MessageHandler(filters.TEXT&~filters.COMMAND,fsm_vx)],
         S_VM:[MessageHandler(filters.TEXT&~filters.COMMAND,fsm_vm)]}))

    app.add_handler(CH(
        [CommandHandler("broadcast",fsm_bc_start),CallbackQueryHandler(fsm_bc_start,"^cr:bc$")],
        {S_BT:[CallbackQueryHandler(fsm_bt,"^bct\\|")],
         S_BM:[MessageHandler(filters.TEXT&~filters.COMMAND,fsm_bm)]}))

    app.add_handler(CH(
        [CommandHandler("search",fsm_srch_start),CallbackQueryHandler(fsm_srch_start,"^mkt:srch$")],
        {S_SQ:[MessageHandler(filters.TEXT&~filters.COMMAND,fsm_sq)]}))

    app.add_handler(CH(
        [CommandHandler("scheduleclass",fsm_lcls_start),CallbackQueryHandler(fsm_lcls_start,"^cr:lcls$")],
        {S_LT:[MessageHandler(filters.TEXT&~filters.COMMAND,fsm_lt)],
         S_LD:[MessageHandler(filters.TEXT&~filters.COMMAND,fsm_ld)]}))

    app.add_handler(CH(
        [CallbackQueryHandler(fsm_adm_bc_entry,"^adm:bc$")],
        {S_AB:[MessageHandler(filters.TEXT&~filters.COMMAND,fsm_ab)]}))

    app.add_handler(CH(
        [CallbackQueryHandler(fsm_bst_upi,"^bst:upi$")],
        {S_BU:[MessageHandler(filters.TEXT&~filters.COMMAND,fsm_bu)]}))

    app.add_handler(CH(
        [CallbackQueryHandler(fsm_wd_entry,"^cr:withdraw$")],
        {S_WD_AMT:[MessageHandler(filters.TEXT&~filters.COMMAND,fsm_wd_amt)]}))

    app.add_handler(CH(
        [CallbackQueryHandler(fsm_flash_entry,"^flash\\|")],
        {S_FS_DISC:[MessageHandler(filters.TEXT&~filters.COMMAND,fsm_fs_disc)],
         S_FS_DUR:[MessageHandler(filters.TEXT&~filters.COMMAND,fsm_fs_dur)]}))

    app.add_handler(CallbackQueryHandler(cb_router))

    for name,fn in [
        ("start",cmd_start),("dashboard",cmd_dashboard),("myproducts",cmd_myproducts),
        ("wallet",cmd_wallet),("refer",cmd_refer),("help",cmd_help),("profile",cmd_profile),
        ("topcreators",cmd_topcreators),("topproducts",cmd_topproducts),
        ("renewpanel",cmd_renewpanel),("approvalmode",cmd_approvalmode),
        ("adminpanel",cmd_adminpanel),("approve_creator",cmd_approve_creator),
        ("verifycreator",cmd_verifycreator),("createcoupon",fsm_cpn_start),
        ("broadcast",fsm_bc_start),("addproduct",fsm_addp_start),("editproduct",fsm_editp_start),
        ("scheduleclass",fsm_lcls_start),("exportdata",cmd_exportdata),
        ("showconfig",cmd_showconfig),("cancel",do_cancel),
    ]: app.add_handler(CommandHandler(name,fn))

    from telegram.ext import JobQueue
    app.job_queue.run_repeating(tick, interval=60, first=20)
    app.job_queue.run_daily(daily_backup, time=datetime.strptime("02:00","%H:%M").time())

    print(f"🚀 UPI Access Hub v6.0 — Railway Production — LIVE!")
    print(f"💳 Platform UPI: {PAY_UPI}")
    print(f"📁 Data: {DATA_F}")
    app.run_polling(drop_pending_updates=True,allowed_updates=["message","callback_query"])

# ── FSM entry wrappers (needed for CH entry points that also handle callbacks)
async def fsm_adm_bc_entry(u,ctx):
    await u.callback_query.answer()
    if not is_admin(str(u.effective_user.id)): return ConversationHandler.END
    await send(u,"📣 *Admin Broadcast*\n\nType message to ALL users:"); return S_AB

async def fsm_wd_entry(u,ctx):
    await u.callback_query.answer()
    uid=str(u.effective_user.id)
    c=_D["creators"].get(uid,{})
    bal=c.get("wallet",0.0)
    if bal<100: await alert(u,f"Min ₹100. Balance: ₹{bal:.0f}"); return ConversationHandler.END
    await send(u,f"💸 *Withdrawal*\nWallet: *₹{bal:,.2f}*\nUPI: `{esc(c.get('upi',''))}`\n\nEnter amount (min ₹100):")
    return S_WD_AMT

async def fsm_flash_entry(u,ctx):
    await u.callback_query.answer()
    pid=u.callback_query.data[6:]
    p=_D["products"].get(pid)
    if not p: return ConversationHandler.END
    uid=str(u.effective_user.id)
    if p.get("cid")!=uid and not is_admin(uid): return ConversationHandler.END
    fs=_D["flash_sales"].get(pid)
    if fs and not is_expired(fs.get("ends_at","")):
        await send(u,f"⚡ Flash Sale already active!\n{fs['disc_pct']}% off until {fs['ends_at']}",
            kb([ib("🗑 Cancel Flash Sale",f"flash:cancel|{pid}")],[ib("🔙 Back","cr:flash")]))
        return ConversationHandler.END
    ctx.user_data["flash_pid"]=pid
    await send(u,f"⚡ *Flash Sale for {esc(p['name'])}*\nOriginal: ₹{p['price']:.0f}\n\nEnter discount % (1-90):")
    return S_FS_DISC

if __name__=="__main__":
    main()
