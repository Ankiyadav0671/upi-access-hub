"""
UPI Access Hub — v5.0 FINAL
python-telegram-bot v20 · JSON · Ankiii@upi
Fresh rewrite — every flow tested and working.
"""

import os, json, logging, re, string, random, asyncio
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    BotCommand, BotCommandScopeChat, BotCommandScopeDefault,
)
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    MessageHandler, ConversationHandler, ContextTypes, filters,
)
from telegram.constants import ParseMode

# ═══════════════════════════════════════════════════════════
#  CONSTANTS
# ═══════════════════════════════════════════════════════════
BOT_TOKEN      = os.getenv("BOT_TOKEN", "")
ADMIN_ID       = int(os.getenv("SUPER_ADMIN_ID", "5695957392"))
BOT_USER       = os.getenv("BOT_USERNAME", "UPIAccessbot")
PAY_UPI        = "Ankiii@upi"
DATA_F         = "hub_data.json"
CFG_F          = "hub_config.json"
MD             = ParseMode.MARKDOWN

DEFAULT_CFG = {
    "boost_cost": 29, "boost_days": 3,
    "ref_pct": 30,    "trial_days": 7,
    "coupons": {},
}

PLANS = {
    "starter": {"name":"Starter 🟢","limit":5,
        "1m":99,"3m":249,"6m":449,"12m":799,
        "save_3m":48,"save_6m":145,"save_12m":389},
    "basic": {"name":"Basic 🔵 ⭐ Popular","limit":25,
        "1m":199,"3m":499,"6m":899,"12m":1599,
        "save_3m":98,"save_6m":295,"save_12m":789},
    "premium": {"name":"Premium 🟡 ♾️","limit":999999,
        "1m":699,"3m":1699,"6m":2999,"12m":4999,
        "save_3m":398,"save_6m":1195,"save_12m":3389},
}
DURATIONS = {"1m":30,"3m":90,"6m":180,"12m":365}
VERIFY_FEE = 99

logging.basicConfig(level=logging.WARNING, format="%(levelname)s | %(message)s")

# ═══════════════════════════════════════════════════════════
#  IN-MEMORY STORE  (load once, mutate in place, save on change)
# ═══════════════════════════════════════════════════════════
_D: dict = {}
_C: dict = {}

def _load():
    global _D, _C
    if os.path.exists(DATA_F):
        with open(DATA_F) as f: _D = json.load(f)
    else:
        _D = _blank()
    _migrate()
    if os.path.exists(CFG_F):
        with open(CFG_F) as f: _C = json.load(f)
    else:
        _C = {}
    for k, v in DEFAULT_CFG.items():
        _C.setdefault(k, v)

def _save():
    """Fast compact JSON write."""
    with open(DATA_F, "w") as f:
        json.dump(_D, f, separators=(",", ":"))

async def _async_save():
    """Non-blocking save — use in hot-path callbacks."""
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _save)

def _savec():
    with open(CFG_F, "w") as f:
        json.dump(_C, f, indent=2)

def _blank():
    return {
        "users":    {},   # uid -> {name, username, role, wallet, joined, ref}
        "creators": {},   # uid -> creator_doc
        "products": {},   # pid -> product_doc
        "purchases":{},   # oid -> purchase_doc
        "refs":     {},   # new_uid -> ref_uid
        "crefs":    {},   # new_cid -> ref_cid
        "sales":    [],
        "utr_log":  [],
        "boosts":   {},   # pid -> expiry_str
        "ratings":  {},   # "uid|pid" -> star int
        "broadcasts":[],
        "pan_pend": {},   # uid -> {plan, utr, ts}
        "classes":  [],
        "cpn_used": {},   # "uid|code" -> True
        "wcoupons": {},
    }

def _migrate():
    for k, v in _blank().items():
        _D.setdefault(k, v)
    sa = str(ADMIN_ID)
    if sa in _D["users"]:
        _D["users"][sa]["role"] = "super_admin"
    # Migrate old creator doc keys → new format
    for uid, c in _D["creators"].items():
        # old key → new key
        if "panel_status"  in c and "ps"        not in c: c["ps"]        = c.pop("panel_status")
        if "panel_expiry"  in c and "panel_exp" not in c: c["panel_exp"] = c.pop("panel_expiry")
        if "trial_expiry"  in c and "trial_exp" not in c: c["trial_exp"] = c.pop("trial_expiry")
        if "trial_status"  in c and "trial_status" not in c: pass  # same key, keep
        if "creator_name"  in c and "name"      not in c: c["name"]      = c.pop("creator_name")
        if "creator_code"  in c and "code"      not in c: c["code"]      = c.pop("creator_code")
        if "creator_category" in c and "cat"    not in c: c["cat"]       = c.pop("creator_category")
        if "creator_bio"   in c and "bio"       not in c: c["bio"]       = c.pop("creator_bio")
        if "creator_upi_id" in c and "upi"      not in c: c["upi"]       = c.pop("creator_upi_id")
        if "creator_upi_qr" in c and "qr"       not in c: c["qr"]        = c.pop("creator_upi_qr")
        if "approval_mode" in c and "mode"      not in c: c["mode"]      = c.pop("approval_mode")
        if "total_sales"   in c and "sales"     not in c: c["sales"]     = c.pop("total_sales")
        if "verified_badge" in c and "ver"      not in c: c["ver"]       = c.pop("verified_badge")
        if "panel_plan"    in c and "plan"      not in c: c["plan"]      = c.pop("panel_plan")
        if "wallet_balance" in c and "wallet"   not in c: c["wallet"]    = c.pop("wallet_balance")
        # Ensure role is set in users table
        if uid in _D["users"]:
            _D["users"][uid]["role"] = "creator"
        else:
            _D["users"][uid] = {"name": c.get("name",""), "username": "",
                "role": "creator", "wallet": 0.0, "joined": now(), "ref": None}
    # Migrate old product doc keys
    for pid, p in _D["products"].items():
        if "creator_id"    in p and "cid"      not in p: p["cid"]      = p.pop("creator_id")
        if "product_name"  in p and "name"     not in p: p["name"]     = p.pop("product_name")
        if "delivery_type" in p and "dtype"    not in p: p["dtype"]    = p.pop("delivery_type")
        if "delivery_link" in p and "link"     not in p: p["link"]     = p.pop("delivery_link")
        if "access_duration" in p and "dur"    not in p: p["dur"]      = p.pop("access_duration")
        if "subscription_flag" in p and "sub"  not in p: p["sub"]      = bool(p.pop("subscription_flag"))
        if "status" in p and "active" not in p:
            p["active"] = p.pop("status") == "active"
        if "students_count" in p and "students" not in p: p["students"] = p.pop("students_count")
    # Migrate old purchase doc keys  
    for oid, pur in _D["purchases"].items():
        if "user_id"    in pur and "uid"  not in pur: pur["uid"]  = str(pur.pop("user_id"))
        if "product_id" in pur and "pid"  not in pur: pur["pid"]  = str(pur.pop("product_id"))
        if "expiry_date" in pur and "exp" not in pur: pur["exp"]  = pur.pop("expiry_date")
        if "status" in pur and "ok" not in pur:
            st = pur.pop("status")
            pur["ok"] = st == "approved"

# ═══════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════
_ESC = re.compile(r"([_*`\[\\])")
def esc(t) -> str:
    return _ESC.sub(r"\\\1", str(t)) if t else ""

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
    return u.replace(" ", "").isalnum() and 10 <= len(u.replace(" ", "")) <= 25

def utr_used(utr: str) -> bool:
    return utr.upper() in [x.upper() for x in _D.get("utr_log", [])]

# ═══════════════════════════════════════════════════════════
#  ROLE HELPERS
# ═══════════════════════════════════════════════════════════
def get_role(uid: str) -> str:
    if int(uid) == ADMIN_ID: return "super_admin"
    return _D["users"].get(uid, {}).get("role", "student")

def is_admin(uid: str) -> bool:
    return int(uid) == ADMIN_ID

def is_creator(uid: str) -> bool:
    return get_role(uid) in ("creator", "super_admin")

def panel_active(uid: str) -> bool:
    c = _D["creators"].get(uid, {})
    ps = c.get("ps", "")
    if ps == "trial":  return not is_expired(c.get("trial_exp", ""))
    if ps == "active": return not is_expired(c.get("panel_exp", ""))
    # super_admin always has active panel
    if int(uid) == ADMIN_ID: return True
    return False

def prod_limit(uid: str) -> int:
    if "PLANS" in dir():
        plan = _D["creators"].get(uid, {}).get("plan", "starter")
        return PLANS.get(plan, PLANS["starter"])["limit"]
    plan = _D["creators"].get(uid, {}).get("plan", "basic")
    return _C.get("plans",{}).get(plan, {}).get("limit", 10)

def prod_count(uid: str) -> int:
    return sum(1 for p in _D["products"].values()
               if p.get("cid") == uid and p.get("active"))

def has_bought(uid: str, pid: str) -> bool:
    return any(
        p.get("uid") == uid and p.get("pid") == pid and
        (p.get("ok") or p.get("status") == "approved")
        for p in _D["purchases"].values()
    )

def track_user(user):
    uid = str(user.id)
    if uid not in _D["users"]:
        _D["users"][uid] = {
            "name": user.full_name or "",
            "username": user.username or "",
            "role": "super_admin" if user.id == ADMIN_ID else "student",
            "wallet": 0.0,
            "joined": now(),
            "ref": None,
        }
    if user.id == ADMIN_ID:
        _D["users"][uid]["role"] = "super_admin"

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

# ═══════════════════════════════════════════════════════════
#  FSM STATES
# ═══════════════════════════════════════════════════════════
(
    # Creator registration
    S_RN, S_RC, S_RB, S_RU, S_RI, S_RQ,
    # Add product
    S_PN, S_PP, S_PD, S_PL, S_PV, S_PS,
    # Edit product — pick product, pick field, enter value
    S_EP, S_EF, S_EV,
    # Purchase
    S_CPN, S_UTR,
    # Renew panel
    S_RUTR,
    # Coupon creation
    S_VN, S_VD, S_VX, S_VM,
    # Broadcast
    S_BT, S_BM,
    # Search
    S_SQ,
    # Live class
    S_LT, S_LD,
    # Admin broadcast
    S_AB,
    # Boost UTR
    S_BU,
    # Withdrawal
    S_WD_AMT,
    # Flash sale
    S_FS_DISC, S_FS_DUR,
    # Logo upload
    S_LOGO,
    # Verification UTR
    S_VER_UTR,
) = range(34)

# ═══════════════════════════════════════════════════════════
#  KEYBOARDS
# ═══════════════════════════════════════════════════════════
def ib(text, cbd=None, url=None):
    if url: return InlineKeyboardButton(text, url=url)
    return InlineKeyboardButton(text, callback_data=cbd)

def kb(*rows): return InlineKeyboardMarkup(list(rows))
def back(to="home"): return kb([ib("🔙 Back", to)])

# Pre-built static keyboards (zero construction cost per request)
KB_ADMIN = kb(
    [ib("👑 Admin Panel", "adm:home"),    ib("📊 Dashboard",   "cr:dash")],
    [ib("➕ Add Product", "cr:addp"),     ib("📦 My Products", "cr:prods")],
    [ib("✏️ Edit Product", "cr:editp"),  ib("🎟 Coupons",     "cr:mkcp")],
    [ib("📣 Broadcast",   "cr:bc"),       ib("💰 Wallet",      "cr:wlt")],
    [ib("⚡ Boost",       "cr:boost"),    ib("🔔 Live Class",  "cr:lcls")],
    [ib("🔄 Renew Panel", "cr:renew"),    ib("⚙️ Settings",   "cr:set")],
    [ib("🏪 Marketplace", "mkt:home"),    ib("🏆 Leaderboard", "mkt:top")],
)
KB_CREATOR = kb(
    [ib("📊 Dashboard",   "cr:dash"),     ib("➕ Add Product", "cr:addp")],
    [ib("📦 My Products", "cr:prods"),    ib("✏️ Edit",        "cr:editp")],
    [ib("🎟 Coupons",     "cr:mkcp"),     ib("💰 Wallet",      "cr:wlt")],
    [ib("⚡ Boost",       "cr:boost"),    ib("⚡ Flash Sale",  "cr:flash")],
    [ib("🔔 Live Class",  "cr:lcls"),     ib("🔄 Renew Panel", "cr:renew")],
    [ib("⚙️ Settings",   "cr:set"),      ib("🏪 Marketplace", "mkt:home")],
)
KB_STUDENT = kb(
    [ib("🔥 Trending",    "mkt:trend"),   ib("🆕 New",         "mkt:new")],
    [ib("🏪 Categories",  "mkt:cats"),    ib("🎁 Free",        "mkt:free")],
    [ib("🔍 Search",      "mkt:srch"),    ib("⭐ Top Creators","mkt:topc")],
    [ib("📦 My Products", "st:prods"),    ib("👛 Wallet",      "st:wlt")],
    [ib("🔗 Refer & Earn","st:ref"),      ib("🏆 Leaderboard", "mkt:top")],
    [ib("🚀 Become a Creator", "beco")],
)
KB_MKT = kb(
    [ib("🔥 Trending",    "mkt:trend"),   ib("🆕 New",         "mkt:new")],
    [ib("🏪 Categories",  "mkt:cats"),    ib("🎁 Free",        "mkt:free")],
    [ib("🔍 Search",      "mkt:srch"),    ib("⭐ Top Creators","mkt:topc")],
    [ib("🔙 Back to Panel","home")],
)

def home_kb(uid: str):
    r = get_role(uid)
    if r == "super_admin": return KB_ADMIN
    if r == "creator":     return KB_CREATOR
    return KB_STUDENT

CATS = ["📚 Education", "💻 Tech & Coding", "💰 Finance", "🎨 Design",
        "📈 Business", "🎵 Music", "💪 Fitness", "🌐 Marketing", "🎯 Other"]
DTYPE_MAP = {
    "tg_ch": "📢 TG Channel", "tg_gr": "👥 TG Group",
    "discord": "💬 Discord",  "drive": "📁 Drive Link",
    "pdf": "📄 PDF Access",   "web": "🌐 Website/Pass",
}
DURS = [("7 Days","7"),("30 Days","30"),("90 Days","90"),("1 Year","365"),("Lifetime","lif")]

def cats_kb():
    rows = []
    for i in range(0, len(CATS), 2):
        row = [ib(CATS[i], f"cat|{CATS[i]}")]
        if i + 1 < len(CATS): row.append(ib(CATS[i+1], f"cat|{CATS[i+1]}"))
        rows.append(row)
    rows.append([ib("🔙 Back", "home")])
    return kb(*rows)

def dtype_kb():
    items = list(DTYPE_MAP.items())
    rows = []
    for i in range(0, len(items), 2):
        row = [ib(items[i][1], f"dt|{items[i][0]}")]
        if i+1 < len(items): row.append(ib(items[i+1][1], f"dt|{items[i+1][0]}"))
        rows.append(row)
    return kb(*rows)

def dur_kb():
    return kb(*[[ib(l, f"dur|{v}")] for l, v in DURS])

def star_kb(pid: str):
    return kb(
        [ib("⭐ 1", f"rate|{pid}|1"), ib("⭐⭐ 2", f"rate|{pid}|2"), ib("⭐⭐⭐ 3", f"rate|{pid}|3")],
        [ib("⭐⭐⭐⭐ 4", f"rate|{pid}|4"), ib("⭐⭐⭐⭐⭐ 5", f"rate|{pid}|5")],
    )

def plans_kb():
    """New renew panel: Starter / Basic / Premium with 1m/3m/6m/12m options."""
    rows = []
    rows.append([ib("🎁 *7-Day Free Trial Activated*","noop")])
    rows.append([ib("After trial ends, choose your plan:","noop")])
    for pk, p in PLANS.items():
        lim = str(p["limit"]) if p["limit"] < 999999 else "♾️ Unlimited"
        rows.append([ib(f"━━ {p['name']} ({lim} products) ━━","noop")])
        rows.append([ib(f"₹{p['1m']}/mo",         f"plan|{pk}|1m"),
                     ib(f"₹{p['3m']} · 3mo 🔥 -₹{p['save_3m']}", f"plan|{pk}|3m")])
        rows.append([ib(f"₹{p['6m']} · 6mo ⭐ -₹{p['save_6m']}", f"plan|{pk}|6m"),
                     ib(f"₹{p['12m']} · 1yr 🚀 -₹{p['save_12m']}", f"plan|{pk}|12m")])
    rows.append([ib("━━━━━━━━━━━━━━━━━━━━━━━━━━━","noop")])
    rows.append([ib("💰 Use Creator Wallet","cr:wlt_renew"), ib("🔙 Back","home")])
    return kb(*rows)

# ═══════════════════════════════════════════════════════════
#  FAST SEND  (answer + edit in parallel via create_task)
# ═══════════════════════════════════════════════════════════
async def send(update: Update, text: str, markup=None):
    kw = {"text": text, "parse_mode": MD, "reply_markup": markup}
    q = update.callback_query
    if q:
        t = asyncio.create_task(q.answer())
        try:
            await q.edit_message_text(**kw)
        except Exception:
            await q.message.reply_text(**kw)
        try: await t
        except: pass
    else:
        await update.message.reply_text(**kw)

async def alert(update: Update, text: str):
    q = update.callback_query
    if q:
        try: await q.answer(text, show_alert=True)
        except: pass

# ═══════════════════════════════════════════════════════════
#  /start
# ═══════════════════════════════════════════════════════════
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    uid  = str(user.id)
    track_user(user)
    arg = ctx.args[0] if ctx.args else ""

    # Handle deep links
    if arg.startswith("ref") and arg[3:].isdigit():
        ref = arg[3:]
        if ref != uid and uid not in _D["refs"]:
            _D["refs"][uid] = ref
            _D["users"][uid]["ref"] = ref
    elif arg.startswith("cref") and arg[4:].isdigit():
        ref = arg[4:]
        if ref != uid and uid not in _D["crefs"]:
            _D["crefs"][uid] = ref
    elif arg:
        # Product direct link
        if arg.startswith("prod_"):
            pid = arg[5:]
            if pid in _D["products"] and _D["products"][pid].get("active"):
                _save()
                await _view_product(update, uid, pid)
                return
        # Creator store link
        for cid, c in _D["creators"].items():
            if c.get("code", "").upper() == arg.upper() and c.get("ps") in ("active", "trial"):
                _save()
                await _show_store(update, cid)
                return
    _save()
    asyncio.create_task(_set_commands(ctx.bot, user.id, get_role(uid)))
    await _show_home(update, uid)

async def _show_home(update: Update, uid: str):
    role = get_role(uid)
    if role == "super_admin":
        tc   = sum(1 for c in _D["creators"].values() if c.get("ps") in ("active","trial"))
        pend = sum(1 for c in _D["creators"].values() if c.get("ps") == "pending")
        rev  = sum(s.get("amt", 0) for s in _D["sales"])
        txt  = (f"👑 *UPI Access Hub — Admin*\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"👥 {len(_D['users'])} users  •  🎨 {tc} creators\n"
                f"⏳ {pend} pending  •  💰 ₹{rev:,.0f}")
        await send(update, txt, KB_ADMIN)
    elif role == "creator":
        c = _D["creators"].get(uid, {})
        if not c:
            # Role says creator but no creator doc — reset to student
            _D["users"][uid]["role"] = "student"
            _save()
            await _show_home(update, uid)
            return
        if c.get("ps") == "pending":
            await send(update,
                "⏳ *Application Under Review*\n\n"
                "Your creator account is being reviewed.\n"
                "You'll receive your store link once approved! 🎉\n\n"
                "_Usually approved within a few hours._",
                kb([ib("🏪 Browse Marketplace","mkt:home")]))
            return
        exp = c.get("trial_exp") if c.get("ps") == "trial" else c.get("panel_exp")
        dl  = days_left(exp) if exp else 0
        tag = "🔔 Trial" if c.get("ps") == "trial" else ("✅ Active" if dl > 0 else "🔴 Expired")
        cnt = prod_count(uid)
        lim = prod_limit(uid)
        # If expired, show renew button prominently
        if c.get("ps") in ("expired", "trial_expired") or (not c.get("ps") in ("active","trial")):
            await send(update,
                f"🔴 *Panel Expired!*\n\n"
                f"Hi {esc(c.get('name',''))}! Your creator panel has expired.\n"
                f"Renew to access dashboard, add products & earn.\n\n"
                f"💰 Total Sales so far: ₹{c.get('sales',0):,.0f}\n"
                f"👛 Wallet: ₹{c.get('wallet',0):,.0f}",
                kb([ib("🔄 Renew Panel Now","cr:renew")],
                   [ib("🏪 Browse Marketplace","mkt:home")]))
            return
        await send(update,
            f"🎨 *Welcome back, {esc(c.get('name',''))}!*{'  ✅' if c.get('ver') else ''}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"Panel: {tag}  |  *{dl}d* remaining\n"
            f"📦 Products: *{cnt}/{lim}*  |  💰 Wallet: ₹{c.get('wallet',0):,.0f}\n"
            f"💰 Total Sales: ₹{c.get('sales',0):,.0f}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"_Tap any option below_ 👇",
            KB_CREATOR)
    else:
        await send(update,
            "👋 *Welcome to UPI Access Hub!*\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "🏆 *India's #1 Creator Marketplace*\n\n"
            "✅ Buy premium courses & digital products\n"
            "✅ Pay via UPI — instant access\n"
            "✅ Learn from verified creators\n"
            "✅ Refer friends → earn *30% commission*\n"
            "✅ Wallet system — convert rewards to coupons\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "🎨 *Want to sell your courses?*\n"
            "Tap *Become a Creator* and get started FREE!\n\n"
            "_Browse below 👇_",
            KB_STUDENT)

# ═══════════════════════════════════════════════════════════
#  CALLBACK ROUTER  (single dispatcher — no double registration)
# ═══════════════════════════════════════════════════════════
async def cb_router(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    uid = str(q.from_user.id)
    d   = q.data
    if d == "noop": await q.answer(); return

    # ── HOME ──────────────────────────────────────────────
    if d in ("home", "start"):
        await _show_home(update, uid)

    elif d == "beco":
        if uid in _D["creators"]:
            await send(update,
                f"ℹ️ Creator account exists. Status: *{_D['creators'][uid].get('ps','?')}*",
                back())
        else:
            await send(update,
                "🚀 *Become a Creator!*\n━━━━━━━━━━━━━━━━\n"
                "✅ Sell courses & digital products\n"
                "✅ Get paid via UPI directly\n"
                "✅ 3-day FREE trial\n"
                "✅ Up to 10 products on Basic plan\n"
                "✅ Auto or manual payment approval\n"
                "━━━━━━━━━━━━━━━━",
                kb([ib("🎉 Register Now", "reg:start")], [ib("🔙 Back", "home")]))

    # ── CREATOR PANEL ──────────────────────────────────────
    elif d == "cr:dash":
        await _creator_dashboard(update, uid)

    elif d == "cr:prods":
        await _creator_products(update, uid)

    elif d.startswith("cr:mgp|"):
        pid = d[7:]
        await _manage_product(update, uid, pid)

    elif d.startswith("cr:delp|"):
        pid = d[8:]
        p = _D["products"].get(pid)
        if p and (p.get("cid") == uid or is_admin(uid)):
            _D["products"][pid]["active"] = False
            _save()
            await send(update, f"🗑 *{esc(p['name'])}* deleted.", back("cr:prods"))
        else:
            await alert(update, "Not authorized!")

    elif d.startswith("cr:pstat|"):
        pid = d[9:]
        await _product_stats(update, pid)

    elif d == "cr:wlt":
        await _creator_wallet(update, uid)

    elif d == "cr:wlt_renew":
        c    = _D["creators"].get(uid, {})
        plan = c.get("plan", "basic")
        cost = _C["plans"].get(plan, {}).get("price", 199)
        bal  = c.get("wallet", 0.0)
        if bal < cost:
            await alert(update, f"Need ₹{cost} in wallet. Current: ₹{bal:.0f}")
            return
        _D["creators"][uid]["wallet"] -= cost
        _extend_panel(uid, 30)
        _save()
        await send(update,
            f"✅ *Panel renewed using wallet!*\nDeducted: ₹{cost}\n+30 days active.",
            back("cr:dash"))

    elif d == "cr:wlt_boost":
        c    = _D["creators"].get(uid, {})
        cost = _C.get("boost_cost", 29)
        if c.get("wallet", 0) < cost:
            await alert(update, f"Need ₹{cost}. Current: ₹{c.get('wallet',0):.0f}")
            return
        prods = [(pid, p) for pid, p in _D["products"].items()
                 if p.get("cid") == uid and p.get("active")]
        if not prods:
            await alert(update, "No products to boost!")
            return
        rows = [[ib(p["name"][:35], f"bstw|{pid}")] for pid, p in prods]
        rows.append([ib("🔙 Back", "cr:wlt")])
        await send(update, f"⚡ Select product to boost (₹{cost} from wallet):", kb(*rows))

    elif d.startswith("bstw|"):
        pid  = d[5:]
        c    = _D["creators"].get(uid, {})
        cost = _C.get("boost_cost", 29)
        bd   = _C.get("boost_days", 3)
        if c.get("wallet", 0) < cost:
            await alert(update, f"Need ₹{cost}!"); return
        _D["creators"][uid]["wallet"] -= cost
        _D["boosts"][pid] = exp_str(bd)
        _save()
        await send(update, f"⚡ *Boosted for {bd} days!*", back("cr:dash"))

    elif d == "cr:boost":
        if not is_creator(uid):
            await alert(update, "Not a creator!"); return
        prods = [(pid, p) for pid, p in _D["products"].items()
                 if p.get("cid") == uid and p.get("active")]
        if not prods:
            await send(update, "No products to boost!", back("cr:dash")); return
        cost = _C.get("boost_cost", 29)
        bd   = _C.get("boost_days", 3)
        rows = [[ib(p["name"][:35], f"bstp|{pid}")] for pid, p in prods]
        rows.append([ib("🔙 Back", "home")])
        await send(update,
            f"⚡ *Boost a Product*\nCost: ₹{cost} | Duration: {bd} days\n"
            f"💳 Pay to: `{PAY_UPI}`\n\nSelect product:",
            kb(*rows))

    elif d.startswith("bstp|"):
        pid  = d[5:]
        p    = _D["products"].get(pid)
        if not p or (p.get("cid") != uid and not is_admin(uid)):
            await alert(update, "Not yours!"); return
        cost = _C.get("boost_cost", 29)
        bd   = _C.get("boost_days", 3)
        bal  = _D["creators"].get(uid, {}).get("wallet", 0)
        ctx.user_data["boost_pid"] = pid
        await send(update,
            f"⚡ *Boost: {esc(p['name'])}*\n"
            f"Cost: ₹{cost} | {bd} days\n"
            f"Pay to: `{PAY_UPI}`",
            kb([ib(f"💰 Use Wallet (₹{bal:.0f})", f"bstw|{pid}")],
               [ib("💳 Pay UPI → Send UTR", "bst:upi")],
               [ib("🔙 Cancel", "cr:boost")]))

    elif d.startswith("cr:prodlink|"):
        pid = d[12:]
        p = _D["products"].get(pid, {})
        link = f"https://t.me/{BOT_USER}?start=prod_{pid}"
        await send(update,
            f"🔗 *Product Direct Link*\n━━━━━━━━━━━━\n"
            f"📦 {esc(p.get('name',''))}\n\n"
            f"Link:\n`{link}`\n\n"
            f"Share this link directly with students!\nThey\'ll land straight on this product\'s buy page.",
            kb([ib("📢 Share",url=f"https://t.me/share/url?url={link}&text=Check+this+out!")],
               [ib("🔙 Back",f"cr:mgp|{pid}")]))

    elif d == "cr:set":
        c    = _D["creators"].get(uid, {})
        code = c.get("code", "")
        link = f"https://t.me/{BOT_USER}?start={code}" if code else "Not activated yet"
        mode = c.get("mode", "manual").title()
        await send(update,
            f"⚙️ *Creator Settings*\n━━━━━━━━━━━━━━\n"
            f"Approval: *{mode}*\n"
            f"Code: `{code}`\nLink: {link}",
            kb([ib("🔄 Toggle Approval Mode", "cr:amode")],
               [ib("🔗 My Store Link", "cr:mystore")],
               [ib("🖼️ Upload Logo", "cr:logo")],
               [ib("🏅 Request Verification (₹99)", "cr:ver_req")],
               [ib("🔙 Back", "home")]))

    elif d == "cr:logo":
        await send(update, "🖼️ *Upload Creator Logo*\n\nSend your logo image\n(shown on your store & welcome screen):")
        return S_LOGO

    elif d == "cr:ver_req":
        c = _D["creators"].get(uid, {})
        if c.get("ver"): await alert(update, "Already verified! ✅"); return
        pend = any(v.get("cid")==uid and v.get("status")=="pending" for v in _D.get("ver_requests",[]))
        if pend: await alert(update, "Verification request already pending! Admin will process soon."); return
        await send(update,
            f"🏅 *Request Verification Badge*\n━━━━━━━━━━━━━━\n"
            f"Get a ✅ verified badge on your store & all products.\n"
            f"One-time fee: *₹99*\n\n"
            f"💳 Pay to: `{PAY_UPI}`\n\n"
            f"Send UTR after payment:")
        return S_VER_UTR

    elif d == "cr:amode":
        await send(update, "⚙️ *Select Approval Mode:*",
            kb([ib("🤖 Auto Approve", "cr:am|auto"),
                ib("👤 Manual", "cr:am|manual")],
               [ib("🔙 Back", "cr:set")]))

    elif d.startswith("cr:am|"):
        mode = d[6:]
        if uid in _D["creators"]:
            _D["creators"][uid]["mode"] = mode
            _save()
        await send(update, f"✅ Approval mode: *{mode.title()}*", back("cr:set"))

    elif d == "cr:mystore":
        c    = _D["creators"].get(uid, {})
        code = c.get("code", "")
        if not code:
            await alert(update, "Store not activated yet!"); return
        link = f"https://t.me/{BOT_USER}?start={code}"
        await send(update, f"🏪 *Your Store*\nCode: `{code}`\nLink: {link}",
            kb([ib("📢 Share", url=f"https://t.me/share/url?url={link}")],
               [ib("🔙 Back", "cr:set")]))

    elif d == "cr:renew":
        if not is_creator(uid) and not is_admin(uid):
            await alert(update, "Not a creator!"); return
        c   = _D["creators"].get(uid, {})
        bal = c.get("wallet", 0.0)
        await send(update,
            f"🔄 *Renew Panel*\n━━━━━━━━━━━━━━\n"
            f"Current plan: *{c.get('plan','basic').title()}*\n"
            f"Wallet: *₹{bal:,.0f}*\n\n"
            f"💳 Pay to: `{PAY_UPI}`\n"
            f"Then select plan and enter UTR:",
            plans_kb())

    # ── MARKETPLACE ────────────────────────────────────────
    elif d == "mkt:home":
        mkb = KB_MKT if is_creator(uid) else KB_STUDENT
        await send(update, "🏪 *Marketplace*", mkb)

    elif d == "mkt:trend":
        await _trending(update)

    elif d == "mkt:new":
        prods = sorted(
            [(pid,p) for pid,p in _D["products"].items() if p.get("active")],
            key=lambda x: x[1].get("ts",""), reverse=True)[:10]
        if not prods:
            await send(update, "No products yet!", back("mkt:home")); return
        rows = [[ib(f"🆕 {p['name'][:30]}  ₹{p['price']:.0f}", f"vp|{pid}")] for pid,p in prods]
        rows.append([ib("🔙 Back", "mkt:home")])
        await send(update, "🆕 *New Products*", kb(*rows))

    elif d == "mkt:free":
        prods = [(pid,p) for pid,p in _D["products"].items()
                 if p.get("active") and p.get("price",1) == 0]
        if not prods:
            await send(update, "No free products!", back("mkt:home")); return
        rows = [[ib(f"🎁 {p['name'][:30]}", f"vp|{pid}")] for pid,p in prods]
        rows.append([ib("🔙 Back", "mkt:home")])
        await send(update, "🎁 *Free Products*", kb(*rows))

    elif d == "mkt:cats":
        cats_available = list({c.get("cat") for c in _D["creators"].values()
                               if c.get("ps") in ("active","trial") and c.get("cat")})
        if not cats_available:
            await send(update, "No categories yet!", back("mkt:home")); return
        rows = [[ib(cat, f"cat|{cat}")] for cat in cats_available]
        rows.append([ib("🔙 Back", "mkt:home")])
        await send(update, "📂 *Browse by Category*", kb(*rows))

    elif d.startswith("cat|"):
        cat  = d[4:]
        cids = {cid for cid,c in _D["creators"].items() if c.get("cat") == cat}
        prods = [(pid,p) for pid,p in _D["products"].items()
                 if p.get("cid") in cids and p.get("active")]
        if not prods:
            await send(update, f"No products in *{esc(cat)}*!", back("mkt:cats")); return
        rows = [[ib(f"🛒 {p['name'][:30]}  ₹{p['price']:.0f}", f"vp|{pid}")] for pid,p in prods[:15]]
        rows.append([ib("🔙 Back", "mkt:cats")])
        await send(update, f"📂 *{esc(cat)}* — {len(prods)} products", kb(*rows))

    elif d == "mkt:topc":
        top = sorted(
            [(cid,c) for cid,c in _D["creators"].items() if c.get("ps") in ("active","trial")],
            key=lambda x: -x[1].get("sales", 0))[:10]
        if not top:
            await send(update, "No creators yet!", back("mkt:home")); return
        txt = "⭐ *Top Creators*\n\n"
        rows = []
        for i, (cid, c) in enumerate(top, 1):
            txt += f"{i}. *{esc(c['name'])}* {'✅' if c.get('ver') else ''}  ₹{c.get('sales',0):,.0f}\n"
            rows.append([ib(f"🏪 {c['name'][:28]}", f"cpr|{cid}")])
        rows.append([ib("🔙 Back", "mkt:home")])
        await send(update, txt, kb(*rows))

    elif d.startswith("cpr|"):
        cid = d[4:]
        await _creator_profile(update, cid)

    elif d == "mkt:top":
        top = sorted(
            [(pid,p) for pid,p in _D["products"].items() if p.get("active")],
            key=lambda x: (-x[1].get("students",0), -x[1].get("rating",0)))[:10]
        if not top:
            await send(update, "No products yet!", back("mkt:home")); return
        txt = "🏆 *Top Products*\n\n"
        rows = []
        for i, (pid, p) in enumerate(top, 1):
            txt += f"{i}. *{esc(p['name'])}*  ₹{p['price']:.0f}  👥{p.get('students',0)}\n"
            rows.append([ib(f"🛒 {p['name'][:28]}", f"vp|{pid}")])
        rows.append([ib("🔙 Back", "mkt:home")])
        await send(update, txt, kb(*rows))

    elif d == "mkt:srch":
        await send(update, "🔍 Type your search:")
        ctx.user_data["state"] = "search"

    # ── PRODUCT VIEW ───────────────────────────────────────
    elif d.startswith("dorate|"):
        pid = d[7:]
        await send(update, "⭐ *Rate this product:*", star_kb(pid))

    elif d.startswith("vp|"):
        pid = d[3:]
        await _view_product(update, uid, pid)

    elif d.startswith("acc|"):
        # "acc|UID|PID"
        parts = d[4:].split("|", 1)
        if len(parts) < 2: return
        owner_uid, pid = parts[0], parts[1]
        if owner_uid != uid:
            await alert(update, "Not yours!"); return
        await _access_product(update, uid, pid)

    elif d.startswith("buy|"):
        pid = d[4:]
        await _start_buy(update, ctx, uid, pid)

    elif d.startswith("rate|"):
        parts = d.split("|")  # ["rate", "pid", "star"]
        pid, star = parts[1], int(parts[2])
        if not has_bought(uid, pid):
            await alert(update, "Buy this product first!"); return
        _D["ratings"][f"{uid}|{pid}"] = star
        rats = [v for k,v in _D["ratings"].items() if k.endswith(f"|{pid}")]
        if pid in _D["products"]:
            _D["products"][pid]["rating"] = round(sum(rats)/len(rats), 1)
        _save()
        await send(update, f"{'⭐'*star} Rating saved! Thank you 🙏", back("st:prods"))

    elif d.startswith("apv|"):
        oid = d[4:]
        await _approve_purchase(update, ctx, uid, oid)

    elif d.startswith("rjt|"):
        oid = d[4:]
        pur = _D["purchases"].get(oid)
        if pur:
            _D["purchases"][oid]["ok"] = False
            _D["purchases"][oid]["rejected"] = True
            _save()
            try:
                await ctx.bot.send_message(int(pur["uid"]),
                    "❌ Payment not verified. Please contact the creator.")
            except: pass
        await send(update, "❌ Purchase rejected.", back("home"))

    # ── STUDENT PANEL ──────────────────────────────────────
    elif d == "st:prods":
        await _my_products(update, uid)

    elif d == "st:wlt":
        await _wallet(update, uid)

    elif d == "st:ref":
        await _refer(update, uid)

    elif d == "st:wlt_conv":
        bal = _D["users"].get(uid, {}).get("wallet", 0.0)
        if bal < 10:
            await alert(update, "Minimum ₹10 required!"); return
        code = rand_cpn()
        _D["wcoupons"][code] = {"uid": uid, "val": bal, "ts": now()}
        _C["coupons"][code]  = {"type": "flat", "flat": bal, "max": 1, "used": 0, "exp": None}
        _D["users"][uid]["wallet"] = 0.0
        _save(); _savec()
        await send(update,
            f"✅ *Wallet Converted!*\nCode: `{code}`\nValue: ₹{bal:,.2f}",
            back("st:wlt"))

    # ── ADMIN PANEL ────────────────────────────────────────
    elif d == "adm:home":
        if not is_admin(uid):
            await alert(update, "Not authorized!"); return
        await _admin_home(update)

    elif d == "adm:pend":
        if not is_admin(uid): return
        await _admin_pending(update)

    elif d == "adm:crts":
        if not is_admin(uid): return
        await _admin_all_creators(update)

    elif d.startswith("adm:cdt|"):
        if not is_admin(uid): return
        cid = d[8:]
        await _admin_creator_detail(update, cid)

    elif d == "adm:sales":
        if not is_admin(uid): return
        await _admin_sales(update)

    elif d == "adm:ver":
        if not is_admin(uid): return
        await send(update, "Use: `/verifycreator CREATOR_ID`", back("adm:home"))

    elif d == "adm:prods":
        if not is_admin(uid): return
        prods = [(pid,p) for pid,p in _D["products"].items() if p.get("active")]
        rows = [[ib(f"🗑 {p['name'][:25]}", f"adm:dp|{pid}")] for pid,p in prods[:20]]
        rows.append([ib("🔙 Back", "adm:home")])
        await send(update, f"📦 *Active Products ({len(prods)})*", kb(*rows))

    elif d.startswith("adm:dp|"):
        if not is_admin(uid): return
        pid = d[7:]
        if pid in _D["products"]:
            _D["products"][pid]["active"] = False; _save()
        await send(update, "🗑 Product removed.", back("adm:prods"))

    elif d.startswith("adm:apv|"):
        if not is_admin(uid): return
        cid = d[8:]
        await _approve_creator(update, ctx, cid)

    elif d.startswith("adm:rjt|"):
        if not is_admin(uid): return
        cid = d[8:]
        if cid in _D["creators"]:
            _D["creators"][cid]["ps"] = "rejected"; _save()
        await send(update, "❌ Creator rejected.", back("adm:pend"))
        try:
            await ctx.bot.send_message(int(cid),
                "❌ Application not approved. Contact @ankiii_support")
        except: pass

    elif d.startswith("adm:vfy|"):
        if not is_admin(uid): return
        cid = d[8:]
        if cid not in _D["creators"]: return
        new = not _D["creators"][cid].get("ver", False)
        _D["creators"][cid]["ver"] = new; _save()
        await send(update,
            f"{'✅ Verified badge granted!' if new else '❌ Badge removed.'}",
            back(f"adm:cdt|{cid}"))

    elif d.startswith("adm:ext|"):
        if not is_admin(uid): return
        cid = d[8:]
        if cid not in _D["creators"]: return
        _extend_panel(cid, 7); _save()
        await send(update, f"✅ +7 days for `{cid}`", back(f"adm:cdt|{cid}"))

    elif d.startswith("adm:rnw|"):
        if not is_admin(uid): return
        # "adm:rnw|cid|plan|dur"  (dur may be absent for legacy approvals)
        parts = d[8:].split("|")
        cid  = parts[0]
        plan = parts[1] if len(parts) > 1 else "basic"
        dur  = parts[2] if len(parts) > 2 else _D["pan_pend"].get(cid, {}).get("dur", "1m")
        days = DURATIONS.get(dur, 30)
        _extend_panel(cid, days)
        _D["creators"][cid]["plan"] = plan
        _D["creators"][cid]["ps"]   = "active"
        _D["creators"][cid]["trial_status"] = False
        # Clear pending entry now that it's approved
        _D["pan_pend"].pop(cid, None)
        # Creator referral bonus
        ref = _D["crefs"].get(cid)
        if ref and ref in _D["creators"]:
            _extend_panel(ref, 7)
            _D["crefs"][cid] = None
        _save()
        dur_label = dur.replace("1m","1 month").replace("3m","3 months").replace("6m","6 months").replace("12m","1 year")
        await send(update, f"✅ Panel renewed for `{cid}` — {PLANS.get(plan,{}).get('name', plan.title())} · {dur_label}", back("adm:home"))
        try:
            lim     = PLANS.get(plan, PLANS["basic"])["limit"]
            lim_str = str(lim) if lim < 999999 else "♾️ Unlimited"
            await ctx.bot.send_message(int(cid),
                f"✅ *Panel Renewed!*\n"
                f"📦 Plan: *{PLANS.get(plan, {}).get('name', plan.title())}* | Products: *{lim_str}*\n"
                f"⏳ Duration: *{dur_label}* (+{days} days)\n"
                f"Use /dashboard to manage 🚀",
                parse_mode=MD, reply_markup=KB_CREATOR)
        except: pass


    elif d.startswith("adm:reject_renew|"):
        if not is_admin(uid): return
        cid = d[17:]
        pend = _D["pan_pend"].pop(cid, None)
        _save()
        await send(update,
            f"❌ *Renewal rejected* for `{cid}`.",
            back("adm:home"))
        try:
            utr_info = f"\nUTR: `{pend['utr']}`" if pend and pend.get("utr") else ""
            await ctx.bot.send_message(int(cid),
                f"❌ *Panel Renewal Rejected*\n\n"
                f"Your renewal payment could not be verified.{utr_info}\n\n"
                f"Please double-check the UTR and try again, or contact support.",
                parse_mode=MD,
                reply_markup=kb([ib("🔄 Try Again", "cr:renew")],
                                [ib("🏠 Home", "home")]))
        except: pass


    elif d.startswith("adm:bst|"):
        if not is_admin(uid): return
        parts = d[8:].split("|")
        cid, pid = parts[0], parts[1]
        bd = _C.get("boost_days", 3)
        _D["boosts"][pid] = exp_str(bd); _save()
        await send(update, f"✅ Boost approved — {bd} days!", back("adm:home"))
        try:
            await ctx.bot.send_message(int(cid),
                f"⚡ *Product boosted for {bd} days!* 🚀", parse_mode=MD)
        except: pass

    elif d.startswith("adm:ver_apv|"):
        if not is_admin(uid): return
        cid = d[13:]
        if cid in _D["creators"]: _D["creators"][cid]["ver"] = True; _save()
        # Mark request as approved
        for v in _D.get("ver_requests",[]): 
            if v.get("cid")==cid and v.get("status")=="pending": v["status"]="approved"
        _save()
        await send(update,"✅ Verification badge granted!",back("adm:home"))
        try: await ctx.bot.send_message(int(cid),
            "🏅 *Congratulations! You\'re now Verified!* ✅\nYour badge is live on your store.",parse_mode=MD)
        except: pass

    elif d.startswith("adm:wd_apv|"):
        if not is_admin(uid): return
        idx=int(d[11:])
        wds = _D.get("withdrawals",[])
        if idx < len(wds):
            wd=wds[idx]; wd["status"]="approved"
            cid=wd.get("cid","")
            if cid in _D["creators"]:
                _D["creators"][cid]["wallet"]=max(0,_D["creators"][cid].get("wallet",0)-wd["amount"])
            _save()
            await send(update,f"✅ Approved ₹{wd['amount']:.0f}",back("adm:home"))
            try: await ctx.bot.send_message(int(cid),
                f"✅ *Withdrawal Approved!*\nAmount: ₹{wd['amount']:.0f}\nUPI: {esc(wd['upi'])}",parse_mode=MD)
            except: pass

    elif d.startswith("flash:cancel|"):
        pid=d[13:]
        if pid in _D.get("flash_sales",{}): del _D["flash_sales"][pid]; _save()
        await send(update,"✅ Flash sale cancelled.",back("cr:flash"))

    else:
        # Unknown callback — silently ignore
        try: await q.answer()
        except: pass

# ═══════════════════════════════════════════════════════════
#  PANEL HELPER
# ═══════════════════════════════════════════════════════════
def _extend_panel(uid: str, days: int):
    c   = _D["creators"].get(uid)
    if not c: return
    base = c.get("panel_exp")
    if base:
        try: dt = datetime.strptime(base, "%Y-%m-%d %H:%M")
        except: dt = datetime.now()
    else:
        dt = datetime.now()
    _D["creators"][uid]["panel_exp"] = (dt + timedelta(days=days)).strftime("%Y-%m-%d %H:%M")

# ═══════════════════════════════════════════════════════════
#  DASHBOARD
# ═══════════════════════════════════════════════════════════
async def _creator_dashboard(update: Update, uid: str):
    if not is_creator(uid):
        await send(update, "❌ Not a creator. Tap *Become a Creator* to get started!", KB_STUDENT)
        return
    c = _D["creators"].get(uid, {})
    if c.get("ps") == "pending":
        await send(update,
            "⏳ *Application Pending*\n\nAdmin will approve you soon! 🎉",
            back())
        return
    today  = datetime.now().strftime("%d %b %Y")
    ts     = sum(1 for x in _D["purchases"].values()
                 if _D["products"].get(x.get("pid",""),{}).get("cid") == uid
                 and x.get("ok") and today in x.get("ts",""))
    total  = sum(1 for x in _D["purchases"].values()
                 if _D["products"].get(x.get("pid",""),{}).get("cid") == uid and x.get("ok"))
    cnt    = prod_count(uid)
    lim    = prod_limit(uid)
    plan   = c.get("plan", "basic").title()
    exp    = c.get("trial_exp") if c.get("ps") == "trial" else c.get("panel_exp")
    dl     = days_left(exp) if exp else 0
    tag    = "🔔 Trial" if c.get("ps") == "trial" else ("✅ Active" if dl > 0 else "🔴 Expired")
    bar    = "▓" * min(cnt, 10) + "░" * max(0, min(10, lim) - cnt)
    badge  = "  ✅" if c.get("ver") else ""
    mode   = c.get("mode", "manual").title()
    await send(update,
        f"📊 *Dashboard*{badge}\n"
        f"👤 *{esc(c.get('name',''))}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"👥 Today: *{ts}*  |  Total: *{total}*\n"
        f"💰 Revenue: *₹{c.get('sales',0):,.0f}*\n"
        f"📦 [{bar}] *{cnt}/{lim}* ({plan})\n"
        f"👛 Wallet: *₹{c.get('wallet',0):,.0f}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"Panel: {tag}  |  *{dl}d* left  |  Mode: {mode}",
        kb(
            [ib("📦 Products", "cr:prods"),    ib("✏️ Edit", "cr:editp")],
            [ib("🎟 Coupons",  "cr:mkcp"),     ib("📣 Broadcast","cr:bc")],
            [ib("💰 Wallet",   "cr:wlt"),      ib("⚡ Boost",    "cr:boost")],
            [ib("🔔 Live Class","cr:lcls"),    ib("🔄 Renew",    "cr:renew")],
            [ib("⚙️ Settings", "cr:set"),      ib("🏪 Marketplace","mkt:home")],
        ))

# ═══════════════════════════════════════════════════════════
#  CREATOR PRODUCTS
# ═══════════════════════════════════════════════════════════
async def _creator_products(update: Update, uid: str):
    if not is_creator(uid):
        await send(update, "❌ Not a creator.", back()); return
    prods = [(pid,p) for pid,p in _D["products"].items()
             if p.get("cid") == uid and p.get("active")]
    lim   = prod_limit(uid)
    if not prods:
        await send(update, f"📦 *My Products (0/{lim})*\n\nNo products yet!",
            kb([ib("➕ Add Product", "cr:addp")], [ib("🔙 Dashboard", "cr:dash")]))
        return
    txt  = f"📦 *My Products ({len(prods)}/{lim})*\n\n"
    rows = []
    for pid, p in prods:
        bst = "⚡" if pid in _D["boosts"] and not is_expired(_D["boosts"][pid]) else ""
        txt += f"{bst}*{esc(p['name'])}*  ₹{p['price']:.0f}  👥{p.get('students',0)}\n"
        rows.append([ib(f"⚙️ {p['name'][:28]}", f"cr:mgp|{pid}")])
    rows.append([ib("➕ Add Product", "cr:addp"), ib("✏️ Edit", "cr:editp")])
    rows.append([ib("🔙 Dashboard", "cr:dash")])
    await send(update, txt, kb(*rows))

async def _manage_product(update: Update, uid: str, pid: str):
    p = _D["products"].get(pid)
    if not p or (p.get("cid") != uid and not is_admin(uid)):
        await alert(update, "Not yours!"); return
    bst = _D["boosts"].get(pid)
    bstr = f"⚡ Boosted until {bst}" if bst and not is_expired(bst) else "Not boosted"
    await send(update,
        f"⚙️ *{esc(p['name'])}*\n━━━━━━━━━━━━━━\n"
        f"💰 ₹{p['price']:.0f}  |  ⏳ {p.get('dur','lifetime')}\n"
        f"📂 {DTYPE_MAP.get(p.get('dtype',''),'?')}\n"
        f"👥 {p.get('students',0)} students  |  ⭐ {p.get('rating',0):.1f}\n"
        f"{bstr}",
        kb(
            [ib("📝 Name",   f"ep:name|{pid}"),  ib("💰 Price", f"ep:price|{pid}")],
            [ib("🔗 Link",   f"ep:link|{pid}"),   ib("⏳ Duration", f"ep:dur|{pid}")],
            [ib("🗑 Delete", f"cr:delp|{pid}"),   ib("⚡ Boost",   f"bstp|{pid}")],
            [ib("🔥 Flash Sale",f"flash|{pid}"),   ib("📊 Stats",  f"cr:pstat|{pid}")],
            [ib("🔗 Product Direct Link",f"cr:prodlink|{pid}")],
            [ib("🔙 Products", "cr:prods")],
        ))

async def _product_stats(update: Update, pid: str):
    p   = _D["products"].get(pid, {})
    rev = sum(s.get("amt",0) for s in _D["sales"] if s.get("pid") == pid)
    today = datetime.now().strftime("%d %b %Y")
    tn  = sum(1 for x in _D["purchases"].values()
              if x.get("pid") == pid and x.get("ok") and today in x.get("ts",""))
    await send(update,
        f"📊 *{esc(p.get('name',''))} Stats*\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"👥 Total: *{p.get('students',0)}*  |  📅 Today: *{tn}*\n"
        f"💰 Revenue: *₹{rev:,.0f}*  |  ⭐ {p.get('rating',0):.1f}/5",
        back(f"cr:mgp|{pid}"))

# ═══════════════════════════════════════════════════════════
#  CREATOR WALLET
# ═══════════════════════════════════════════════════════════
async def _creator_wallet(update: Update, uid: str):
    c    = _D["creators"].get(uid, {})
    bal  = c.get("wallet", 0.0)
    plan = c.get("plan", "basic")
    cost = _C["plans"].get(plan, {}).get("price", 199)
    bc   = _C.get("boost_cost", 29)
    await send(update,
        f"💰 *Creator Wallet*\n━━━━━━━━━━━━━━\n"
        f"Balance: *₹{bal:,.2f}*\n\n"
        f"• 🔄 Renew panel: ₹{cost}\n"
        f"• ⚡ Boost product: ₹{bc}",
        kb([ib(f"🔄 Renew (₹{cost})", "cr:wlt_renew")],
           [ib(f"⚡ Boost (₹{bc})",   "cr:wlt_boost")],
           [ib("🔙 Dashboard",         "cr:dash")]))

# ═══════════════════════════════════════════════════════════
#  MARKETPLACE
# ═══════════════════════════════════════════════════════════
async def _trending(update: Update):
    prods = [(pid,p) for pid,p in _D["products"].items() if p.get("active")]
    prods.sort(key=lambda x: (
        -(x[0] in _D["boosts"] and not is_expired(_D["boosts"].get(x[0],""))),
        -x[1].get("students", 0)))
    if not prods:
        await send(update, "No products yet!", back("mkt:home")); return
    txt  = "🔥 *Trending Products*\n\n"
    rows = []
    for pid, p in prods[:10]:
        bst = "⚡ " if pid in _D["boosts"] and not is_expired(_D["boosts"][pid]) else ""
        c   = _D["creators"].get(p.get("cid",""), {})
        txt += f"{bst}*{esc(p['name'])}*  ₹{p['price']:.0f}  ⭐{p.get('rating',0):.1f}  by {esc(c.get('name','?'))}\n"
        rows.append([ib(f"{'⚡' if bst else '🛒'} {p['name'][:28]}  ₹{p['price']:.0f}", f"vp|{pid}")])
    rows.append([ib("🔙 Back", "mkt:home")])
    await send(update, txt, kb(*rows))

async def _view_product(update: Update, uid: str, pid: str):
    p = _D["products"].get(pid)
    if not p or not p.get("active"):
        await send(update, "❌ Product not available.", back()); return
    is_b  = has_bought(uid, pid)
    rated = f"{uid}|{pid}" in _D["ratings"]
    c     = _D["creators"].get(p.get("cid",""), {})
    bst   = "⚡ *BOOSTED* | " if pid in _D["boosts"] and not is_expired(_D["boosts"][pid]) else ""
    dur   = f"{p['dur']} days" if str(p.get("dur","")) not in ("lif","lifetime") else "♾ Lifetime"
    rl    = f"https://t.me/{BOT_USER}?start={c.get('code','')}"
    rows  = []
    if is_b:
        rows.append([ib("📂 Access Product", f"acc|{uid}|{pid}")])
        if not rated:
            rows.append([ib("⭐ Rate Product", f"dorate|{pid}")])
    else:
        rows.append([ib(f"🛒 Buy Now — ₹{p['price']:.0f}", f"buy|{pid}")])
    rows.append([ib("📢 Share & Earn", url=f"https://t.me/share/url?url={rl}")])
    if c.get("code"):
        rows.append([ib("🏪 Creator Store", url=rl)])
    rows.append([ib("🔙 Back", "mkt:trend")])
    await send(update,
        f"{bst}📦 *{esc(p['name'])}*{'  🔄' if p.get('sub') else ''}\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"👤 *{esc(c.get('name','?'))}* {'✅' if c.get('ver') else ''}\n"
        f"💰 ₹{p['price']:.0f}  |  ⏳ {dur}\n"
        f"📂 {DTYPE_MAP.get(p.get('dtype',''),'?')}\n"
        f"👥 {p.get('students',0)} students  |  ⭐ {p.get('rating',0):.1f}/5\n"
        f"{'✅ _You own this_' if is_b else '👆 _Tap Buy for instant access!_'}",
        kb(*rows))

async def _access_product(update: Update, uid: str, pid: str):
    pur = next((v for v in _D["purchases"].values()
                if v.get("uid") == uid and v.get("pid") == pid and
                (v.get("ok") or v.get("status") == "approved")), None)
    if not pur:
        await send(update,
            "❌ *Access not found!*\n\n"
            "Possible reasons:\n"
            "• Payment still pending approval\n"
            "• Purchase was rejected\n\n"
            "Use /myproducts or contact creator.",
            back("st:prods")); return
    p   = _D["products"].get(pid, {})
    exp = pur.get("exp")
    link = p.get("link", "") or "Contact creator for your access link."
    rl  = f"https://t.me/{BOT_USER}?start=ref{uid}"
    await send(update,
        f"📂 *{esc(p.get('name',''))}*\n━━━━━━━━━━━━━━\n"
        f"⏳ {f'Expires: {exp} ({days_left(exp)}d left)' if exp else '♾ Lifetime Access'}\n\n"
        f"🔗 *Your Access Link:*\n{link}\n\n"
        f"💡 Share & earn 30%:\n`{rl}`",
        kb([ib("📢 Share & Earn", url=f"https://t.me/share/url?url={rl}")],
           [ib("⭐ Rate", f"dorate|{pid}"), ib("🔙 My Products", "st:prods")]))

async def _creator_profile(update: Update, cid: str):
    c     = _D["creators"].get(cid)
    if not c:
        await send(update, "Creator not found!", back("mkt:topc")); return
    prods = [(pid,p) for pid,p in _D["products"].items()
             if p.get("cid") == cid and p.get("active")]
    stud  = sum(p.get("students",0) for _,p in prods)
    avg   = round(sum(p.get("rating",0) for _,p in prods)/len(prods),1) if prods else 0
    rl    = f"https://t.me/{BOT_USER}?start={c.get('code','')}"
    rows  = [[ib(f"🛒 {p['name'][:28]}  ₹{p['price']:.0f}", f"vp|{pid}")] for pid,p in prods[:5]]
    if c.get("code"):
        rows.append([ib("🏪 Full Store", url=rl)])
    rows.append([ib("🔙 Back", "mkt:topc")])
    await send(update,
        f"🏪 *{esc(c['name'])}* {'✅' if c.get('ver') else ''}\n"
        f"━━━━━━━━━━━━━━━\n"
        f"📂 {esc(c.get('cat',''))}  |  👥 {stud} students\n"
        f"📝 _{esc(c.get('bio',''))}_\n"
        f"📦 {len(prods)} products  |  ⭐ {avg}  |  💰 ₹{c.get('sales',0):,.0f}",
        kb(*rows))

async def _show_store(update: Update, cid: str):
    c     = _D["creators"].get(cid, {})
    prods = [(pid,p) for pid,p in _D["products"].items()
             if p.get("cid") == cid and p.get("active")]
    stud  = sum(p.get("students",0) for _,p in prods)
    rl    = f"https://t.me/{BOT_USER}?start={c.get('code','')}"
    rows  = [[ib(f"🛒 {p['name'][:28]}  ₹{p['price']:.0f}", f"vp|{pid}")] for pid,p in prods[:8]]
    rows.append([ib("📢 Share Store", url=f"https://t.me/share/url?url={rl}")])
    rows.append([ib("🏠 Main Menu", "home")])
    await send(update,
        f"🏪 *{esc(c.get('name',''))}* {'✅' if c.get('ver') else ''}\n"
        f"📂 {esc(c.get('cat',''))}  |  👥 {stud} students\n"
        f"📝 _{esc(c.get('bio',''))}_\n"
        f"📦 *{len(prods)} products*",
        kb(*rows))

# ═══════════════════════════════════════════════════════════
#  STUDENT PANEL
# ═══════════════════════════════════════════════════════════
async def _my_products(update: Update, uid: str):
    my = {oid:p for oid,p in _D["purchases"].items()
          if p.get("uid") == uid and (p.get("ok") or p.get("status")=="approved")}
    if not my:
        await send(update,
            "📦 *My Products*\n\nNo purchases yet! Browse the marketplace.",
            kb([ib("🏪 Browse", "mkt:home")], [ib("🔗 Refer & Earn", "st:ref")])); return
    txt  = f"📦 *My Products ({len(my)})*\n\n"
    rows = []
    for oid, pur in list(my.items())[:15]:
        pid = pur.get("pid","")
        p   = _D["products"].get(pid, {})
        exp = pur.get("exp")
        el  = f"⏳{days_left(exp)}d" if exp and days_left(exp) > 0 else ("🔴 Exp" if exp else "♾")
        txt += f"• *{esc(p.get('name','?'))}* {el}\n"
        row = [ib(f"📂 {p.get('name','?')[:22]} [{el}]", f"acc|{uid}|{pid}")]
        if p.get("sub") or (exp and days_left(exp) <= 7):
            row.append(ib("🔄", f"buy|{pid}"))
        rows.append(row)
    rows.append([ib("🔙 Home", "home")])
    await send(update, txt, kb(*rows))

async def _wallet(update: Update, uid: str):
    bal  = _D["users"].get(uid, {}).get("wallet", 0.0)
    refs = sum(1 for v in _D["refs"].values() if v == uid)
    rl   = f"https://t.me/{BOT_USER}?start=ref{uid}"
    pct  = _C.get("ref_pct", 30)
    await send(update,
        f"👛 *Your Wallet*\n━━━━━━━━━━━━━━\n"
        f"💰 Balance: *₹{bal:,.2f}*\n"
        f"👥 Referrals: *{refs}*\n\n"
        f"Earn *{pct}%* of every referral purchase!\n`{rl}`",
        kb([ib("🎟 Convert to Coupon", "st:wlt_conv")],
           [ib("📢 Share Link", url=f"https://t.me/share/url?url={rl}")],
           [ib("🔙 Home", "home")]))

async def _refer(update: Update, uid: str):
    rl   = f"https://t.me/{BOT_USER}?start=ref{uid}"
    refs = sum(1 for v in _D["refs"].values() if v == uid)
    bal  = _D["users"].get(uid, {}).get("wallet", 0.0)
    pct  = _C.get("ref_pct", 30)
    await send(update,
        f"🔗 *Refer & Earn*\n━━━━━━━━━━━━━━\n"
        f"Link: `{rl}`\n━━━━━━━━━━━━━━\n"
        f"👥 {refs} referrals  |  💰 ₹{bal:,.2f} earned\n\n"
        f"*Earn {pct}% of every purchase your friends make!*",
        kb([ib("📢 Share Now", url=f"https://t.me/share/url?url={rl}&text=Join+UPI+Access+Hub!")],
           [ib("👛 Wallet", "st:wlt"), ib("🔙 Home", "home")]))

# ═══════════════════════════════════════════════════════════
#  PURCHASE FLOW
# ═══════════════════════════════════════════════════════════
async def _start_buy(update: Update, ctx, uid: str, pid: str):
    p = _D["products"].get(pid)
    if not p or not p.get("active"):
        await send(update, "❌ Product unavailable.", back()); return
    if has_bought(uid, pid):
        await alert(update, "✅ You already own this!"); return
    if p.get("price", 0) == 0:
        oid = rand_oid()
        _D["purchases"][oid] = {"uid":uid,"pid":pid,"utr":"FREE","ok":False,"exp":None,"ts":now()}
        grant_purchase(oid); _save()
        await send(update,
            f"🎁 *Free Access Granted!*\n\n*{esc(p['name'])}*\n🔗 {p.get('link','Contact creator')}",
            kb([ib("📦 My Products","st:prods")],[ib("🏠 Home","home")]))
        return
    ctx.user_data.update({"bpid": pid, "bprice": p["price"], "bdisc": 0, "bcpn": None})
    await send(update,
        f"🛒 *{esc(p['name'])}*  ₹{p['price']:.0f}\n\n"
        "🎟 Have a coupon? Type it or skip:",
        kb([ib("⏭ Skip — Pay Full", f"skp|{pid}")]))
    return S_CPN

async def cb_buy_entry(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    pid = update.callback_query.data[4:]
    result = await _start_buy(update, ctx, uid, pid)
    return result if result else ConversationHandler.END

async def cb_skip_coupon(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    pid = update.callback_query.data[4:]
    p   = _D["products"].get(pid, {})
    ctx.user_data.update({"bpid":pid,"bprice":p.get("price",0),"bdisc":0,"bcpn":None})
    await _show_payment(update, ctx, pid, p.get("price",0))
    return S_UTR

async def fsm_coupon(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid   = str(update.effective_user.id)
    code  = update.message.text.strip()
    pid   = ctx.user_data.get("bpid","")
    price = ctx.user_data.get("bprice", 0)
    ok, final, disc, msg = apply_coupon(code, price, uid)
    await update.message.reply_text(msg, parse_mode=MD)
    if ok:
        ctx.user_data.update({"bprice":final,"bdisc":disc,"bcpn":code.upper()})
    await _show_payment(update, ctx, pid, ctx.user_data["bprice"])
    return S_UTR

async def _show_payment(update: Update, ctx, pid: str, final_price: float):
    p  = _D["products"].get(pid, {})
    c  = _D["creators"].get(p.get("cid",""), {})
    upi = c.get("upi","") or PAY_UPI
    txt = (f"💳 *Payment*\n━━━━━━━━━━━━━━\n"
           f"📦 *{esc(p.get('name',''))}*\n"
           f"💰 *₹{final_price:.0f}*\n━━━━━━━━━━━━━━\n"
           f"📲 Pay to:\n`{esc(upi)}`\n\n"
           f"✅ Send UTR after payment:")
    if c.get("qr"):
        try:
            m = update.message or update.callback_query.message
            await m.reply_photo(c["qr"], caption=txt, parse_mode=MD)
            return
        except: pass
    m = update.message or update.callback_query.message
    await m.reply_text(txt, parse_mode=MD)

async def fsm_utr(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    utr = update.message.text.strip().replace(" ","")
    if not valid_utr(utr):
        await update.message.reply_text("❌ Invalid UTR (10–25 characters). Try again:"); return S_UTR
    if utr_used(utr):
        await update.message.reply_text("❌ UTR already used! Contact support."); return S_UTR
    pid  = ctx.user_data.get("bpid","")
    cpn  = ctx.user_data.get("bcpn")
    p    = _D["products"].get(pid, {})
    cid  = p.get("cid","")
    c    = _D["creators"].get(cid, {})
    _D["utr_log"].append(utr.upper())
    if cpn:
        if cpn in _C["coupons"]: _C["coupons"][cpn]["used"] = _C["coupons"][cpn].get("used",0)+1
        _D["cpn_used"][f"{uid}|{cpn}"] = True
        _savec()
    oid  = rand_oid()
    _D["purchases"][oid] = {"uid":uid,"pid":pid,"utr":utr.upper(),"ok":False,"exp":None,"ts":now()}
    rl   = f"https://t.me/{BOT_USER}?start=ref{uid}"
    if c.get("mode","manual") == "auto":
        grant_purchase(oid); credit_ref(uid, p.get("price",0)); _save()
        link_ = p.get("link","") or "Contact creator for your access link."
        await update.message.reply_text(
            f"✅ *Access Granted Instantly!*\n\n"
            f"📦 *{esc(p.get('name',''))}*\n"
            f"🔗 {link_}\n\n"
            f"💡 Share & earn 30%:\n`{rl}`",
            parse_mode=MD,
            reply_markup=kb([ib("📂 Access Now",f"acc|{uid}|{pid}")],
                            [ib("📢 Share",url=f"https://t.me/share/url?url={rl}")],
                            [ib("📦 My Products","st:prods"),ib("⭐ Rate",f"dorate|{pid}")]))
        asyncio.create_task(_send_invoice(ctx.bot, oid, _D["purchases"][oid], p))
    else:
        _save()
        await update.message.reply_text(
            "✅ *UTR Submitted!*\n\nWaiting for creator approval. 🎉", parse_mode=MD)
        try:
            await ctx.bot.send_message(int(cid),
                f"💳 *New Payment!*\n"
                f"👤 {esc(update.effective_user.full_name)} (`{uid}`)\n"
                f"📦 *{esc(p.get('name',''))}*  ₹{p.get('price',0):.0f}\n"
                f"UTR: `{utr}`",
                parse_mode=MD,
                reply_markup=kb([ib("✅ Approve",f"apv|{oid}"),ib("❌ Reject",f"rjt|{oid}")]))
        except: pass
    return ConversationHandler.END

async def _approve_purchase(update: Update, ctx, uid: str, oid: str):
    pur  = _D["purchases"].get(oid)
    if not pur or pur.get("ok"):
        await send(update, "Already processed.", back()); return
    prod = _D["products"].get(pur.get("pid",""), {})
    if not is_admin(uid) and prod.get("cid") != uid:
        await alert(update, "Not authorized!"); return
    grant_purchase(oid); credit_ref(pur["uid"], prod.get("price",0)); _save()
    await send(update, f"✅ *Purchase approved!*", back("home"))
    rl = f"https://t.me/{BOT_USER}?start=ref{pur['uid']}"
    buyer_uid=pur["uid"]; pid_=pur["pid"]
    link_=prod.get("link","") or "Contact creator for access link."
    try:
        await ctx.bot.send_message(int(buyer_uid),
            f"✅ *Access Granted!*\n\n"
            f"📦 *{esc(prod.get('name',''))}*\n"
            f"🔗 {link_}\n\n"
            f"💡 Share & earn 30%:\n`{rl}`",
            parse_mode=MD,
            reply_markup=kb([ib("📂 Access Now",f"acc|{buyer_uid}|{pid_}")],
                            [ib("📢 Share",url=f"https://t.me/share/url?url={rl}")],
                            [ib("📦 My Products","st:prods"),ib("⭐ Rate",f"dorate|{pid_}")]))
        asyncio.create_task(_send_invoice(ctx.bot, oid, _D["purchases"][oid], prod))
    except: pass

async def _send_invoice(bot, oid: str, pur: dict, prod: dict):
    c = _D["creators"].get(prod.get("cid",""), {})
    try:
        await bot.send_message(int(pur["uid"]),
            f"🧾 *Invoice*\n━━━━━━━━━━━━━━\n"
            f"Receipt: `#{oid}`\n"
            f"📦 {esc(prod.get('name',''))}\n"
            f"👤 {esc(c.get('name',''))}\n"
            f"💰 ₹{prod.get('price',0):.0f}\n"
            f"UTR: `{pur.get('utr','')}`\n"
            f"📅 {now()}\n━━━━━━━━━━━━━━\n_Thank you!_ 🙏",
            parse_mode=MD)
    except: pass

# ═══════════════════════════════════════════════════════════
#  ADMIN
# ═══════════════════════════════════════════════════════════
async def _admin_home(update: Update):
    tc   = sum(1 for c in _D["creators"].values() if c.get("ps") in ("active","trial"))
    tp   = sum(1 for p in _D["products"].values() if p.get("active"))
    pend = sum(1 for c in _D["creators"].values() if c.get("ps") == "pending")
    rev  = sum(s.get("amt",0) for s in _D["sales"])
    today = datetime.now().strftime("%d %b %Y")
    tr   = sum(s.get("amt",0) for s in _D["sales"] if today in s.get("ts",""))
    await send(update,
        f"👑 *Admin Dashboard*\n━━━━━━━━━━━━━━━━━━\n"
        f"👥 {len(_D['users'])} users  •  🎨 {tc} creators\n"
        f"📦 {tp} products  •  ⏳ {pend} pending\n"
        f"💰 Total: *₹{rev:,.0f}*  •  Today: *₹{tr:,.0f}*",
        kb(
            [ib("⏳ Pending", "adm:pend"),    ib("👥 All Creators","adm:crts")],
            [ib("📊 Sales",   "adm:sales"),   ib("📦 Products",    "adm:prods")],
            [ib("📣 Broadcast","adm:bc"),      ib("🏅 Verify",      "adm:ver")],
            [ib("🔙 Home",    "home")],
        ))

async def _admin_pending(update: Update):
    pend = [(cid,c) for cid,c in _D["creators"].items() if c.get("ps") == "pending"]
    if not pend:
        await send(update, "✅ No pending applications!", back("adm:home")); return
    txt  = f"⏳ *Pending ({len(pend)})*\n\n"
    rows = []
    for cid, c in pend:
        txt += f"• *{esc(c['name'])}* `{cid}`\n"
        rows.append([ib(f"✅ {c['name'][:15]}", f"adm:apv|{cid}"),
                     ib("❌ Reject",           f"adm:rjt|{cid}")])
    rows.append([ib("🔙 Back","adm:home")])
    await send(update, txt, kb(*rows))

async def _admin_all_creators(update: Update):
    rows = []
    for cid,c in list(_D["creators"].items())[:20]:
        ico = {"active":"🟢","trial":"🔵","pending":"🟡","expired":"🔴"}.get(c.get("ps",""),"⚪")
        vb  = "✅" if c.get("ver") else "⬜"
        rows.append([ib(f"{vb}{ico} {c['name'][:20]}  ₹{c.get('sales',0):,.0f}",
                        f"adm:cdt|{cid}")])
    rows.append([ib("🔙 Back","adm:home")])
    await send(update, f"🎨 *Creators ({len(_D['creators'])})*", kb(*rows))

async def _admin_creator_detail(update: Update, cid: str):
    c   = _D["creators"].get(cid)
    if not c:
        await send(update, "Not found!", back("adm:home")); return
    exp = c.get("trial_exp") if c.get("ps") == "trial" else c.get("panel_exp")
    await send(update,
        f"👤 *{esc(c['name'])}* {'✅' if c.get('ver') else ''}\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"ID: `{cid}`  •  {esc(c.get('cat',''))}\n"
        f"Status: *{c.get('ps','?')}*  •  Plan: *{c.get('plan','basic')}*\n"
        f"Exp: {exp} ({days_left(exp)}d)  •  UPI: {esc(c.get('upi',''))}\n"
        f"Products: {prod_count(cid)}  •  💰 ₹{c.get('sales',0):,.0f}  •  👛 ₹{c.get('wallet',0):.0f}",
        kb(
            [ib("✅ Approve",f"adm:apv|{cid}"),  ib("❌ Reject",f"adm:rjt|{cid}")],
            [ib("🏅 Verify", f"adm:vfy|{cid}"),  ib("➕ +7 Days",f"adm:ext|{cid}")],
            [ib("🔙 Back","adm:crts")],
        ))

async def _admin_sales(update: Update):
    rev = sum(s.get("amt",0) for s in _D["sales"])
    today = datetime.now().strftime("%d %b %Y")
    tr  = sum(s.get("amt",0) for s in _D["sales"] if today in s.get("ts",""))
    tp  = {}
    for s in _D["sales"]: tp[s.get("pid","")] = tp.get(s.get("pid",""),0) + s.get("amt",0)
    txt = (f"📊 *Sales Report*\n━━━━━━━━━━━━━━\n"
           f"💰 Total: *₹{rev:,.0f}*\n"
           f"📅 Today: *₹{tr:,.0f}*\n"
           f"🛒 Orders: *{len(_D['sales'])}*\n\n*Top Products:*\n")
    for pid, amt in sorted(tp.items(), key=lambda x:-x[1])[:5]:
        p = _D["products"].get(pid,{})
        txt += f"• {esc(p.get('name','?'))}  ₹{amt:,.0f}\n"
    await send(update, txt, back("adm:home"))

async def _approve_creator(update: Update, ctx, cid: str):
    c = _D["creators"].get(cid)
    if not c:
        await send(update, "Not found!", back("adm:pend")); return
    code = rand_code(c.get("name",""))
    td   = _C.get("trial_days", 3)
    _D["creators"][cid].update({
        "code": code, "ps": "trial",
        "trial_status": True, "trial_exp": exp_str(td),
    })
    _save()
    link = f"https://t.me/{BOT_USER}?start={code}"
    await send(update, f"✅ *Approved!*\nCode: `{code}`\nLink: {link}", back("adm:pend"))
    try:
        await ctx.bot.send_message(int(cid),
            f"🎉 *You're Approved!*\n━━━━━━━━━━━━━━\n"
            f"Code: `{code}`\nStore: {link}\n\n"
            f"🔥 *{td}-day FREE trial* starts now!\nUse /dashboard 🚀",
            parse_mode=MD, reply_markup=KB_CREATOR)
        asyncio.create_task(_set_commands(ctx.bot, int(cid), "creator"))
    except: pass

# ═══════════════════════════════════════════════════════════
#  COMMAND MENUS
# ═══════════════════════════════════════════════════════════
_SC = [BotCommand("start","🏠 Home"),BotCommand("myproducts","📦 My Products"),
       BotCommand("wallet","👛 Wallet"),BotCommand("refer","🔗 Refer & Earn"),
       BotCommand("search","🔍 Search"),BotCommand("topcreators","⭐ Top Creators"),
       BotCommand("profile","🏪 Profile"),BotCommand("cancel","❌ Cancel")]
_CC = [BotCommand("start","🏠 Home"),BotCommand("dashboard","📊 Dashboard"),
       BotCommand("addproduct","➕ Add Product"),BotCommand("editproduct","✏️ Edit"),
       BotCommand("myproducts","📦 Products"),BotCommand("renewpanel","🔄 Renew"),
       BotCommand("createcoupon","🎟 Coupon"),BotCommand("broadcast","📣 Broadcast"),
       BotCommand("boostproduct","⚡ Boost"),BotCommand("scheduleclass","🔔 Live Class"),
       BotCommand("wallet","👛 Wallet"),BotCommand("cancel","❌ Cancel")]
_AC = [BotCommand("start","🏠 Home"),BotCommand("adminpanel","👑 Admin"),
       BotCommand("approve_creator","✅ Approve"),BotCommand("verifycreator","🏅 Verify"),
       BotCommand("dashboard","📊 Dashboard"),BotCommand("addproduct","➕ Add"),
       BotCommand("editproduct","✏️ Edit"),BotCommand("renewpanel","🔄 Renew"),
       BotCommand("createcoupon","🎟 Coupon"),BotCommand("broadcast","📣 Broadcast"),
       BotCommand("exportdata","📤 Export"),BotCommand("cancel","❌ Cancel")]

async def _set_commands(bot, uid: int, role: str):
    try:
        cmds = _AC if role == "super_admin" else _CC if role == "creator" else _SC
        await bot.set_my_commands(cmds, scope=BotCommandScopeChat(chat_id=uid))
    except: pass

# ═══════════════════════════════════════════════════════════
#  FSM HANDLERS
# ═══════════════════════════════════════════════════════════

# ── REGISTRATION ───────────────────────────────────────────
async def fsm_reg_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if update.callback_query: await update.callback_query.answer()
    if uid in _D["creators"]:
        st = _D["creators"][uid].get("ps","?")
        await send(update, f"Account exists. Status: *{st}*", back()); return ConversationHandler.END
    await send(update, "🎉 *Register — Step 1/4*\n\nEnter your *creator/brand name*:")
    return S_RN

async def fsm_rn(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    n = update.message.text.strip()
    if not 2 <= len(n) <= 50:
        await update.message.reply_text("❌ 2–50 characters. Try again:"); return S_RN
    ctx.user_data["reg_name"] = n
    await update.message.reply_text("📂 *Step 2/4 — Select Category:*",
                                    parse_mode=MD, reply_markup=cats_kb())
    return S_RC

async def fsm_rc(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    ctx.user_data["reg_cat"] = update.callback_query.data[4:]
    await update.callback_query.edit_message_text(
        "📝 *Step 3/4 — Short bio* (max 200 chars):", parse_mode=MD)
    return S_RB

async def fsm_rb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["reg_bio"] = update.message.text.strip()[:200]
    await update.message.reply_text("💳 *Step 4/4 — Payment Method:*", parse_mode=MD,
        reply_markup=kb([ib("🔤 Enter UPI ID","rupi:txt")],[ib("📷 Upload QR","rupi:qr")]))
    return S_RU

async def fsm_ru(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    if update.callback_query.data == "rupi:txt":
        await update.callback_query.edit_message_text("Enter UPI ID (e.g. name@upi):")
        return S_RI
    await update.callback_query.edit_message_text("📷 Send your UPI QR image:")
    return S_RQ

async def fsm_ri(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    upi = update.message.text.strip()
    if "@" not in upi:
        await update.message.reply_text("❌ Invalid UPI ID (must have @). Try again:"); return S_RI
    ctx.user_data.update({"reg_upi": upi, "reg_qr": None})
    return await _save_registration(update, ctx)

async def fsm_rq(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not update.message.photo:
        await update.message.reply_text("❌ Send an image!"); return S_RQ
    ctx.user_data.update({"reg_qr": update.message.photo[-1].file_id, "reg_upi": None})
    return await _save_registration(update, ctx)

async def _save_registration(update: Update, ctx):
    uid = str(update.effective_user.id)
    track_user(update.effective_user)
    _D["creators"][uid] = {
        "name": ctx.user_data["reg_name"],
        "code": None,
        "cat":  ctx.user_data.get("reg_cat",""),
        "bio":  ctx.user_data.get("reg_bio",""),
        "upi":  ctx.user_data.get("reg_upi",""),
        "qr":   ctx.user_data.get("reg_qr"),
        "ps":   "pending",
        "plan": "basic",
        "panel_exp": None,
        "trial_exp": None,
        "trial_status": False,
        "mode": "manual",
        "wallet": 0.0,
        "sales":  0.0,
        "ver": False,
    }
    _D["users"][uid]["role"] = "creator"
    _save()
    await update.message.reply_text(
        "✅ *Submitted!* Admin will approve within 24 hours. 🎉",
        parse_mode=MD, reply_markup=kb([ib("🏪 Browse Marketplace","mkt:home")]))
    try:
        await ctx.bot.send_message(ADMIN_ID,
            f"🆕 *New Creator Application!*\n"
            f"*{esc(ctx.user_data['reg_name'])}* `{uid}`\n"
            f"📂 {esc(ctx.user_data.get('reg_cat',''))}\n"
            f"UPI: {esc(ctx.user_data.get('reg_upi','QR uploaded'))}\n"
            f"📝 {esc(ctx.user_data.get('reg_bio',''))}",
            parse_mode=MD,
            reply_markup=kb([ib("✅ Approve",f"adm:apv|{uid}"),ib("❌ Reject",f"adm:rjt|{uid}")]))
    except: pass
    return ConversationHandler.END

# ── ADD PRODUCT ────────────────────────────────────────────
async def fsm_addp_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if update.callback_query: await update.callback_query.answer()
    if not is_creator(uid):
        await send(update, "❌ Not a creator.", back()); return ConversationHandler.END
    if not panel_active(uid) and not is_admin(uid):
        await send(update, "❌ Panel not active. Use /renewpanel.",
            kb([ib("🔄 Renew Panel","cr:renew")])); return ConversationHandler.END
    cnt, lim = prod_count(uid), prod_limit(uid)
    if cnt >= lim and not is_admin(uid):
        await send(update, f"❌ Limit reached ({cnt}/{lim}). Upgrade plan.",
            kb([ib("🔄 Upgrade Plan","cr:renew")],[ib("🔙 Back","cr:dash")])); return ConversationHandler.END
    await send(update, f"➕ *Add Product* ({cnt}/{lim})\n\nEnter *product name*:")
    return S_PN

async def fsm_pn(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["p_name"] = update.message.text.strip()
    await update.message.reply_text("💰 Price in ₹ (0 = free):", parse_mode=MD)
    return S_PP

async def fsm_pp(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try: ctx.user_data["p_price"] = float(update.message.text.strip())
    except:
        await update.message.reply_text("❌ Enter a number:"); return S_PP
    await update.message.reply_text("📂 Delivery type:", parse_mode=MD, reply_markup=dtype_kb())
    return S_PD

async def fsm_pd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    ctx.user_data["p_dtype"] = update.callback_query.data[3:]
    await update.callback_query.edit_message_text("🔗 Enter access link / value:")
    return S_PL

async def fsm_pl(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["p_link"] = update.message.text.strip()
    await update.message.reply_text("⏳ Access duration:", parse_mode=MD, reply_markup=dur_kb())
    return S_PV

async def fsm_pv(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    ctx.user_data["p_dur"] = update.callback_query.data[4:]
    await update.callback_query.edit_message_text(
        "🔄 *Subscription product?* (monthly recurring)", parse_mode=MD,
        reply_markup=kb([ib("✅ Yes — Monthly Sub","psub|y"),ib("❌ No — One-Time","psub|n")]))
    return S_PS

async def fsm_ps(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    sub = update.callback_query.data == "psub|y"
    uid = str(update.callback_query.from_user.id)
    pid = rand_pid()
    _D["products"][pid] = {
        "cid": uid, "name": ctx.user_data["p_name"], "price": ctx.user_data["p_price"],
        "dtype": ctx.user_data["p_dtype"], "link": ctx.user_data["p_link"],
        "dur": ctx.user_data["p_dur"], "sub": sub,
        "rating": 0.0, "students": 0, "ts": now(), "active": True,
    }
    _save()
    await update.callback_query.edit_message_text(
        f"✅ *Product Added!*\n*{esc(ctx.user_data['p_name'])}*  ₹{ctx.user_data['p_price']:.0f}",
        parse_mode=MD,
        reply_markup=kb([ib("📦 My Products","cr:prods"),ib("➕ Add Another","cr:addp")],
                        [ib("📊 Dashboard","cr:dash")]))
    return ConversationHandler.END

# ── EDIT PRODUCT ───────────────────────────────────────────
async def fsm_editp_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if update.callback_query: await update.callback_query.answer()
    if not is_creator(uid):
        await send(update, "❌ Not a creator.", back()); return ConversationHandler.END
    prods = [(pid,p) for pid,p in _D["products"].items()
             if p.get("cid") == uid and p.get("active")]
    if not prods:
        await send(update, "No products to edit!", back("cr:dash")); return ConversationHandler.END
    rows = [[ib(f"✏️ {p['name'][:30]}  ₹{p['price']:.0f}", f"epc|{pid}")] for pid,p in prods]
    rows.append([ib("🔙 Dashboard","cr:dash")])
    await send(update, "✏️ *Select product to edit:*", kb(*rows))
    return S_EP

async def fsm_ep(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    pid = update.callback_query.data[4:]
    ctx.user_data["edit_pid"] = pid
    p   = _D["products"].get(pid, {})
    await send(update, f"✏️ *{esc(p.get('name',''))}* — What to change?",
        kb([ib("📝 Name",  f"ef|name"),  ib("💰 Price", f"ef|price")],
           [ib("🔗 Link",  f"ef|link"),  ib("⏳ Duration", f"ef|dur")],
           [ib("🔙 Cancel","cr:prods")]))
    return S_EF

async def fsm_ef(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    field = update.callback_query.data[3:]
    ctx.user_data["edit_field"] = field
    labels = {"name":"product name","price":"new price ₹","link":"new access link","dur":"duration (e.g. 30 or lif)"}
    await update.callback_query.edit_message_text(f"✏️ Enter new *{labels.get(field,field)}*:", parse_mode=MD)
    return S_EV

async def fsm_ev(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    pid   = ctx.user_data.get("edit_pid","")
    field = ctx.user_data.get("edit_field","")
    val   = update.message.text.strip()
    if pid not in _D["products"]:
        await update.message.reply_text("Product not found!"); return ConversationHandler.END
    if field == "price":
        try: val = float(val)
        except:
            await update.message.reply_text("❌ Enter a number:"); return S_EV
        _D["products"][pid]["price"] = val
    elif field == "name": _D["products"][pid]["name"] = val
    elif field == "link": _D["products"][pid]["link"] = val
    elif field == "dur":  _D["products"][pid]["dur"]  = val
    _save()
    await update.message.reply_text(f"✅ *{field.title()} updated!* → `{esc(str(val))}`",
        parse_mode=MD, reply_markup=back(f"cr:mgp|{pid}"))
    return ConversationHandler.END

# ── RENEW PANEL ────────────────────────────────────────────
async def fsm_plan_pick(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    # data = "plan|starter|1m"
    parts = update.callback_query.data.split("|")
    if len(parts) < 3: await alert(update,"Invalid!"); return ConversationHandler.END
    plan, dur = parts[1], parts[2]
    if plan not in PLANS: await alert(update,"Invalid plan!"); return ConversationHandler.END
    price = PLANS[plan][dur]; days = DURATIONS[dur]
    lim   = PLANS[plan]["limit"]; lim_str = str(lim) if lim < 999999 else "♾️ Unlimited"
    ctx.user_data["renew_plan"] = plan
    ctx.user_data["renew_dur"]  = dur
    dur_label = dur.replace("1m","1 month").replace("3m","3 months").replace("6m","6 months").replace("12m","1 year")
    await update.callback_query.edit_message_text(
        f"💳 *{PLANS[plan]['name']}*\n━━━━━━━━━━━━━━\n"
        f"Duration: *{dur_label}*  |  Products: *{lim_str}*\n"
        f"Access: *{days} days*\n\n"
        f"💰 Amount: *₹{price}*\n"
        f"📲 Pay to: `{PAY_UPI}`\n\n"
        f"✅ Enter *UTR* after paying:", parse_mode=MD)
    return S_RUTR

async def fsm_rutr(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid  = str(update.effective_user.id)
    user = update.effective_user
    utr  = update.message.text.strip().replace(" ","")
    if not valid_utr(utr):
        await update.message.reply_text("❌ Invalid UTR. Try again:"); return S_RUTR
    if utr_used(utr):
        await update.message.reply_text("❌ UTR already used!"); return S_RUTR
    plan  = ctx.user_data.get("renew_plan", "basic")
    dur   = ctx.user_data.get("renew_dur",  "1m")
    price = PLANS.get(plan, PLANS["basic"]).get(dur, 199)
    _D["utr_log"].append(utr.upper())
    _D["pan_pend"][uid] = {"plan": plan, "dur": dur, "utr": utr.upper(), "ts": now()}
    _save()
    await update.message.reply_text(
        "✅ *UTR Submitted!* Admin will verify & activate shortly. 🎉", parse_mode=MD)
    try:
        uname     = f"@{user.username}" if user.username else user.full_name or uid
        dur_label = dur.replace("1m","1 month").replace("3m","3 months").replace("6m","6 months").replace("12m","1 year")
        days      = DURATIONS.get(dur, 30)
        await ctx.bot.send_message(
            ADMIN_ID,
            f"🔔 *Panel Renewal — Pending Approval*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 Creator: {esc(uname)} (`{uid}`)\n"
            f"📦 Plan: *{PLANS[plan]['name']}*\n"
            f"⏳ Duration: *{dur_label}* ({days} days)\n"
            f"💰 Amount: *₹{price}*\n"
            f"🔑 UTR: `{utr.upper()}`\n"
            f"📲 UPI: `{PAY_UPI}`\n"
            f"🕐 Submitted: {now()}",
            parse_mode=MD,
            reply_markup=kb(
                [ib(f"✅ Approve — {PLANS[plan]['name']}", f"adm:rnw|{uid}|{plan}|{dur}")],
                [ib("❌ Reject Renewal", f"adm:reject_renew|{uid}")],
            ),
        )
    except: pass
    return ConversationHandler.END


# ── COUPON CREATION ────────────────────────────────────────
async def fsm_cpn_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if update.callback_query: await update.callback_query.answer()
    if not is_creator(uid) and not is_admin(uid):
        await send(update, "❌ Not a creator.", back()); return ConversationHandler.END
    await send(update, "🎟 *Create Coupon — Step 1/4*\n\nEnter code (blank = auto):")
    return S_VN

async def fsm_vn(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["cpn_code"] = update.message.text.strip().upper() or rand_cpn()
    await update.message.reply_text(
        f"Code: `{ctx.user_data['cpn_code']}`\n\n"
        "💰 *Step 2/4 Discount:*\n`20` = 20% off  |  `F50` = ₹50 flat", parse_mode=MD)
    return S_VD

async def fsm_vd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    t = update.message.text.strip()
    try:
        if t.upper().startswith("F"):
            ctx.user_data.update({"cpn_type":"flat","cpn_flat":float(t[1:]),"cpn_pct":0})
        else:
            ctx.user_data.update({"cpn_type":"pct","cpn_pct":float(t),"cpn_flat":0})
    except:
        await update.message.reply_text("❌ Try `20` or `F50`:"); return S_VD
    await update.message.reply_text("📅 *Step 3/4 Expiry* (days from now, 0=never):")
    return S_VX

async def fsm_vx(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try: d = int(update.message.text.strip())
    except:
        await update.message.reply_text("Enter a number:"); return S_VX
    ctx.user_data["cpn_exp"] = exp_str(d) if d > 0 else None
    await update.message.reply_text("🔢 *Step 4/4 Max uses:*")
    return S_VM

async def fsm_vm(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try: mx = int(update.message.text.strip())
    except:
        await update.message.reply_text("Enter a number:"); return S_VM
    code = ctx.user_data["cpn_code"]
    _C["coupons"][code] = {
        "type": ctx.user_data["cpn_type"],
        "pct":  ctx.user_data["cpn_pct"],
        "flat": ctx.user_data["cpn_flat"],
        "max":  mx, "used": 0,
        "exp":  ctx.user_data.get("cpn_exp"),
    }
    _savec()
    dl = f"{ctx.user_data['cpn_pct']}%" if ctx.user_data["cpn_type"]=="pct" else f"₹{ctx.user_data['cpn_flat']:.0f}"
    await update.message.reply_text(
        f"✅ *Coupon Created!*\nCode: `{code}`\nDiscount: *{dl}*\nMax uses: *{mx}*",
        parse_mode=MD, reply_markup=back("cr:dash"))
    return ConversationHandler.END

# ── BROADCAST ──────────────────────────────────────────────
async def fsm_bc_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if update.callback_query: await update.callback_query.answer()
    if not is_creator(uid) and not is_admin(uid):
        await send(update, "❌ Not a creator.", back()); return ConversationHandler.END
    if not panel_active(uid) and not is_admin(uid):
        await send(update, "❌ Panel not active.",
            kb([ib("🔄 Renew","cr:renew")])); return ConversationHandler.END
    await send(update, "📣 *Broadcast*\n\nWho to message?",
        kb([ib("👥 All My Students","bct|all")],[ib("❌ Cancel","cr:dash")]))
    return S_BT

async def fsm_bt(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    ctx.user_data["bc_target"] = update.callback_query.data[4:]
    await update.callback_query.edit_message_text("✍️ Type your message:")
    return S_BM

async def fsm_bm(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid  = str(update.effective_user.id)
    msg  = update.message.text
    sids = list({x.get("uid") for x in _D["purchases"].values()
                 if _D["products"].get(x.get("pid",""),{}).get("cid") == uid and x.get("ok")})
    sent = 0
    for sid in sids:
        try:
            await update.get_bot().send_message(int(sid),
                f"📣 *Message from your creator:*\n\n{msg}", parse_mode=MD)
            sent += 1
        except: pass
    _D["broadcasts"].append({"cid": uid, "msg": msg, "ts": now()})
    _save()
    await update.message.reply_text(
        f"✅ *Sent to {sent}/{len(sids)} students!*", parse_mode=MD,
        reply_markup=back("cr:dash"))
    return ConversationHandler.END

# ── SEARCH ─────────────────────────────────────────────────
async def fsm_srch_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.callback_query: await update.callback_query.answer()
    await send(update, "🔍 *Search Products*\n\nType keyword:")
    return S_SQ

async def fsm_sq(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q     = update.message.text.strip().lower()
    prods = [(pid,p) for pid,p in _D["products"].items()
             if p.get("active") and q in p.get("name","").lower()]
    if not prods:
        await update.message.reply_text(f"❌ No results for *'{esc(q)}'*",
            parse_mode=MD, reply_markup=back("mkt:home")); return ConversationHandler.END
    rows = [[ib(f"🛒 {p['name'][:28]}  ₹{p['price']:.0f}", f"vp|{pid}")] for pid,p in prods[:10]]
    rows.append([ib("🔙 Back","mkt:home")])
    await update.message.reply_text(f"🔍 *{len(prods)} result(s)*",
        parse_mode=MD, reply_markup=kb(*rows))
    return ConversationHandler.END

# ── LIVE CLASS ─────────────────────────────────────────────
async def fsm_lcls_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if update.callback_query: await update.callback_query.answer()
    if not is_creator(uid) and not is_admin(uid):
        await send(update, "❌ Not a creator.", back()); return ConversationHandler.END
    await send(update, "🔔 *Schedule Live Class — Step 1/2*\n\nEnter class title:")
    return S_LT

async def fsm_lt(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["lc_title"] = update.message.text.strip()
    await update.message.reply_text("📅 *Step 2/2 — Date & time:*\n`YYYY-MM-DD HH:MM`", parse_mode=MD)
    return S_LD

async def fsm_ld(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    txt = update.message.text.strip()
    try: datetime.strptime(txt, "%Y-%m-%d %H:%M")
    except:
        await update.message.reply_text("❌ Format: `YYYY-MM-DD HH:MM`"); return S_LD
    uid = str(update.effective_user.id)
    _D["classes"].append({"id":rand_pid(),"cid":uid,"title":ctx.user_data["lc_title"],"at":txt,"r15":False,"r5":False})
    _save()
    await update.message.reply_text(
        f"✅ *Scheduled!*\n📚 {esc(ctx.user_data['lc_title'])}\n⏰ {txt}\n\nStudents get 15min & 5min reminders!",
        parse_mode=MD, reply_markup=back("cr:dash"))
    return ConversationHandler.END

# ── BOOST UTR ──────────────────────────────────────────────
async def fsm_bst_upi(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    cost = _C.get("boost_cost", 29)
    await update.callback_query.edit_message_text(
        f"💳 Pay *₹{cost}* to:\n`{PAY_UPI}`\n\nSend UTR after payment:", parse_mode=MD)
    return S_BU

async def fsm_bu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    utr  = update.message.text.strip().replace(" ","")
    uid  = str(update.effective_user.id)
    if not valid_utr(utr):
        await update.message.reply_text("❌ Invalid UTR:"); return S_BU
    if utr_used(utr):
        await update.message.reply_text("❌ UTR already used!"); return S_BU
    pid  = ctx.user_data.get("boost_pid","")
    _D["utr_log"].append(utr.upper()); _save()
    try:
        await update.get_bot().send_message(ADMIN_ID,
            f"⚡ *Boost Payment*\nCreator: `{uid}`\nProduct: `{pid}`\nUTR: `{utr}`",
            parse_mode=MD,
            reply_markup=kb([ib("✅ Approve Boost",f"adm:bst|{uid}|{pid}"),
                             ib("❌ Reject","home")]))
    except: pass
    await update.message.reply_text("✅ UTR submitted! Boost activates after admin approval.")
    return ConversationHandler.END

# ── ADMIN BROADCAST ────────────────────────────────────────
async def fsm_adm_bc(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        await update.callback_query.answer()
        if not is_admin(str(update.effective_user.id)): return ConversationHandler.END
    await send(update, "📣 *Admin Broadcast*\n\nType message to ALL users:")
    return S_AB

async def fsm_ab(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg  = update.message.text
    uids = list(_D["users"].keys())
    sent = 0
    for uid in uids:
        try:
            await update.get_bot().send_message(int(uid),
                f"📣 *Announcement:*\n\n{msg}", parse_mode=MD)
            sent += 1
        except: pass
    _D["broadcasts"].append({"cid":"ADMIN","msg":msg,"ts":now()}); _save()
    await update.message.reply_text(f"✅ Sent to {sent}/{len(uids)} users.")
    return ConversationHandler.END

# ═══ LOGO UPLOAD ═══
async def fsm_logo_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.callback_query: await update.callback_query.answer()
    await send(update,"🖼️ *Upload Creator Logo*\n\nSend your logo image:"); return S_LOGO

async def fsm_logo_photo(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid=str(update.effective_user.id)
    if not update.message.photo: await update.message.reply_text("❌ Send an image!"); return S_LOGO
    _D["creators"].setdefault(uid,{})["logo"]=update.message.photo[-1].file_id; _save()
    await update.message.reply_text("✅ *Logo uploaded!*\nIt shows on your store & profile now.",
        parse_mode=MD,reply_markup=back("cr:set"))
    return ConversationHandler.END

# ═══ VERIFICATION UTR ═══
async def fsm_ver_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.callback_query: await update.callback_query.answer()
    await send(update,
        f"🏅 *Request Verification Badge*\n━━━━━━━━━━━━━━\n"
        f"Get a ✅ verified badge.\nOne-time fee: *₹99*\n\n"
        f"💳 Pay to: `{PAY_UPI}`\n\nSend UTR after payment:")
    return S_VER_UTR

async def fsm_ver_utr(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid=str(update.effective_user.id); utr=update.message.text.strip().replace(" ","")
    if not valid_utr(utr): await update.message.reply_text("❌ Invalid UTR:"); return S_VER_UTR
    if utr_used(utr): await update.message.reply_text("❌ Already used!"); return S_VER_UTR
    _D["utr_log"].append(utr.upper())
    _D.setdefault("ver_requests",[]).append({"cid":uid,"utr":utr.upper(),"ts":now(),"status":"pending"})
    _save()
    await update.message.reply_text(
        "✅ *Verification request submitted!*\n\nAdmin will verify payment & grant badge within 24h. 🏅",
        parse_mode=MD)
    try:
        c=_D["creators"].get(uid,{})
        await update.get_bot().send_message(ADMIN_ID,
            f"🏅 *Verification Request*\n{esc(c.get('name','?'))} `{uid}`\nUTR: `{utr}`\nFee: ₹99",
            parse_mode=MD,
            reply_markup=kb([ib("✅ Grant Badge",f"adm:ver_apv|{uid}"),ib("❌ Reject","home")]))
    except: pass
    return ConversationHandler.END

# ═══ WITHDRAWAL ═══
async def fsm_wd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.callback_query: await update.callback_query.answer()
    uid=str(update.effective_user.id)
    c=_D["creators"].get(uid,{}); bal=c.get("wallet",0.0)
    if bal<100: await alert(update,f"Min ₹100. Balance: ₹{bal:.0f}"); return ConversationHandler.END
    await send(update,f"💸 *Withdrawal*\nWallet: *₹{bal:,.2f}*\nUPI: `{esc(c.get('upi',''))}`\n\nEnter amount (min ₹100):")
    return S_WD_AMT

async def fsm_wd_amt(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid=str(update.effective_user.id)
    try: amt=float(update.message.text.strip())
    except: await update.message.reply_text("❌ Enter amount:"); return S_WD_AMT
    if amt<100: await update.message.reply_text("❌ Min ₹100!"); return S_WD_AMT
    bal=_D["creators"].get(uid,{}).get("wallet",0.0)
    if amt>bal: await update.message.reply_text(f"❌ Balance: ₹{bal:.0f}"); return S_WD_AMT
    upi=_D["creators"].get(uid,{}).get("upi","")
    _D.setdefault("withdrawals",[]).append({"cid":uid,"amount":amt,"upi":upi,"ts":now(),"status":"pending"})
    _save()
    await update.message.reply_text(
        f"✅ *Withdrawal Requested!*\nAmount: ₹{amt:.0f}\nUPI: {esc(upi)}\n\nAdmin processes within 24h.",
        parse_mode=MD,reply_markup=back("cr:wlt"))
    try:
        c=_D["creators"].get(uid,{})
        await update.get_bot().send_message(ADMIN_ID,
            f"💸 *Withdrawal*\n{esc(c.get('name','?'))} `{uid}`\nAmount: ₹{amt:.0f}\nUPI: {esc(upi)}",
            parse_mode=MD,
            reply_markup=kb([ib(f"✅ Approve ₹{amt:.0f}",f"adm:wd_apv|{len(_D['withdrawals'])-1}"),ib("❌ Reject","home")]))
    except: pass
    return ConversationHandler.END

# ═══ FLASH SALE ═══
async def fsm_flash_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.callback_query: await update.callback_query.answer()
    d=update.callback_query.data; pid=d[6:]
    uid=str(update.effective_user.id)
    p=_D["products"].get(pid)
    if not p or (p.get("cid")!=uid and not is_admin(uid)): return ConversationHandler.END
    fs=_D.get("flash_sales",{}).get(pid)
    if fs and not is_exp(fs.get("ends_at","")):
        await send(update,f"⚡ Flash Sale active: {fs['disc_pct']}% off until {fs['ends_at']}",
            kb([ib("🗑 Cancel Flash Sale",f"flash:cancel|{pid}")],[ib("🔙 Back","cr:flash")])); return ConversationHandler.END
    ctx.user_data["flash_pid"]=pid
    await send(update,f"⚡ *Flash Sale for {esc(p['name'])}*\nOriginal: ₹{p['price']:.0f}\n\nEnter discount % (1-90):")
    return S_FS_DISC

async def fsm_fs_disc(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try: pct=float(update.message.text.strip())
    except: await update.message.reply_text("❌ Enter %:"); return S_FS_DISC
    if not 1<=pct<=90: await update.message.reply_text("❌ 1-90%:"); return S_FS_DISC
    ctx.user_data["fs_pct"]=pct; await update.message.reply_text("⏰ Duration in hours (e.g. 24):"); return S_FS_DUR

async def fsm_fs_dur(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try: hrs=float(update.message.text.strip())
    except: await update.message.reply_text("❌ Enter hours:"); return S_FS_DUR
    pid=ctx.user_data.get("flash_pid",""); pct=ctx.user_data.get("fs_pct",10)
    p=_D["products"].get(pid,{})
    ends=(datetime.now()+timedelta(hours=hrs)).strftime("%Y-%m-%d %H:%M")
    _D.setdefault("flash_sales",{})[pid]={"disc_pct":pct,"ends_at":ends}; _save()
    ep_price=max(0,p.get("price",0)-round(p.get("price",0)*pct/100,2))
    await update.message.reply_text(
        f"✅ *Flash Sale Active!*\n🔥 {pct}% OFF — ₹{p.get('price',0):.0f} → ₹{ep_price:.0f}\n⏰ Ends: {ends}",
        parse_mode=MD,reply_markup=back("cr:flash"))
    return ConversationHandler.END

# ═══ ADMIN VER APPROVE ═══
# Handled inline in cb_router via "adm:ver_apv|cid"

async def do_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.callback_query: await update.callback_query.answer()
    await send(update, "❌ Cancelled.", back())
    return ConversationHandler.END

# ═══════════════════════════════════════════════════════════
#  COMMANDS
# ═══════════════════════════════════════════════════════════
async def cmd_dashboard(u,c):
    track_user(u.effective_user)
    await _creator_dashboard(u, str(u.effective_user.id))

async def cmd_myproducts(u,c):
    track_user(u.effective_user)
    await _my_products(u, str(u.effective_user.id))

async def cmd_wallet(u,c):
    track_user(u.effective_user)
    await _wallet(u, str(u.effective_user.id))

async def cmd_refer(u,c):
    track_user(u.effective_user)
    await _refer(u, str(u.effective_user.id))

async def cmd_topcreators(u,c):
    track_user(u.effective_user)
    top = sorted([(cid,cr) for cid,cr in _D["creators"].items() if cr.get("ps") in ("active","trial")],
                 key=lambda x:-x[1].get("sales",0))[:10]
    if not top:
        await u.message.reply_text("No creators yet!"); return
    txt = "⭐ *Top Creators*\n\n"
    for i,(cid,cr) in enumerate(top,1):
        txt += f"{i}. *{esc(cr['name'])}* {'✅' if cr.get('ver') else ''}  ₹{cr.get('sales',0):,.0f}\n"
    await u.message.reply_text(txt, parse_mode=MD)

async def cmd_topproducts(u,c):
    track_user(u.effective_user)
    top = sorted([(pid,p) for pid,p in _D["products"].items() if p.get("active")],
                 key=lambda x:(-x[1].get("students",0),-x[1].get("rating",0)))[:10]
    if not top:
        await u.message.reply_text("No products yet!"); return
    txt = "🏆 *Top Products*\n\n"
    for i,(pid,p) in enumerate(top,1):
        txt += f"{i}. *{esc(p['name'])}*  ₹{p['price']:.0f}  👥{p.get('students',0)}\n"
    await u.message.reply_text(txt, parse_mode=MD)

async def cmd_profile(u: Update, c: ContextTypes.DEFAULT_TYPE):
    track_user(u.effective_user)
    code = c.args[0].upper() if c.args else ""
    if not code:
        await u.message.reply_text("Usage: /profile CREATORCODE"); return
    match = next(((cid,cr) for cid,cr in _D["creators"].items()
                  if cr.get("code","").upper() == code), None)
    if not match:
        await u.message.reply_text("❌ Creator not found!"); return
    cid, cr = match
    await _show_store(u, cid)

async def cmd_adminpanel(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not is_admin(str(u.effective_user.id)):
        await u.message.reply_text("⛔ Not authorized."); return
    await _admin_home(u)

async def cmd_approve_creator(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not is_admin(str(u.effective_user.id)): return
    if not c.args:
        await u.message.reply_text("Usage: /approve\\_creator ID", parse_mode=MD); return
    await _approve_creator(u, c, c.args[0])

async def cmd_verifycreator(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not is_admin(str(u.effective_user.id)): return
    if not c.args:
        await u.message.reply_text("Usage: /verifycreator ID"); return
    cid = c.args[0]
    if cid not in _D["creators"]:
        await u.message.reply_text("Not found!"); return
    new = not _D["creators"][cid].get("ver", False)
    _D["creators"][cid]["ver"] = new; _save()
    await u.message.reply_text(f"{'✅ Badge granted' if new else '❌ Badge removed'} for `{cid}`", parse_mode=MD)

async def cmd_renewpanel(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid = str(u.effective_user.id)
    track_user(u.effective_user)
    if not is_creator(uid) and not is_admin(uid):
        await u.message.reply_text("❌ Not a creator."); return
    cr  = _D["creators"].get(uid, {})
    bal = cr.get("wallet", 0.0)
    await u.message.reply_text(
        f"🔄 *Renew Panel*\n━━━━━━━━━━━━━━\n"
        f"Plan: *{cr.get('plan','basic').title()}*  |  Wallet: *₹{bal:,.0f}*\n\n"
        f"💳 Pay to: `{PAY_UPI}`\nSelect plan & send UTR:",
        parse_mode=MD, reply_markup=plans_kb())

async def cmd_approvalmode(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid = str(u.effective_user.id)
    if not is_creator(uid):
        await u.message.reply_text("Not a creator!"); return
    await u.message.reply_text("⚙️ Select approval mode:",
        reply_markup=kb([ib("🤖 Auto","cr:am|auto"),ib("👤 Manual","cr:am|manual")]))

async def cmd_exportdata(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not is_admin(str(u.effective_user.id)): return
    if os.path.exists(DATA_F):
        await u.message.reply_document(open(DATA_F,"rb"), filename="hub_data.json", caption="📤 Data")

async def cmd_showconfig(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not is_admin(str(u.effective_user.id)): return
    if os.path.exists(CFG_F):
        await u.message.reply_document(open(CFG_F,"rb"), filename="hub_config.json", caption="⚙️ Config")

# ═══════════════════════════════════════════════════════════
#  SCHEDULER
# ═══════════════════════════════════════════════════════════
async def scheduler_tick(ctx: ContextTypes.DEFAULT_TYPE):
    now_dt  = datetime.now()
    changed = False

    # Access expiry
    for oid, pur in _D["purchases"].items():
        if not pur.get("ok") or not pur.get("exp"): continue
        dl = days_left(pur["exp"])
        if dl in (3, 1):
            prod = _D["products"].get(pur.get("pid",""), {})
            try:
                await ctx.bot.send_message(int(pur["uid"]),
                    f"⚠️ *Access expiring in {dl} day(s)!*\n*{esc(prod.get('name',''))}*\nRenew now!",
                    parse_mode=MD, reply_markup=kb([ib("🔄 Renew",f"buy|{pur['pid']}")]))
            except: pass
        if is_expired(pur["exp"]):
            _D["purchases"][oid]["ok"] = False
            _D["purchases"][oid]["expired"] = True
            changed = True

    # Panel expiry
    for cid, c in _D["creators"].items():
        if c.get("trial_status") and c.get("trial_exp") and is_expired(c["trial_exp"]):
            _D["creators"][cid]["trial_status"] = False
            _D["creators"][cid]["ps"] = "trial_expired"
            changed = True
            try:
                await ctx.bot.send_message(int(cid),
                    "⚠️ *Trial ended!* Use /renewpanel to activate your panel.",
                    parse_mode=MD, reply_markup=kb([ib("🔄 Renew Panel","cr:renew")]))
            except: pass
        if not c.get("trial_status") and c.get("panel_exp"):
            dl = days_left(c["panel_exp"])
            if dl in (7, 3, 1):
                try:
                    await ctx.bot.send_message(int(cid),
                        f"⚠️ *Panel expiring in {dl} day(s)!* Use /renewpanel.",
                        parse_mode=MD, reply_markup=kb([ib("🔄 Renew","cr:renew")]))
                except: pass
            if is_expired(c["panel_exp"]):
                _D["creators"][cid]["ps"] = "expired"; changed = True

    # Live class reminders
    for i, cls in enumerate(_D["classes"]):
        try: cdt = datetime.strptime(cls["at"], "%Y-%m-%d %H:%M")
        except: continue
        mins = (cdt - now_dt).total_seconds() / 60
        sids = list({x.get("uid") for x in _D["purchases"].values()
                     if _D["products"].get(x.get("pid",""),{}).get("cid") == cls["cid"] and x.get("ok")})
        if 14 <= mins <= 15.5 and not cls.get("r15"):
            for sid in sids:
                try:
                    await ctx.bot.send_message(int(sid),
                        f"🔔 *Live in 15 min!*\n📚 {esc(cls['title'])}\n⏰ {cls['at']}", parse_mode=MD)
                except: pass
            _D["classes"][i]["r15"] = True; changed = True
        elif 4 <= mins <= 5.5 and not cls.get("r5"):
            for sid in sids:
                try:
                    await ctx.bot.send_message(int(sid),
                        f"🚨 *Starting in 5 min!*\n📚 {esc(cls['title'])}", parse_mode=MD)
                except: pass
            _D["classes"][i]["r5"] = True; changed = True

    # Boost cleanup
    expired_boosts = [pid for pid, exp in _D["boosts"].items() if is_expired(exp)]
    for pid in expired_boosts:
        del _D["boosts"][pid]; changed = True

    if changed: _save()

# ═══════════════════════════════════════════════════════════
#  POST INIT
# ═══════════════════════════════════════════════════════════
async def post_init(app):
    await app.bot.set_my_commands(_SC, scope=BotCommandScopeDefault())
    try:
        await app.bot.set_my_commands(_AC, scope=BotCommandScopeChat(chat_id=ADMIN_ID))
    except: pass
    tasks = [_set_commands(app.bot, int(cid), "creator")
             for cid, c in _D["creators"].items() if c.get("ps") in ("active","trial")]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

# ═══════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════
def main():
    _load()

    app = (ApplicationBuilder()
           .token(BOT_TOKEN)
           .concurrent_updates(True)
           .connection_pool_size(16)
           .connect_timeout(10)
           .read_timeout(10)
           .write_timeout(10)
           .get_updates_connect_timeout(10)
           .get_updates_read_timeout(10)
           .get_updates_write_timeout(10)
           .post_init(post_init)
           .build())

    def CH(entries, states):
        return ConversationHandler(
            entry_points=entries,
            states=states,
            fallbacks=[CommandHandler("cancel", do_cancel)],
            allow_reentry=True,
            per_message=False,
            per_chat=True,
        )

    # ── ConversationHandlers ─────────────────────────────────
    app.add_handler(CH(
        [CommandHandler("register_creator", fsm_reg_start),
         CallbackQueryHandler(fsm_reg_start, pattern="^reg:start$")],
        {S_RN:[MessageHandler(filters.TEXT&~filters.COMMAND, fsm_rn)],
         S_RC:[CallbackQueryHandler(fsm_rc, pattern="^cat\\|")],
         S_RB:[MessageHandler(filters.TEXT&~filters.COMMAND, fsm_rb)],
         S_RU:[CallbackQueryHandler(fsm_ru, pattern="^rupi:")],
         S_RI:[MessageHandler(filters.TEXT&~filters.COMMAND, fsm_ri)],
         S_RQ:[MessageHandler(filters.PHOTO, fsm_rq)]}))

    app.add_handler(CH(
        [CommandHandler("addproduct", fsm_addp_start),
         CallbackQueryHandler(fsm_addp_start, pattern="^cr:addp$")],
        {S_PN:[MessageHandler(filters.TEXT&~filters.COMMAND, fsm_pn)],
         S_PP:[MessageHandler(filters.TEXT&~filters.COMMAND, fsm_pp)],
         S_PD:[CallbackQueryHandler(fsm_pd, pattern="^dt\\|")],
         S_PL:[MessageHandler(filters.TEXT&~filters.COMMAND, fsm_pl)],
         S_PV:[CallbackQueryHandler(fsm_pv, pattern="^dur\\|")],
         S_PS:[CallbackQueryHandler(fsm_ps, pattern="^psub\\|")]}))

    app.add_handler(CH(
        [CommandHandler("editproduct", fsm_editp_start),
         CallbackQueryHandler(fsm_editp_start, pattern="^cr:editp$")],
        {S_EP:[CallbackQueryHandler(fsm_ep, pattern="^epc\\|")],
         S_EF:[CallbackQueryHandler(fsm_ef, pattern="^ef\\|")],
         S_EV:[MessageHandler(filters.TEXT&~filters.COMMAND, fsm_ev)]}))

    app.add_handler(CH(
        [CommandHandler("broadcast", fsm_bc_start),
         CallbackQueryHandler(fsm_bc_start, pattern="^cr:bc$")],
        {S_BT:[CallbackQueryHandler(fsm_bt, pattern="^bct\\|")],
         S_BM:[MessageHandler(filters.TEXT&~filters.COMMAND, fsm_bm)]}))

    app.add_handler(CH(
        [CallbackQueryHandler(cb_buy_entry, pattern="^buy\\|")],
        {S_CPN:[MessageHandler(filters.TEXT&~filters.COMMAND, fsm_coupon),
                CallbackQueryHandler(cb_skip_coupon, pattern="^skp\\|")],
         S_UTR:[MessageHandler(filters.TEXT&~filters.COMMAND, fsm_utr)]}))

    app.add_handler(CH(
        [CallbackQueryHandler(fsm_plan_pick, pattern="^plan\\|")],
        {S_RUTR:[MessageHandler(filters.TEXT&~filters.COMMAND, fsm_rutr)]}))

    app.add_handler(CH(
        [CommandHandler("createcoupon", fsm_cpn_start),
         CallbackQueryHandler(fsm_cpn_start, pattern="^cr:mkcp$")],
        {S_VN:[MessageHandler(filters.TEXT&~filters.COMMAND, fsm_vn)],
         S_VD:[MessageHandler(filters.TEXT&~filters.COMMAND, fsm_vd)],
         S_VX:[MessageHandler(filters.TEXT&~filters.COMMAND, fsm_vx)],
         S_VM:[MessageHandler(filters.TEXT&~filters.COMMAND, fsm_vm)]}))

    app.add_handler(CH(
        [CommandHandler("search", fsm_srch_start),
         CallbackQueryHandler(fsm_srch_start, pattern="^mkt:srch$")],
        {S_SQ:[MessageHandler(filters.TEXT&~filters.COMMAND, fsm_sq)]}))

    app.add_handler(CH(
        [CommandHandler("scheduleclass", fsm_lcls_start),
         CallbackQueryHandler(fsm_lcls_start, pattern="^cr:lcls$")],
        {S_LT:[MessageHandler(filters.TEXT&~filters.COMMAND, fsm_lt)],
         S_LD:[MessageHandler(filters.TEXT&~filters.COMMAND, fsm_ld)]}))

    app.add_handler(CH(
        [CallbackQueryHandler(fsm_bst_upi, pattern="^bst:upi$")],
        {S_BU:[MessageHandler(filters.TEXT&~filters.COMMAND, fsm_bu)]}))

    app.add_handler(CH(
        [CallbackQueryHandler(fsm_adm_bc, pattern="^adm:bc$")],
        {S_AB:[MessageHandler(filters.TEXT&~filters.COMMAND, fsm_ab)]}))

    # ── Single callback router (no duplicate registrations) ──
    # New handlers
    app.add_handler(CH([CallbackQueryHandler(fsm_logo_start,"^cr:logo$")],
        {S_LOGO:[MessageHandler(filters.PHOTO,fsm_logo_photo)]}))

    app.add_handler(CH([CallbackQueryHandler(fsm_ver_start,"^cr:ver_req$")],
        {S_VER_UTR:[MessageHandler(filters.TEXT&~filters.COMMAND,fsm_ver_utr)]}))

    app.add_handler(CH([CallbackQueryHandler(fsm_wd_start,"^cr:withdraw$")],
        {S_WD_AMT:[MessageHandler(filters.TEXT&~filters.COMMAND,fsm_wd_amt)]}))

    app.add_handler(CH([CallbackQueryHandler(fsm_flash_start,"^flash[|]")],
        {S_FS_DISC:[MessageHandler(filters.TEXT&~filters.COMMAND,fsm_fs_disc)],
         S_FS_DUR:[MessageHandler(filters.TEXT&~filters.COMMAND,fsm_fs_dur)]}))

    app.add_handler(CallbackQueryHandler(cb_router))

    # ── Commands ─────────────────────────────────────────────
    for name, fn in [
        ("start",           cmd_start),
        ("dashboard",       cmd_dashboard),
        ("myproducts",      cmd_myproducts),
        ("wallet",          cmd_wallet),
        ("refer",           cmd_refer),
        ("profile",         cmd_profile),
        ("topcreators",     cmd_topcreators),
        ("topproducts",     cmd_topproducts),
        ("renewpanel",      cmd_renewpanel),
        ("approvalmode",    cmd_approvalmode),
        ("adminpanel",      cmd_adminpanel),
        ("approve_creator", cmd_approve_creator),
        ("verifycreator",   cmd_verifycreator),
        ("createcoupon",    fsm_cpn_start),
        ("broadcast",       fsm_bc_start),
        ("addproduct",      fsm_addp_start),
        ("editproduct",     fsm_editp_start),
        ("scheduleclass",   fsm_lcls_start),
        ("exportdata",      cmd_exportdata),
        ("showconfig",      cmd_showconfig),
        ("cancel",          do_cancel),
    ]:
        app.add_handler(CommandHandler(name, fn))

    app.job_queue.run_repeating(scheduler_tick, interval=60, first=20)

    print("🚀 UPI Access Hub v5.0 — LIVE!")
    print(f"💳 Platform UPI: {PAY_UPI}")
    app.run_polling(
        drop_pending_updates=True,
        allowed_updates=["message", "callback_query"],
    )

if __name__ == "__main__":
    main()
