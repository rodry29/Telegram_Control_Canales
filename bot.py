import sqlite3
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ContextTypes,
    CallbackQueryHandler
)
from apscheduler.schedulers.background import BackgroundScheduler

# -------- CONFIG --------
TOKEN = "8782944509:AAFqTBOCPwJdhRgt2Qxx4Usj45DNF83Y86s"
VIP_GROUP_ID = -1003842587095
ADMIN_ID = 8682208062

# -------- DB --------
conn = sqlite3.connect("database.db", check_same_thread=False)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    username TEXT PRIMARY KEY,
    plan TEXT,
    start_date TEXT,
    end_date TEXT,
    status TEXT,
    trial_used INTEGER DEFAULT 0
)
""")
conn.commit()

# -------- PLANES --------
PLANS = {
    "trial": 1,
    "semanal": 7,
    "mensual": 30
}

# -------- MENU --------
def main_menu():
    keyboard = [
        [InlineKeyboardButton("➕ Agregar", callback_data="add")],
        [InlineKeyboardButton("🔄 Renovar", callback_data="renew")],
        [InlineKeyboardButton("📊 Activos", callback_data="list")],
        [InlineKeyboardButton("📢 Broadcast", callback_data="broadcast")]
    ]
    return InlineKeyboardMarkup(keyboard)

# -------- START --------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != ADMIN_ID:
        return
    await update.message.reply_text("📊 Panel CRM", reply_markup=main_menu())

# -------- BOTONES PANEL --------
async def panel_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.from_user.id != ADMIN_ID:
        return

    if query.data == "add":
        await query.message.reply_text("Formato:\n/add @usuario plan")

    elif query.data == "renew":
        await query.message.reply_text("Formato:\n/renovar @usuario plan")

    elif query.data == "list":
        cursor.execute("SELECT username, end_date FROM users WHERE status='activo'")
        users = cursor.fetchall()

        msg = "🟢 Activos:\n"
        for u in users[:30]:
            msg += f"{u[0]} → {u[1][:10]}\n"

        await query.message.reply_text(msg)

    elif query.data == "broadcast":
        await query.message.reply_text("Usa:\n/msg mensaje")

# -------- AGREGAR --------
async def add_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != ADMIN_ID:
        return

    try:
        username = context.args[0]
        plan = context.args[1]

        if plan not in PLANS:
            await update.message.reply_text("Plan inválido")
            return

        cursor.execute("SELECT * FROM users WHERE username=?", (username,))
        user = cursor.fetchone()

        if plan == "trial" and user and user[5] == 1:
            await update.message.reply_text("❌ Ya usó el trial")
            return

        days = PLANS[plan]
        start = datetime.now()
        end = start + timedelta(days=days)

        trial_used = 1 if plan == "trial" else (user[5] if user else 0)

        cursor.execute("""
        INSERT OR REPLACE INTO users VALUES (?,?,?,?,?,?)
        """, (username, plan, start.isoformat(), end.isoformat(), "activo", trial_used))
        conn.commit()

        try:
            await context.bot.unban_chat_member(VIP_GROUP_ID, username)
        except:
            pass

        await update.message.reply_text(f"✅ {username} activo hasta {end.date()}")

    except:
        await update.message.reply_text("Error en formato")

# -------- RENOVAR --------
async def renew_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != ADMIN_ID:
        return

    username = context.args[0]
    plan = context.args[1]

    if plan == "trial":
        await update.message.reply_text("No se puede renovar trial")
        return

    days = PLANS[plan]
    end = datetime.now() + timedelta(days=days)

    cursor.execute("""
    UPDATE users SET plan=?, end_date=?, status='activo'
    WHERE username=?
    """, (plan, end.isoformat(), username))
    conn.commit()

    await update.message.reply_text("🔄 Renovado")

# -------- BOTONES ALERTA --------
def alert_buttons(username):
    keyboard = [
        [
            InlineKeyboardButton("🔄 Renovar 7d", callback_data=f"renew7|{username}"),
            InlineKeyboardButton("📆 Renovar 30d", callback_data=f"renew30|{username}")
        ],
        [
            InlineKeyboardButton("➕ +1 día", callback_data=f"extend1|{username}"),
            InlineKeyboardButton("🚪 Expulsar", callback_data=f"kick|{username}")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

# -------- ACCIONES ALERTA --------
async def alert_actions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.from_user.id != ADMIN_ID:
        return

    action, username = query.data.split("|")

    if action == "renew7":
        days = 7
    elif action == "renew30":
        days = 30
    elif action == "extend1":
        days = 1
    elif action == "kick":
        try:
            await context.bot.ban_chat_member(VIP_GROUP_ID, username)
        except:
            pass

        cursor.execute("UPDATE users SET status='vencido' WHERE username=?", (username,))
        conn.commit()

        await query.message.reply_text(f"🚪 {username} expulsado")
        return

    end = datetime.now() + timedelta(days=days)

    cursor.execute("""
    UPDATE users SET end_date=?, status='activo' WHERE username=?
    """, (end.isoformat(), username))
    conn.commit()

    await query.message.reply_text(f"✅ {username} actualizado")

# -------- RECORDATORIOS --------
async def check_expired():
    now = datetime.now()

    cursor.execute("SELECT username, plan, end_date FROM users WHERE status='activo'")
    users = cursor.fetchall()

    for username, plan, end_date in users:
        end = datetime.fromisoformat(end_date)
        days_left = (end - now).days

        if days_left == 1:
            msg = f"⚠️ Vence mañana\n{username}\nPlan: {plan}\nFecha: {end.date()}"
            from telegram import Bot

            bot = Bot(token=TOKEN)
            await bot.send_message(
                ADMIN_ID,
                msg,
                reply_markup=alert_buttons(username)
            )

        if now > end:
            msg = f"❌ Vencido\n{username}\nFecha: {end.date()}"
            await context.bot.send_message(
                ADMIN_ID,
                msg,
                reply_markup=alert_buttons(username)
            )

            try:
                await context.bot.ban_chat_member(VIP_GROUP_ID, username)
            except:
                pass

            cursor.execute("""
            UPDATE users SET status='vencido' WHERE username=?
            """, (username,))
            conn.commit()

# -------- BROADCAST --------
async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != ADMIN_ID:
        return

    msg = " ".join(context.args)

    cursor.execute("SELECT username FROM users WHERE status='activo'")
    users = cursor.fetchall()

    enviados = 0

    for (u,) in users:
        try:
            await context.bot.send_message(u, msg)
            enviados += 1
        except:
            pass

    await update.message.reply_text(f"📢 Enviado a {enviados}")

# -------- MAIN --------
app = ApplicationBuilder().token(TOKEN).build()

app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("add", add_user))
app.add_handler(CommandHandler("renovar", renew_user))
app.add_handler(CommandHandler("msg", broadcast))

app.add_handler(CallbackQueryHandler(panel_buttons))
app.add_handler(CallbackQueryHandler(alert_actions))

scheduler = BackgroundScheduler()
scheduler.add_job(check_expired, "interval", hours=6)
scheduler.start()

app.run_polling()
