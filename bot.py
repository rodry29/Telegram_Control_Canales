import psycopg2
import os
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ContextTypes,
    CallbackQueryHandler
)
from apscheduler.schedulers.background import BackgroundScheduler

scheduler = BackgroundScheduler()
          
# -------- CONFIG --------
TOKEN = "8782944509:AAFqTBOCPwJdhRgt2Qxx4Usj45DNF83Y86s"
VIP_GROUP_ID = -1003842587095
ADMIN_ID = 8682208062

# -------- DB --------
conn = psycopg2.connect(os.getenv("DATABASE_URL"))
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    username TEXT PRIMARY KEY,
    plan TEXT,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    status TEXT,
    trial_used INTEGER DEFAULT 0,
    payment_date TIMESTAMP DEFAULT NULL
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
        [InlineKeyboardButton("📊 Status de suscriptores", callback_data="list")],
        [InlineKeyboardButton("💰 Ganancias", callback_data="ganancias")]
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
        from datetime import datetime

        cursor.execute("SELECT username, end_date FROM users WHERE status='activo'")
        users = cursor.fetchall()

        if not users:
            await query.message.reply_text("No hay usuarios activos")
            return

        msg = "📊 STATUS DE SUSCRIPTORES\n\n"

        for username, end_date in users[:30]:
            end = end_date
            hoy = datetime.now()

            dias_restantes = (end - hoy).days

            msg += f"👤 {username}\n"
            msg += f"📅 Expira: {end.date()}\n"
            msg += f"⏳ Días restantes: {dias_restantes}\n\n"

        await query.message.reply_text(msg)

    elif query.data == "broadcast":
        await query.message.reply_text("Usa:\n/msg mensaje")

    elif query.data == "ganancias":
        from datetime import datetime, timedelta
    
        now = datetime.now()

        # Inicio mes actual
        inicio_mes = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        # Inicio mes anterior
        mes_anterior = (inicio_mes - timedelta(days=1)).replace(day=1)

        # ---- MES ACTUAL ----
        cursor.execute("""
        SELECT plan FROM users
        WHERE payment_date >= %s
        """, (inicio_mes,))
        data_actual = cursor.fetchall()

        # ---- MES ANTERIOR ----
        cursor.execute("""
        SELECT plan FROM users
        WHERE payment_date >= %s AND payment_date < %s
        """, (mes_anterior, inicio_mes))
        data_anterior = cursor.fetchall()

        PRICES = {
            "trial": 0,
            "semanal": 10,
            "mensual": 20
        }

        # ---- CALCULO MES ACTUAL ----
        total_actual = 0
        conteo_actual = {"trial": 0, "semanal": 0, "mensual": 0}

        for (plan,) in data_actual:
            conteo_actual[plan] += 1
            total_actual += PRICES.get(plan, 0)

        # ---- CALCULO MES ANTERIOR ----
        total_anterior = 0
        for (plan,) in data_anterior:
            total_anterior += PRICES.get(plan, 0)

        # ---- CRECIMIENTO ----
        if total_anterior > 0:
            crecimiento = ((total_actual - total_anterior) / total_anterior) * 100
        else:
            crecimiento = 100 if total_actual > 0 else 0

        crecimiento = round(crecimiento, 2)

        # ---- MENSAJE ----
        msg = "💰 GANANCIAS DEL MES\n\n"
        msg += f"🆓 Trial: {conteo_actual['trial']}\n"
        msg += f"📅 Semanal: {conteo_actual['semanal']} ($10)\n"
        msg += f"📆 Mensual: {conteo_actual['mensual']} ($20)\n\n"
    
        msg += f"💵 TOTAL MES: ${total_actual}\n"
        msg += f"📊 Mes anterior: ${total_anterior}\n"

        # Emoji según crecimiento
        if crecimiento > 0:
            msg += f"📈 Crecimiento: +{crecimiento}%"
        elif crecimiento < 0:
            msg += f"📉 Crecimiento: {crecimiento}%"
        else:
            msg += f"➖ Sin cambio: {crecimiento}%"
    
        await query.message.reply_text(msg)

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

        cursor.execute("SELECT * FROM users WHERE username=%s", (username,))
        user = cursor.fetchone()

        if plan == "trial" and user and user[5] == 1:
            await update.message.reply_text("❌ Ya usó el trial")
            return

        days = PLANS[plan]
        start = datetime.now()
        end = start + timedelta(days=days)

        trial_used = 1 if plan == "trial" else (user[5] if user else 0)

        cursor.execute("""
        INSERT INTO users (username, plan, start_date, end_date, status, trial_used)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (username) DO UPDATE SET
            plan = EXCLUDED.plan,
            start_date = EXCLUDED.start_date,
            end_date = EXCLUDED.end_date,
            status = EXCLUDED.status,
            trial_used = EXCLUDED.trial_used
        """, (username, plan, start.isoformat(), end.isoformat(), "activo", trial_used))
        conn.commit()

        try:
            await context.bot.unban_chat_member(VIP_GROUP_ID, username)
        except:
            pass

        await update.message.reply_text(f"✅ {username} activo hasta {end.date()}")

    except Exception as e:
        await update.message.reply_text(
        "❌ Formato incorrecto\n\nUsa:\n/add @usuario plan\n\nPlanes:\n- trial\n- semanal\n- mensual"
        )
        print("ERROR ADD:", e)
    if len(context.args) < 2:
        await update.message.reply_text("❌ Falta información\nUsa: /add @usuario plan")
        return
        
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

        cursor.execute("UPDATE users SET status='vencido' WHERE username=%s", (username,))
        conn.commit()

        await query.message.reply_text(f"🚪 {username} expulsado")
        return

    end = datetime.now() + timedelta(days=days)

    cursor.execute("""
    UPDATE users SET end_date=%s, status='activo' WHERE username=%s
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
            bot = Bot(token=TOKEN)
            await bot.send_message(
                ADMIN_ID,
                msg,
                reply_markup=alert_buttons(username)
            )

            try:
                await bot.ban_chat_member(VIP_GROUP_ID, username)
            except:
                pass

            try:
                cursor.execute("UPDATE users SET status='vencido' WHERE username=%s", (username,))
                conn.commit()
            except:
                conn.rollback()

# -------- MANEJADOR UNIFICADO --------
async def handle_all_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if query.from_user.id != ADMIN_ID:
        return
    
    # Separar datos del callback
    if "|" in query.data:
        # Es una alerta (renew7|username, renew30|username, etc.)
        await alert_actions(update, context)
    else:
        # Es un botón del panel (add, list, ganancias, etc.)
        await panel_buttons(update, context)
        
# -------- MAIN --------
app = ApplicationBuilder().token(TOKEN).build()

app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("add", add_user))

app.add_handler(CallbackQueryHandler(handle_all_callbacks))

scheduler = BackgroundScheduler()
scheduler.add_job(check_expired, "interval", hours=6)
scheduler.start()

if __name__ == "__main__":
    app.run_polling()
