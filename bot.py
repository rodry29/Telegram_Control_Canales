import os
import csv
import asyncio
import logging
from io import StringIO
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    ContextTypes, Defaults, MessageHandler, filters
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ==================== LOGGING ====================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==================== CONFIGURACIÓN ====================
DATABASE_URL = os.getenv("DATABASE_URL")
SUPER_ADMIN_ID = 5054216496
TOKEN = os.getenv("TELEGRAM_TOKEN")

if not TOKEN:
    print("❌ ERROR: No se encontró token")
    exit(1)

PLANS = {
    "trial": {"days": 1, "price": 0, "name": "🎁 Trial (1 día)"},
    "semanal": {"days": 7, "price": 10, "name": "📅 Semanal (7 días)"},
    "mensual": {"days": 30, "price": 20, "name": "📆 Mensual (30 días)"}
}

GROUPS_CONFIG = os.getenv("GROUPS_CONFIG", "")
GROUPS = []

for group_config in GROUPS_CONFIG.split(","):
    if group_config.strip():
        parts = group_config.strip().split(":")
        if len(parts) == 4:
            GROUPS.append({
                "group_id": int(parts[0]),
                "type": parts[1].upper(),
                "group_name": parts[2],
                "admin_id": int(parts[3])
            })

# ==================== FUNCIONES DE UTILIDAD ====================
def get_group_by_id(group_id: int) -> Optional[dict]:
    for group in GROUPS:
        if group["group_id"] == group_id:
            return group
    return None

def get_groups_by_admin(admin_id: int, group_type: str = None) -> list:
    print(f"🔍 Buscando grupos para admin: {admin_id}")
    print(f"🔍 GROUPS actual: {GROUPS}")
    if admin_id == SUPER_ADMIN_ID:
        groups = GROUPS
    else:
        groups = [g for g in GROUPS if g["admin_id"] == admin_id]
    if group_type:
        groups = [g for g in groups if g.get("type", "VIP") == group_type]
    return groups
    
def can_manage_group(user_id: int, group_id: int) -> bool:
    if user_id == SUPER_ADMIN_ID:
        return True
    group = get_group_by_id(group_id)
    return group and group["admin_id"] == user_id

# ==================== BASE DE DATOS ====================
class Database:
    def __init__(self, db_url: str):
        self.db_url = db_url

    def get_connection(self):
        conn = psycopg2.connect(self.db_url)
        conn.autocommit = True
        return conn

    async def init_tables(self):
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                CREATE TABLE IF NOT EXISTS groups (
                    group_id BIGINT PRIMARY KEY,
                    group_name TEXT,
                    group_type TEXT DEFAULT 'VIP',
                    admin_id BIGINT,
                    super_admin_id BIGINT,
                    created_at TIMESTAMP DEFAULT NOW(),
                    settings JSONB DEFAULT '{}'::jsonb
                )
                """)
                cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    group_id BIGINT NOT NULL,
                    username TEXT,
                    first_name TEXT,
                    plan TEXT NOT NULL,
                    start_date TIMESTAMP NOT NULL,
                    end_date TIMESTAMP NOT NULL,
                    status TEXT DEFAULT 'active',
                    trial_used BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(user_id, group_id)
                )
                """)
                cur.execute("""
                CREATE TABLE IF NOT EXISTS payments (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    group_id BIGINT NOT NULL,
                    username TEXT,
                    plan TEXT NOT NULL,
                    amount INTEGER NOT NULL,
                    payment_date TIMESTAMP DEFAULT NOW()
                )
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_users_group ON users(group_id, status)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_users_end_date ON users(group_id, end_date)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_payments_group ON payments(group_id, payment_date)")
                conn.commit()
        logger.info("✅ Base de datos inicializada")

    async def load_groups_from_db(self):
        global GROUPS
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT group_id, group_name, admin_id, COALESCE(group_type, 'VIP') as group_type FROM groups")
                db_groups = cur.fetchall()
                if db_groups:
                    GROUPS.clear()
                    for g in db_groups:
                        GROUPS.append({
                            "group_id": g["group_id"],
                            "group_name": g["group_name"],
                            "admin_id": g["admin_id"],
                            "type": g["group_type"]
                        })
                    logger.info(f"📦 {len(GROUPS)} grupos cargados")
                    return True
                return False

    async def save_group(self, group_id: int, group_name: str, admin_id: int, group_type: str = "VIP"):
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                INSERT INTO groups (group_id, group_name, admin_id, super_admin_id, group_type)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (group_id) DO UPDATE SET
                    group_name = EXCLUDED.group_name,
                    admin_id = EXCLUDED.admin_id,
                    group_type = EXCLUDED.group_type
                """, (group_id, group_name, admin_id, SUPER_ADMIN_ID, group_type))
                conn.commit()

    async def get_user_by_username(self, username: str, group_id: int = None):
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if group_id:
                    cur.execute("SELECT * FROM users WHERE LOWER(username) = LOWER(%s) AND group_id = %s", (username, group_id))
                else:
                    cur.execute("SELECT * FROM users WHERE LOWER(username) = LOWER(%s)", (username,))
                return cur.fetchone()

    async def register_user_auto(self, group_id: int, user_id: int, username: str, first_name: str):
        now = datetime.now()
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT user_id, trial_used, status, end_date FROM users WHERE user_id = %s AND group_id = %s", (user_id, group_id))
                existing = cur.fetchone()
                if not existing:
                    end_date = now + timedelta(days=PLANS["trial"]["days"])
                    cur.execute("""
                    INSERT INTO users (user_id, group_id, username, first_name, plan, start_date, end_date, trial_used, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'active')
                    """, (user_id, group_id, username, first_name, "trial", now, end_date, True))
                    cur.execute("INSERT INTO payments (user_id, group_id, username, plan, amount, payment_date) VALUES (%s, %s, %s, %s, %s, %s)", 
                               (user_id, group_id, username, "trial", 0, now))
                    conn.commit()
                    return True, "trial_nuevo"
                elif existing['status'] == 'active' and existing['end_date'] > now:
                    cur.execute("UPDATE users SET username = %s, first_name = %s WHERE user_id = %s AND group_id = %s", 
                               (username, first_name, user_id, group_id))
                    conn.commit()
                    return True, "activo"
                return False, "expirado"

    async def add_or_update_user(self, group_id: int, username: str, plan: str):
        now = datetime.now()
        if plan not in PLANS:
            return False, "❌ Plan inválido"
        config = PLANS[plan]
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT user_id, trial_used, status FROM users WHERE LOWER(username) = LOWER(%s) AND group_id = %s", (username, group_id))
                existing = cur.fetchone()
                if existing:
                    if plan == "trial" and existing['trial_used']:
                        return False, "❌ Este usuario ya usó su prueba gratuita"
                    end_date = now + timedelta(days=config['days'])
                    cur.execute("""
                    UPDATE users SET plan=%s, start_date=%s, end_date=%s, status='active', updated_at=NOW(), username=%s, trial_used=trial_used OR %s
                    WHERE user_id=%s AND group_id=%s
                    """, (plan, now, end_date, username, plan == "trial", existing['user_id'], group_id))
                    cur.execute("INSERT INTO payments (user_id, group_id, username, plan, amount, payment_date) VALUES (%s, %s, %s, %s, %s, %s)",
                               (existing['user_id'], group_id, username, plan, config['price'], now))
                    conn.commit()
                    return True, f"✅ @{username} activado con {config['name']}\n📅 Expira: {end_date.strftime('%d/%m/%Y')}"
                return False, f"❌ No tengo registro de @{username}. Pídele que envíe un mensaje al bot."

    async def get_all_active_users(self, group_id: int):
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                SELECT user_id, username, plan, end_date, EXTRACT(DAY FROM (end_date - NOW())) as days_left
                FROM users WHERE group_id=%s AND status='active' AND end_date>NOW() ORDER BY end_date ASC
                """, (group_id,))
                return cur.fetchall()

    async def get_monthly_earnings(self, group_id: int):
        now = datetime.now()
        start_date = datetime(now.year, now.month, 1)
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT plan, COUNT(*) as count, COALESCE(SUM(amount),0) as total FROM payments WHERE group_id=%s AND payment_date>=%s GROUP BY plan", (group_id, start_date))
                summary = cur.fetchall()
                total = sum(row['total'] for row in summary)
                cur.execute("SELECT COUNT(*) as new_users FROM users WHERE group_id=%s AND created_at>=%s", (group_id, start_date))
                new_users = cur.fetchone()['new_users']
                return {"summary": summary, "total": total, "new_users": new_users}

    async def get_total_monthly_earnings(self):
        """Ganancias totales de todos los grupos del mes actual"""
        now = datetime.now()
        start_date = datetime(now.year, now.month, 1)
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COALESCE(SUM(amount), 0) FROM payments WHERE payment_date >= %s", (start_date,))
                return cur.fetchone()[0]

    async def get_expired_users(self, group_id: int):
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT user_id, username, plan, end_date FROM users WHERE group_id=%s AND status='active' AND end_date<NOW()", (group_id,))
                return cur.fetchall()

    async def expire_user(self, user_id: int, group_id: int):
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE users SET status='expired' WHERE user_id=%s AND group_id=%s", (user_id, group_id))
                conn.commit()

# ==================== INSTANCIA GLOBAL ====================
db = Database(DATABASE_URL)
scheduler = AsyncIOScheduler()
bot_app = None

# ==================== HANDLERS ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id == SUPER_ADMIN_ID:
        vip_count = len([g for g in GROUPS if g.get("type", "VIP") == "VIP"])
        free_count = len([g for g in GROUPS if g.get("type", "VIP") == "FREE"])
        keyboard = [
            [InlineKeyboardButton(f"👑 Grupos VIP ({vip_count})", callback_data="vip_groups")],
            [InlineKeyboardButton(f"📋 Grupos FREE ({free_count})", callback_data="free_groups")],
            [InlineKeyboardButton("✏️ Editar grupo", callback_data="edit_group_menu")],
            [InlineKeyboardButton("💰 Ganancias", callback_data="total_earnings")],
            [InlineKeyboardButton("➕ Agregar grupo", callback_data="add_group")],
        ]
        await update.message.reply_text(
            f"👑 *Panel Super Admin*\nVIP: {vip_count} | FREE: {free_count}",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
        return
    user_groups = get_groups_by_admin(user_id)
    if not user_groups:
        await update.message.reply_text("❌ No tienes grupos asignados")
        return
    if len(user_groups) == 1:
        group = user_groups[0]
        context.user_data['current_group'] = group['group_id']
        if group.get("type", "VIP") == "VIP":
            keyboard = [[InlineKeyboardButton("➕ Agregar usuario", callback_data="add_user")], [InlineKeyboardButton("📊 Usuarios activos", callback_data="list_active")], [InlineKeyboardButton("💰 Ganancias", callback_data="earnings")], [InlineKeyboardButton("📥 Exportar mes", callback_data="export_month")]]
            await update.message.reply_text(f"👑 *Panel VIP - {group['group_name']}*", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
        else:
            keyboard = [[InlineKeyboardButton("📋 Clientes potenciales", callback_data="list_potential")], [InlineKeyboardButton("📥 Exportar clientes", callback_data="export_clients")]]
            await update.message.reply_text(f"📋 *Panel FREE - {group['group_name']}*", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def total_earnings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra las ganancias totales del mes de todos los grupos (solo Super Admin)"""
    query = update.callback_query
    if query:
        await query.answer()
        message = query.message
    else:
        message = update.message
    
    total = await db.get_total_monthly_earnings()
    now = datetime.now()
    
    msg = f"💰 *GANANCIAS TOTALES DEL MES*\n\n"
    msg += f"📅 {now.strftime('%B %Y')}\n"
    msg += f"💵 Total recaudado: **${total}**\n\n"
    msg += f"📊 Incluye todos los grupos configurados."
    
    await message.reply_text(msg, parse_mode="Markdown")

async def add_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    current_group = context.user_data.get('current_group')
    if not current_group:
        group = get_group_by_id(chat_id)
        if group and can_manage_group(user_id, chat_id):
            current_group = chat_id
        else:
            await update.message.reply_text("❌ Usa /start primero")
            return
    if not can_manage_group(user_id, current_group):
        await update.message.reply_text("❌ No autorizado")
        return
    if len(context.args) < 2:
        await update.message.reply_text("❌ Usa: `/add @username plan`\nPlanes: trial, semanal, mensual", parse_mode="Markdown")
        return
    username = context.args[0].replace("@", "")
    plan = context.args[1].lower()
    if plan not in PLANS:
        await update.message.reply_text("❌ Plan inválido")
        return
    success, msg = await db.add_or_update_user(current_group, username, plan)
    await update.message.reply_text(msg)

async def list_active_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query:
        await query.answer()
        message = query.message
    else:
        message = update.message
    group_id = context.user_data.get('current_group')
    if not group_id:
        await message.reply_text("❌ Selecciona un grupo con /start")
        return
    users = await db.get_all_active_users(group_id)
    if not users:
        await message.reply_text("📭 No hay usuarios activos")
        return
    msg = f"📊 *USUARIOS ACTIVOS* ({len(users)})\n\n"
    for user in users[:30]:
        days_left = int(user['days_left']) if user['days_left'] else 0
        emoji = "🟢" if days_left > 7 else "🟡" if days_left > 2 else "🔴"
        msg += f"{emoji} @{user['username'] or user['user_id']}\n   📅 Expira: {user['end_date'].strftime('%d/%m/%Y')} ({days_left} días)\n   📋 {user['plan']}\n\n"
    await message.reply_text(msg, parse_mode="Markdown")

async def show_earnings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query:
        await query.answer()
        message = query.message
    else:
        message = update.message
    group_id = context.user_data.get('current_group')
    if not group_id:
        await message.reply_text("❌ Selecciona un grupo con /start")
        return
    earnings = await db.get_monthly_earnings(group_id)
    now = datetime.now()
    msg = f"💰 *GANANCIAS DE {now.strftime('%B %Y').upper()}*\n\n"
    if not earnings['summary']:
        msg += "📭 No hay ventas"
    else:
        for plan in earnings['summary']:
            plan_name = PLANS.get(plan['plan'], {}).get('name', plan['plan'])
            msg += f"• {plan_name}: {plan['count']} - ${plan['total']}\n"
        msg += f"\n💵 *TOTAL*: ${earnings['total']}\n👥 *Nuevos*: {earnings['new_users']}"
    await message.reply_text(msg, parse_mode="Markdown")

async def export_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query:
        await query.answer()
        message = query.message
    else:
        message = update.message
    group_id = context.user_data.get('current_group')
    if not group_id:
        await message.reply_text("❌ Selecciona un grupo con /start")
        return
    now = datetime.now()
    start_date = datetime(now.year, now.month, 1)
    with db.get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT user_id, username, plan, amount, payment_date FROM payments WHERE group_id=%s AND payment_date>=%s ORDER BY payment_date DESC", (group_id, start_date))
            transactions = cur.fetchall()
    if not transactions:
        await message.reply_text(f"📭 No hay transacciones en {now.strftime('%B %Y')}")
        return
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(['Fecha', 'User ID', 'Username', 'Plan', 'Monto'])
    for t in transactions:
        writer.writerow([t['payment_date'].strftime('%Y-%m-%d %H:%M:%S'), t['user_id'], t['username'] or 'Sin username', t['plan'].upper(), f"${t['amount']}"])
    output.seek(0)
    await message.reply_document(document=output.getvalue().encode('utf-8-sig'), filename=f"reporte_{now.year}_{now.month:02d}.csv", caption=f"📊 Reporte de {now.strftime('%B %Y')}")
    output.close()

async def list_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query:
        await query.answer()
        message = query.message
    else:
        message = update.message
    if update.effective_user.id != SUPER_ADMIN_ID:
        await message.reply_text("❌ Solo Super Admin")
        return
    if not GROUPS:
        await message.reply_text("📭 No hay grupos")
        return
    msg = "📊 *GRUPOS CONFIGURADOS*\n\n"
    for group in GROUPS:
        msg += f"📌 *{group['group_name']}*\n   🆔 ID: `{group['group_id']}`\n   👑 Admin: `{group['admin_id']}`\n   📋 Tipo: {group.get('type', 'VIP')}\n\n"
    await message.reply_text(msg, parse_mode="Markdown")

async def add_group_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != SUPER_ADMIN_ID:
        await update.message.reply_text("❌ Solo Super Admin")
        return
    if len(context.args) < 4:
        await update.message.reply_text("❌ Formato: `/addgroup group_id TIPO \"nombre\" admin_id`", parse_mode="Markdown")
        return
    try:
        group_id = int(context.args[0])
        group_type = context.args[1].upper()
        group_name = " ".join(context.args[2:-1]).strip('"')
        admin_id = int(context.args[-1])
        if group_type not in ["VIP", "FREE"]:
            await update.message.reply_text("❌ TIPO debe ser VIP o FREE")
            return
        GROUPS.append({"group_id": group_id, "type": group_type, "group_name": group_name, "admin_id": admin_id})
        await db.save_group(group_id, group_name, admin_id, group_type)
        await update.message.reply_text(f"✅ Grupo {group_name} agregado", parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")

async def global_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query:
        await query.answer()
        message = query.message
    else:
        message = update.message
    if update.effective_user.id != SUPER_ADMIN_ID:
        await message.reply_text("❌ No autorizado")
        return
    total_users, total_active, total_monthly = 0, 0, 0
    msg = "🌍 *ESTADÍSTICAS GLOBALES*\n\n"
    for group in GROUPS:
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM users WHERE group_id=%s", (group["group_id"],))
                total = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM users WHERE group_id=%s AND status='active' AND end_date>NOW()", (group["group_id"],))
                active = cur.fetchone()[0]
                cur.execute("SELECT COALESCE(SUM(amount),0) FROM payments WHERE group_id=%s AND payment_date>=date_trunc('month',NOW())", (group["group_id"],))
                monthly = cur.fetchone()[0]
                total_users += total
                total_active += active
                total_monthly += monthly
        msg += f"📌 *{group['group_name']}*\n   👥 {active}/{total} activos\n   💰 ${monthly}\n\n"
    msg += f"━━━━━━━━━━━━━━━\n📊 *TOTALES*\n👥 Usuarios: {total_active}/{total_users}\n💰 Ganancias mes: ${total_monthly}"
    await message.reply_text(msg, parse_mode="Markdown")

async def show_groups_by_type(update: Update, context: ContextTypes.DEFAULT_TYPE, group_type: str, select_mode: bool = False):
    query = update.callback_query
    groups = [g for g in GROUPS if g.get("type", "VIP") == group_type]
    if not groups:
        await query.edit_message_text(f"📭 No hay grupos {group_type}")
        return
    keyboard = [[InlineKeyboardButton(f"📌 {g['group_name']}", callback_data=f"select_group_{g['group_id']}")] for g in groups]
    await query.edit_message_text(f"📋 *Grupos {group_type}*", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def select_group(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query
    group = get_group_by_id(group_id)
    if not group:
        await query.edit_message_text("❌ Grupo no encontrado")
        return
    context.user_data['current_group'] = group_id
    if group.get("type", "VIP") == "VIP":
        keyboard = [[InlineKeyboardButton("➕ Agregar usuario", callback_data="add_user")], [InlineKeyboardButton("📊 Usuarios activos", callback_data="list_active")], [InlineKeyboardButton("💰 Ganancias", callback_data="earnings")], [InlineKeyboardButton("📥 Exportar mes", callback_data="export_month")]]
        await query.edit_message_text(f"👑 *Panel VIP - {group['group_name']}*", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    else:
        keyboard = [[InlineKeyboardButton("📋 Clientes potenciales", callback_data="list_potential")], [InlineKeyboardButton("📥 Exportar clientes", callback_data="export_clients")]]
        await query.edit_message_text(f"📋 *Panel FREE - {group['group_name']}*", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def list_potential_clients(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    group_id = context.user_data.get('current_group')
    if not group_id:
        await query.edit_message_text("❌ Selecciona un grupo con /start")
        return
    with db.get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT user_id, username, first_name, created_at FROM users WHERE group_id=%s AND status='potencial' ORDER BY created_at DESC", (group_id,))
            clients = cur.fetchall()
    if not clients:
        await query.edit_message_text("📭 No hay clientes potenciales")
        return
    msg = f"📋 *CLIENTES POTENCIALES*\n\n"
    for c in clients[:30]:
        msg += f"👤 @{c['username'] or c['user_id']}\n   📅 Registrado: {c['created_at'].strftime('%d/%m/%Y')}\n\n"
    await query.edit_message_text(msg, parse_mode="Markdown")

async def export_clients(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    group_id = context.user_data.get('current_group')
    if not group_id:
        await query.edit_message_text("❌ Selecciona un grupo con /start")
        return
    with db.get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT user_id, username, first_name, created_at FROM users WHERE group_id=%s AND status='potencial' ORDER BY created_at DESC", (group_id,))
            clients = cur.fetchall()
    if not clients:
        await query.edit_message_text("📭 No hay clientes")
        return
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(['User ID', 'Username', 'Nombre', 'Fecha Registro'])
    for c in clients:
        writer.writerow([c['user_id'], c['username'] or '', c['first_name'] or '', c['created_at'].strftime('%Y-%m-%d %H:%M:%S')])
    output.seek(0)
    await query.message.reply_document(document=output.getvalue().encode('utf-8-sig'), filename=f"clientes_{datetime.now().strftime('%Y%m%d')}.csv", caption="📋 Clientes potenciales")
    output.close()

async def edit_group_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra el menú para seleccionar qué grupo editar"""
    query = update.callback_query
    await query.answer()
    
    if not GROUPS:
        await query.edit_message_text("📭 No hay grupos configurados")
        return
    
    keyboard = []
    for group in GROUPS:
        emoji = "👑" if group.get("type", "VIP") == "VIP" else "📋"
        keyboard.append([InlineKeyboardButton(f"{emoji} {group['group_name']}", callback_data=f"edit_select_{group['group_id']}")])
    
    keyboard.append([InlineKeyboardButton("🔙 Volver", callback_data="back_to_admin")])
    
    await query.edit_message_text(
        "✏️ *Selecciona el grupo que deseas editar*",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def edit_group_form(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    """Muestra el formulario para editar un grupo"""
    query = update.callback_query
    await query.answer()
    
    group = get_group_by_id(group_id)
    if not group:
        await query.edit_message_text("❌ Grupo no encontrado")
        return
    
    context.user_data['editing_group_id'] = group_id
    
    keyboard = [
        [InlineKeyboardButton("✏️ Cambiar nombre", callback_data=f"edit_name_{group_id}")],
        [InlineKeyboardButton("👤 Cambiar administrador", callback_data=f"edit_admin_{group_id}")],
        [InlineKeyboardButton("🔄 Cambiar tipo", callback_data=f"edit_type_{group_id}")],
        [InlineKeyboardButton("🔙 Volver", callback_data="edit_group_menu")]
    ]
    
    await query.edit_message_text(
        f"✏️ *Editando: {group['group_name']}*\n\n"
        f"📋 Tipo: {group.get('type', 'VIP')}\n"
        f"👑 Admin: `{group['admin_id']}`\n\n"
        f"*¿Qué deseas modificar?*",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def edit_group_name_request(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    """Solicita el nuevo nombre del grupo"""
    query = update.callback_query
    await query.answer()
    
    context.user_data['editing_field'] = 'name'
    context.user_data['editing_group_id'] = group_id
    
    await query.edit_message_text(
        f"✏️ *Cambiar nombre del grupo*\n\n"
        f"Envía el *nuevo nombre* en el chat.\n\n"
        f"*Escribe 'cancelar' para cancelar.*",
        parse_mode="Markdown"
    )

async def edit_group_admin_request(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    """Solicita el nuevo admin del grupo"""
    query = update.callback_query
    await query.answer()
    
    context.user_data['editing_field'] = 'admin'
    context.user_data['editing_group_id'] = group_id
    
    await query.edit_message_text(
        f"👤 *Cambiar administrador*\n\n"
        f"Envía el *ID del nuevo administrador* en el chat.\n\n"
        f"*Para obtener un ID, usa @userinfobot*\n\n"
        f"*Escribe 'cancelar' para cancelar.*",
        parse_mode="Markdown"
    )

async def edit_group_type_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    """Muestra opciones para cambiar el tipo"""
    query = update.callback_query
    await query.answer()
    
    keyboard = [
        [InlineKeyboardButton("👑 VIP", callback_data=f"set_type_{group_id}_VIP")],
        [InlineKeyboardButton("📋 FREE", callback_data=f"set_type_{group_id}_FREE")],
        [InlineKeyboardButton("🔙 Volver", callback_data=f"edit_select_{group_id}")]
    ]
    
    await query.edit_message_text(
        "🔄 *Selecciona el nuevo tipo*",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def set_group_type(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int, new_type: str):
    """Cambia el tipo del grupo"""
    query = update.callback_query
    await query.answer()
    
    group = get_group_by_id(group_id)
    if not group:
        await query.edit_message_text("❌ Grupo no encontrado")
        return
    
    old_type = group.get("type", "VIP")
    
    # Actualizar en memoria
    for g in GROUPS:
        if g["group_id"] == group_id:
            g["type"] = new_type
            break
    
    # Actualizar en BD
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE groups SET group_type = %s WHERE group_id = %s", (new_type, group_id))
            conn.commit()
    
    await query.edit_message_text(
        f"✅ *Tipo actualizado*\n\n{old_type} → {new_type}",
        parse_mode="Markdown"
    )
    await asyncio.sleep(1)
    await edit_group_form(update, context, group_id)

async def detect_new_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.new_chat_members:
        return
    chat_id = update.message.chat_id
    group = get_group_by_id(chat_id)
    if not group:
        return
    for new_member in update.message.new_chat_members:
        if new_member.id == context.bot.id:
            continue
        user_id = new_member.id
        username = new_member.username or f"user_{user_id}"
        first_name = new_member.first_name or ""
        if group["type"] == "VIP":
            registered, result = await db.register_user_auto(chat_id, user_id, username, first_name)
            if registered and result == "trial_nuevo":
                await context.bot.send_message(user_id, f"🎉 Bienvenido @{username}!\n✨ Trial gratis de 1 día", parse_mode="Markdown")
        else:
            existing = await db.get_user_by_username(username, chat_id)
            if not existing:
                with db.get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("INSERT INTO users (user_id, group_id, username, first_name, plan, start_date, end_date, status, trial_used) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                                   (user_id, chat_id, username, first_name, "FREE", datetime.now(), datetime.now() + timedelta(days=365), "potencial", False))
                        conn.commit()
                await context.bot.send_message(group["admin_id"], f"📋 Nuevo cliente potencial: @{username} en {group['group_name']}")
                
async def handle_edit_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja la entrada de texto para editar grupo"""
    user_id = update.effective_user.id
    
    if user_id != SUPER_ADMIN_ID:
        return
    
    text = update.message.text.strip()
    
    if text.lower() == 'cancelar':
        await update.message.reply_text("❌ Edición cancelada")
        context.user_data.pop('editing_field', None)
        context.user_data.pop('editing_group_id', None)
        return
    
    field = context.user_data.get('editing_field')
    group_id = context.user_data.get('editing_group_id')
    
    if not field or not group_id:
        return
    
    group = get_group_by_id(group_id)
    if not group:
        await update.message.reply_text("❌ Grupo no encontrado")
        return
    
    if field == 'name':
        old_name = group['group_name']
        for g in GROUPS:
            if g["group_id"] == group_id:
                g["group_name"] = text
                break
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE groups SET group_name = %s WHERE group_id = %s", (text, group_id))
                conn.commit()
        await update.message.reply_text(f"✅ *Nombre actualizado*\n`{old_name}` → `{text}`", parse_mode="Markdown")
        
    elif field == 'admin':
        try:
            new_admin = int(text)
            old_admin = group['admin_id']
            for g in GROUPS:
                if g["group_id"] == group_id:
                    g["admin_id"] = new_admin
                    break
            with db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE groups SET admin_id = %s WHERE group_id = %s", (new_admin, group_id))
                    conn.commit()
            await update.message.reply_text(f"✅ *Administrador actualizado*\n`{old_admin}` → `{new_admin}`", parse_mode="Markdown")
        except ValueError:
            await update.message.reply_text("❌ Error: El ID debe ser un número")
            return
    
    context.user_data.pop('editing_field', None)
    context.user_data.pop('editing_group_id', None)

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    if data == "add_user":
        await query.edit_message_text("📝 Usa: `/add @username plan`", parse_mode="Markdown")
    elif data == "list_active":
        await list_active_users(update, context)
    elif data == "earnings":
        await show_earnings(update, context)
    elif data == "export_month":
        await export_report(update, context)
    elif data == "list_potential":
        await list_potential_clients(update, context)
    elif data == "export_clients":
        await export_clients(update, context)
    elif data == "vip_groups":
        await show_groups_by_type(update, context, "VIP")
    elif data == "free_groups":
        await show_groups_by_type(update, context, "FREE")
    elif data == "total_earnings":
        await total_earnings(update, context)
    elif data == "all_groups":
        await list_groups(update, context)
    elif data == "add_group":
        await query.edit_message_text("📝 Usa: `/addgroup group_id TIPO \"nombre\" admin_id`", parse_mode="Markdown")
    elif data == "global_stats":
        await global_stats(update, context)
    elif data.startswith("select_group_"):
        group_id = int(data.replace("select_group_", ""))
        await select_group(update, context, group_id)
    elif data == "edit_group_menu":
        await edit_group_menu(update, context)
    elif data.startswith("edit_select_"):
        group_id = int(data.replace("edit_select_", ""))
        await edit_group_form(update, context, group_id)
    elif data.startswith("edit_name_"):
        group_id = int(data.replace("edit_name_", ""))
        await edit_group_name_request(update, context, group_id)
    elif data.startswith("edit_admin_"):
        group_id = int(data.replace("edit_admin_", ""))
        await edit_group_admin_request(update, context, group_id)
    elif data.startswith("edit_type_"):
        group_id = int(data.replace("edit_type_", ""))
        await edit_group_type_menu(update, context, group_id)
    elif data.startswith("set_type_"):
        parts = data.split("_")
        group_id = int(parts[2])
        new_type = parts[3]
        await set_group_type(update, context, group_id, new_type)
    elif data == "back_to_admin":
        await start(update, context)



# ==================== TAREAS PROGRAMADAS ====================
async def check_expired_subscriptions():
    for group in GROUPS:
        if group.get("type", "VIP") != "VIP":
            continue
        expired_users = await db.get_expired_users(group["group_id"])
        for user in expired_users:
            await db.expire_user(user['user_id'], group["group_id"])
            try:
                await bot_app.bot.ban_chat_member(group["group_id"], user['user_id'])
                await bot_app.bot.send_message(group["admin_id"], f"🚫 @{user['username']} expulsado")
            except:
                pass

# ==================== MAIN ====================
async def main():
    global bot_app
    print("🚀 Iniciando bot...")
    await db.init_tables()
    await db.load_groups_from_db()
    logger.info(f"📦 {len(GROUPS)} grupos disponibles")
    defaults = Defaults(parse_mode="HTML")
    bot_app = ApplicationBuilder().token(TOKEN).defaults(defaults).build()
    bot_app.add_handler(CommandHandler("start", start))
    bot_app.add_handler(CommandHandler("add", add_user_command))
    bot_app.add_handler(CommandHandler("groups", list_groups))
    bot_app.add_handler(CommandHandler("addgroup", add_group_command))
    bot_app.add_handler(CallbackQueryHandler(handle_callback))
    bot_app.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, detect_new_member))
    bot_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_edit_input))
    scheduler.add_job(check_expired_subscriptions, 'interval', hours=6)
    scheduler.start()
    logger.info("🤖 Bot iniciado")
    await bot_app.initialize()
    await bot_app.start()
    await bot_app.updater.start_polling(drop_pending_updates=True)
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
