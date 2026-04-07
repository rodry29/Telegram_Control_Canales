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
        # Calcular estadísticas rápidas
        vip_count = len([g for g in GROUPS if g.get("type", "VIP") == "VIP"])
        free_count = len([g for g in GROUPS if g.get("type", "VIP") == "FREE"])
        total_earnings = await db.get_total_monthly_earnings()
        total_users = 0
        for group in GROUPS:
            with db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM users WHERE group_id = %s", (group["group_id"],))
                    total_users += cur.fetchone()[0]
        
        keyboard = [
            [InlineKeyboardButton("📋 Grupos", callback_data="menu_groups")],
            [InlineKeyboardButton("💰 Ganancias", callback_data="total_earnings")],
            [InlineKeyboardButton("📟 Comandos", callback_data="menu_commands")],
        ]
        
        await update.message.reply_text(
            f"👑 *Panel Super Administrador*\n\n"
            f"📊 *Resumen rápido:*\n"
            f"• 👑 Grupos VIP: {vip_count}\n"
            f"• 📋 Grupos FREE: {free_count}\n"
            f"• 👥 Total usuarios: {total_users}\n"
            f"• 💰 Ganancias del mes: ${total_earnings}\n\n"
            f"Selecciona una opción:",
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

async def menu_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Menú principal de grupos"""
    query = update.callback_query
    await query.answer()
    
    keyboard = [
        [InlineKeyboardButton("➕ Agregar grupo", callback_data="add_group")],
        [InlineKeyboardButton("👁️ Ver grupos", callback_data="menu_view_groups")],
        [InlineKeyboardButton("✏️ Editar grupo", callback_data="menu_edit_group_select")],
        [InlineKeyboardButton("❌ Eliminar grupo", callback_data="menu_delete_group_select")],
        [InlineKeyboardButton("🔙 Volver", callback_data="back_to_admin")],
    ]
    
    await query.edit_message_text(
        "📋 *Gestión de Grupos*\n\n"
        "Selecciona una opción:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def menu_view_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Menú para seleccionar qué tipo de grupos ver"""
    query = update.callback_query
    await query.answer()
    
    keyboard = [
        [InlineKeyboardButton("👑 Grupos VIP", callback_data="view_vip_groups")],
        [InlineKeyboardButton("📋 Grupos FREE", callback_data="view_free_groups")],
        [InlineKeyboardButton("🔙 Volver", callback_data="menu_groups")],
    ]
    
    await query.edit_message_text(
        "👁️ *Ver Grupos*\n\n"
        "Selecciona el tipo de grupo:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

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

async def auto_backup():
    """Genera backup automático cada 15 días y lo envía al Super Admin"""
    global bot_app
    
    if not bot_app:
        return
    
    # Verificar cuándo fue el último backup
    last_backup_file = "last_backup.txt"
    last_backup_date = None
    
    try:
        if os.path.exists(last_backup_file):
            with open(last_backup_file, 'r') as f:
                last_backup_date = datetime.fromisoformat(f.read().strip())
    except:
        pass
    
    now = datetime.now()
    
    # Si no hay registro de último backup o pasaron 15 días
    if not last_backup_date or (now - last_backup_date).days >= 15:
        
        # Crear backup
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(['group_id', 'group_name', 'group_type', 'admin_id', 'backup_date'])
        
        for group in GROUPS:
            writer.writerow([
                group['group_id'],
                group['group_name'],
                group.get('type', 'VIP'),
                group['admin_id'],
                now.strftime('%Y-%m-%d %H:%M:%S')
            ])
        
        output.seek(0)
        
        # Enviar backup al Super Admin
        try:
            await bot_app.bot.send_document(
                SUPER_ADMIN_ID,
                document=output.getvalue().encode('utf-8-sig'),
                filename=f"backup_automatico_{now.strftime('%Y%m%d')}.csv",
                caption=f"📦 *Backup Automático*\n\n"
                        f"📅 Fecha: {now.strftime('%d/%m/%Y %H:%M:%S')}\n"
                        f"📊 Grupos incluidos: {len(GROUPS)}\n\n"
                        f"*Próximo backup:* {(now + timedelta(days=15)).strftime('%d/%m/%Y')}\n\n"
                        f"⚠️ Guarda este archivo en un lugar seguro.",
                parse_mode="Markdown"
            )
            output.close()
            
            # Registrar fecha del backup
            with open(last_backup_file, 'w') as f:
                f.write(now.isoformat())
            
            logger.info(f"✅ Backup automático enviado a {SUPER_ADMIN_ID}")
            
        except Exception as e:
            logger.error(f"❌ Error enviando backup automático: {e}")

async def manual_backup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /backup - Genera backup manual inmediato"""
    if update.effective_user.id != SUPER_ADMIN_ID:
        await update.message.reply_text("❌ Solo Super Admin")
        return
    
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(['group_id', 'group_name', 'group_type', 'admin_id', 'backup_date'])
    
    now = datetime.now()
    for group in GROUPS:
        writer.writerow([
            group['group_id'],
            group['group_name'],
            group.get('type', 'VIP'),
            group['admin_id'],
            now.strftime('%Y-%m-%d %H:%M:%S')
        ])
    
    output.seek(0)
    await update.message.reply_document(
        document=output.getvalue().encode('utf-8-sig'),
        filename=f"backup_manual_{now.strftime('%Y%m%d_%H%M%S')}.csv",
        caption=f"📦 *Backup Manual*\n\n"
                f"📅 Fecha: {now.strftime('%d/%m/%Y %H:%M:%S')}\n"
                f"📊 Grupos incluidos: {len(GROUPS)}",
        parse_mode="Markdown"
    )
    output.close()

async def restore_backup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /restore - Restaura configuración desde archivo CSV"""
    if update.effective_user.id != SUPER_ADMIN_ID:
        await update.message.reply_text("❌ Solo Super Admin")
        return
    
    if not update.message.document:
        await update.message.reply_text(
            "❌ Envía el archivo CSV de backup junto con el comando.\n\n"
            "Ejemplo: Envía `/restore` y luego el archivo adjunto.",
            parse_mode="Markdown"
        )
        return
    
    await update.message.reply_text("🔄 Restaurando configuración...")
    
    try:
        file = await update.message.document.get_file()
        file_content = await file.download_as_bytearray()
        
        import io
        content = file_content.decode('utf-8')
        reader = csv.reader(io.StringIO(content))
        
        headers = next(reader)  # Saltar encabezados
        
        restored_count = 0
        for row in reader:
            if len(row) >= 4:
                group_id = int(row[0])
                group_name = row[1]
                group_type = row[2]
                admin_id = int(row[3])
                
                existing = get_group_by_id(group_id)
                if existing:
                    # Actualizar grupo existente
                    for g in GROUPS:
                        if g["group_id"] == group_id:
                            g["group_name"] = group_name
                            g["type"] = group_type
                            g["admin_id"] = admin_id
                            break
                    with db.get_connection() as conn:
                        with conn.cursor() as cur:
                            cur.execute("""
                            UPDATE groups SET group_name=%s, group_type=%s, admin_id=%s
                            WHERE group_id=%s
                            """, (group_name, group_type, admin_id, group_id))
                            conn.commit()
                else:
                    # Crear nuevo grupo
                    GROUPS.append({
                        "group_id": group_id,
                        "group_name": group_name,
                        "type": group_type,
                        "admin_id": admin_id
                    })
                    await db.save_group(group_id, group_name, admin_id, group_type)
                
                restored_count += 1
        
        await update.message.reply_text(
            f"✅ *Restauración completa*\n\n"
            f"📊 Grupos restaurados: {restored_count}\n"
            f"🔄 Reinicia el bot para aplicar todos los cambios.",
            parse_mode="Markdown"
        )
        
    except Exception as e:
        await update.message.reply_text(f"❌ Error al restaurar: {e}")

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

async def view_vip_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra todos los grupos VIP"""
    await show_groups_by_type(update, context, "VIP", True)

async def view_free_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra todos los grupos FREE"""
    await show_groups_by_type(update, context, "FREE", True)

async def show_groups_by_type(update: Update, context: ContextTypes.DEFAULT_TYPE, group_type: str, select_mode: bool = False):
    """Muestra grupos por tipo (VIP o FREE)"""
    query = update.callback_query
    groups = [g for g in GROUPS if g.get("type", "VIP") == group_type]
    if not groups:
        await query.edit_message_text(f"📭 No hay grupos {group_type}")
        return
    keyboard = [[InlineKeyboardButton(f"📌 {g['group_name']}", callback_data=f"select_group_{g['group_id']}")] for g in groups]
    if select_mode:
        keyboard.append([InlineKeyboardButton("🔙 Volver", callback_data="menu_view_groups")])
    await query.edit_message_text(f"📋 *Grupos {group_type}*", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def menu_edit_group_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra lista de grupos para seleccionar cuál editar (solo edición múltiple)"""
    query = update.callback_query
    await query.answer()
    
    if not GROUPS:
        await query.edit_message_text("📭 No hay grupos configurados")
        return
    
    keyboard = []
    for group in GROUPS:
        emoji = "👑" if group.get("type", "VIP") == "VIP" else "📋"
        keyboard.append([InlineKeyboardButton(f"{emoji} {group['group_name']}", callback_data=f"edit_multiple_{group['group_id']}")])
    
    keyboard.append([InlineKeyboardButton("🔙 Volver", callback_data="menu_groups")])
    
    await query.edit_message_text(
        "✏️ *Selecciona el grupo que deseas editar*\n\n"
        "Podrás cambiar nombre, administrador y tipo.\n"
        "Puedes hacer varios cambios antes de aplicar.",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )
    
async def menu_delete_group_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra lista de grupos para seleccionar cuál eliminar"""
    query = update.callback_query
    await query.answer()
    
    if not GROUPS:
        await query.edit_message_text("📭 No hay grupos configurados")
        return
    
    keyboard = []
    for group in GROUPS:
        emoji = "👑" if group.get("type", "VIP") == "VIP" else "📋"
        keyboard.append([InlineKeyboardButton(f"{emoji} {group['group_name']}", callback_data=f"delete_confirm_{group['group_id']}")])
    
    keyboard.append([InlineKeyboardButton("🔙 Volver", callback_data="menu_groups")])
    
    await query.edit_message_text(
        "❌ *Eliminar Grupo*\n\n"
        "⚠️ Esta acción es irreversible.\n"
        "Selecciona el grupo que deseas eliminar:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def delete_group_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    """Solicita confirmación para eliminar un grupo"""
    query = update.callback_query
    await query.answer()
    
    group = get_group_by_id(group_id)
    if not group:
        await query.edit_message_text("❌ Grupo no encontrado")
        return
    
    keyboard = [
        [InlineKeyboardButton("✅ Sí, eliminar", callback_data=f"delete_yes_{group_id}")],
        [InlineKeyboardButton("❌ Cancelar", callback_data="menu_groups")],
    ]
    
    await query.edit_message_text(
        f"⚠️ *Confirmar eliminación*\n\n"
        f"¿Estás seguro de que quieres eliminar el grupo?\n\n"
        f"📌 *{group['group_name']}*\n"
        f"🆔 ID: `{group['group_id']}`\n"
        f"👑 Admin: `{group['admin_id']}`\n\n"
        f"*Esta acción no se puede deshacer.*",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def delete_group_execute(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    """Elimina el grupo de la base de datos y memoria"""
    query = update.callback_query
    await query.answer()
    
    group = get_group_by_id(group_id)
    if not group:
        await query.edit_message_text("❌ Grupo no encontrado")
        return
    
    group_name = group['group_name']
    
    # Eliminar de memoria
    global GROUPS
    GROUPS = [g for g in GROUPS if g["group_id"] != group_id]
    
    # Eliminar de base de datos
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM groups WHERE group_id = %s", (group_id,))
            conn.commit()
    
    await query.edit_message_text(
        f"✅ *Grupo eliminado*\n\n"
        f"📌 {group_name}\n"
        f"🆔 ID: `{group_id}`\n\n"
        f"El grupo ha sido eliminado correctamente.",
        parse_mode="Markdown"
    )
    
    # Volver al menú de grupos después de 2 segundos
    await asyncio.sleep(2)
    await menu_groups(update, context)

async def menu_commands(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra la lista de comandos disponibles"""
    query = update.callback_query
    await query.answer()
    
    commands_text = """
📟 *Comandos Disponibles*

*Super Admin:*
• `/start` - Panel principal
• `/addgroup` - Agregar nuevo grupo
• `/groups` - Ver todos los grupos
• `/global` - Estadísticas globales

*Admin de Grupo:*
• `/start` - Panel de control
• `/add @user plan` - Agregar usuario
• `/add` en el grupo - Mismo que arriba

*Planes disponibles:*
• `trial` - 1 día ($0)
• `semanal` - 7 días ($10)
• `mensual` - 30 días ($20)

*Ejemplos:*
• `/add @juan semanal`
• `/add @maria mensual`
"""
    
    keyboard = [[InlineKeyboardButton("🔙 Volver", callback_data="back_to_admin")]]
    
    await query.edit_message_text(
        commands_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

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

async def edit_group_multiple(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    """Permite editar múltiples campos a la vez"""
    print(f"🔔 edit_group_multiple llamado para grupo: {group_id}")
    logger.info(f"🔔 edit_group_multiple llamado para grupo: {group_id}")
    query = update.callback_query
    await query.answer()
    
    group = get_group_by_id(group_id)
    if not group:
        await query.edit_message_text("❌ Grupo no encontrado")
        return
    
    context.user_data['editing_group_id'] = group_id
    context.user_data['editing_mode'] = 'multiple'
    # Inicializar cambios pendientes
    if 'pending_changes' not in context.user_data:
        context.user_data['pending_changes'] = {}
    
    keyboard = [
        [InlineKeyboardButton("📝 Cambiar nombre", callback_data=f"multi_name_{group_id}")],
        [InlineKeyboardButton("👤 Cambiar admin", callback_data=f"multi_admin_{group_id}")],
        [InlineKeyboardButton("🔄 Cambiar tipo", callback_data=f"multi_type_{group_id}")],
        [InlineKeyboardButton("✅ Aplicar todos los cambios", callback_data=f"multi_apply_{group_id}")],
        [InlineKeyboardButton("🔙 Volver", callback_data="menu_edit_group_select")]
    ]
    
    # Mostrar cambios pendientes actuales
    pending = context.user_data.get('pending_changes', {})
    pending_text = ""
    if pending:
        pending_text = "\n\n📝 *Cambios pendientes:*\n"
        if 'name' in pending:
            pending_text += f"• Nuevo nombre: `{pending['name']}`\n"
        if 'admin' in pending:
            pending_text += f"• Nuevo admin: `{pending['admin']}`\n"
        if 'type' in pending:
            pending_text += f"• Nuevo tipo: `{pending['type']}`\n"
    
    await query.edit_message_text(
        f"✏️ *Edición múltiple - {group['group_name']}*\n\n"
        f"📋 Tipo actual: {group.get('type', 'VIP')}\n"
        f"👑 Admin actual: `{group['admin_id']}`{pending_text}\n\n"
        f"Puedes hacer varios cambios antes de aplicarlos.\n"
        f"*Escribe 'cancelar' para cancelar la edición.*",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def multi_name_request(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    """Solicita el nuevo nombre para edición múltiple"""
    query = update.callback_query
    await query.answer()
    
    context.user_data['editing_field'] = 'multi_name'
    context.user_data['editing_group_id'] = group_id
    
    await query.edit_message_text(
        f"✏️ *Cambiar nombre (edición múltiple)*\n\n"
        f"Envía el *nuevo nombre* en el chat.\n\n"
        f"*Escribe 'cancelar' para cancelar.*",
        parse_mode="Markdown"
    )

async def multi_admin_request(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    """Solicita el nuevo admin para edición múltiple"""
    query = update.callback_query
    await query.answer()
    
    context.user_data['editing_field'] = 'multi_admin'
    context.user_data['editing_group_id'] = group_id
    
    await query.edit_message_text(
        f"👤 *Cambiar administrador (edición múltiple)*\n\n"
        f"Envía el *ID del nuevo administrador* en el chat.\n\n"
        f"*Para obtener un ID, usa @userinfobot*\n\n"
        f"*Escribe 'cancelar' para cancelar.*",
        parse_mode="Markdown"
    )

async def multi_type_request(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    """Muestra opciones para cambiar el tipo en edición múltiple"""
    query = update.callback_query
    await query.answer()
    
    keyboard = [
        [InlineKeyboardButton("👑 VIP", callback_data=f"multi_set_type_{group_id}_VIP")],
        [InlineKeyboardButton("📋 FREE", callback_data=f"multi_set_type_{group_id}_FREE")],
        [InlineKeyboardButton("🔙 Volver", callback_data=f"edit_multiple_{group_id}")]
    ]
    
    await query.edit_message_text(
        "🔄 *Selecciona el nuevo tipo*",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def multi_set_type(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int, new_type: str):
    """Guarda el cambio de tipo en pendientes"""
    query = update.callback_query
    await query.answer()
    
    if 'pending_changes' not in context.user_data:
        context.user_data['pending_changes'] = {}
    
    context.user_data['pending_changes']['type'] = new_type
    
    await query.edit_message_text(f"✅ *Tipo guardado:* {new_type}\n\nContinuando con edición múltiple...")
    await asyncio.sleep(1)
    await edit_group_multiple(update, context, group_id)
    
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

async def search_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /searchgrupo nombre - Busca grupos por nombre"""
    if update.effective_user.id != SUPER_ADMIN_ID:
        await update.message.reply_text("❌ Solo Super Admin")
        return
    
    if len(context.args) < 1:
        await update.message.reply_text("❌ Usa: `/searchgrupo nombre`", parse_mode="Markdown")
        return
    
    search_term = " ".join(context.args).lower()
    results = [g for g in GROUPS if search_term in g['group_name'].lower()]
    
    if not results:
        await update.message.reply_text(f"📭 No se encontraron grupos con '{search_term}'")
        return
    
    msg = f"🔍 *Resultados para '{search_term}'*\n\n"
    for group in results:
        emoji = "👑" if group.get("type", "VIP") == "VIP" else "📋"
        msg += f"{emoji} *{group['group_name']}*\n"
        msg += f"   🆔 ID: `{group['group_id']}`\n"
        msg += f"   👑 Admin: `{group['admin_id']}`\n\n"
    
    await update.message.reply_text(msg, parse_mode="Markdown")
                
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
        context.user_data.pop('pending_changes', None)
        context.user_data.pop('editing_mode', None)
        return
    
    field = context.user_data.get('editing_field')
    group_id = context.user_data.get('editing_group_id')
    
    if not field or not group_id:
        return
    
    group = get_group_by_id(group_id)
    if not group:
        await update.message.reply_text("❌ Grupo no encontrado")
        return
    
    # Para edición múltiple
    if field == 'multi_name':
        # Guardar cambio pendiente
        if 'pending_changes' not in context.user_data:
            context.user_data['pending_changes'] = {}
        context.user_data['pending_changes']['name'] = text
        
        await update.message.reply_text(f"✅ *Nombre guardado:* {text}\n\nContinuando con edición múltiple...")
        context.user_data.pop('editing_field', None)
        await edit_group_multiple(update, context, group_id)
        return
    
    elif field == 'multi_admin':
        try:
            new_admin = int(text)
            if 'pending_changes' not in context.user_data:
                context.user_data['pending_changes'] = {}
            context.user_data['pending_changes']['admin'] = new_admin
            
            await update.message.reply_text(f"✅ *Administrador guardado:* `{new_admin}`\n\nContinuando con edición múltiple...")
            context.user_data.pop('editing_field', None)
            await edit_group_multiple(update, context, group_id)
        except ValueError:
            await update.message.reply_text("❌ Error: El ID debe ser un número")
        return
    
    # Para edición simple (si la mantienes)
    elif field == 'name':
        # ... tu código existente ...
        pass
    elif field == 'admin':
        # ... tu código existente ...
        pass

async def multi_apply_changes(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    """Aplica todos los cambios pendientes"""
    query = update.callback_query
    await query.answer()
    
    pending = context.user_data.get('pending_changes', {})
    
    if not pending:
        await query.edit_message_text("❌ No hay cambios pendientes para aplicar")
        return
    
    group = get_group_by_id(group_id)
    if not group:
        await query.edit_message_text("❌ Grupo no encontrado")
        return
    
    changes_made = []
    
    # Aplicar cambios en orden
    if 'name' in pending:
        for g in GROUPS:
            if g["group_id"] == group_id:
                g["group_name"] = pending['name']
                break
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE groups SET group_name = %s WHERE group_id = %s", (pending['name'], group_id))
                conn.commit()
        changes_made.append(f"📝 Nombre: → {pending['name']}")
    
    if 'admin' in pending:
        for g in GROUPS:
            if g["group_id"] == group_id:
                g["admin_id"] = pending['admin']
                break
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE groups SET admin_id = %s WHERE group_id = %s", (pending['admin'], group_id))
                conn.commit()
        changes_made.append(f"👤 Admin: → {pending['admin']}")
    
    if 'type' in pending:
        for g in GROUPS:
            if g["group_id"] == group_id:
                g["type"] = pending['type']
                break
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE groups SET group_type = %s WHERE group_id = %s", (pending['type'], group_id))
                conn.commit()
        changes_made.append(f"🔄 Tipo: → {pending['type']}")
    
    # Limpiar pendientes
    context.user_data.pop('pending_changes', None)
    context.user_data.pop('editing_mode', None)
    context.user_data.pop('editing_group_id', None)
    
    msg = f"✅ *Cambios aplicados correctamente*\n\n" + "\n".join(changes_made)
    
    await query.edit_message_text(msg, parse_mode="Markdown")
    await asyncio.sleep(2)
    await menu_edit_group_select(update, context)

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    print(f"🔔 CALLBACK RECIBIDO: {data}")
    logger.info(f"🔔 CALLBACK RECIBIDO: {data}")
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
    elif data.startswith("select_group_"):
        group_id = int(data.replace("select_group_", ""))
        await select_group(update, context, group_id)
    elif data == "back_to_admin":
        await start(update, context)
    elif data == "menu_groups":
        await menu_groups(update, context)
    elif data == "menu_view_groups":
        await menu_view_groups(update, context)
    elif data == "view_vip_groups":
        await view_vip_groups(update, context)
    elif data == "view_free_groups":
        await view_free_groups(update, context)
    elif data == "menu_edit_group_select":
        await menu_edit_group_select(update, context)
    elif data == "menu_delete_group_select":
        await menu_delete_group_select(update, context)
    elif data.startswith("delete_confirm_"):
        group_id = int(data.replace("delete_confirm_", ""))
        await delete_group_confirm(update, context, group_id)
    elif data.startswith("delete_yes_"):
        group_id = int(data.replace("delete_yes_", ""))
        await delete_group_execute(update, context, group_id)
    elif data == "menu_commands":
        await menu_commands(update, context)
    elif data.startswith("multi_apply_"):
        group_id = int(data.replace("multi_apply_", ""))
        await multi_apply_changes(update, context, group_id)
    elif data.startswith("multi_name_"):
        group_id = int(data.replace("multi_name_", ""))
        await multi_name_request(update, context, group_id)
    elif data.startswith("multi_admin_"):
        group_id = int(data.replace("multi_admin_", ""))
        await multi_admin_request(update, context, group_id)    
    elif data.startswith("multi_type_"):
        group_id = int(data.replace("multi_type_", ""))
        await multi_type_request(update, context, group_id)    
    elif data.startswith("multi_set_type_"):
        parts = data.split("_")
        group_id = int(parts[3])
        new_type = parts[4]
        await multi_set_type(update, context, group_id, new_type)    
    
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
    bot_app.add_handler(CommandHandler("backup", manual_backup))
    bot_app.add_handler(CommandHandler("restore", restore_backup))
    scheduler.add_job(check_expired_subscriptions, 'interval', hours=6)
    scheduler.start()
    scheduler.add_job(auto_backup, 'interval', hours=24)  # Revisa cada 24 horas si es momento de backup
    logger.info("🤖 Bot iniciado")
    await bot_app.initialize()
    await bot_app.start()
    await bot_app.updater.start_polling(drop_pending_updates=True)
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
