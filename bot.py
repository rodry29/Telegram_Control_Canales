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

# ==================== CONFIGURACIÓN ====================
TOKEN = "8782944509:AAFqTBOCPwJdhRgt2Qxx4Usj45DNF83Y86s"
DATABASE_URL = os.getenv("DATABASE_URL")
SUPER_ADMIN_ID = 5054216496

# Planes y precios
PLANS = {
    "trial": {"days": 1, "price": 0, "name": "🎁 Trial (1 día)"},
    "semanal": {"days": 7, "price": 10, "name": "📅 Semanal (7 días)"},
    "mensual": {"days": 30, "price": 20, "name": "📆 Mensual (30 días)"}
}

# Configuración de grupos desde variable de entorno
# Formato: "GROUP_ID:GROUP_NAME:ADMIN_ID,GROUP_ID:GROUP_NAME:ADMIN_ID"
GROUPS_CONFIG = os.getenv("GROUPS_CONFIG", "")
GROUPS = []

for group_config in GROUPS_CONFIG.split(","):
    if group_config.strip():
        parts = group_config.strip().split(":")
        if len(parts) == 3:
            GROUPS.append({
                "group_id": int(parts[0]),
                "group_name": parts[1],
                "admin_id": int(parts[2])
            })

# Logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


# ==================== FUNCIONES DE UTILIDAD ====================
def get_group_by_id(group_id: int) -> Optional[dict]:
    """Obtiene la configuración de un grupo por su ID"""
    for group in GROUPS:
        if group["group_id"] == group_id:
            return group
    return None


def get_groups_by_admin(admin_id: int) -> list:
    """Obtiene todos los grupos donde un usuario es admin"""
    if admin_id == SUPER_ADMIN_ID:
        return GROUPS
    return [g for g in GROUPS if g["admin_id"] == admin_id]


def can_manage_group(user_id: int, group_id: int) -> bool:
    """Verifica si un usuario puede gestionar un grupo"""
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
        """Inicializa las tablas con soporte multi-grupo"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Tabla groups
                cur.execute("""
                CREATE TABLE IF NOT EXISTS groups (
                    group_id BIGINT PRIMARY KEY,
                    group_name TEXT,
                    admin_id BIGINT,
                    super_admin_id BIGINT,
                    created_at TIMESTAMP DEFAULT NOW(),
                    settings JSONB DEFAULT '{}'::jsonb
                )
                """)
                logger.info("✅ Tabla 'groups' lista")

                # Tabla users
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
                logger.info("✅ Tabla 'users' lista")

                # Tabla payments
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
                logger.info("✅ Tabla 'payments' lista")

                # Índices
                cur.execute("CREATE INDEX IF NOT EXISTS idx_users_group ON users(group_id, status)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_users_end_date ON users(group_id, end_date)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_payments_group ON payments(group_id, payment_date)")
                logger.info("✅ Índices creados")

                conn.commit()

                # Registrar grupos configurados
                for group in GROUPS:
                    cur.execute("""
                    INSERT INTO groups (group_id, group_name, admin_id, super_admin_id)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (group_id) DO UPDATE SET
                        group_name = EXCLUDED.group_name,
                        admin_id = EXCLUDED.admin_id
                    """, (group["group_id"], group["group_name"], group["admin_id"], SUPER_ADMIN_ID))
                conn.commit()
                logger.info(f"✅ {len(GROUPS)} grupos registrados")

        logger.info("✅ Base de datos inicializada correctamente")

    async def get_user_by_username(self, username: str, group_id: int = None) -> Optional[Dict]:
        """Busca usuario por username (opcionalmente por grupo)"""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if group_id:
                    cur.execute("SELECT * FROM users WHERE username = %s AND group_id = %s", (username, group_id))
                else:
                    cur.execute("SELECT * FROM users WHERE username = %s", (username,))
                return cur.fetchone()

    async def get_user_by_id(self, user_id: int, group_id: int = None) -> Optional[Dict]:
        """Busca usuario por user_id (opcionalmente por grupo)"""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if group_id:
                    cur.execute("SELECT * FROM users WHERE user_id = %s AND group_id = %s", (user_id, group_id))
                else:
                    cur.execute("SELECT * FROM users WHERE user_id = %s", (user_id,))
                return cur.fetchone()

    async def register_user_auto(self, group_id: int, user_id: int, username: str, first_name: str) -> Tuple[bool, str]:
        """Registra usuario automáticamente al entrar al grupo"""
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

                elif existing['end_date'] <= now:
                    time_since_expiry = now - existing['end_date']
                    if time_since_expiry.total_seconds() < 3600:
                        new_end = now + timedelta(hours=1)
                        cur.execute("UPDATE users SET end_date = %s, updated_at = NOW() WHERE user_id = %s AND group_id = %s", 
                                   (new_end, user_id, group_id))
                        conn.commit()
                        return True, "periodo_gracia"
                    else:
                        return False, "expirado"
                else:
                    return False, "expirado"

    async def add_or_update_user(self, group_id: int, username: str, plan: str, user_id: int = None) -> Tuple[bool, str]:
        """Agrega o renueva usuario"""
        now = datetime.now()

        if plan not in PLANS:
            return False, "❌ Plan inválido"

        config = PLANS[plan]

        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                existing = await self.get_user_by_username(username, group_id)

                if existing:
                    user_id = existing['user_id']
                    if plan == "trial" and existing['trial_used']:
                        return False, "❌ Este usuario ya usó su prueba gratuita"

                    end_date = now + timedelta(days=config['days'])
                    cur.execute("""
                    UPDATE users 
                    SET plan = %s, start_date = %s, end_date = %s, status = 'active',
                        updated_at = NOW(), username = %s,
                        trial_used = trial_used OR %s
                    WHERE user_id = %s AND group_id = %s
                    """, (plan, now, end_date, username, plan == "trial", user_id, group_id))
                else:
                    if not user_id:
                        return False, f"❌ No tengo el ID de @{username}. Pídele que envíe un mensaje al bot."

                    end_date = now + timedelta(days=config['days'])
                    trial_used = (plan == "trial")
                    cur.execute("""
                    INSERT INTO users (user_id, group_id, username, first_name, plan, start_date, end_date, trial_used, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'active')
                    """, (user_id, group_id, username, "", plan, now, end_date, trial_used))

                cur.execute("""
                INSERT INTO payments (user_id, group_id, username, plan, amount, payment_date)
                VALUES (%s, %s, %s, %s, %s, %s)
                """, (user_id, group_id, username, plan, config['price'], now))

                conn.commit()
                return True, f"✅ @{username} activado con {config['name']}\n📅 Expira: {end_date.strftime('%d/%m/%Y')}"

    async def get_expiring_users(self, group_id: int, days_before: int) -> List[Dict]:
        """Obtiene usuarios que expiran en X días en un grupo"""
        target_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=days_before)
        target_end = target_date + timedelta(days=1)

        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                SELECT user_id, username, plan, end_date
                FROM users
                WHERE group_id = %s AND status = 'active'
                AND end_date >= %s AND end_date < %s
                """, (group_id, target_date, target_end))
                return cur.fetchall()

    async def get_expired_users(self, group_id: int) -> List[Dict]:
        """Obtiene usuarios ya expirados en un grupo"""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                SELECT user_id, username, plan, end_date
                FROM users
                WHERE group_id = %s AND status = 'active' AND end_date < NOW()
                """, (group_id,))
                return cur.fetchall()

    async def expire_user(self, user_id: int, group_id: int):
        """Marca usuario como expirado"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                UPDATE users SET status = 'expired', updated_at = NOW()
                WHERE user_id = %s AND group_id = %s
                """, (user_id, group_id))
                conn.commit()

    async def get_all_active_users(self, group_id: int) -> List[Dict]:
        """Obtiene todos los usuarios activos de un grupo"""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                SELECT user_id, username, plan, end_date,
                       EXTRACT(DAY FROM (end_date - NOW())) as days_left
                FROM users
                WHERE group_id = %s AND status = 'active' AND end_date > NOW()
                ORDER BY end_date ASC
                """, (group_id,))
                return cur.fetchall()

    async def get_active_users_count(self, group_id: int) -> int:
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM users WHERE group_id = %s AND status = 'active' AND end_date > NOW()", (group_id,))
                return cur.fetchone()[0]

    async def get_monthly_earnings(self, group_id: int) -> Dict:
        """Obtiene ganancias del mes para un grupo"""
        now = datetime.now()
        start_date = datetime(now.year, now.month, 1)
        
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                SELECT plan, COUNT(*) as count, COALESCE(SUM(amount), 0) as total
                FROM payments
                WHERE group_id = %s AND payment_date >= %s
                GROUP BY plan
                """, (group_id, start_date))
                summary = cur.fetchall()
                
                total = sum(row['total'] for row in summary)
                
                cur.execute("""
                SELECT COUNT(*) as new_users
                FROM users
                WHERE group_id = %s AND created_at >= %s
                """, (group_id, start_date))
                new_users = cur.fetchone()['new_users']
                
                return {"summary": summary, "total": total, "new_users": new_users}


# ==================== INSTANCIA GLOBAL ====================
db = Database(DATABASE_URL)
scheduler = AsyncIOScheduler()
bot_app = None


# ==================== HANDLERS ====================
async def detect_new_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Detecta nuevos miembros en cualquier grupo configurado"""
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

        registered, result = await db.register_user_auto(chat_id, user_id, username, first_name)

        if registered and result == "trial_nuevo":
            await context.bot.send_message(
                user_id,
                f"🎉 ¡Bienvenido @{username}!\n\n✨ Has recibido un **TRIAL GRATIS de 1 día**\n📅 Expira: {(datetime.now() + timedelta(days=1)).strftime('%d/%m/%Y')}",
                parse_mode="Markdown"
            )
            await context.bot.send_message(
                group["admin_id"],
                f"🆕 Nuevo usuario @{username} en {group['group_name']} - Trial activado"
            )
        elif not registered and result == "expirado":
            await context.bot.ban_chat_member(chat_id, user_id)
            await context.bot.send_message(
                group["admin_id"],
                f"🚫 @{username} intentó reingresar pero está expirado"
            )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Panel de control según rol"""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id

    # Super Admin
    if user_id == SUPER_ADMIN_ID:
        keyboard = [
            [InlineKeyboardButton("📊 Todos los grupos", callback_data="all_groups")],
            [InlineKeyboardButton("➕ Agregar grupo", callback_data="add_group")],
            [InlineKeyboardButton("📈 Estadísticas globales", callback_data="global_stats")],
            [InlineKeyboardButton("📥 Reporte consolidado", callback_data="consolidated_report")]
        ]
        await update.message.reply_text(
            "👑 *Panel de Super Administrador*\n\nTienes control sobre todos los grupos.",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
        return

    # Admin de grupos
    user_groups = get_groups_by_admin(user_id)
    if not user_groups:
        await update.message.reply_text("❌ No tienes grupos asignados")
        return

    if len(user_groups) == 1:
        group = user_groups[0]
        context.user_data['current_group'] = group['group_id']
        keyboard = [
            [InlineKeyboardButton("➕ Agregar usuario", callback_data="add_user")],
            [InlineKeyboardButton("📊 Usuarios activos", callback_data="list_active")],
            [InlineKeyboardButton("💰 Ganancias", callback_data="earnings")],
            [InlineKeyboardButton("📥 Exportar mes", callback_data="export_month")]
        ]
        await update.message.reply_text(
            f"🤖 *Panel de Control - {group['group_name']}*",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
    else:
        keyboard = [[InlineKeyboardButton(f"📌 {g['group_name']}", callback_data=f"select_group_{g['group_id']}")] for g in user_groups]
        await update.message.reply_text(
            "📋 *Selecciona un grupo*",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )


async def add_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/add @username plan - Agrega o renueva usuario"""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id

    # Obtener grupo actual
    current_group = context.user_data.get('current_group')
    if not current_group:
        group = get_group_by_id(chat_id)
        if group and can_manage_group(user_id, chat_id):
            current_group = chat_id
        else:
            await update.message.reply_text("❌ Usa /start primero para seleccionar un grupo")
            return

    if not can_manage_group(user_id, current_group):
        await update.message.reply_text("❌ No tienes permiso para gestionar este grupo")
        return

    if len(context.args) < 2:
        await update.message.reply_text("❌ Usa: `/add @username plan`\nPlanes: trial, semanal, mensual", parse_mode="Markdown")
        return

    username = context.args[0].replace("@", "")
    plan = context.args[1].lower()

    if plan not in PLANS:
        await update.message.reply_text("❌ Plan inválido")
        return

    existing = await db.get_user_by_username(username, current_group)
    success, msg = await db.add_or_update_user(current_group, username, plan, existing['user_id'] if existing else None)
    await update.message.reply_text(msg)

    if success and existing:
        await context.bot.unban_chat_member(current_group, existing['user_id'])


async def list_active_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Lista usuarios activos del grupo actual"""
    user_id = update.effective_user.id
    query = update.callback_query
    message = query.message if query else update.message

    group_id = context.user_data.get('current_group')
    if not group_id:
        await message.reply_text("❌ Selecciona un grupo primero con /start")
        return

    if not can_manage_group(user_id, group_id):
        await message.reply_text("❌ No autorizado")
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
    """Muestra ganancias del mes"""
    user_id = update.effective_user.id
    query = update.callback_query
    message = query.message if query else update.message

    group_id = context.user_data.get('current_group')
    if not group_id:
        await message.reply_text("❌ Selecciona un grupo primero con /start")
        return

    if not can_manage_group(user_id, group_id):
        await message.reply_text("❌ No autorizado")
        return

    earnings = await db.get_monthly_earnings(group_id)
    now = datetime.now()

    msg = f"💰 *GANANCIAS DE {now.strftime('%B %Y').upper()}*\n\n"
    if not earnings['summary']:
        msg += "📭 No hay ventas registradas"
    else:
        for plan in earnings['summary']:
            plan_name = PLANS.get(plan['plan'], {}).get('name', plan['plan'])
            msg += f"• {plan_name}: {plan['count']} ventas - ${plan['total']}\n"
        msg += f"\n💵 *TOTAL*: ${earnings['total']}\n👥 *Nuevos usuarios*: {earnings['new_users']}"

    await message.reply_text(msg, parse_mode="Markdown")


async def export_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Exporta reporte del mes a CSV"""
    user_id = update.effective_user.id
    query = update.callback_query
    if query:
        await query.answer()
        message = query.message
    else:
        message = update.message

    group_id = context.user_data.get('current_group')
    if not group_id:
        await message.reply_text("❌ Selecciona un grupo primero con /start")
        return

    if not can_manage_group(user_id, group_id):
        await message.reply_text("❌ No autorizado")
        return

    now = datetime.now()
    start_date = datetime(now.year, now.month, 1)

    with db.get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
            SELECT user_id, username, plan, amount, payment_date
            FROM payments
            WHERE group_id = %s AND payment_date >= %s
            ORDER BY payment_date DESC
            """, (group_id, start_date))
            transactions = cur.fetchall()

    if not transactions:
        await message.reply_text(f"📭 No hay transacciones en {now.strftime('%B %Y')}")
        return

    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(['Fecha', 'User ID', 'Username', 'Plan', 'Monto'])
    writer.writerow(['='*50, '='*10, '='*20, '='*10, '='*15])

    for t in transactions:
        writer.writerow([
            t['payment_date'].strftime('%Y-%m-%d %H:%M:%S'),
            t['user_id'],
            t['username'] or 'Sin username',
            t['plan'].upper(),
            f"${t['amount']}"
        ])

    output.seek(0)
    await message.reply_document(
        document=output.getvalue().encode('utf-8-sig'),
        filename=f"reporte_{now.year}_{now.month:02d}.csv",
        caption=f"📊 Reporte de {now.strftime('%B %Y')}"
    )
    output.close()


async def list_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Lista todos los grupos (solo Super Admin)"""
    query = update.callback_query
    if query:
        await query.answer()
        message = query.message
    else:
        message = update.message

    if update.effective_user.id != SUPER_ADMIN_ID:
        await message.reply_text("❌ Solo el Super Admin puede ver esto")
        return

    if not GROUPS:
        await message.reply_text("📭 No hay grupos configurados")
        return

    msg = "📊 *GRUPOS CONFIGURADOS*\n\n"
    for group in GROUPS:
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM users WHERE group_id = %s AND status = 'active' AND end_date > NOW()", (group["group_id"],))
                active = cur.fetchone()[0]
                cur.execute("SELECT COALESCE(SUM(amount), 0) FROM payments WHERE group_id = %s AND payment_date >= date_trunc('month', NOW())", (group["group_id"],))
                monthly = cur.fetchone()[0]

        msg += f"📌 *{group['group_name']}*\n   🆔 ID: `{group['group_id']}`\n   👑 Admin: `{group['admin_id']}`\n   👥 Activos: {active}\n   💰 Mes: ${monthly}\n\n"

    await message.reply_text(msg, parse_mode="Markdown")


async def add_group_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Agrega un nuevo grupo (solo Super Admin)"""
    if update.effective_user.id != SUPER_ADMIN_ID:
        await update.message.reply_text("❌ Solo el Super Admin puede agregar grupos")
        return

    if len(context.args) < 3:
        await update.message.reply_text("❌ Formato: `/addgroup group_id \"nombre\" admin_id`", parse_mode="Markdown")
        return

    try:
        group_id = int(context.args[0])
        group_name = " ".join(context.args[1:-1]).strip('"')
        admin_id = int(context.args[-1])

        if get_group_by_id(group_id):
            await update.message.reply_text(f"❌ El grupo {group_id} ya existe")
            return

        GROUPS.append({"group_id": group_id, "group_name": group_name, "admin_id": admin_id})

        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                INSERT INTO groups (group_id, group_name, admin_id, super_admin_id)
                VALUES (%s, %s, %s, %s)
                """, (group_id, group_name, admin_id, SUPER_ADMIN_ID))
                conn.commit()

        await update.message.reply_text(f"✅ Grupo '{group_name}' agregado correctamente", parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")


async def global_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Estadísticas globales (solo Super Admin)"""
    query = update.callback_query
    if query:
        await query.answer()
        message = query.message
    else:
        message = update.message

    if update.effective_user.id != SUPER_ADMIN_ID:
        await message.reply_text("❌ No autorizado")
        return

    if not GROUPS:
        await message.reply_text("📭 No hay grupos")
        return

    total_users, total_active, total_monthly = 0, 0, 0
    msg = "🌍 *ESTADÍSTICAS GLOBALES*\n\n"

    for group in GROUPS:
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM users WHERE group_id = %s", (group["group_id"],))
                total = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM users WHERE group_id = %s AND status = 'active' AND end_date > NOW()", (group["group_id"],))
                active = cur.fetchone()[0]
                cur.execute("SELECT COALESCE(SUM(amount), 0) FROM payments WHERE group_id = %s AND payment_date >= date_trunc('month', NOW())", (group["group_id"],))
                monthly = cur.fetchone()[0]

                total_users += total
                total_active += active
                total_monthly += monthly

        msg += f"📌 *{group['group_name']}*\n   👥 {active}/{total} activos\n   💰 ${monthly}\n\n"

    msg += f"━━━━━━━━━━━━━━━\n📊 *TOTALES*\n👥 Usuarios: {total_active}/{total_users}\n💰 Ganancias mes: ${total_monthly}"
    await message.reply_text(msg, parse_mode="Markdown")


async def consolidated_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Reporte consolidado (solo Super Admin)"""
    query = update.callback_query
    if query:
        await query.answer()
        message = query.message
    else:
        message = update.message

    if update.effective_user.id != SUPER_ADMIN_ID:
        await message.reply_text("❌ No autorizado")
        return

    if not GROUPS:
        await message.reply_text("📭 No hay grupos")
        return

    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(['Grupo', 'Usuario', 'Plan', 'Monto', 'Fecha'])

    for group in GROUPS:
        with db.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                SELECT username, plan, amount, payment_date
                FROM payments
                WHERE group_id = %s AND payment_date >= date_trunc('month', NOW())
                ORDER BY payment_date DESC
                """, (group["group_id"],))
                for p in cur.fetchall():
                    writer.writerow([group['group_name'], p['username'] or 'Desconocido', p['plan'], f"${p['amount']}", p['payment_date'].strftime('%Y-%m-%d')])

    output.seek(0)
    await message.reply_document(
        document=output.getvalue().encode('utf-8-sig'),
        filename=f"consolidado_{datetime.now().strftime('%Y%m')}.csv",
        caption="📊 Reporte Consolidado"
    )
    output.close()


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja todos los callbacks"""
    query = update.callback_query
    await query.answer()
    logger.info(f"📱 Callback: {query.data}")

    data = query.data

    if data == "add_user":
        await query.edit_message_text("📝 Usa: `/add @username plan`", parse_mode="Markdown")
    elif data == "list_active":
        await list_active_users(update, context)
    elif data == "earnings":
        await show_earnings(update, context)
    elif data == "export_month":
        await export_report(update, context)
    elif data == "all_groups":
        await list_groups(update, context)
    elif data == "add_group":
        await query.edit_message_text("📝 Usa: `/addgroup group_id \"nombre\" admin_id`", parse_mode="Markdown")
    elif data == "global_stats":
        await global_stats(update, context)
    elif data == "consolidated_report":
        await consolidated_report(update, context)
    elif data.startswith("select_group_"):
        group_id = int(data.replace("select_group_", ""))
        context.user_data['current_group'] = group_id
        group = get_group_by_id(group_id)
        if group:
            keyboard = [
                [InlineKeyboardButton("➕ Agregar usuario", callback_data="add_user")],
                [InlineKeyboardButton("📊 Usuarios activos", callback_data="list_active")],
                [InlineKeyboardButton("💰 Ganancias", callback_data="earnings")],
                [InlineKeyboardButton("📥 Exportar mes", callback_data="export_month")]
            ]
            await query.edit_message_text(
                f"🤖 *Panel - {group['group_name']}*",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="Markdown"
            )


# ==================== TAREAS PROGRAMADAS ====================
async def check_expired_subscriptions():
    """Expulsa usuarios con suscripción vencida"""
    for group in GROUPS:
        expired_users = await db.get_expired_users(group["group_id"])
        for user in expired_users:
            await db.expire_user(user['user_id'], group["group_id"])
            try:
                await bot_app.bot.ban_chat_member(group["group_id"], user['user_id'])
                await bot_app.bot.send_message(group["admin_id"], f"🚫 @{user['username']} expulsado - suscripción vencida")
            except Exception as e:
                logger.error(f"Error expulsando: {e}")


# ==================== MAIN ====================
async def main():
    global bot_app

    await db.init_tables()
    logger.info("📦 Base de datos lista")

    defaults = Defaults(parse_mode="HTML")
    bot_app = ApplicationBuilder().token(TOKEN).defaults(defaults).build()

    # Comandos
    bot_app.add_handler(CommandHandler("start", start))
    bot_app.add_handler(CommandHandler("add", add_user_command))
    bot_app.add_handler(CommandHandler("groups", list_groups))
    bot_app.add_handler(CommandHandler("addgroup", add_group_command))
    bot_app.add_handler(CallbackQueryHandler(handle_callback))

    # Detectar nuevos miembros
    bot_app.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, detect_new_member))

    # Tareas
    scheduler.add_job(check_expired_subscriptions, 'interval', hours=6)
    scheduler.start()

    logger.info("🤖 Bot iniciado")

    await bot_app.initialize()
    await bot_app.start()
    await bot_app.updater.start_polling()
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
