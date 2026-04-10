import os
import csv
import asyncio
import logging
from io import StringIO
from datetime import datetime, timedelta
from typing import Optional

import psycopg2
from psycopg2 import pool
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
    logger.critical("❌ ERROR: No se encontró TELEGRAM_TOKEN")
    exit(1)

if not DATABASE_URL:
    logger.critical("❌ ERROR: No se encontró DATABASE_URL")
    exit(1)

PLANS = {
    "trial":   {"days": 1,  "price": 0,  "name": "🎁 Trial (1 día)"},
    "semanal": {"days": 7,  "price": 10, "name": "📅 Semanal (7 días)"},
    "mensual": {"days": 30, "price": 20, "name": "📆 Mensual (30 días)"}
}

GROUPS_CONFIG = os.getenv("GROUPS_CONFIG", "")
GROUPS = []

for _group_config in GROUPS_CONFIG.split(","):
    if _group_config.strip():
        _parts = _group_config.strip().split(":")
        if len(_parts) == 4:
            GROUPS.append({
                "group_id":   int(_parts[0]),
                "type":       _parts[1].upper(),
                "group_name": _parts[2],
                "admin_id":   int(_parts[3])
            })

# ==================== FUNCIONES DE UTILIDAD ====================
def get_group_by_id(group_id: int) -> Optional[dict]:
    for group in GROUPS:
        if group["group_id"] == group_id:
            return group
    return None

def get_groups_by_admin(admin_id: int, group_type: str = None) -> list:
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

# ==================== BASE DE DATOS (con pool de conexiones) ====================
class Database:
    """
    Usa un ThreadedConnectionPool para reutilizar conexiones
    en lugar de abrir/cerrar una nueva por cada operación.
    Con 10k usuarios y plan gratuito de Railway esto es crítico.
    """

    def __init__(self, db_url: str):
        self.db_url = db_url
        # min=2 conexiones siempre listas, max=8 para no saturar Railway Free
        self._pool: pool.ThreadedConnectionPool = None

    def _get_pool(self) -> pool.ThreadedConnectionPool:
        if self._pool is None or self._pool.closed:
            self._pool = pool.ThreadedConnectionPool(
                minconn=2,
                maxconn=8,
                dsn=self.db_url
            )
            logger.info("✅ Connection pool creado (min=2, max=8)")
        return self._pool

    def _execute(self, func):
        """
        Obtiene una conexión del pool, ejecuta func(conn) y la devuelve.
        Wrapper síncrono para usar con asyncio.to_thread().
        """
        p = self._get_pool()
        conn = p.getconn()
        try:
            conn.autocommit = False          # control manual explícito
            result = func(conn)
            conn.commit()
            return result
        except Exception:
            conn.rollback()
            raise
        finally:
            p.putconn(conn)

    async def _run(self, func):
        """
        Ejecuta una operación de BD en un thread separado para no bloquear
        el event loop de asyncio (crítico con python-telegram-bot v20+).
        """
        return await asyncio.to_thread(self._execute, func)

    # ── Helper para registrar usuario FREE (evita duplicación de código) ──
    def _insert_free_user_sync(self, conn, user_id: int, chat_id: int,
                               username: str, first_name: str):
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO users
                    (user_id, group_id, username, first_name, plan,
                     start_date, end_date, status, trial_used)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (user_id, group_id) DO NOTHING
            """, (user_id, chat_id, username, first_name, "FREE",
                  datetime.now(), datetime.now() + timedelta(days=365),
                  "potencial", False))

    async def init_tables(self):
        def _init(conn):
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS groups (
                        group_id     BIGINT PRIMARY KEY,
                        group_name   TEXT,
                        group_type   TEXT DEFAULT 'VIP',
                        admin_id     BIGINT,
                        super_admin_id BIGINT,
                        created_at   TIMESTAMP DEFAULT NOW(),
                        settings     JSONB DEFAULT '{}'::jsonb
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        id          SERIAL PRIMARY KEY,
                        user_id     BIGINT NOT NULL,
                        group_id    BIGINT NOT NULL,
                        username    TEXT,
                        first_name  TEXT,
                        plan        TEXT NOT NULL,
                        start_date  TIMESTAMP NOT NULL,
                        end_date    TIMESTAMP NOT NULL,
                        status      TEXT DEFAULT 'active',
                        trial_used  BOOLEAN DEFAULT FALSE,
                        created_at  TIMESTAMP DEFAULT NOW(),
                        updated_at  TIMESTAMP DEFAULT NOW(),
                        UNIQUE(user_id, group_id)
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS payments (
                        id           SERIAL PRIMARY KEY,
                        user_id      BIGINT NOT NULL,
                        group_id     BIGINT NOT NULL,
                        username     TEXT,
                        first_name   TEXT,
                        plan         TEXT NOT NULL,
                        amount       INTEGER NOT NULL,
                        payment_date TIMESTAMP DEFAULT NOW()
                    )
                """)
                # Índices para queries frecuentes con 10k usuarios
                cur.execute("CREATE INDEX IF NOT EXISTS idx_users_group    ON users(group_id, status)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_users_end_date ON users(group_id, end_date)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_users_uid_gid  ON users(user_id, group_id)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_payments_group ON payments(group_id, payment_date)")
        await self._run(_init)
        logger.info("✅ Base de datos inicializada")

    async def load_groups_from_db(self):
        def _load(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT group_id, group_name, admin_id,
                           COALESCE(group_type, 'VIP') as group_type
                    FROM groups
                """)
                return cur.fetchall()
        rows = await self._run(_load)
        if rows:
            GROUPS.clear()
            for g in rows:
                GROUPS.append({
                    "group_id":   g["group_id"],
                    "group_name": g["group_name"],
                    "admin_id":   g["admin_id"],
                    "type":       g["group_type"]
                })
            logger.info(f"📦 {len(GROUPS)} grupos cargados desde BD")
            return True
        return False

    async def save_group(self, group_id: int, group_name: str,
                         admin_id: int, group_type: str = "VIP"):
        def _save(conn):
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO groups (group_id, group_name, admin_id, super_admin_id, group_type)
                    VALUES (%s,%s,%s,%s,%s)
                    ON CONFLICT (group_id) DO UPDATE SET
                        group_name = EXCLUDED.group_name,
                        admin_id   = EXCLUDED.admin_id,
                        group_type = EXCLUDED.group_type
                """, (group_id, group_name, admin_id, SUPER_ADMIN_ID, group_type))
        await self._run(_save)

    async def get_user_by_username(self, username: str, group_id: int = None):
        def _get(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if group_id:
                    cur.execute(
                        "SELECT * FROM users WHERE LOWER(username)=LOWER(%s) AND group_id=%s",
                        (username, group_id)
                    )
                else:
                    cur.execute(
                        "SELECT * FROM users WHERE LOWER(username)=LOWER(%s)",
                        (username,)
                    )
                return cur.fetchone()
        return await self._run(_get)

    async def get_user_by_id(self, user_id: int, group_id: int):
        """Lookup por user_id+group_id — usa el índice idx_users_uid_gid, muy rápido."""
        def _get(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT user_id FROM users WHERE user_id=%s AND group_id=%s LIMIT 1",
                    (user_id, group_id)
                )
                return cur.fetchone()
        return await self._run(_get)

    async def register_user_auto(self, group_id: int, user_id: int,
                                  username: str, first_name: str):
        now = datetime.now()

        def _register(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT user_id, trial_used, status, end_date FROM users WHERE user_id=%s AND group_id=%s",
                    (user_id, group_id)
                )
                existing = cur.fetchone()
                if not existing:
                    end_date = now + timedelta(days=PLANS["trial"]["days"])
                    cur.execute("""
                        INSERT INTO users
                            (user_id, group_id, username, first_name, plan,
                             start_date, end_date, trial_used, status)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'active')
                    """, (user_id, group_id, username, first_name, "trial", now, end_date, True))
                    cur.execute("""
                        INSERT INTO payments
                            (user_id, group_id, username, first_name, plan, amount, payment_date)
                        VALUES (%s,%s,%s,%s,%s,%s,%s)
                    """, (user_id, group_id, username, first_name, "trial", 0, now))
                    return True, "trial_nuevo"
                elif existing['status'] == 'active' and existing['end_date'] > now:
                    cur.execute(
                        "UPDATE users SET username=%s, first_name=%s WHERE user_id=%s AND group_id=%s",
                        (username, first_name, user_id, group_id)
                    )
                    return True, "activo"
                return False, "expirado"

        return await self._run(_register)

    async def register_free_user(self, chat_id: int, user_id: int,
                                  username: str, first_name: str) -> bool:
        """Registra usuario en grupo FREE. Retorna True si fue insertado (era nuevo)."""
        def _reg(conn):
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM users WHERE user_id=%s AND group_id=%s",
                    (user_id, chat_id)
                )
                if cur.fetchone():
                    return False
                self._insert_free_user_sync(conn, user_id, chat_id, username, first_name)
                return True
        return await self._run(_reg)

    async def add_or_update_user(self, group_id: int, username: str,
                                  plan: str, first_name: str = ""):
        now = datetime.now()
        if plan not in PLANS:
            return False, "❌ Plan inválido"
        config = PLANS[plan]

        def _add(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT user_id, trial_used, status, first_name FROM users WHERE LOWER(username)=LOWER(%s) AND group_id=%s",
                    (username, group_id)
                )
                existing = cur.fetchone()
                if existing:
                    if plan == "trial" and existing['trial_used']:
                        return False, "❌ Este usuario ya usó su prueba gratuita"
                    end_date = now + timedelta(days=config['days'])
                    fn = first_name or existing.get('first_name', '')
                    cur.execute("""
                        UPDATE users SET
                            plan=%s, start_date=%s, end_date=%s, status='active',
                            updated_at=NOW(), username=%s, first_name=%s,
                            trial_used = trial_used OR %s
                        WHERE user_id=%s AND group_id=%s
                    """, (plan, now, end_date, username, fn,
                          plan == "trial", existing['user_id'], group_id))
                    cur.execute("""
                        INSERT INTO payments
                            (user_id, group_id, username, first_name, plan, amount, payment_date)
                        VALUES (%s,%s,%s,%s,%s,%s,%s)
                    """, (existing['user_id'], group_id, username, fn,
                          plan, config['price'], now))
                    return True, f"✅ @{username} activado con {config['name']}\n📅 Expira: {end_date.strftime('%d/%m/%Y')}"
                return False, f"❌ No tengo registro de @{username}. Pídele que envíe un mensaje al bot."

        return await self._run(_add)

    async def get_all_active_users(self, group_id: int):
        def _get(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT user_id, username, first_name, plan, end_date,
                           EXTRACT(DAY FROM (end_date - NOW())) AS days_left
                    FROM users
                    WHERE group_id=%s AND status='active' AND end_date > NOW()
                    ORDER BY end_date ASC
                """, (group_id,))
                return cur.fetchall()
        return await self._run(_get)

    async def get_monthly_earnings(self, group_id: int):
        now = datetime.now()
        start_date = datetime(now.year, now.month, 1)

        def _get(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT plan, COUNT(*) AS count, COALESCE(SUM(amount),0) AS total
                    FROM payments
                    WHERE group_id=%s AND payment_date>=%s
                    GROUP BY plan
                """, (group_id, start_date))
                summary = cur.fetchall()
                total = sum(row['total'] for row in summary)
                cur.execute(
                    "SELECT COUNT(*) AS new_users FROM users WHERE group_id=%s AND created_at>=%s",
                    (group_id, start_date)
                )
                new_users = cur.fetchone()['new_users']
                # últimos 10 pagos
                cur.execute("""
                    SELECT user_id, username, first_name, plan, amount, payment_date
                    FROM payments
                    WHERE group_id=%s AND payment_date >= date_trunc('month', NOW())
                    ORDER BY payment_date DESC
                    LIMIT 10
                """, (group_id,))
                recent = cur.fetchall()
                return {"summary": summary, "total": total,
                        "new_users": new_users, "recent": recent}

        return await self._run(_get)

    async def get_total_monthly_earnings(self):
        now = datetime.now()
        start_date = datetime(now.year, now.month, 1)

        def _get(conn):
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COALESCE(SUM(amount),0) FROM payments WHERE payment_date>=%s",
                    (start_date,)
                )
                return cur.fetchone()[0]

        return await self._run(_get)

    async def get_expired_users(self, group_id: int):
        def _get(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT user_id, username, plan, end_date
                    FROM users
                    WHERE group_id=%s AND status='active' AND end_date < NOW()
                """, (group_id,))
                return cur.fetchall()
        return await self._run(_get)

    async def expire_user(self, user_id: int, group_id: int):
        def _expire(conn):
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE users SET status='expired' WHERE user_id=%s AND group_id=%s",
                    (user_id, group_id)
                )
        await self._run(_expire)

    async def get_potential_clients_stats(self, group_id: int):
        now = datetime.now()
        start_of_month = datetime(now.year, now.month, 1)
        if now.month == 1:
            start_last_month = datetime(now.year - 1, 12, 1)
            end_last_month   = datetime(now.year, 1, 1)
        else:
            start_last_month = datetime(now.year, now.month - 1, 1)
            end_last_month   = datetime(now.year, now.month, 1)

        def _get(conn):
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) FROM users
                    WHERE group_id=%s AND status='potencial' AND created_at>=%s
                """, (group_id, start_of_month))
                count_month = cur.fetchone()[0]
                cur.execute("""
                    SELECT COUNT(*) FROM users
                    WHERE group_id=%s AND status='potencial'
                      AND created_at>=%s AND created_at<%s
                """, (group_id, start_last_month, end_last_month))
                count_last_month = cur.fetchone()[0]
                cur.execute(
                    "SELECT COUNT(*) FROM users WHERE group_id=%s AND status='potencial'",
                    (group_id,)
                )
                total_all = cur.fetchone()[0]
                return count_month, count_last_month, total_all

        return await self._run(_get)

    async def get_potential_clients_list(self, group_id: int):
        def _get(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT user_id, username, first_name, created_at
                    FROM users
                    WHERE group_id=%s AND status='potencial'
                    ORDER BY created_at DESC
                """, (group_id,))
                return cur.fetchall()
        return await self._run(_get)

    async def get_export_data(self, group_id: int):
        now = datetime.now()
        start_date = datetime(now.year, now.month, 1)

        def _get(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT user_id, username, first_name, plan, amount, payment_date
                    FROM payments
                    WHERE group_id=%s AND payment_date>=%s
                    ORDER BY payment_date DESC
                """, (group_id, start_date))
                return cur.fetchall()

        return await self._run(_get)

    async def update_group_fields(self, group_id: int, changes: dict):
        """Aplica cambios de nombre/admin/tipo en una sola transacción."""
        if not changes:
            return

        def _upd(conn):
            with conn.cursor() as cur:
                if 'name' in changes:
                    cur.execute("UPDATE groups SET group_name=%s WHERE group_id=%s",
                                (changes['name'], group_id))
                if 'admin' in changes:
                    cur.execute("UPDATE groups SET admin_id=%s WHERE group_id=%s",
                                (changes['admin'], group_id))
                if 'type' in changes:
                    cur.execute("UPDATE groups SET group_type=%s WHERE group_id=%s",
                                (changes['type'], group_id))

        await self._run(_upd)

    async def delete_group_from_db(self, group_id: int):
        def _del(conn):
            with conn.cursor() as cur:
                cur.execute("DELETE FROM groups WHERE group_id=%s", (group_id,))
        await self._run(_del)

    async def get_total_users_count(self):
        def _get(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM users")
                return cur.fetchone()[0]
        return await self._run(_get)

# ==================== INSTANCIAS GLOBALES ====================
db = Database(DATABASE_URL)
scheduler = AsyncIOScheduler()
bot_app = None

# ==================== HANDLERS ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id  = update.effective_user.id
    # Soporta tanto /start directo como "Volver" desde callback
    if update.callback_query:
        send = update.callback_query.message.reply_text
    else:
        send = update.message.reply_text

    if user_id == SUPER_ADMIN_ID:
        vip_count     = len([g for g in GROUPS if g.get("type", "VIP") == "VIP"])
        free_count    = len([g for g in GROUPS if g.get("type", "VIP") == "FREE"])
        total_earnings = await db.get_total_monthly_earnings()
        total_users    = await db.get_total_users_count()

        keyboard = [
            [InlineKeyboardButton("📋 Grupos",    callback_data="menu_groups")],
            [InlineKeyboardButton("💰 Ganancias", callback_data="total_earnings")],
            [InlineKeyboardButton("📟 Comandos",  callback_data="menu_commands")],
        ]
        await send(
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
        await send(
            "❌ No tienes grupos asignados como administrador.\n\n"
            "Contacta al Super Administrador para que te asigne un grupo."
        )
        return

    if len(user_groups) == 1:
        group = user_groups[0]
        context.user_data['current_group'] = group['group_id']
        if group.get("type", "VIP") == "VIP":
            keyboard = [
                [InlineKeyboardButton("📊 Usuarios activos", callback_data="list_active")],
                [InlineKeyboardButton("💰 Ganancias",        callback_data="earnings")],
                [InlineKeyboardButton("📥 Exportar mes",     callback_data="export_month")]
            ]
            await send(
                f"👑 *Panel VIP - {group['group_name']}*\n\n"
                f"🆔 ID del grupo: `{group['group_id']}`\n\n"
                f"Comandos disponibles:\n"
                f"• `/add @usuario plan` - Agregar suscripción\n"
                f"• Los usuarios expiran automáticamente",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="Markdown"
            )
        else:
            keyboard = [
                [InlineKeyboardButton("📋 Clientes potenciales", callback_data="list_potential")],
                [InlineKeyboardButton("📥 Exportar clientes",    callback_data="export_clients")]
            ]
            await send(
                f"📋 *Panel FREE - {group['group_name']}*\n\n"
                f"🆔 ID del grupo: `{group['group_id']}`",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="Markdown"
            )
    else:
        keyboard = []
        for group in user_groups:
            emoji = "👑" if group.get("type", "VIP") == "VIP" else "📋"
            keyboard.append([InlineKeyboardButton(
                f"{emoji} {group['group_name']}",
                callback_data=f"select_group_{group['group_id']}"
            )])
        await send(
            "📋 *Selecciona el grupo que quieres gestionar*",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )

async def test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("✅ El bot funciona!")

async def menu_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    keyboard = [
        [InlineKeyboardButton("➕ Agregar grupo",  callback_data="add_group")],
        [InlineKeyboardButton("👁️ Ver grupos",     callback_data="menu_view_groups")],
        [InlineKeyboardButton("✏️ Editar grupo",   callback_data="menu_edit_group_select")],
        [InlineKeyboardButton("❌ Eliminar grupo", callback_data="menu_delete_group_select")],
        [InlineKeyboardButton("🔙 Volver",         callback_data="back_to_admin")],
    ]
    await query.edit_message_text(
        "📋 *Gestión de Grupos*\n\nSelecciona una opción:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def menu_view_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    keyboard = [
        [InlineKeyboardButton("👑 Grupos VIP",  callback_data="view_vip_groups")],
        [InlineKeyboardButton("📋 Grupos FREE", callback_data="view_free_groups")],
        [InlineKeyboardButton("🔙 Volver",      callback_data="menu_groups")],
    ]
    await query.edit_message_text(
        "👁️ *Ver Grupos*\n\nSelecciona el tipo de grupo:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def select_group(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query
    await query.answer()
    group = get_group_by_id(group_id)
    if not group:
        await query.edit_message_text("❌ Grupo no encontrado")
        return
    context.user_data['current_group'] = group_id
    if group.get("type", "VIP") == "VIP":
        keyboard = [
            [InlineKeyboardButton("➕ Agregar usuario",  callback_data="add_user")],
            [InlineKeyboardButton("📊 Usuarios activos", callback_data="list_active")],
            [InlineKeyboardButton("💰 Ganancias",        callback_data="earnings")],
            [InlineKeyboardButton("📥 Exportar mes",     callback_data="export_month")]
        ]
        await query.edit_message_text(
            f"👑 *Panel VIP - {group['group_name']}*\n\n"
            f"🆔 ID: `{group['group_id']}`\n"
            f"👑 Admin: `{group['admin_id']}`",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
    else:
        keyboard = [
            [InlineKeyboardButton("📋 Clientes potenciales", callback_data="list_potential")],
            [InlineKeyboardButton("📥 Exportar clientes",    callback_data="export_clients")]
        ]
        await query.edit_message_text(
            f"📋 *Panel FREE - {group['group_name']}*\n\n"
            f"🆔 ID: `{group['group_id']}`\n"
            f"👑 Admin: `{group['admin_id']}`",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )

async def total_earnings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query:
        await query.answer()
        message = query.message
    else:
        message = update.message
    total = await db.get_total_monthly_earnings()
    now = datetime.now()
    msg = (
        f"💰 *GANANCIAS TOTALES DEL MES*\n\n"
        f"📅 {now.strftime('%B %Y')}\n"
        f"💵 Total recaudado: *${total}*\n\n"
        f"📊 Incluye todos los grupos configurados."
    )
    await message.reply_text(msg, parse_mode="Markdown")

async def add_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id     = update.effective_user.id
    chat_id     = update.effective_chat.id
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
        await update.message.reply_text(
            "❌ Usa: `/add @username plan`\nPlanes: trial, semanal, mensual",
            parse_mode="Markdown"
        )
        return
    username = context.args[0].replace("@", "")
    plan     = context.args[1].lower()
    if plan not in PLANS:
        await update.message.reply_text("❌ Plan inválido. Usa: trial, semanal, mensual")
        return
    first_name = ""
    try:
        member = await context.bot.get_chat_member(current_group, username)
        if member and member.user:
            first_name = member.user.first_name or ""
    except Exception:
        pass
    success, msg = await db.add_or_update_user(current_group, username, plan, first_name)
    await update.message.reply_text(msg)

async def list_active_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    message = query.message if query else update.message
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
        days_left  = int(user['days_left']) if user['days_left'] else 0
        emoji      = "🟢" if days_left > 7 else "🟡" if days_left > 2 else "🔴"
        first_name = user.get('first_name', '') or 'Sin nombre'
        username   = user.get('username', '')
        if username and not username.startswith('user_'):
            display = f"{first_name} (@{username})"
        else:
            display = f"{first_name} (ID: `{user['user_id']}`)"
        chat_link = f"tg://user?id={user['user_id']}"
        msg += (
            f"{emoji} {display}\n"
            f"   📅 Expira: {user['end_date'].strftime('%d/%m/%Y')} ({days_left} días)\n"
            f"   📋 Plan: {user['plan']}\n"
            f"   🔗 [Abrir chat]({chat_link})\n\n"
        )
    await message.reply_text(msg, parse_mode="Markdown")

async def show_earnings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    message = query.message if query else update.message
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
            msg += f"• {plan_name}: {plan['count']} ventas - ${plan['total']}\n"
        msg += f"\n💵 *TOTAL*: ${earnings['total']}\n👥 *Nuevos*: {earnings['new_users']}"
    if earnings.get('recent'):
        msg += "\n\n📋 *Últimos pagos:*\n"
        for p in earnings['recent']:
            name = p['first_name'] or p['username'] or f"ID:{p['user_id']}"
            msg += f"• {name} - {p['plan']} - ${p['amount']}\n"
    await message.reply_text(msg, parse_mode="Markdown")

async def export_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    message = query.message if query else update.message
    group_id = context.user_data.get('current_group')
    if not group_id:
        await message.reply_text("❌ Selecciona un grupo con /start")
        return
    now          = datetime.now()
    transactions = await db.get_export_data(group_id)
    if not transactions:
        await message.reply_text(f"📭 No hay transacciones en {now.strftime('%B %Y')}")
        return
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(['Fecha', 'User ID', 'Username', 'Nombre', 'Plan', 'Monto', 'Link de contacto'])
    for t in transactions:
        writer.writerow([
            t['payment_date'].strftime('%Y-%m-%d %H:%M:%S'),
            t['user_id'],
            t['username'] or 'Sin username',
            t['first_name'] or 'Sin nombre',
            t['plan'].upper(),
            f"${t['amount']}",
            f"tg://user?id={t['user_id']}"
        ])
    output.seek(0)
    await message.reply_document(
        document=output.getvalue().encode('utf-8-sig'),
        filename=f"reporte_{now.year}_{now.month:02d}.csv",
        caption=f"📊 Reporte de {now.strftime('%B %Y')}"
    )
    output.close()

async def auto_backup():
    global bot_app
    if not bot_app:
        return
    last_backup_file = "last_backup.txt"
    last_backup_date = None
    try:
        if os.path.exists(last_backup_file):
            with open(last_backup_file, 'r') as f:
                last_backup_date = datetime.fromisoformat(f.read().strip())
    except Exception:
        pass
    now = datetime.now()
    if not last_backup_date or (now - last_backup_date).days >= 15:
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(['group_id', 'group_name', 'group_type', 'admin_id', 'backup_date'])
        for group in GROUPS:
            writer.writerow([
                group['group_id'], group['group_name'],
                group.get('type', 'VIP'), group['admin_id'],
                now.strftime('%Y-%m-%d %H:%M:%S')
            ])
        output.seek(0)
        try:
            await bot_app.bot.send_document(
                SUPER_ADMIN_ID,
                document=output.getvalue().encode('utf-8-sig'),
                filename=f"backup_automatico_{now.strftime('%Y%m%d')}.csv",
                caption=(
                    f"📦 *Backup Automático*\n\n"
                    f"📅 Fecha: {now.strftime('%d/%m/%Y %H:%M:%S')}\n"
                    f"📊 Grupos incluidos: {len(GROUPS)}\n\n"
                    f"*Próximo backup:* {(now + timedelta(days=15)).strftime('%d/%m/%Y')}\n\n"
                    f"⚠️ Guarda este archivo en un lugar seguro."
                ),
                parse_mode="Markdown"
            )
            output.close()
            with open(last_backup_file, 'w') as f:
                f.write(now.isoformat())
            logger.info(f"✅ Backup automático enviado")
        except Exception as e:
            logger.error(f"❌ Error enviando backup automático: {e}")

async def manual_backup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != SUPER_ADMIN_ID:
        await update.message.reply_text("❌ Solo Super Admin")
        return
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(['group_id', 'group_name', 'group_type', 'admin_id', 'backup_date'])
    now = datetime.now()
    for group in GROUPS:
        writer.writerow([
            group['group_id'], group['group_name'],
            group.get('type', 'VIP'), group['admin_id'],
            now.strftime('%Y-%m-%d %H:%M:%S')
        ])
    output.seek(0)
    await update.message.reply_document(
        document=output.getvalue().encode('utf-8-sig'),
        filename=f"backup_manual_{now.strftime('%Y%m%d_%H%M%S')}.csv",
        caption=(
            f"📦 *Backup Manual*\n\n"
            f"📅 Fecha: {now.strftime('%d/%m/%Y %H:%M:%S')}\n"
            f"📊 Grupos incluidos: {len(GROUPS)}"
        ),
        parse_mode="Markdown"
    )
    output.close()

async def restore_backup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != SUPER_ADMIN_ID:
        await update.message.reply_text("❌ Solo Super Admin")
        return
    if not update.message.document:
        await update.message.reply_text(
            "❌ Envía el archivo CSV de backup junto con el comando.",
            parse_mode="Markdown"
        )
        return
    await update.message.reply_text("🔄 Restaurando configuración...")
    try:
        import io
        file         = await update.message.document.get_file()
        file_content = await file.download_as_bytearray()
        content      = file_content.decode('utf-8')
        reader       = csv.reader(io.StringIO(content))
        next(reader)   # saltar encabezados
        restored_count = 0
        for row in reader:
            if len(row) >= 4:
                group_id   = int(row[0])
                group_name = row[1]
                group_type = row[2]
                admin_id   = int(row[3])
                existing = get_group_by_id(group_id)
                if existing:
                    for g in GROUPS:
                        if g["group_id"] == group_id:
                            g["group_name"] = group_name
                            g["type"]       = group_type
                            g["admin_id"]   = admin_id
                            break
                    await db.update_group_fields(group_id, {
                        'name': group_name, 'admin': admin_id, 'type': group_type
                    })
                else:
                    GROUPS.append({
                        "group_id": group_id, "group_name": group_name,
                        "type": group_type,   "admin_id": admin_id
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
    query   = update.callback_query
    message = query.message if query else update.message
    if update.effective_user.id != SUPER_ADMIN_ID:
        await message.reply_text("❌ Solo Super Admin")
        return
    if not GROUPS:
        await message.reply_text("📭 No hay grupos")
        return
    msg = "📊 *GRUPOS CONFIGURADOS*\n\n"
    for group in GROUPS:
        msg += (
            f"📌 *{group['group_name']}*\n"
            f"   🆔 ID: `{group['group_id']}`\n"
            f"   👑 Admin: `{group['admin_id']}`\n"
            f"   📋 Tipo: {group.get('type', 'VIP')}\n\n"
        )
    await message.reply_text(msg, parse_mode="Markdown")

async def add_group_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != SUPER_ADMIN_ID:
        await update.message.reply_text("❌ Solo Super Admin")
        return
    if len(context.args) < 4:
        await update.message.reply_text(
            "❌ Formato: `/addgroup group_id TIPO \"nombre\" admin_id`",
            parse_mode="Markdown"
        )
        return
    try:
        group_id   = int(context.args[0])
        group_type = context.args[1].upper()
        group_name = " ".join(context.args[2:-1]).strip('"')
        admin_id   = int(context.args[-1])
        if group_type not in ["VIP", "FREE"]:
            await update.message.reply_text("❌ TIPO debe ser VIP o FREE")
            return
        GROUPS.append({"group_id": group_id, "type": group_type,
                       "group_name": group_name, "admin_id": admin_id})
        await db.save_group(group_id, group_name, admin_id, group_type)
        await update.message.reply_text(f"✅ Grupo {group_name} agregado")
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")

async def view_vip_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_groups_by_type(update, context, "VIP", True)

async def view_free_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_groups_by_type(update, context, "FREE", True)

async def show_groups_by_type(update: Update, context: ContextTypes.DEFAULT_TYPE,
                               group_type: str, select_mode: bool = False):
    query  = update.callback_query
    groups = [g for g in GROUPS if g.get("type", "VIP") == group_type]
    if not groups:
        await query.edit_message_text(f"📭 No hay grupos {group_type}")
        return
    keyboard = [[InlineKeyboardButton(
        f"📌 {g['group_name']}", callback_data=f"select_group_{g['group_id']}"
    )] for g in groups]
    if select_mode:
        keyboard.append([InlineKeyboardButton("🔙 Volver", callback_data="menu_view_groups")])
    await query.edit_message_text(
        f"📋 *Grupos {group_type}*",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def menu_edit_group_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not GROUPS:
        await query.edit_message_text("📭 No hay grupos configurados")
        return
    keyboard = []
    for group in GROUPS:
        emoji = "👑" if group.get("type", "VIP") == "VIP" else "📋"
        keyboard.append([InlineKeyboardButton(
            f"{emoji} {group['group_name']}",
            callback_data=f"edit_multiple_{group['group_id']}"
        )])
    keyboard.append([InlineKeyboardButton("🔙 Volver", callback_data="menu_groups")])
    await query.edit_message_text(
        "✏️ *Selecciona el grupo que deseas editar*\n\n"
        "Podrás cambiar nombre, administrador y tipo.\n"
        "Puedes hacer varios cambios antes de aplicar.",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def menu_delete_group_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not GROUPS:
        await query.edit_message_text("📭 No hay grupos configurados")
        return
    keyboard = []
    for group in GROUPS:
        emoji = "👑" if group.get("type", "VIP") == "VIP" else "📋"
        keyboard.append([InlineKeyboardButton(
            f"{emoji} {group['group_name']}",
            callback_data=f"delete_confirm_{group['group_id']}"
        )])
    keyboard.append([InlineKeyboardButton("🔙 Volver", callback_data="menu_groups")])
    await query.edit_message_text(
        "❌ *Eliminar Grupo*\n\n"
        "⚠️ Esta acción es irreversible.\n"
        "Selecciona el grupo que deseas eliminar:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def delete_group_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query
    await query.answer()
    group = get_group_by_id(group_id)
    if not group:
        await query.edit_message_text("❌ Grupo no encontrado")
        return
    keyboard = [
        [InlineKeyboardButton("✅ Sí, eliminar", callback_data=f"delete_yes_{group_id}")],
        [InlineKeyboardButton("❌ Cancelar",      callback_data="menu_groups")],
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
    query = update.callback_query
    await query.answer()
    group = get_group_by_id(group_id)
    if not group:
        await query.edit_message_text("❌ Grupo no encontrado")
        return
    group_name = group['group_name']
    # Eliminar de memoria (operación atómica)
    global GROUPS
    GROUPS = [g for g in GROUPS if g["group_id"] != group_id]
    await db.delete_group_from_db(group_id)
    await query.edit_message_text(
        f"✅ *Grupo eliminado*\n\n"
        f"📌 {group_name}\n"
        f"🆔 ID: `{group_id}`\n\n"
        f"El grupo ha sido eliminado correctamente.",
        parse_mode="Markdown"
    )
    await asyncio.sleep(2)
    await menu_groups(update, context)

async def menu_commands(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    commands_text = (
        "📟 *Comandos Disponibles*\n\n"
        "*Super Admin:*\n"
        "• `/start` - Panel principal\n"
        "• `/addgroup` - Agregar nuevo grupo\n"
        "• `/groups` - Ver todos los grupos\n"
        "• `/backup` - Backup manual\n"
        "• `/searchgrupo nombre` - Buscar grupo\n\n"
        "*Admin de Grupo:*\n"
        "• `/start` - Panel de control\n"
        "• `/add @user plan` - Agregar usuario\n\n"
        "*Planes disponibles:*\n"
        "• `trial` - 1 día ($0)\n"
        "• `semanal` - 7 días ($10)\n"
        "• `mensual` - 30 días ($20)\n\n"
        "*Ejemplos:*\n"
        "• `/add @juan semanal`\n"
        "• `/add @maria mensual`"
    )
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
    group = get_group_by_id(group_id)
    if not group or group.get("type") != "FREE":
        await query.edit_message_text("❌ Este comando solo funciona en grupos FREE")
        return
    count_month, count_last_month, total_all = await db.get_potential_clients_stats(group_id)
    now = datetime.now()
    if count_last_month > 0:
        growth       = ((count_month - count_last_month) / count_last_month) * 100
        growth_emoji = "📈" if growth > 0 else "📉" if growth < 0 else "➖"
        growth_text  = f"{growth_emoji} {growth:+.1f}% vs mes anterior"
    else:
        growth_text = "📊 Primer mes con registros" if count_month > 0 else "📊 Sin registros este mes"
    msg = (
        f"📋 *CLIENTES POTENCIALES - {group['group_name']}*\n\n"
        f"📅 *{now.strftime('%B %Y')}:* {count_month} nuevos\n"
        f"📆 *Mes anterior:* {count_last_month}\n"
        f"📊 *Total histórico:* {total_all}\n\n"
        f"{growth_text}"
    )
    await query.edit_message_text(msg, parse_mode="Markdown")

async def export_clients(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query    = update.callback_query
    await query.answer()
    group_id = context.user_data.get('current_group')
    if not group_id:
        await query.edit_message_text("❌ Selecciona un grupo con /start")
        return
    clients = await db.get_potential_clients_list(group_id)
    if not clients:
        await query.edit_message_text("📭 No hay clientes")
        return
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(['User ID', 'Username', 'Nombre', 'Fecha Registro'])
    for c in clients:
        writer.writerow([
            c['user_id'], c['username'] or '',
            c['first_name'] or '', c['created_at'].strftime('%Y-%m-%d %H:%M:%S')
        ])
    output.seek(0)
    await query.message.reply_document(
        document=output.getvalue().encode('utf-8-sig'),
        filename=f"clientes_{datetime.now().strftime('%Y%m%d')}.csv",
        caption="📋 Clientes potenciales"
    )
    output.close()

async def edit_group_multiple(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query
    await query.answer()
    group = get_group_by_id(group_id)
    if not group:
        await query.edit_message_text("❌ Grupo no encontrado")
        return
    context.user_data['editing_group_id'] = group_id
    context.user_data['editing_mode']     = 'multiple'
    if 'pending_changes' not in context.user_data:
        context.user_data['pending_changes'] = {}
    keyboard = [
        [InlineKeyboardButton("📝 Cambiar nombre", callback_data=f"multi_name_{group_id}")],
        [InlineKeyboardButton("👤 Cambiar admin",  callback_data=f"multi_admin_{group_id}")],
        [InlineKeyboardButton("🔄 Cambiar tipo",   callback_data=f"multi_type_{group_id}")],
        [InlineKeyboardButton("✅ Aplicar todos los cambios", callback_data=f"multi_apply_{group_id}")],
        [InlineKeyboardButton("🔙 Volver",         callback_data="menu_edit_group_select")]
    ]
    pending      = context.user_data.get('pending_changes', {})
    pending_text = ""
    if pending:
        pending_text = "\n\n📝 *Cambios pendientes:*\n"
        if 'name'  in pending: pending_text += f"• Nuevo nombre: `{pending['name']}`\n"
        if 'admin' in pending: pending_text += f"• Nuevo admin: `{pending['admin']}`\n"
        if 'type'  in pending: pending_text += f"• Nuevo tipo: `{pending['type']}`\n"
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
    query = update.callback_query
    await query.answer()
    context.user_data['editing_field']    = 'multi_name'
    context.user_data['editing_group_id'] = group_id
    await query.edit_message_text(
        f"✏️ *Cambiar nombre (edición múltiple)*\n\n"
        f"Envía el *nuevo nombre* en el chat.\n\n"
        f"*Escribe 'cancelar' para cancelar.*",
        parse_mode="Markdown"
    )

async def multi_admin_request(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query
    await query.answer()
    context.user_data['editing_field']    = 'multi_admin'
    context.user_data['editing_group_id'] = group_id
    await query.edit_message_text(
        f"👤 *Cambiar administrador (edición múltiple)*\n\n"
        f"Envía el *ID del nuevo administrador* en el chat.\n\n"
        f"*Para obtener un ID, usa @userinfobot*\n\n"
        f"*Escribe 'cancelar' para cancelar.*",
        parse_mode="Markdown"
    )

async def multi_type_request(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query
    await query.answer()
    keyboard = [
        [InlineKeyboardButton("👑 VIP",    callback_data=f"multi_set_type_{group_id}_VIP")],
        [InlineKeyboardButton("📋 FREE",   callback_data=f"multi_set_type_{group_id}_FREE")],
        [InlineKeyboardButton("🔙 Volver", callback_data=f"edit_multiple_{group_id}")]
    ]
    await query.edit_message_text(
        "🔄 *Selecciona el nuevo tipo*",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def multi_set_type(update: Update, context: ContextTypes.DEFAULT_TYPE,
                          group_id: int, new_type: str):
    query = update.callback_query
    await query.answer()
    if 'pending_changes' not in context.user_data:
        context.user_data['pending_changes'] = {}
    context.user_data['pending_changes']['type'] = new_type
    await query.edit_message_text(f"✅ *Tipo guardado:* {new_type}\n\nContinuando con edición múltiple...")
    await asyncio.sleep(1)
    await edit_group_multiple(update, context, group_id)

async def multi_apply_changes(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
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
    # Aplicar en memoria
    for g in GROUPS:
        if g["group_id"] == group_id:
            if 'name'  in pending: g["group_name"] = pending['name'];  changes_made.append(f"📝 Nombre: → {pending['name']}")
            if 'admin' in pending: g["admin_id"]   = pending['admin']; changes_made.append(f"👤 Admin: → {pending['admin']}")
            if 'type'  in pending: g["type"]        = pending['type'];  changes_made.append(f"🔄 Tipo: → {pending['type']}")
            break
    # Persistir en BD en una sola transacción
    await db.update_group_fields(group_id, pending)
    # Limpiar estado
    context.user_data.pop('pending_changes', None)
    context.user_data.pop('editing_mode', None)
    context.user_data.pop('editing_group_id', None)
    await query.edit_message_text(
        f"✅ *Cambios aplicados correctamente*\n\n" + "\n".join(changes_made),
        parse_mode="Markdown"
    )
    await asyncio.sleep(2)
    await menu_edit_group_select(update, context)

async def detect_new_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.new_chat_members:
        return
    chat_id = update.message.chat_id
    group   = get_group_by_id(chat_id)
    if not group:
        return
    for new_member in update.message.new_chat_members:
        if new_member.id == context.bot.id:
            continue
        user_id    = new_member.id
        username   = new_member.username or f"user_{user_id}"
        first_name = new_member.first_name or ""
        if group["type"] == "VIP":
            registered, result = await db.register_user_auto(chat_id, user_id, username, first_name)
            if registered and result == "trial_nuevo":
                try:
                    await context.bot.send_message(
                        user_id,
                        f"🎉 Bienvenido @{username}!\n✨ Trial gratis de 1 día",
                        parse_mode="Markdown"
                    )
                except Exception as e:
                    logger.warning(f"No se pudo enviar mensaje a {user_id}: {e}")
        else:
            # Grupo FREE - usar método unificado
            is_new = await db.register_free_user(chat_id, user_id, username, first_name)
            if is_new:
                display_name = first_name if first_name else (
                    username if not username.startswith('user_') else f"Usuario {user_id}"
                )
                chat_link = f"tg://user?id={user_id}"
                try:
                    await context.bot.send_message(
                        group["admin_id"],
                        f"📋 *Nuevo cliente potencial*\n\n"
                        f"👤 *Nombre:* {display_name}\n"
                        f"🆔 *ID:* `{user_id}`\n"
                        f"📌 *Grupo:* {group['group_name']}\n"
                        f"🔗 [Abrir chat]({chat_link})",
                        parse_mode="Markdown"
                    )
                except Exception as e:
                    logger.warning(f"No se pudo notificar al admin: {e}")


async def detect_active_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Se dispara cuando cualquier usuario escribe en un grupo configurado.
    Captura usuarios que estaban en el grupo antes del bot, o que entraron
    mientras el bot estaba caído — no detectables via NEW_CHAT_MEMBERS.

    - FREE: registra como cliente potencial y avisa al admin.
    - VIP:  avisa al admin para que los active manualmente con /add.
            NO se les da trial automático (podrían ser intrusos).
    """
    if not update.message or update.effective_chat.type not in ("group", "supergroup"):
        return

    chat_id = update.effective_chat.id
    group   = get_group_by_id(chat_id)
    if not group:
        return

    user       = update.effective_user
    user_id    = user.id
    username   = user.username or f"user_{user_id}"
    first_name = user.first_name or ""

    # Verificar si ya está registrado (usa índice user_id+group_id, muy rápido)
    existing = await db.get_user_by_id(user_id, chat_id)
    if existing:
        return  # Ya conocido, no hacer nada

    display_name = first_name if first_name else (
        username if not username.startswith("user_") else f"Usuario {user_id}"
    )
    chat_link = f"tg://user?id={user_id}"

    if group["type"] == "FREE":
        is_new = await db.register_free_user(chat_id, user_id, username, first_name)
        if is_new:
            logger.info(f"📋 Potencial detectado en FREE {group['group_name']}: {display_name}")
            try:
                await context.bot.send_message(
                    group["admin_id"],
                    f"📋 *Nuevo cliente potencial detectado*\n\n"
                    f"👤 *Nombre:* {display_name}\n"
                    f"🆔 *ID:* `{user_id}`\n"
                    f"📌 *Grupo:* {group['group_name']}\n"
                    f"🔗 [Abrir chat]({chat_link})\n\n"
                    f"_Registrado al escribir en el grupo._",
                    parse_mode="Markdown"
                )
            except Exception as e:
                logger.warning(f"No se pudo notificar al admin (potencial): {e}")

    else:  # VIP
        # Avisar al admin — él decide si activar o expulsar
        logger.info(f"⚠️ Sin registro en VIP {group['group_name']}: {display_name}")
        try:
            await context.bot.send_message(
                group["admin_id"],
                f"⚠️ *Usuario sin registro en grupo VIP*\n\n"
                f"👤 *Nombre:* {display_name}\n"
                f"🆔 *ID:* `{user_id}`\n"
                f"📌 *Grupo:* {group['group_name']}\n"
                f"🔗 [Abrir chat]({chat_link})\n\n"
                f"Para activarlo usa:\n`/add @{username} semanal`\n"
                f"Para expulsarlo: hazlo desde Telegram directamente.",
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.warning(f"No se pudo notificar al admin (VIP sin registro): {e}")

async def search_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != SUPER_ADMIN_ID:
        await update.message.reply_text("❌ Solo Super Admin")
        return
    if len(context.args) < 1:
        await update.message.reply_text("❌ Usa: `/searchgrupo nombre`", parse_mode="Markdown")
        return
    search_term = " ".join(context.args).lower()
    results     = [g for g in GROUPS if search_term in g['group_name'].lower()]
    if not results:
        await update.message.reply_text(f"📭 No se encontraron grupos con '{search_term}'")
        return
    msg = f"🔍 *Resultados para '{search_term}'*\n\n"
    for group in results:
        emoji = "👑" if group.get("type", "VIP") == "VIP" else "📋"
        msg += (
            f"{emoji} *{group['group_name']}*\n"
            f"   🆔 ID: `{group['group_id']}`\n"
            f"   👑 Admin: `{group['admin_id']}`\n\n"
        )
    await update.message.reply_text(msg, parse_mode="Markdown")

async def handle_edit_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != SUPER_ADMIN_ID:
        return
    text = update.message.text.strip()
    if text.lower() == 'cancelar':
        await update.message.reply_text("❌ Edición cancelada")
        context.user_data.pop('editing_field', None)
        context.user_data.pop('editing_group_id', None)
        context.user_data.pop('pending_changes', None)
        context.user_data.pop('editing_mode', None)
        return
    field    = context.user_data.get('editing_field')
    group_id = context.user_data.get('editing_group_id')
    if not field or not group_id:
        return
    group = get_group_by_id(group_id)
    if not group:
        await update.message.reply_text("❌ Grupo no encontrado")
        return
    if field == 'multi_name':
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
            await update.message.reply_text(
                f"✅ *Administrador guardado:* `{new_admin}`\n\nContinuando con edición múltiple..."
            )
            context.user_data.pop('editing_field', None)
            await edit_group_multiple(update, context, group_id)
        except ValueError:
            await update.message.reply_text("❌ Error: El ID debe ser un número")
        return

async def get_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in [SUPER_ADMIN_ID, 8682208062]:
        await update.message.reply_text("❌ No autorizado")
        return
    if len(context.args) < 1:
        await update.message.reply_text(
            "❌ Usa: `/getlink @username` o `/getlink ID`",
            parse_mode="Markdown"
        )
        return
    identifier = context.args[0].replace("@", "")
    try:
        if identifier.isdigit():
            user_id   = int(identifier)
            chat_link = f"tg://user?id={user_id}"
            await update.message.reply_text(
                f"🔗 Enlace para abrir chat:\n`{chat_link}`",
                parse_mode="Markdown"
            )
        else:
            group_id = context.user_data.get('current_group')
            if not group_id:
                await update.message.reply_text("❌ Selecciona un grupo con /start")
                return
            user = await db.get_user_by_username(identifier, group_id)
            if user:
                chat_link = f"tg://user?id={user['user_id']}"
                name      = user.get('first_name', 'Usuario') or user.get('username', identifier)
                await update.message.reply_text(
                    f"🔗 Enlace para abrir chat con {name}:\n`{chat_link}`",
                    parse_mode="Markdown"
                )
            else:
                await update.message.reply_text(f"❌ No se encontró al usuario @{identifier}")
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")

async def sync_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    NOTA: La API de Telegram eliminó get_chat_members para bots.
    El registro de usuarios se hace automáticamente cuando entran al grupo
    via detect_new_member(). Este comando informa sobre esa limitación.
    """
    await update.message.reply_text(
        "ℹ️ *Sincronización manual no disponible*\n\n"
        "La API de Telegram no permite a los bots listar todos los miembros de un grupo.\n\n"
        "Los usuarios se registran automáticamente al *entrar al grupo*.\n"
        "Para registrar un usuario manualmente usa:\n"
        "`/add @username plan`",
        parse_mode="Markdown"
    )

async def sync_all_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    La API de Telegram no permite listar miembros de grupos a bots.
    Muestra resumen de usuarios activos por grupo.
    """
    if update.effective_user.id != SUPER_ADMIN_ID:
        await update.message.reply_text("❌ Solo Super Admin")
        return
    msg = "📊 *Estado actual de grupos*\n\n"
    for group in GROUPS:
        users = await db.get_all_active_users(group["group_id"])
        emoji = "👑" if group.get("type", "VIP") == "VIP" else "📋"
        msg += f"{emoji} *{group['group_name']}*: {len(users)} usuarios activos\n"
    msg += (
        "\n\nℹ️ La API de Telegram no permite listar miembros de grupos.\n"
        "Los usuarios se registran al *entrar al grupo* automáticamente."
    )
    await update.message.reply_text(msg, parse_mode="Markdown")

# Nota: get_chat_members fue eliminado de la API de Telegram para bots.
# El registro ocurre en detect_new_member() cuando los usuarios entran al grupo.
# No hay sincronización automática en background.

# ==================== CALLBACK HANDLER (con manejo de errores) ====================
async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    logger.info(f"🔔 CALLBACK: {data}")
    try:
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
            await query.edit_message_text(
                "📝 Usa: `/addgroup group_id TIPO \"nombre\" admin_id`",
                parse_mode="Markdown"
            )
        elif data.startswith("select_group_"):
            group_id = int(data.replace("select_group_", ""))
            await select_group(update, context, group_id)
        elif data == "back_to_admin":
            # Usar reply_text en lugar de edit para evitar conflictos de contexto
            await query.message.reply_text("🔙 Volviendo al panel...")
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
            parts    = data.split("_")
            group_id = int(parts[3])
            new_type = parts[4]
            await multi_set_type(update, context, group_id, new_type)
        elif data.startswith("edit_multiple_"):
            group_id = int(data.replace("edit_multiple_", ""))
            await edit_group_multiple(update, context, group_id)
        else:
            logger.warning(f"Callback desconocido: {data}")

    except Exception as e:
        logger.error(f"❌ Error en callback '{data}': {e}", exc_info=True)
        try:
            await query.answer("❌ Ocurrió un error, intenta de nuevo", show_alert=True)
        except Exception:
            pass

# ==================== TAREAS PROGRAMADAS ====================
async def check_expired_subscriptions():
    logger.info("🔍 Verificando suscripciones expiradas...")
    for group in GROUPS:
        if group.get("type", "VIP") != "VIP":
            continue
        expired_users = await db.get_expired_users(group["group_id"])
        logger.info(f"Usuarios expirados en {group['group_name']}: {len(expired_users)}")
        for user in expired_users:
            await db.expire_user(user['user_id'], group["group_id"])
            try:
                await bot_app.bot.ban_chat_member(group["group_id"], user['user_id'])
                await bot_app.bot.send_message(
                    group["admin_id"],
                    f"🚫 @{user['username']} expulsado - suscripción vencida"
                )
            except Exception as e:
                logger.error(f"Error expulsando a {user['username']}: {e}")

# ==================== MAIN ====================
async def main():
    global bot_app
    await db.init_tables()
    await db.load_groups_from_db()
    logger.info(f"📦 {len(GROUPS)} grupos disponibles")

    defaults = Defaults(parse_mode="HTML")
    bot_app  = ApplicationBuilder().token(TOKEN).defaults(defaults).build()

    # Registrar handlers
    bot_app.add_handler(CommandHandler("start",       start))
    bot_app.add_handler(CommandHandler("add",         add_user_command))
    bot_app.add_handler(CommandHandler("groups",      list_groups))
    bot_app.add_handler(CommandHandler("addgroup",    add_group_command))
    bot_app.add_handler(CommandHandler("backup",      manual_backup))
    bot_app.add_handler(CommandHandler("restore",     restore_backup))
    bot_app.add_handler(CommandHandler("getlink",     get_link))
    bot_app.add_handler(CommandHandler("syncgroup",   sync_group))
    bot_app.add_handler(CommandHandler("syncall",     sync_all_groups))
    bot_app.add_handler(CommandHandler("searchgrupo", search_group))
    bot_app.add_handler(CommandHandler("test",        test))
    bot_app.add_handler(CallbackQueryHandler(handle_callback))
    bot_app.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, detect_new_member))
    # Detecta usuarios ya existentes en el grupo que nunca dispararon NEW_CHAT_MEMBERS
    bot_app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.ChatType.GROUPS,
        detect_active_member
    ))
    bot_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_edit_input))

    # Tareas programadas
    scheduler.add_job(check_expired_subscriptions, 'interval', hours=6)
    scheduler.add_job(auto_backup,                 'interval', hours=24)
    # scheduled_sync eliminado: get_chat_members no disponible en PTB v20+
    scheduler.start()

    logger.info("🤖 Bot iniciado")
    await bot_app.initialize()
    await bot_app.start()
    await bot_app.updater.start_polling(drop_pending_updates=True)

    # Registro de usuarios: ocurre via detect_new_member() en tiempo real

    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
