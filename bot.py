import os
import csv
import asyncio
import logging
import re
import json
from io import StringIO
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import TelegramError, BadRequest
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    ContextTypes, MessageHandler, filters, ChatMemberHandler
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
EC_TZ = ZoneInfo("America/Guayaquil")
BOT_USERNAME = os.getenv("BOT_USERNAME", "AyudanteVIP_bot")

if not TOKEN:
    logger.critical("❌ ERROR: No se encontró TELEGRAM_TOKEN")
    exit(1)

if not DATABASE_URL:
    logger.critical("❌ ERROR: No se encontró DATABASE_URL")
    exit(1)

PLANS = {
    "trial":   {"minutes": 1440, "price": 0,  "name": "🎁 Trial (1 día)"},
    "semanal": {"days": 7,       "price": 10, "name": "📅 Semanal (7 días)"},
    "mensual": {"days": 30,      "price": 20, "name": "📆 Mensual (30 días)"}
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
                "admin_id":   int(_parts[3]),
                "settings":   {}
            })

# ==================== UTILIDADES DE TIEMPO (ECUADOR) ====================
def now_ec() -> datetime:
    """Retorna la hora actual en zona horaria de Ecuador (UTC-5)."""
    return datetime.now(EC_TZ).replace(tzinfo=None)

def fmt_dt(dt: datetime, include_time: bool = True) -> str:
    """Formatea un datetime (naive, guardado en UTC) a hora Ecuador para mostrar."""
    if dt is None:
        return "N/A"
    # Los datetimes almacenados son naive (sin tzinfo); se asume que el servidor
    # los guarda en UTC. Convertir: primero asignar UTC, luego convertir a Ecuador.
    try:
        dt_utc = dt.replace(tzinfo=ZoneInfo("UTC"))
        dt_ec  = dt_utc.astimezone(EC_TZ)
    except Exception:
        # Fallback: usar el datetime tal cual si falla la conversión
        dt_ec = dt
    if include_time:
        return dt_ec.strftime('%d/%m/%Y %H:%M')
    return dt_ec.strftime('%d/%m/%Y')

# ==================== FUNCIONES DE UTILIDAD ====================
def get_group_by_id(group_id: int) -> Optional[dict]:
    for group in GROUPS:
        if group["group_id"] == group_id:
            return group
    return None

def get_groups_by_admin(admin_id: int, group_type: str = None) -> list:
    groups = GROUPS if admin_id == SUPER_ADMIN_ID else [g for g in GROUPS if g["admin_id"] == admin_id]
    if group_type:
        groups = [g for g in groups if g.get("type", "VIP") == group_type]
    return groups

def can_manage_group(user_id: int, group_id: int) -> bool:
    if user_id == SUPER_ADMIN_ID:
        return True
    group = get_group_by_id(group_id)
    return group is not None and group["admin_id"] == user_id

def get_group_plan_config(group_id: int, plan: str) -> dict:
    base     = dict(PLANS.get(plan, {}))
    group    = get_group_by_id(group_id)
    if not group or not base:
        return base
    settings = group.get("settings", {})

    if plan == "trial":
        mins = settings.get("trial_minutes", base.get("minutes", 1440))
        base["minutes"] = mins
        h, m = divmod(mins, 60)
        if h >= 24:
            d = h // 24; base["name"] = f"🎁 Trial ({d} día{'s' if d != 1 else ''})"
        elif h > 0:
            base["name"] = f"🎁 Trial ({h}h {m}m)" if m else f"🎁 Trial ({h}h)"
        else:
            base["name"] = f"🎁 Trial ({m} min)"
    elif plan == "semanal":
        base["days"]  = settings.get("duration_semanal", base.get("days", 7))
        base["price"] = float(settings.get("price_semanal", base.get("price", 10)))
        base["name"]  = f"📅 Semanal ({base['days']} días) - ${base['price']:.2f}"
    elif plan == "mensual":
        base["days"]  = settings.get("duration_mensual", base.get("days", 30))
        base["price"] = float(settings.get("price_mensual", base.get("price", 20)))
        base["name"]  = f"📆 Mensual ({base['days']} días) - ${base['price']:.2f}"
    return base

def fmt_price(amount) -> str:
    f = float(amount)
    return f"${f:.2f}" if round(f, 2) != round(f) else f"${int(round(f))}"

def fmt_minutes(mins: int) -> str:
    h, m = divmod(mins, 60)
    if h >= 24:
        d = h // 24
        return f"{d} día{'s' if d != 1 else ''}"
    elif h > 0:
        return f"{h}h {m}m" if m else f"{h}h"
    return f"{m} min"

# ==================== HELPERS DE PAGO ====================
def get_payment_keyboard(group_id: int, user_id: int, spin_used: bool = False) -> InlineKeyboardMarkup:
    """Genera teclado de pago. Si spin_used=True, oculta botón de ruleta."""
    keyboard = []
    
    if not spin_used:
        keyboard.append([InlineKeyboardButton("🎰 GIRAR RULETA VIP", callback_data=f"spin_{group_id}_{user_id}")])
    else:
        keyboard.append([InlineKeyboardButton("✅ Ruleta ya usada", callback_data="spin_used")])
    
    keyboard.append([InlineKeyboardButton("💳 Pagar Acceso", callback_data=f"pay_{group_id}_{user_id}")])
    
    return InlineKeyboardMarkup(keyboard)

def get_payment_info_text(group_id: int) -> str:
    group = get_group_by_id(group_id)
    if not group:
        return "Grupo no encontrado"

    settings        = group.get("settings", {})
    cfg_s           = get_group_plan_config(group_id, "semanal")
    cfg_m           = get_group_plan_config(group_id, "mensual")
    bank_data       = settings.get("bank_data", "").strip()
    payment_contact = settings.get("payment_contact", "ZonaEcFuego").strip()
    paypal_data     = settings.get("paypal_data", "").strip()
    group_name      = group.get("group_name", "VIP")

    lines = [
        f"💰 *INFORMACIÓN DE PAGO — {group_name}*", "",
        "📋 *Planes disponibles:*",
        f"• 📅 Semanal ({cfg_s.get('days', 7)} días): *{fmt_price(cfg_s.get('price', 10))}*",
        f"• 📆 Mensual ({cfg_m.get('days', 30)} días): *{fmt_price(cfg_m.get('price', 20))}*",
        "",
    ]
    if bank_data:
        lines += ["🏦 *Transferencia Bancaria:*", bank_data, ""]
    if paypal_data:
        lines += ["🅿️ *PayPal:*", paypal_data, ""]
    if not bank_data and not paypal_data:
        lines += ["⚠️ *Métodos de pago no configurados aún.*",
                  "Contacta al administrador para más información.", ""]
    lines += [
        f"📤 *Después de pagar:*",
        f"Envía el comprobante a @{payment_contact}", "",
        "⏱ *Tiempo de activación:*",
        "Tu acceso se activará cuando un administrador valide la transferencia.", "",
        "🔄 ¿No recibiste los datos? Presiona el botón de nuevo."
    ]
    return "\n".join(lines)

async def send_payment_info(bot, user_id: int, group_id: int, triggered_by: str = "comando"):
    group = get_group_by_id(group_id)
    if not group:
        return False
    try:
        # Consultar estado de ruleta del usuario
        spin_data = await db.get_spin_data(user_id, group_id)
        spin_used = spin_data.get("used", False) if spin_data.get("spin_count", 0) > 0 else False
        
        await bot.send_message(
            user_id,
            get_payment_info_text(group_id),
            parse_mode="Markdown",
            reply_markup=get_payment_keyboard(group_id, user_id, spin_used=spin_used)
        )
        chat_link  = f"tg://user?id={user_id}"
        group_name = group.get("group_name", "VIP")
        await _safe_send(
            group["admin_id"],
            f"💳 *Nuevo lead de pago*\n\n"
            f"👤 Usuario: [Usuario {user_id}]({chat_link})\n"
            f"🆔 ID: `{user_id}`\n"
            f"📌 Grupo: {group_name}\n"
            f"🔔 Acción: *{triggered_by}*\n\n"
            f"_Esperando comprobante en el contacto configurado._",
            parse_mode="Markdown"
        )
        return True
    except Exception as e:
        logger.warning(f"No se pudo enviar info de pago a {user_id}: {e}")
        return False

# ==================== BASE DE DATOS ====================
class Database:
    def __init__(self, db_url: str):
        self.db_url  = db_url
        self._pool: pool.ThreadedConnectionPool = None

    def _get_pool(self) -> pool.ThreadedConnectionPool:
        if self._pool is None or self._pool.closed:
            self._pool = pool.ThreadedConnectionPool(minconn=2, maxconn=10, dsn=self.db_url)
            logger.info("✅ Connection pool creado (min=2, max=10)")
        return self._pool

    def _execute(self, func):
        p    = self._get_pool()
        conn = p.getconn()
        try:
            conn.autocommit = False
            result = func(conn)
            conn.commit()
            return result
        except Exception:
            conn.rollback()
            raise
        finally:
            p.putconn(conn)

    async def _run(self, func):
        return await asyncio.to_thread(self._execute, func)

    # ── Inserción básica de usuario free ──────────────────────────────────────
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
                  datetime.utcnow(), datetime.utcnow() + timedelta(days=365),
                  "potencial", False))

    # ── Inicialización de tablas ───────────────────────────────────────────────
    async def init_tables(self):
        def _init(conn):
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS groups (
                        group_id       BIGINT PRIMARY KEY,
                        group_name     TEXT,
                        group_type     TEXT DEFAULT 'VIP',
                        admin_id       BIGINT,
                        super_admin_id BIGINT,
                        created_at     TIMESTAMP DEFAULT NOW(),
                        settings       JSONB DEFAULT '{}'::jsonb
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
                        id               SERIAL PRIMARY KEY,
                        user_id          BIGINT NOT NULL,
                        group_id         BIGINT NOT NULL,
                        username         TEXT,
                        first_name       TEXT,
                        plan             TEXT NOT NULL,
                        amount           NUMERIC(10,2) NOT NULL,
                        duration_minutes INTEGER,
                        payment_date     TIMESTAMP DEFAULT NOW()
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS warning_logs (
                        id           SERIAL PRIMARY KEY,
                        user_id      BIGINT NOT NULL,
                        group_id     BIGINT NOT NULL,
                        warning_type TEXT NOT NULL,
                        sent_at      TIMESTAMP DEFAULT NOW(),
                        UNIQUE(user_id, group_id, warning_type)
                    )
                """)
                # NUEVA TABLA: historial de broadcasts
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS broadcast_logs (
                        id              SERIAL PRIMARY KEY,
                        group_id        BIGINT NOT NULL,
                        filter_type     TEXT NOT NULL,
                        message_preview TEXT,
                        total_count     INTEGER DEFAULT 0,
                        success_count   INTEGER DEFAULT 0,
                        fail_count      INTEGER DEFAULT 0,
                        sent_by         BIGINT,
                        sent_at         TIMESTAMP DEFAULT NOW()
                    )
                """)
                # NUEVA TABLA: spins de descuento (ruleta VIP)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS discount_spins (
                        user_id         BIGINT NOT NULL,
                        group_id        BIGINT NOT NULL,
                        spin_count      INTEGER DEFAULT 1,
                        last_spin       TIMESTAMP DEFAULT NOW(),
                        best_discount   INTEGER DEFAULT 0,
                        used            BOOLEAN DEFAULT FALSE,
                        applied_to_plan TEXT,
                        UNIQUE(user_id, group_id)
                    )
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_discount_spins_user ON discount_spins(user_id, group_id)")
                # Índices
                cur.execute("CREATE INDEX IF NOT EXISTS idx_warning_logs_lookup ON warning_logs(user_id, group_id, warning_type)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_warning_logs_group  ON warning_logs(group_id, sent_at)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_users_group         ON users(group_id, status)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_users_end_date      ON users(group_id, end_date)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_users_uid_gid       ON users(user_id, group_id)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_payments_group      ON payments(group_id, payment_date)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_broadcast_logs_group ON broadcast_logs(group_id, sent_at)")
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_users_expired
                    ON users(group_id, status, end_date)
                    WHERE status = 'active'
                """)
                # Migraciones seguras
                cur.execute("""
                    DO $$
                    BEGIN
                        IF EXISTS (
                            SELECT 1 FROM information_schema.columns
                            WHERE table_name='payments' AND column_name='amount' AND data_type='integer'
                        ) THEN
                            ALTER TABLE payments ALTER COLUMN amount TYPE NUMERIC(10,2);
                        END IF;
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.columns
                            WHERE table_name='payments' AND column_name='duration_minutes'
                        ) THEN
                            ALTER TABLE payments ADD COLUMN duration_minutes INTEGER;
                        END IF;
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.columns
                            WHERE table_name='users' AND column_name='kicked_at'
                        ) THEN
                            ALTER TABLE users ADD COLUMN kicked_at TIMESTAMP DEFAULT NULL;
                        END IF;
                    END $$;
                """)
                # Corregir trial_used para usuarios que usaron trial
                cur.execute("""
                    UPDATE users u
                    SET trial_used = TRUE, updated_at = NOW()
                    WHERE (u.trial_used = FALSE OR u.trial_used IS NULL)
                      AND EXISTS (
                          SELECT 1 FROM payments p
                          WHERE p.user_id = u.user_id AND p.group_id = u.group_id AND p.plan = 'trial'
                      )
                """)
                fixed_count = cur.rowcount
                if fixed_count > 0:
                    logger.info(f"🔧 Migración: {fixed_count} usuarios corregidos a trial_used=TRUE")
        await self._run(_init)
        logger.info("✅ Base de datos inicializada")

    async def load_groups_from_db(self):
        def _load(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT group_id, group_name, admin_id,
                           COALESCE(group_type, 'VIP') as group_type,
                           COALESCE(settings, '{}') as settings
                    FROM groups
                """)
                return cur.fetchall()
        rows = await self._run(_load)
        if rows:
            GROUPS.clear()
            for g in rows:
                settings = g["settings"] if isinstance(g["settings"], dict) else {}
                GROUPS.append({
                    "group_id":   g["group_id"],
                    "group_name": g["group_name"],
                    "admin_id":   g["admin_id"],
                    "type":       g["group_type"],
                    "settings":   settings
                })
            logger.info(f"📦 {len(GROUPS)} grupos cargados desde BD")
            return True
        return False

    async def save_group(self, group_id: int, group_name: str, admin_id: int, group_type: str = "VIP"):
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
                    cur.execute("SELECT * FROM users WHERE LOWER(username)=LOWER(%s)", (username,))
                return cur.fetchone()
        return await self._run(_get)

    async def get_user_by_id(self, user_id: int, group_id: int):
        def _get(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT user_id, status, end_date, plan, trial_used FROM users "
                    "WHERE user_id=%s AND group_id=%s LIMIT 1",
                    (user_id, group_id)
                )
                return cur.fetchone()
        return await self._run(_get)

    async def get_user_by_id_full(self, user_id: int, group_id: int):
        def _get(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM users WHERE user_id=%s AND group_id=%s LIMIT 1",
                    (user_id, group_id)
                )
                return cur.fetchone()
        return await self._run(_get)

    async def register_user_auto(self, group_id: int, user_id: int, username: str, first_name: str):
        now = datetime.utcnow()
        trial_cfg     = get_group_plan_config(group_id, "trial")
        trial_minutes = trial_cfg.get("minutes", 1440)

        def _register(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT user_id, trial_used, status, end_date, plan "
                    "FROM users WHERE user_id=%s AND group_id=%s FOR UPDATE",
                    (user_id, group_id)
                )
                existing = cur.fetchone()

                if not existing:
                    end_date = now + timedelta(minutes=trial_minutes)
                    cur.execute("""
                        INSERT INTO users
                            (user_id, group_id, username, first_name, plan,
                             start_date, end_date, trial_used, status)
                        VALUES (%s,%s,%s,%s,'trial',%s,%s,TRUE,'active')
                    """, (user_id, group_id, username, first_name, now, end_date))
                    cur.execute("""
                        INSERT INTO payments
                            (user_id, group_id, username, first_name, plan,
                             amount, duration_minutes, payment_date)
                        VALUES (%s,%s,%s,%s,'trial',0,%s,%s)
                    """, (user_id, group_id, username, first_name, trial_minutes, now))
                    return True, "trial_nuevo", end_date

                cur.execute("""
                    UPDATE users SET username=%s, first_name=%s, updated_at=NOW()
                    WHERE user_id=%s AND group_id=%s
                """, (username, first_name, user_id, group_id))

                if existing["status"] == "active" and existing["end_date"] > now:
                    cur.execute("""
                        UPDATE users SET kicked_at=NULL, updated_at=NOW()
                        WHERE user_id=%s AND group_id=%s AND kicked_at IS NOT NULL
                    """, (user_id, group_id))
                    return True, "activo", existing["end_date"]

                if existing["status"] in ("expired", "active") and existing["end_date"] <= now:
                    return False, "expirado", None

                if existing["trial_used"]:
                    return False, "sin_trial", None

                end_date = now + timedelta(minutes=trial_minutes)
                cur.execute("""
                    UPDATE users SET
                        plan='trial', start_date=%s, end_date=%s,
                        trial_used=TRUE, status='active',
                        kicked_at=NULL, updated_at=NOW()
                    WHERE user_id=%s AND group_id=%s
                """, (now, end_date, user_id, group_id))
                cur.execute("""
                    INSERT INTO payments
                        (user_id, group_id, username, first_name, plan,
                         amount, duration_minutes, payment_date)
                    VALUES (%s,%s,%s,%s,'trial',0,%s,%s)
                """, (user_id, group_id, username, first_name, trial_minutes, now))
                return True, "trial_nuevo", end_date

        return await self._run(_register)

    async def register_free_user(self, chat_id: int, user_id: int, username: str, first_name: str) -> bool:
        def _reg(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM users WHERE user_id=%s AND group_id=%s", (user_id, chat_id))
                if cur.fetchone():
                    return False
                self._insert_free_user_sync(conn, user_id, chat_id, username, first_name)
                return True
        return await self._run(_reg)

    async def add_or_update_user(self, group_id: int, username: str, plan: str,
                                  first_name: str = "", custom_price: float = None,
                                  custom_days: int = None):
        now = datetime.utcnow()
        if plan not in PLANS:
            return False, "❌ Plan inválido"
        config          = get_group_plan_config(group_id, plan)
        effective_price = custom_price if custom_price is not None else config.get('price', 0)
        effective_days  = custom_days  if custom_days  is not None else config.get('days', 7)
        effective_mins  = config.get('minutes', effective_days * 1440)

        def _add(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT user_id, trial_used, status, first_name FROM users "
                    "WHERE LOWER(username)=LOWER(%s) AND group_id=%s FOR UPDATE",
                    (username, group_id)
                )
                existing = cur.fetchone()
                if not existing:
                    return False, f"❌ No tengo registro de @{username}. Pídele que envíe un mensaje al bot."
                if plan == "trial" and existing['trial_used']:
                    return False, "❌ Este usuario ya usó su prueba gratuita"
                if plan == "trial":
                    end_date    = now + timedelta(minutes=effective_mins)
                    dur_minutes = effective_mins
                    expiry_str  = fmt_dt(end_date)
                else:
                    end_date    = now + timedelta(days=effective_days)
                    dur_minutes = effective_days * 1440
                    expiry_str  = fmt_dt(end_date, include_time=False)
                fn = first_name or existing.get('first_name', '')
                cur.execute("""
                    UPDATE users SET
                        plan=%s, start_date=%s, end_date=%s, status='active',
                        updated_at=NOW(), username=%s, first_name=%s,
                        trial_used = trial_used OR %s, kicked_at = NULL
                    WHERE user_id=%s AND group_id=%s
                """, (plan, now, end_date, username, fn, plan == "trial", existing['user_id'], group_id))
                cur.execute("""
                    INSERT INTO payments
                        (user_id, group_id, username, first_name, plan, amount, duration_minutes, payment_date)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """, (existing['user_id'], group_id, username, fn, plan, effective_price, dur_minutes, now))
                price_str = fmt_price(effective_price) if effective_price > 0 else "gratis"
                return True, f"✅ *@{username}* activado\n📋 Plan: {plan} | 💰 {price_str}\n📅 Expira: {expiry_str}"
        return await self._run(_add)

    async def add_or_update_user_by_id(self, group_id: int, user_id: int, plan: str,
                                        first_name: str = "", custom_price: float = None,
                                        custom_days: int = None):
        now = datetime.utcnow()
        if plan not in PLANS:
            return False, "❌ Plan inválido"
        config          = get_group_plan_config(group_id, plan)
        effective_price = custom_price if custom_price is not None else config.get('price', 0)
        effective_days  = custom_days  if custom_days  is not None else config.get('days', 7)
        effective_mins  = config.get('minutes', effective_days * 1440)

        def _add(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT user_id, trial_used, status, first_name, username FROM users "
                    "WHERE user_id=%s AND group_id=%s FOR UPDATE",
                    (user_id, group_id)
                )
                existing = cur.fetchone()
                if not existing:
                    return False, f"❌ No tengo registro del usuario con ID {user_id}. Pídele que envíe un mensaje al bot."
                if plan == "trial" and existing['trial_used']:
                    return False, "❌ Este usuario ya usó su prueba gratuita"
                if plan == "trial":
                    end_date    = now + timedelta(minutes=effective_mins)
                    dur_minutes = effective_mins
                    expiry_str  = fmt_dt(end_date)
                else:
                    end_date    = now + timedelta(days=effective_days)
                    dur_minutes = effective_days * 1440
                    expiry_str  = fmt_dt(end_date, include_time=False)
                fn       = first_name or existing.get('first_name', '')
                username = existing.get('username', f"user_{user_id}")
                cur.execute("""
                    UPDATE users SET
                        plan=%s, start_date=%s, end_date=%s, status='active',
                        updated_at=NOW(), first_name=%s,
                        trial_used = trial_used OR %s, kicked_at = NULL
                    WHERE user_id=%s AND group_id=%s
                """, (plan, now, end_date, fn, plan == "trial", user_id, group_id))
                cur.execute("""
                    INSERT INTO payments
                        (user_id, group_id, username, first_name, plan, amount, duration_minutes, payment_date)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """, (user_id, group_id, username, fn, plan, effective_price, dur_minutes, now))
                price_str    = fmt_price(effective_price) if effective_price > 0 else "gratis"
                display_name = fn if fn else username
                return True, f"✅ *{display_name}* activado\n📋 Plan: {plan} | 💰 {price_str}\n📅 Expira: {expiry_str}"
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
        start_date = datetime.utcnow().replace(day=1, hour=0, minute=0, second=0, microsecond=0)

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
                cur.execute("""
                    SELECT user_id, username, first_name, plan, amount,
                           duration_minutes, payment_date
                    FROM payments
                    WHERE group_id=%s AND payment_date >= date_trunc('month', NOW())
                    ORDER BY payment_date DESC
                    LIMIT 10
                """, (group_id,))
                recent = cur.fetchall()
                return {"summary": summary, "total": total, "new_users": new_users, "recent": recent}
        return await self._run(_get)

    async def get_total_monthly_earnings(self):
        start_date = datetime.utcnow().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
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
                    SELECT user_id, username, first_name, plan, end_date
                    FROM users
                    WHERE group_id=%s AND status='active' AND end_date < NOW() AND kicked_at IS NULL
                """, (group_id,))
                return cur.fetchall()
        return await self._run(_get)

    async def expire_user(self, user_id: int, group_id: int):
        def _expire(conn):
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE users
                    SET status='expired', kicked_at=NOW(), updated_at=NOW()
                    WHERE user_id=%s AND group_id=%s AND status='active' AND kicked_at IS NULL
                    RETURNING user_id
                """, (user_id, group_id))
                return cur.fetchone() is not None
        return await self._run(_expire)

    async def get_potential_clients_stats(self, group_id: int):
        now              = datetime.utcnow()
        start_of_month   = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if now.month == 1:
            start_last_month = now.replace(year=now.year - 1, month=12, day=1, hour=0, minute=0, second=0, microsecond=0)
            end_last_month   = start_of_month
        else:
            start_last_month = now.replace(month=now.month - 1, day=1, hour=0, minute=0, second=0, microsecond=0)
            end_last_month   = start_of_month

        def _get(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM users WHERE group_id=%s AND status='potencial' AND created_at>=%s",
                            (group_id, start_of_month))
                count_month = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM users WHERE group_id=%s AND status='potencial' AND created_at>=%s AND created_at<%s",
                            (group_id, start_last_month, end_last_month))
                count_last_month = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM users WHERE group_id=%s AND status='potencial'", (group_id,))
                total_all = cur.fetchone()[0]
                return count_month, count_last_month, total_all
        return await self._run(_get)

    async def get_potential_clients_list(self, group_id: int):
        def _get(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT user_id, username, first_name, created_at
                    FROM users WHERE group_id=%s AND status='potencial' ORDER BY created_at DESC
                """, (group_id,))
                return cur.fetchall()
        return await self._run(_get)

    async def get_export_data(self, group_id: int):
        start_date = datetime.utcnow().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        def _get(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT user_id, username, first_name, plan, amount,
                           duration_minutes, payment_date
                    FROM payments WHERE group_id=%s AND payment_date>=%s ORDER BY payment_date DESC
                """, (group_id, start_date))
                return cur.fetchall()
        return await self._run(_get)

    async def update_group_fields(self, group_id: int, changes: dict):
        if not changes:
            return
        def _upd(conn):
            with conn.cursor() as cur:
                if 'settings' in changes:
                    cur.execute("UPDATE groups SET settings=%s WHERE group_id=%s",
                                (json.dumps(changes['settings']), group_id))
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

    async def get_trial_stats(self, group_id: int) -> dict:
        def _get(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT COUNT(*) as total_trial_users FROM users
                    WHERE group_id=%s AND (plan='trial' OR trial_used=TRUE)
                """, (group_id,))
                total_trial_users = cur.fetchone()['total_trial_users']
                cur.execute("""
                    SELECT user_id, username, first_name, end_date,
                           EXTRACT(EPOCH FROM (end_date - NOW())) / 86400 as days_left,
                           EXTRACT(EPOCH FROM (end_date - NOW())) / 3600  as hours_left
                    FROM users
                    WHERE group_id=%s AND plan='trial' AND status='active' AND end_date > NOW()
                    ORDER BY end_date ASC
                """, (group_id,))
                active_trials = cur.fetchall()
                cur.execute("""
                    SELECT COUNT(*) as expired_from_trial FROM users
                    WHERE group_id=%s AND plan='trial' AND status='expired' AND end_date < NOW()
                """, (group_id,))
                expired_from_trial = cur.fetchone()['expired_from_trial']
                cur.execute("""
                    SELECT COUNT(DISTINCT u.user_id) as converted_users
                    FROM users u
                    JOIN payments p ON u.user_id = p.user_id AND u.group_id = p.group_id
                    WHERE u.group_id=%s AND u.trial_used=TRUE AND p.amount > 0
                """, (group_id,))
                converted_users = cur.fetchone()['converted_users']
                return {
                    "total_trial_users":  total_trial_users,
                    "active_trials":      list(active_trials),
                    "active_count":       len(active_trials),
                    "expired_from_trial": expired_from_trial,
                    "converted_users":    converted_users
                }
        return await self._run(_get)

    # ── Broadcast ─────────────────────────────────────────────────────────────
    async def get_broadcast_targets(self, group_id: int, filter_type: str) -> list:
        """
        Retorna usuarios según el filtro. Usa OR (plan='trial' OR trial_used=TRUE)
        para capturar usuarios antiguos con registros de trial previos a migraciones.
        """
        def _get(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if filter_type == 'trial_only':
                    # Usaron trial pero NUNCA pagaron
                    cur.execute("""
                        SELECT DISTINCT u.user_id, u.username, u.first_name, u.group_id
                        FROM users u
                        WHERE u.group_id = %s
                          AND (u.plan = 'trial' OR u.trial_used = TRUE OR u.trial_used IS NULL)
                          AND NOT EXISTS (
                              SELECT 1 FROM payments p
                              WHERE p.user_id = u.user_id AND p.group_id = u.group_id AND p.amount > 0
                          )
                        ORDER BY u.user_id
                    """, (group_id,))
                elif filter_type == 'subscribed_only':
                    # Suscripción de pago activa
                    cur.execute("""
                        SELECT DISTINCT u.user_id, u.username, u.first_name, u.group_id
                        FROM users u
                        WHERE u.group_id = %s
                          AND (u.trial_used = TRUE OR u.trial_used IS NULL)
                          AND u.plan != 'trial'
                          AND u.status = 'active'
                          AND u.end_date > NOW()
                        ORDER BY u.user_id
                    """, (group_id,))
                elif filter_type == 'expired_only':
                    # Compraron antes, ahora expirados
                    cur.execute("""
                        SELECT DISTINCT u.user_id, u.username, u.first_name, u.group_id
                        FROM users u
                        WHERE u.group_id = %s
                          AND (u.trial_used = TRUE OR u.trial_used IS NULL)
                          AND u.status = 'expired'
                          AND EXISTS (
                              SELECT 1 FROM payments p
                              WHERE p.user_id = u.user_id AND p.group_id = u.group_id AND p.amount > 0
                          )
                        ORDER BY u.user_id
                    """, (group_id,))
                elif filter_type == 'all_trial':
                    # Todos los que usaron trial (cualquier estado)
                    cur.execute("""
                        SELECT DISTINCT u.user_id, u.username, u.first_name, u.group_id
                        FROM users u
                        WHERE u.group_id = %s
                          AND (u.plan = 'trial' OR u.trial_used = TRUE OR u.trial_used IS NULL)
                        ORDER BY u.user_id
                    """, (group_id,))
                else:
                    return []
                return cur.fetchall()
        return await self._run(_get)

    async def log_broadcast_sent(self, group_id: int, filter_type: str, message_text: str,
                                  total: int, success: int, fail: int, sent_by: int):
        preview = message_text[:100] if message_text else ""
        def _log(conn):
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO broadcast_logs
                        (group_id, filter_type, message_preview, total_count, success_count, fail_count, sent_by)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (group_id, filter_type, preview, total, success, fail, sent_by))
        await self._run(_log)

    async def get_broadcast_history(self, group_id: int, limit: int = 10) -> list:
        def _get(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT filter_type, message_preview, total_count,
                           success_count, fail_count, sent_at
                    FROM broadcast_logs
                    WHERE group_id = %s
                    ORDER BY sent_at DESC
                    LIMIT %s
                """, (group_id, limit))
                return cur.fetchall()
        return await self._run(_get)

    # ── Avisos de expiración ───────────────────────────────────────────────────
    async def get_warning_config(self, group_id: int) -> dict:
        def _get(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT settings FROM groups WHERE group_id = %s", (group_id,))
                row = cur.fetchone()
                if row and row['settings']:
                    settings = row['settings'] if isinstance(row['settings'], dict) else {}
                    wc = settings.get('warning_config')
                    if wc:
                        return wc
                return None
        result = await self._run(_get)
        if result:
            return result
        return {
            "enabled": False,
            "warnings": [
                {"minutes_before": 15, "message": "⏳ *Tu trial expira en {minutes_left} minutos*\n\n📅 Expira: {expiry_time}\n\n💳 ¿Quieres seguir disfrutando del contenido?\nPresiona el botón para ver los datos de pago:"}
            ]
        }

    async def save_warning_config(self, group_id: int, config: dict):
        def _save(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT settings FROM groups WHERE group_id = %s", (group_id,))
                row      = cur.fetchone()
                settings = row[0] if row and row[0] else {}
                if isinstance(settings, str):
                    settings = json.loads(settings)
                settings['warning_config'] = config
                cur.execute("UPDATE groups SET settings = %s WHERE group_id = %s",
                            (json.dumps(settings), group_id))
        await self._run(_save)
        group = get_group_by_id(group_id)
        if group:
            group.setdefault('settings', {})['warning_config'] = config

    async def get_users_for_warning(self, group_id: int, minutes_before: int) -> list:
        warning_threshold = datetime.utcnow() + timedelta(minutes=minutes_before)
        warning_type      = f"warning_{minutes_before}min"
        def _get(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT u.user_id, u.username, u.first_name, u.end_date, u.plan
                    FROM users u
                    LEFT JOIN warning_logs w
                        ON u.user_id = w.user_id AND u.group_id = w.group_id AND w.warning_type = %s
                    WHERE u.group_id = %s AND u.status = 'active' AND u.plan = 'trial'
                      AND u.end_date <= %s AND u.end_date > NOW() AND w.id IS NULL
                    ORDER BY u.end_date ASC
                """, (warning_type, group_id, warning_threshold))
                return cur.fetchall()
        return await self._run(_get)

    async def log_warning_sent(self, user_id: int, group_id: int, warning_type: str):
        def _log(conn):
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO warning_logs (user_id, group_id, warning_type, sent_at)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (user_id, group_id, warning_type) DO UPDATE SET sent_at = NOW()
                """, (user_id, group_id, warning_type))
        await self._run(_log)

    async def get_warning_stats(self, group_id: int) -> list:
        def _get(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT warning_type, COUNT(*) as count FROM warning_logs
                    WHERE group_id = %s GROUP BY warning_type
                """, (group_id,))
                return cur.fetchall()
        return await self._run(_get)

    async def clear_warning_logs(self, group_id: int = None):
        def _clear(conn):
            with conn.cursor() as cur:
                if group_id:
                    cur.execute("DELETE FROM warning_logs WHERE group_id = %s", (group_id,))
                else:
                    cur.execute("DELETE FROM warning_logs")
        await self._run(_clear)
        
    # ── Ruleta VIP (discount_spins) ───────────────────────────────────────────
    async def get_spin_data(self, user_id: int, group_id: int) -> dict:
        def _get(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT * FROM discount_spins 
                    WHERE user_id = %s AND group_id = %s
                """, (user_id, group_id))
                row = cur.fetchone()
                if not row:
                    return {"spin_count": 0, "used": False, "best_discount": 0}
                return dict(row)
        return await self._run(_get)

    async def record_spin(self, user_id: int, group_id: int, discount: int):
        def _save(conn):
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO discount_spins (user_id, group_id, spin_count, last_spin, best_discount)
                    VALUES (%s, %s, 1, NOW(), %s)
                    ON CONFLICT (user_id, group_id) DO UPDATE SET
                        spin_count = discount_spins.spin_count + 1,
                        last_spin = NOW(),
                        best_discount = GREATEST(discount_spins.best_discount, EXCLUDED.best_discount)
                """, (user_id, group_id, discount))
        await self._run(_save)

    async def mark_discount_used(self, user_id: int, group_id: int, plan: str = None):
        def _upd(conn):
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE discount_spins 
                    SET used = TRUE, applied_to_plan = %s
                    WHERE user_id = %s AND group_id = %s
                """, (plan, user_id, group_id))
        await self._run(_upd)
        

# ==================== INSTANCIAS GLOBALES ====================
db        = Database(DATABASE_URL)
scheduler = AsyncIOScheduler()
bot_app   = None


# ==================== HELPERS INTERNOS ====================
async def _safe_send(chat_id: int, text: str, **kwargs):
    try:
        return await bot_app.bot.send_message(chat_id, text, **kwargs)
    except Exception as e:
        logger.warning(f"No se pudo enviar mensaje a {chat_id}: {e}")
        return None

# ==================== EXPULSIÓN CON REINTENTOS ====================
async def _kick_user_with_retry(group_id: int, user_id: int, retries: int = 3) -> bool:
    for attempt in range(retries):
        try:
            try:
                member = await bot_app.bot.get_chat_member(group_id, user_id)
                if member.status in ("left", "kicked", "banned"):
                    logger.info(f"Usuario {user_id} ya no está en el grupo {group_id} (status: {member.status})")
                    return True
            except BadRequest as e:
                err = str(e).lower()
                if "user not found" in err or "member_id_invalid" in err:
                    return True
                raise

            await bot_app.bot.ban_chat_member(group_id, user_id)
            await asyncio.sleep(1)
            await bot_app.bot.unban_chat_member(group_id, user_id)
            logger.info(f"✅ Usuario {user_id} expulsado del grupo {group_id}")
            return True

        except TelegramError as e:
            error_str = str(e).lower()
            if any(phrase in error_str for phrase in [
                "not enough rights", "bot is not a member", "chat not found",
                "user not found", "participant_id_invalid", "member_id_invalid"
            ]):
                logger.error(f"❌ Error no recuperable expulsando {user_id} de {group_id}: {e}")
                return False
            wait = 2 ** attempt
            logger.warning(f"⚠️ Intento {attempt+1}/{retries} fallido para expulsar {user_id}: {e}. Reintentando en {wait}s...")
            await asyncio.sleep(wait)

    logger.error(f"❌ No se pudo expulsar al usuario {user_id} del grupo {group_id} tras {retries} intentos")
    return False


# ==================== HANDLERS ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    send = (update.callback_query.message.reply_text if update.callback_query
            else update.message.reply_text if update.message else None)
    if not send:
        return

    if user_id == SUPER_ADMIN_ID:
        vip_count      = sum(1 for g in GROUPS if g.get("type", "VIP") == "VIP")
        free_count     = sum(1 for g in GROUPS if g.get("type") == "FREE")
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
            f"• 👑 Grupos VIP: {vip_count}\n• 📋 Grupos FREE: {free_count}\n"
            f"• 👥 Total usuarios: {total_users}\n• 💰 Ganancias del mes: ${total_earnings}\n\n"
            f"Selecciona una opción:",
            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
        )
        return

    user_groups = get_groups_by_admin(user_id)
    
    # Si no tiene grupos asignados, mostrar panel de usuario normal
    if not user_groups:
        # Verificar si llegó con parámetro de grupo (link de inicio)
        if update.message and context.args:
            # ... (tu código actual de /start group_id ya está aquí) ...
            # Si ya implementaste el paso 1 de la ruleta, esto ya existe
            pass  # Se maneja más abajo o arriba en tu start()
        
        # Si no es admin y no llegó por link, mostrar mensaje genérico
        # que invite a usar el bot como usuario normal
        free_groups = [g for g in GROUPS if g.get("type") == "FREE"]
        vip_groups = [g for g in GROUPS if g.get("type", "VIP") == "VIP"]
        
        keyboard = []
        
        # Botones para grupos VIP (para que pueda solicitar info)
        for group in vip_groups:
            keyboard.append([
                InlineKeyboardButton(
                    f"🔥 Info {group['group_name']}", 
                    callback_data=f"pay_{group['group_id']}_{user_id}"
                )
            ])
        
        # O link para unirse a canal FREE si existe
        if free_groups:
            keyboard.append([
                InlineKeyboardButton(
                    "📢 Ver canal FREE", 
                    url=f"https://t.me/c/{str(free_groups[0]['group_id']).replace('-100', '')}"
                )
            ])
        
        await send(
            f"👋 *¡Bienvenido!*\n\n"
            f"Soy el bot de acceso VIP. ¿Quieres unirte a uno de nuestros grupos?\n\n"
            f"🎰 *Gira la ruleta y gana hasta 50% de descuento*\n"
            f"en tu primera suscripción.\n\n"
            f"Selecciona un grupo para ver información:",
            reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
            parse_mode="Markdown"
        )
        return

    if len(user_groups) == 1:
        group = user_groups[0]
        context.user_data['current_group'] = group['group_id']
        if group.get("type", "VIP") == "VIP":
            cfg_t = get_group_plan_config(group["group_id"], "trial")
            cfg_s = get_group_plan_config(group["group_id"], "semanal")
            cfg_m = get_group_plan_config(group["group_id"], "mensual")
            keyboard = [
                [InlineKeyboardButton("📊 Usuarios activos",   callback_data="list_active")],
                [InlineKeyboardButton("💰 Ganancias",          callback_data="earnings")],
                [InlineKeyboardButton("📥 Exportar mes",       callback_data="export_month")],
                [InlineKeyboardButton("📊 Estadísticas Trial", callback_data="trial_stats")],
                [InlineKeyboardButton("⚙️ Precios y Trial",    callback_data=f"cfg_group_{group['group_id']}")],
                [InlineKeyboardButton("📢 Broadcast Trial",    callback_data="broadcast_menu")],
            ]
            await send(
                f"👑 *Panel VIP - {group['group_name']}*\n\n"
                f"🆔 ID del grupo: `{group['group_id']}`\n\n"
                f"💡 *Tarifas actuales:*\n"
                f"• ⏱ Trial: {fmt_minutes(cfg_t.get('minutes', 1440))}\n"
                f"• 📅 Semanal: ${cfg_s['price']}\n• 📆 Mensual: ${cfg_m['price']}\n\n"
                f"Comandos disponibles:\n"
                f"• `/add @usuario plan` - Agregar suscripción\n"
                f"• También puedes RESPONDER al mensaje de registro con `/add plan`\n"
                f"• O usar `/add ID plan` (ej: `/add 123456789 mensual`)",
                reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
            )
        else:
            keyboard = [
                [InlineKeyboardButton("📋 Clientes potenciales", callback_data="list_potential")],
                [InlineKeyboardButton("📥 Exportar clientes",    callback_data="export_clients")]
            ]
            await send(
                f"📋 *Panel FREE - {group['group_name']}*\n\n🆔 ID del grupo: `{group['group_id']}`",
                reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
            )
    else:
        keyboard = []
        for group in user_groups:
            emoji = "👑" if group.get("type", "VIP") == "VIP" else "📋"
            keyboard.append([InlineKeyboardButton(f"{emoji} {group['group_name']}",
                             callback_data=f"select_group_{group['group_id']}")])
        await send("📋 *Selecciona el grupo que quieres gestionar*",
                   reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


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
        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
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
        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
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
        cfg   = get_group_plan_config(group_id, "trial")
        cfg_s = get_group_plan_config(group_id, "semanal")
        cfg_m = get_group_plan_config(group_id, "mensual")
        keyboard = [
            [InlineKeyboardButton("➕ Agregar usuario",    callback_data="add_user")],
            [InlineKeyboardButton("📊 Usuarios activos",   callback_data="list_active")],
            [InlineKeyboardButton("💰 Ganancias",          callback_data="earnings")],
            [InlineKeyboardButton("📥 Exportar mes",       callback_data="export_month")],
            [InlineKeyboardButton("📊 Estadísticas Trial", callback_data="trial_stats")],
            [InlineKeyboardButton("⚙️ Precios y Trial",    callback_data=f"cfg_group_{group_id}")],
            [InlineKeyboardButton("📢 Broadcast Trial",    callback_data="broadcast_menu")],
        ]
        await query.edit_message_text(
            f"👑 *Panel VIP - {group['group_name']}*\n\n"
            f"🆔 ID: `{group['group_id']}`\n👑 Admin: `{group['admin_id']}`\n\n"
            f"💡 *Tarifas actuales:*\n"
            f"• ⏱ Trial: {fmt_minutes(cfg.get('minutes', 1440))}\n"
            f"• 📅 Semanal: ${cfg_s['price']}\n• 📆 Mensual: ${cfg_m['price']}",
            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
        )
    else:
        keyboard = [
            [InlineKeyboardButton("📋 Clientes potenciales", callback_data="list_potential")],
            [InlineKeyboardButton("📥 Exportar clientes",    callback_data="export_clients")]
        ]
        await query.edit_message_text(
            f"📋 *Panel FREE - {group['group_name']}*\n\n"
            f"🆔 ID: `{group['group_id']}`\n👑 Admin: `{group['admin_id']}`",
            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
        )


async def total_earnings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    message = query.message if query else update.message
    if query:
        await query.answer()
    total = await db.get_total_monthly_earnings()
    now   = now_ec()
    await message.reply_text(
        f"💰 *GANANCIAS TOTALES DEL MES*\n\n"
        f"📅 {now.strftime('%B %Y')}\n💵 Total recaudado: *${total}*\n\n"
        f"📊 Incluye todos los grupos configurados.",
        parse_mode="Markdown"
    )


# ==================== HANDLER PARA REPLY CON /add ====================
async def handle_reply_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.reply_to_message:
        return
    if not update.message.text or not update.message.text.startswith('/add'):
        return

    user_id   = update.effective_user.id
    chat_id   = update.effective_chat.id
    replied_msg = update.message.reply_to_message
    text      = replied_msg.text or replied_msg.caption or ""

    logger.info(f"📝 Procesando reply a mensaje: {text[:100]}")

    target_user_id  = None
    target_username = None
    target_first_name = None

    id_match = re.search(r'🆔 (?:ID:?|ID)\s*`?(\d+)`?', text)
    if id_match:
        target_user_id = int(id_match.group(1))
    name_match = re.search(r'👤 Nombre:\s*(.+?)(?:\n|$)', text)
    if name_match:
        target_first_name = name_match.group(1).strip()
    username_match = re.search(r'@(\w+)', text)
    if username_match:
        target_username = username_match.group(1)

    if not target_user_id:
        await update.message.reply_text(
            "❌ No pude encontrar el ID del usuario en el mensaje.\n"
            "Asegúrate de responder al mensaje de registro del trial.\n\n"
            "También puedes usar:\n• `/add 123456789 mensual`\n• `/add @usuario mensual`",
            parse_mode="Markdown"
        )
        return

    current_group = context.user_data.get('current_group')
    if not current_group:
        if update.effective_chat.type in ("group", "supergroup"):
            current_group = chat_id
        else:
            await update.message.reply_text("❌ Usa /start primero para seleccionar un grupo")
            return

    if not can_manage_group(user_id, current_group):
        await update.message.reply_text("❌ No autorizado para gestionar este grupo")
        return

    args = update.message.text.split()
    if len(args) < 2:
        await update.message.reply_text(
            "❌ *Uso:* `/add plan [precio] [días]`\n\n*Ejemplos:*\n• `/add mensual`\n• `/add semanal 5.99`",
            parse_mode="Markdown"
        )
        return

    plan = args[1].lower()
    if plan not in PLANS:
        await update.message.reply_text("❌ Plan inválido. Usa: trial, semanal, mensual")
        return

    custom_price = None
    custom_days  = None
    if len(args) >= 3:
        try:
            custom_price = float(args[2].replace(",", "."))
            if custom_price < 0:
                raise ValueError
        except ValueError:
            await update.message.reply_text("❌ Precio inválido", parse_mode="Markdown")
            return
    if len(args) >= 4:
        try:
            custom_days = int(args[3])
            if custom_days <= 0:
                raise ValueError
        except ValueError:
            await update.message.reply_text("❌ Días inválidos", parse_mode="Markdown")
            return

    username   = target_username or f"user_{target_user_id}"
    first_name = target_first_name or ""

    existing = await db.get_user_by_id(target_user_id, current_group)
    if not existing:
        await db.register_free_user(current_group, target_user_id, username, first_name)

    success, msg = await db.add_or_update_user_by_id(
        current_group, target_user_id, plan, first_name, custom_price, custom_days
    )
    await update.message.reply_text(f"✅ *Activación por respuesta*\n\n{msg}", parse_mode="Markdown")

    if success:
        group = get_group_by_id(current_group)
        try:
            # Marcar descuento como usado si tenía uno pendiente
            spin_data = await db.get_spin_data(target_user_id, current_group)
            if spin_data and not spin_data.get("used") and spin_data.get("spin_count", 0) > 0:
                await db.mark_discount_used(target_user_id, current_group, plan)
            
            await context.bot.send_message(
                target_user_id,
                f"🎉 *¡Suscripción activada!*\n\n"
                f"Tu plan *{plan}* en *{group['group_name'] if group else 'VIP'}* ha sido activado.\n"
                f"¡Gracias por tu compra!",
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.warning(f"No se pudo notificar al usuario {target_user_id}: {e}")


# ==================== COMANDO /add ====================
async def add_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message and update.message.reply_to_message:
        await handle_reply_add(update, context)
        return

    user_id       = update.effective_user.id
    chat_id       = update.effective_chat.id
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
        cfg_t = get_group_plan_config(current_group, "trial")
        cfg_s = get_group_plan_config(current_group, "semanal")
        cfg_m = get_group_plan_config(current_group, "mensual")
        await update.message.reply_text(
            "❌ *Uso:* `/add @username|ID plan [precio] [días]`\n\n"
            "*Planes:*\n"
            f"• `trial`   — {cfg_t['name']}\n"
            f"• `semanal` — {cfg_s['name']}\n"
            f"• `mensual` — {cfg_m['name']}\n\n"
            "*Ejemplos:*\n"
            "`/add @juan semanal`\n`/add 123456789 mensual`\n"
            "`/add @juan semanal 5.99`\n`/add @juan semanal 5.99 10`\n\n"
            "*O responde al mensaje de registro con:* `/add mensual`",
            parse_mode="Markdown"
        )
        return

    identifier   = context.args[0].replace("@", "")
    plan         = context.args[1].lower()
    if plan not in PLANS:
        await update.message.reply_text("❌ Plan inválido. Usa: trial, semanal, mensual")
        return

    custom_price = None
    custom_days  = None
    if len(context.args) >= 3:
        try:
            custom_price = float(context.args[2].replace(",", "."))
            if custom_price < 0:
                raise ValueError
        except ValueError:
            await update.message.reply_text("❌ Precio inválido. Usa punto decimal (ej: `5.99`)", parse_mode="Markdown")
            return
    if len(context.args) >= 4:
        try:
            custom_days = int(context.args[3])
            if custom_days <= 0:
                raise ValueError
        except ValueError:
            await update.message.reply_text("❌ Días inválidos. Debe ser entero positivo.", parse_mode="Markdown")
            return

    if identifier.isdigit():
        target_user_id = int(identifier)
        first_name = ""
        username   = f"user_{target_user_id}"
        try:
            member = await context.bot.get_chat_member(current_group, target_user_id)
            if member and member.user:
                first_name = member.user.first_name or ""
                username   = member.user.username or f"user_{target_user_id}"
        except Exception as e:
            logger.warning(f"No se pudo obtener info del usuario {target_user_id}: {e}")
        existing = await db.get_user_by_id(target_user_id, current_group)
        if not existing:
            await db.register_free_user(current_group, target_user_id, username, first_name)
        success, msg = await db.add_or_update_user_by_id(current_group, target_user_id, plan, first_name, custom_price, custom_days)
        
        # Marcar descuento como usado si tenía uno pendiente
        if success:
            spin_data = await db.get_spin_data(target_user_id, current_group)
            if spin_data and not spin_data.get("used") and spin_data.get("spin_count", 0) > 0:
                await db.mark_discount_used(target_user_id, current_group, plan)
        
        await update.message.reply_text(msg, parse_mode="Markdown")
        return

    username   = identifier
    first_name = ""
    try:
        member = await context.bot.get_chat_member(current_group, username)
        if member and member.user:
            first_name = member.user.first_name or ""
    except Exception:
        pass
    success, msg = await db.add_or_update_user(current_group, username, plan, first_name, custom_price, custom_days)
    
    # Marcar descuento como usado si tenía uno pendiente
    if success:
        # Buscar el user_id para marcar el descuento
        user_record = await db.get_user_by_username(username, current_group)
        if user_record:
            target_user_id = user_record['user_id']
            spin_data = await db.get_spin_data(target_user_id, current_group)
            if spin_data and not spin_data.get("used") and spin_data.get("spin_count", 0) > 0:
                await db.mark_discount_used(target_user_id, current_group, plan)
    
    await update.message.reply_text(msg, parse_mode="Markdown")


async def list_active_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query    = update.callback_query
    message  = query.message if query else update.message
    group_id = context.user_data.get('current_group')
    if not group_id:
        await message.reply_text("❌ Selecciona un grupo con /start")
        return
    users = await db.get_all_active_users(group_id)
    if not users:
        await message.reply_text("📭 No hay usuarios activos")
        return
    msg = f"📊 *USUARIOS ACTIVOS* ({len(users)})\n\n"
    now = datetime.utcnow()
    for user in users[:30]:
        end_date   = user['end_date']
        remaining  = end_date - now
        total_mins = max(0, int(remaining.total_seconds() / 60))
        days_left  = total_mins // 1440
        emoji      = "🟢" if days_left > 7 else "🟡" if days_left > 1 else "🔴"

        if total_mins < 60:
            expiry_text = f"{total_mins} min restantes"
        elif total_mins < 1440:
            h = total_mins // 60; m = total_mins % 60
            expiry_text = f"{h}h {m}m restantes" if m else f"{h}h restantes"
        else:
            expiry_text = f"{days_left} días restantes"

        expiry_date  = fmt_dt(end_date, include_time=(total_mins < 1440))
        first_name   = user.get('first_name', '') or 'Sin nombre'
        username     = user.get('username', '')
        display      = (f"{first_name} (@{username})"
                        if username and not username.startswith('user_')
                        else f"{first_name} (ID: `{user['user_id']}`)")
        chat_link    = f"tg://user?id={user['user_id']}"
        msg += (f"{emoji} {display}\n"
                f"   📅 Expira: {expiry_date} ({expiry_text})\n"
                f"   📋 Plan: {user['plan']}\n"
                f"   🔗 [Abrir chat]({chat_link})\n\n")
    await message.reply_text(msg, parse_mode="Markdown")


async def show_earnings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query    = update.callback_query
    message  = query.message if query else update.message
    group_id = context.user_data.get('current_group')
    if not group_id:
        await message.reply_text("❌ Selecciona un grupo con /start")
        return
    earnings = await db.get_monthly_earnings(group_id)
    now      = now_ec()
    msg      = f"💰 *GANANCIAS DE {now.strftime('%B %Y').upper()}*\n\n"
    if not earnings['summary']:
        msg += "📭 No hay ventas"
    else:
        for plan in earnings['summary']:
            plan_name = PLANS.get(plan['plan'], {}).get('name', plan['plan'])
            msg += f"• {plan_name}: {plan['count']} ventas - {fmt_price(plan['total'])}\n"
        msg += f"\n💵 *TOTAL*: {fmt_price(earnings['total'])}\n👥 *Nuevos*: {earnings['new_users']}"
    if earnings.get('recent'):
        msg += "\n\n📋 *Últimos pagos:*\n"
        for p in earnings['recent']:
            name    = p['first_name'] or p['username'] or f"ID:{p['user_id']}"
            dur_str = f" ({fmt_minutes(p['duration_minutes'])})" if p.get('duration_minutes') else ""
            msg += f"• {name} - {p['plan']}{dur_str} - {fmt_price(p['amount'])}\n"
    await message.reply_text(msg, parse_mode="Markdown")


async def export_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query    = update.callback_query
    message  = query.message if query else update.message
    group_id = context.user_data.get('current_group')
    if not group_id:
        await message.reply_text("❌ Selecciona un grupo con /start")
        return
    now          = now_ec()
    transactions = await db.get_export_data(group_id)
    if not transactions:
        await message.reply_text(f"📭 No hay transacciones en {now.strftime('%B %Y')}")
        return
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(['Fecha', 'User ID', 'Username', 'Nombre', 'Plan', 'Duración', 'Monto USD', 'Link de contacto'])
    for t in transactions:
        dur = fmt_minutes(t['duration_minutes']) if t.get('duration_minutes') else ""
        writer.writerow([
            fmt_dt(t['payment_date']),
            t['user_id'], t['username'] or 'Sin username', t['first_name'] or 'Sin nombre',
            t['plan'].upper(), dur, f"{float(t['amount']):.2f}", f"tg://user?id={t['user_id']}"
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
    now = now_ec()
    if not last_backup_date or (now - last_backup_date).days >= 15:
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(['group_id', 'group_name', 'group_type', 'admin_id', 'backup_date'])
        for group in GROUPS:
            writer.writerow([group['group_id'], group['group_name'],
                             group.get('type', 'VIP'), group['admin_id'],
                             now.strftime('%Y-%m-%d %H:%M:%S')])
        output.seek(0)
        try:
            await bot_app.bot.send_document(
                SUPER_ADMIN_ID,
                document=output.getvalue().encode('utf-8-sig'),
                filename=f"backup_automatico_{now.strftime('%Y%m%d')}.csv",
                caption=(f"📦 *Backup Automático*\n\n📅 Fecha: {now.strftime('%d/%m/%Y %H:%M')} (EC)\n"
                         f"📊 Grupos incluidos: {len(GROUPS)}\n\n"
                         f"⚠️ Guarda este archivo en un lugar seguro."),
                parse_mode="Markdown"
            )
            output.close()
            with open(last_backup_file, 'w') as f:
                f.write(now.isoformat())
            logger.info("✅ Backup automático enviado")
        except Exception as e:
            logger.error(f"❌ Error enviando backup automático: {e}")


async def manual_backup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != SUPER_ADMIN_ID:
        await update.message.reply_text("❌ Solo Super Admin")
        return
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(['group_id', 'group_name', 'group_type', 'admin_id', 'backup_date'])
    now = now_ec()
    for group in GROUPS:
        writer.writerow([group['group_id'], group['group_name'],
                         group.get('type', 'VIP'), group['admin_id'],
                         now.strftime('%Y-%m-%d %H:%M:%S')])
    output.seek(0)
    await update.message.reply_document(
        document=output.getvalue().encode('utf-8-sig'),
        filename=f"backup_manual_{now.strftime('%Y%m%d_%H%M%S')}.csv",
        caption=f"📦 *Backup Manual*\n\n📅 Fecha: {now.strftime('%d/%m/%Y %H:%M')} (EC)\n📊 Grupos incluidos: {len(GROUPS)}",
        parse_mode="Markdown"
    )
    output.close()


async def restore_backup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != SUPER_ADMIN_ID:
        await update.message.reply_text("❌ Solo Super Admin")
        return
    if not update.message.document:
        await update.message.reply_text("❌ Envía el archivo CSV de backup junto con el comando.")
        return
    await update.message.reply_text("🔄 Restaurando configuración...")
    try:
        import io
        file         = await update.message.document.get_file()
        file_content = await file.download_as_bytearray()
        reader       = csv.reader(io.StringIO(file_content.decode('utf-8')))
        next(reader)
        restored_count = 0
        for row in reader:
            if len(row) >= 4:
                group_id   = int(row[0])
                group_name = row[1]
                group_type = row[2]
                admin_id   = int(row[3])
                if get_group_by_id(group_id):
                    for g in GROUPS:
                        if g["group_id"] == group_id:
                            g.update({"group_name": group_name, "type": group_type, "admin_id": admin_id})
                            break
                    await db.update_group_fields(group_id, {'admin': admin_id, 'type': group_type})
                else:
                    GROUPS.append({"group_id": group_id, "group_name": group_name,
                                   "type": group_type, "admin_id": admin_id, "settings": {}})
                    await db.save_group(group_id, group_name, admin_id, group_type)
                restored_count += 1
        await update.message.reply_text(
            f"✅ *Restauración completa*\n\n📊 Grupos restaurados: {restored_count}",
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
        msg += (f"📌 *{group['group_name']}*\n"
                f"   🆔 ID: `{group['group_id']}`\n"
                f"   👑 Admin: `{group['admin_id']}`\n"
                f"   📋 Tipo: {group.get('type', 'VIP')}\n\n")
    await message.reply_text(msg, parse_mode="Markdown")


async def add_group_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != SUPER_ADMIN_ID:
        await update.message.reply_text("❌ Solo Super Admin")
        return
    if len(context.args) < 4:
        await update.message.reply_text(
            "❌ Formato: `/addgroup group_id TIPO \"nombre\" admin_id`", parse_mode="Markdown"
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
                       "group_name": group_name, "admin_id": admin_id, "settings": {}})
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
    keyboard = [[InlineKeyboardButton(f"📌 {g['group_name']}",
                                      callback_data=f"select_group_{g['group_id']}")] for g in groups]
    if select_mode:
        keyboard.append([InlineKeyboardButton("🔙 Volver", callback_data="menu_view_groups")])
    await query.edit_message_text(f"📋 *Grupos {group_type}*",
                                  reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


async def menu_edit_group_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not GROUPS:
        await query.edit_message_text("📭 No hay grupos configurados")
        return
    keyboard = []
    for group in GROUPS:
        emoji = "👑" if group.get("type", "VIP") == "VIP" else "📋"
        keyboard.append([InlineKeyboardButton(f"{emoji} {group['group_name']}",
                                              callback_data=f"edit_multiple_{group['group_id']}")])
    keyboard.append([InlineKeyboardButton("🔙 Volver", callback_data="menu_groups")])
    await query.edit_message_text("✏️ *Selecciona el grupo que deseas editar*",
                                  reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


async def menu_delete_group_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not GROUPS:
        await query.edit_message_text("📭 No hay grupos configurados")
        return
    keyboard = []
    for group in GROUPS:
        emoji = "👑" if group.get("type", "VIP") == "VIP" else "📋"
        keyboard.append([InlineKeyboardButton(f"{emoji} {group['group_name']}",
                                              callback_data=f"delete_confirm_{group['group_id']}")])
    keyboard.append([InlineKeyboardButton("🔙 Volver", callback_data="menu_groups")])
    await query.edit_message_text("❌ *Eliminar Grupo*\n\n⚠️ Esta acción es irreversible. Selecciona el grupo:",
                                  reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


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
        f"⚠️ *Confirmar eliminación*\n\n📌 *{group['group_name']}*\n"
        f"🆔 ID: `{group['group_id']}`\n\n*Esta acción no se puede deshacer.*",
        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
    )


async def delete_group_execute(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query
    await query.answer()
    group = get_group_by_id(group_id)
    if not group:
        await query.edit_message_text("❌ Grupo no encontrado")
        return
    group_name = group['group_name']
    global GROUPS
    GROUPS = [g for g in GROUPS if g["group_id"] != group_id]
    await db.delete_group_from_db(group_id)
    await query.edit_message_text(f"✅ *Grupo eliminado*\n\n📌 {group_name}\n🆔 ID: `{group_id}`",
                                  parse_mode="Markdown")
    await asyncio.sleep(2)
    await menu_groups(update, context)


async def menu_commands(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    commands_text = (
        "📟 *Comandos Disponibles*\n\n"
        "*Super Admin:*\n"
        "• `/start` - Panel principal\n• `/addgroup` - Agregar nuevo grupo\n"
        "• `/groups` - Ver todos los grupos\n• `/backup` - Backup manual\n"
        "• `/searchgrupo nombre` - Buscar grupo\n\n"
        "*Admin de Grupo:*\n"
        "• `/start` - Panel de control\n"
        "• `/add @usuario|ID plan [precio] [días]` - Agregar usuario\n"
        "• También puedes RESPONDER al mensaje de registro con `/add plan`\n"
        "• `/configpago group_id` - Configurar datos de pago\n"
        "• `/broadcast group_id` - Enviar mensaje masivo\n\n"
        "*Usuarios (VIP):*\n• `/pagar` - Ver datos de pago\n\n"
        "*Planes:*\n• `trial` / `semanal` / `mensual`\n\n"
        "*Ejemplos:*\n"
        "• `/add @juan semanal`\n• `/add 123456789 mensual`\n"
        "• `/add @juan semanal 5.99`\n• Responder al registro: `/add mensual`"
    )
    keyboard = [[InlineKeyboardButton("🔙 Volver", callback_data="back_to_admin")]]
    await query.edit_message_text(commands_text, reply_markup=InlineKeyboardMarkup(keyboard),
                                  parse_mode="Markdown")


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
    now = now_ec()
    if count_last_month > 0:
        growth      = ((count_month - count_last_month) / count_last_month) * 100
        growth_text = f"{'📈' if growth > 0 else '📉' if growth < 0 else '➖'} {growth:+.1f}% vs mes anterior"
    else:
        growth_text = "📊 Primer mes con registros" if count_month > 0 else "📊 Sin registros este mes"
    msg = (f"📋 *CLIENTES POTENCIALES - {group['group_name']}*\n\n"
           f"📅 *{now.strftime('%B %Y')}:* {count_month} nuevos\n"
           f"📆 *Mes anterior:* {count_last_month}\n"
           f"📊 *Total histórico:* {total_all}\n\n{growth_text}")
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
        writer.writerow([c['user_id'], c['username'] or '', c['first_name'] or '',
                         fmt_dt(c['created_at'])])
    output.seek(0)
    await query.message.reply_document(
        document=output.getvalue().encode('utf-8-sig'),
        filename=f"clientes_{now_ec().strftime('%Y%m%d')}.csv",
        caption="📋 Clientes potenciales"
    )
    output.close()


async def trial_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    message = query.message if query else update.message
    if query:
        await query.answer()

    group_id = context.user_data.get('current_group')
    if not group_id:
        await message.reply_text("❌ Selecciona un grupo con /start")
        return
    group = get_group_by_id(group_id)
    if not group or group.get("type") != "VIP":
        await message.reply_text("❌ Solo disponible en grupos VIP")
        return

    stats = await db.get_trial_stats(group_id)
    msg   = (f"📊 *ESTADÍSTICAS DE TRIAL — {group['group_name']}*\n\n"
             f"• 🆓 *Usaron trial:* {stats['total_trial_users']} personas\n"
             f"• 🟢 *Trial activos ahora:* {stats['active_count']} personas\n"
             f"• 🔴 *Expirados desde trial:* {stats['expired_from_trial']} personas\n"
             f"• 💰 *Convertidos a pago:* {stats['converted_users']} personas\n")

    if stats['active_trials']:
        msg += "\n⏳ *TIEMPO RESTANTE POR USUARIO:*\n"
        for trial in stats['active_trials']:
            total_hours = float(trial['hours_left']) if trial['hours_left'] else 0
            days  = int(total_hours // 24)
            hours = int(total_hours % 24)
            mins  = int((total_hours * 60) % 60)
            name  = trial['first_name'] or trial['username'] or f"ID:{trial['user_id']}"
            username_str = (f"@{trial['username']}"
                            if trial['username'] and not str(trial['username']).startswith('user_')
                            else f"ID:{trial['user_id']}")
            time_left  = (f"{days}d {hours}h {mins}m" if days > 0
                          else f"{hours}h {mins}m" if hours > 0 else f"{mins}m")
            expiry_str = fmt_dt(trial['end_date'])
            chat_link  = f"tg://user?id={trial['user_id']}"
            msg += (f"\n👤 [{name}]({chat_link}) ({username_str})\n"
                    f"   ⏳ Tiempo restante: *{time_left}*\n"
                    f"   📅 Expira: {expiry_str}\n")

    if stats['total_trial_users'] > 0:
        conversion = (stats['converted_users'] / stats['total_trial_users']) * 100
        msg += f"\n\n📈 *Tasa de conversión:* {conversion:.1f}%"

    keyboard = [[InlineKeyboardButton("🔙 Volver", callback_data=f"select_group_{group_id}")]]
    if query:
        await query.edit_message_text(msg, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await message.reply_text(msg, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))


async def edit_group_multiple(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query
    await query.answer()
    group = get_group_by_id(group_id)
    if not group:
        await query.edit_message_text("❌ Grupo no encontrado")
        return
    context.user_data['editing_group_id'] = group_id
    context.user_data['editing_mode']     = 'multiple'
    context.user_data.setdefault('pending_changes', {})
    keyboard = [
        [InlineKeyboardButton("📝 Cambiar nombre", callback_data=f"multi_name_{group_id}")],
        [InlineKeyboardButton("👤 Cambiar admin",  callback_data=f"multi_admin_{group_id}")],
        [InlineKeyboardButton("🔄 Cambiar tipo",   callback_data=f"multi_type_{group_id}")],
        [InlineKeyboardButton("✅ Aplicar cambios", callback_data=f"multi_apply_{group_id}")],
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
        f"*Escribe 'cancelar' para cancelar la edición.*",
        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
    )


async def multi_name_request(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query
    await query.answer()
    context.user_data['editing_field']    = 'multi_name'
    context.user_data['editing_group_id'] = group_id
    await query.edit_message_text(
        "✏️ *Cambiar nombre*\n\nEnvía el nuevo nombre.\n*Escribe 'cancelar' para cancelar.*",
        parse_mode="Markdown"
    )


async def multi_admin_request(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query
    await query.answer()
    context.user_data['editing_field']    = 'multi_admin'
    context.user_data['editing_group_id'] = group_id
    await query.edit_message_text(
        "👤 *Cambiar administrador*\n\nEnvía el ID del nuevo administrador.\n*Escribe 'cancelar' para cancelar.*",
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
    await query.edit_message_text("🔄 *Selecciona el nuevo tipo*",
                                  reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


async def multi_set_type(update: Update, context: ContextTypes.DEFAULT_TYPE,
                         group_id: int, new_type: str):
    query = update.callback_query
    await query.answer()
    context.user_data.setdefault('pending_changes', {})['type'] = new_type
    await query.edit_message_text(f"✅ *Tipo guardado:* {new_type}", parse_mode="Markdown")
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
    for g in GROUPS:
        if g["group_id"] == group_id:
            if 'name'  in pending: g["group_name"] = pending['name'];  changes_made.append(f"📝 Nombre → {pending['name']}")
            if 'admin' in pending: g["admin_id"]   = pending['admin']; changes_made.append(f"👤 Admin → {pending['admin']}")
            if 'type'  in pending: g["type"]        = pending['type'];  changes_made.append(f"🔄 Tipo → {pending['type']}")
            break
    await db.update_group_fields(group_id, pending)
    for key in ('pending_changes', 'editing_mode', 'editing_group_id'):
        context.user_data.pop(key, None)
    await query.edit_message_text(f"✅ *Cambios aplicados*\n\n" + "\n".join(changes_made), parse_mode="Markdown")
    await asyncio.sleep(2)
    await menu_edit_group_select(update, context)


# ==================== DETECCIÓN DE MIEMBROS ====================
async def _process_new_vip_member(chat_id: int, user_id: int, username: str,
                                  first_name: str, group: dict):
    display   = first_name or (f"@{username}" if not username.startswith('user_') else f"Usuario {user_id}")
    chat_link = f"tg://user?id={user_id}"

    logger.info(f"🔔 Nuevo miembro VIP: user={user_id} ({display}) en {group['group_name']}")

    allow_access, result_code, end_date = await db.register_user_auto(chat_id, user_id, username, first_name)

    logger.info(f"register_user_auto → allow={allow_access}, code={result_code}, end_date={end_date}")

    if allow_access:
        if result_code == "trial_nuevo":
            cfg_t      = get_group_plan_config(chat_id, "trial")
            trial_str  = fmt_minutes(cfg_t.get('minutes', 1440))
            expiry_str = fmt_dt(end_date) if end_date else "N/A"
            await _safe_send(
                user_id,
                f"🎉 *¡Bienvenido al grupo VIP!*\n\n"
                f"✨ Tu período de prueba de *{trial_str}* ha comenzado.\n"
                f"📅 Tu acceso expira el: *{expiry_str}* (hora Ecuador)\n\n"
                f"🔥 *Oferta por tiempo limitado:* si pagas *antes* de que termine tu prueba, obtienes un *precio especial*. ¡Asegura tu precio preferencial hoy mismo!\n\n"
                f"🎰 *¿Sabías que puedes ganar hasta 50% de descuento?*\n"
                f"Gira la ruleta VIP y usa tu descuento cuando quieras:\n\n",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🎰 GIRAR RULETA VIP", callback_data=f"spin_{chat_id}_{user_id}")],
                    [InlineKeyboardButton("💳 Ver Datos de Pago", callback_data=f"pay_{chat_id}_{user_id}")]
                ]),
                parse_mode="Markdown"
            )
            await _safe_send(
                group["admin_id"],
                f"🆕 *Nuevo usuario en TRIAL*\n\n"
                f"👤 *Nombre:* [{display}]({chat_link})\n"
                f"🆔 *ID:* `{user_id}`\n"
                f"📌 *Grupo:* {group['group_name']}\n"
                f"⏱ *Duración:* {trial_str}\n"
                f"📅 *Expira:* {expiry_str} (hora Ecuador)\n\n"
                f"💡 *Para activar suscripción:* RESPONDE a este mensaje con:\n"
                f"• `/add mensual`\n• `/add semanal 5.99`\n• O usa: `/add {user_id} mensual`",
                parse_mode="Markdown"
            )
        elif result_code == "activo":
            logger.info(f"✅ Reingreso permitido (plan activo): {display} en {group['group_name']} — expira {end_date}")
        return True

    # Acceso denegado
    if result_code == "expirado":
        motivo_usuario = "Tu período de prueba o suscripción ha expirado."
        motivo_admin   = "Plan vencido"
    else:
        motivo_usuario = "Ya usaste tu prueba gratuita y no tienes suscripción activa."
        motivo_admin   = "Trial ya utilizado"

    try:
        member_check = await bot_app.bot.get_chat_member(chat_id, user_id)
        if member_check.status in ("left", "kicked", "banned"):
            logger.info(f"⏭️ Usuario {display} ya está fuera del grupo — omitiendo")
            return False
    except Exception as e:
        logger.debug(f"No se pudo verificar estado previo de {user_id}: {e}")

    kick_success = await _kick_user_with_retry(chat_id, user_id)

    if kick_success:
        await _safe_send(
            user_id,
            f"🚫 *Acceso denegado — {group['group_name']}*\n\n"
            f"{motivo_usuario}\n\n💳 ¿Quieres renovar? Presiona el botón:",
            reply_markup=get_payment_keyboard(chat_id, user_id), parse_mode="Markdown"
        )
        await _safe_send(
            group["admin_id"],
            f"🚫 *Reingreso denegado y expulsado*\n\n"
            f"👤 *Usuario:* [{display}]({chat_link})\n"
            f"🆔 *ID:* `{user_id}`\n"
            f"📌 *Grupo:* {group['group_name']}\n"
            f"⚠️ *Motivo:* {motivo_admin}",
            parse_mode="Markdown"
        )
    else:
        await _safe_send(
            group["admin_id"],
            f"⚠️ *No se pudo expulsar automáticamente*\n\n"
            f"👤 Usuario: [{display}]({chat_link})\n"
            f"📌 Grupo: {group['group_name']}\n"
            f"⚠️ Motivo: {motivo_admin}\n\n"
            f"🔧 *Acción manual requerida.*",
            parse_mode="Markdown"
        )
    return False


async def _process_new_free_member(chat_id: int, user_id: int, username: str,
                                   first_name: str, group: dict):
    display   = first_name or (f"@{username}" if not username.startswith('user_') else f"Usuario {user_id}")
    chat_link = f"tg://user?id={user_id}"
    is_new    = await db.register_free_user(chat_id, user_id, username, first_name)
    if is_new:
        await _safe_send(
            group["admin_id"],
            f"📋 *Nuevo cliente potencial*\n\n"
            f"👤 *Nombre:* {display}\n🆔 *ID:* `{user_id}`\n"
            f"📌 *Grupo:* {group['group_name']}\n🔗 [Abrir chat]({chat_link})",
            parse_mode="Markdown"
        )
    return is_new


async def track_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    result = update.chat_member
    if not result:
        return
    new_member = result.new_chat_member.user
    if new_member.is_bot:
        return
    chat_id    = result.chat.id
    old_status = result.old_chat_member.status
    new_status = result.new_chat_member.status
    joined     = (old_status in ("left", "kicked", "restricted", "banned") and
                  new_status in ("member", "administrator", "creator"))
    if not joined:
        return
    group = get_group_by_id(chat_id)
    if not group:
        return
    user_id    = new_member.id
    username   = new_member.username or f"user_{user_id}"
    first_name = new_member.first_name or ""
    if group["type"] == "VIP":
        await _process_new_vip_member(chat_id, user_id, username, first_name, group)
    else:
        await _process_new_free_member(chat_id, user_id, username, first_name, group)


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
            await _process_new_vip_member(chat_id, user_id, username, first_name, group)
        else:
            await _process_new_free_member(chat_id, user_id, username, first_name, group)


async def detect_active_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
    cache_key  = f"known_{chat_id}_{user_id}"
    if context.bot_data.get(cache_key):
        return
    existing  = await db.get_user_by_id(user_id, chat_id)
    display   = first_name or (f"@{username}" if not username.startswith("user_") else f"Usuario {user_id}")
    chat_link = f"tg://user?id={user_id}"

    if group["type"] == "FREE":
        is_new = await db.register_free_user(chat_id, user_id, username, first_name)
        if is_new:
            await _safe_send(
                group["admin_id"],
                f"📋 *Nuevo cliente potencial detectado*\n\n"
                f"👤 *Nombre:* {display}\n🆔 *ID:* `{user_id}`\n"
                f"📌 *Grupo:* {group['group_name']}\n🔗 [Abrir chat]({chat_link})\n\n"
                f"_Detectado al escribir en el grupo._",
                parse_mode="Markdown"
            )
        if existing and existing.get("status") == "active" or not existing:
            context.bot_data[cache_key] = True
    else:
        if not existing:
            await _safe_send(
                group["admin_id"],
                f"⚠️ *Usuario sin registro en grupo VIP*\n\n"
                f"👤 *Nombre:* {display}\n🆔 *ID:* `{user_id}`\n"
                f"📌 *Grupo:* {group['group_name']}\n🔗 [Abrir chat]({chat_link})\n\n"
                f"Para activarlo: `/add {user_id} semanal`",
                parse_mode="Markdown"
            )
        elif existing.get("status") == "active":
            context.bot_data[cache_key] = True


# ==================== CONFIGURACIÓN DE PRECIOS Y TRIAL ====================
async def menu_group_settings(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query
    await query.answer()
    group = get_group_by_id(group_id)
    if not group:
        await query.edit_message_text("❌ Grupo no encontrado")
        return
    cfg_trial   = get_group_plan_config(group_id, "trial")
    cfg_semanal = get_group_plan_config(group_id, "semanal")
    cfg_mensual = get_group_plan_config(group_id, "mensual")
    trial_str   = fmt_minutes(cfg_trial.get('minutes', 1440))
    keyboard = [
        [InlineKeyboardButton(f"⏱ Trial: {trial_str}",                                callback_data=f"cfg_trial_{group_id}")],
        [InlineKeyboardButton(f"📅 Días semanal: {cfg_semanal['days']}d",              callback_data=f"cfg_dur_semanal_{group_id}")],
        [InlineKeyboardButton(f"💲 Precio semanal: {fmt_price(cfg_semanal['price'])}", callback_data=f"cfg_price_semanal_{group_id}")],
        [InlineKeyboardButton(f"📆 Días mensual: {cfg_mensual['days']}d",              callback_data=f"cfg_dur_mensual_{group_id}")],
        [InlineKeyboardButton(f"💲 Precio mensual: {fmt_price(cfg_mensual['price'])}", callback_data=f"cfg_price_mensual_{group_id}")],
        [InlineKeyboardButton("💳 Configurar Pago",                                   callback_data=f"cfg_payment_{group_id}")],
        [InlineKeyboardButton("🔔 Configurar Avisos",                                 callback_data=f"cfg_warnings_{group_id}")],
        [InlineKeyboardButton("🔙 Volver",                                             callback_data=f"select_group_{group_id}")]
    ]
    await query.edit_message_text(
        f"⚙️ *Configuración - {group['group_name']}*\n\n"
        f"• ⏱ *Trial:* {trial_str}\n"
        f"• 📅 *Semanal:* {cfg_semanal['days']} días | {fmt_price(cfg_semanal['price'])}\n"
        f"• 📆 *Mensual:* {cfg_mensual['days']} días | {fmt_price(cfg_mensual['price'])}\n\n"
        f"_Con `/add` puedes sobreescribir precio y días por usuario._",
        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
    )


async def cfg_trial_request(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query
    await query.answer()
    context.user_data['cfg_field']    = 'trial_minutes'
    context.user_data['cfg_group_id'] = group_id
    cfg = get_group_plan_config(group_id, "trial")
    await query.edit_message_text(
        f"⏱ *Cambiar duración del Trial*\n\nValor actual: *{fmt_minutes(cfg.get('minutes', 1440))}*\n\n"
        f"Envía el número de *minutos*:\n• 20 min = 20\n• 1 hora = 60\n• 1 día = 1440\n• 7 días = 10080\n\n"
        f"*Escribe 'cancelar' para cancelar.*",
        parse_mode="Markdown"
    )


async def cfg_price_request(update: Update, context: ContextTypes.DEFAULT_TYPE,
                             group_id: int, plan: str):
    query = update.callback_query
    await query.answer()
    context.user_data['cfg_field']    = f'price_{plan}'
    context.user_data['cfg_group_id'] = group_id
    cfg       = get_group_plan_config(group_id, plan)
    days      = cfg.get('days', 7 if plan == "semanal" else 30)
    plan_name = f"Semanal ({days} días)" if plan == "semanal" else f"Mensual ({days} días)"
    await query.edit_message_text(
        f"💲 *Precio base - {plan_name}*\n\nValor actual: *{fmt_price(cfg['price'])}*\n\n"
        f"Envía el precio (ej: `10`, `5.99`)\n\n*Escribe 'cancelar' para cancelar.*",
        parse_mode="Markdown"
    )


async def cfg_duration_request(update: Update, context: ContextTypes.DEFAULT_TYPE,
                                group_id: int, plan: str):
    query = update.callback_query
    await query.answer()
    context.user_data['cfg_field']    = f'duration_{plan}'
    context.user_data['cfg_group_id'] = group_id
    cfg       = get_group_plan_config(group_id, plan)
    plan_name = "Semanal" if plan == "semanal" else "Mensual"
    await query.edit_message_text(
        f"📅 *Días de duración - {plan_name}*\n\n"
        f"Valor actual: *{cfg.get('days', 7 if plan == 'semanal' else 30)} días*\n\n"
        f"Envía el número de días (ej: `7`, `14`, `30`)\n\n*Escribe 'cancelar' para cancelar.*",
        parse_mode="Markdown"
    )


async def handle_cfg_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        return
    field    = context.user_data.get('cfg_field')
    group_id = context.user_data.get('cfg_group_id')
    if not field or not group_id:
        return
    if not can_manage_group(update.effective_user.id, group_id):
        return
    text = update.message.text.strip().replace(",", ".")
    if text.lower() == 'cancelar':
        context.user_data.pop('cfg_field', None)
        context.user_data.pop('cfg_group_id', None)
        await update.message.reply_text("❌ Configuración cancelada")
        return
    is_price   = field.startswith("price_")
    is_minutes = field == "trial_minutes"
    try:
        if is_price:
            value = round(float(text), 2)
            if value < 0:
                raise ValueError
        else:
            value = int(float(text))
            if value <= 0:
                raise ValueError
    except ValueError:
        if is_price:
            await update.message.reply_text("❌ Precio inválido. Ej: `5.99` o `10`", parse_mode="Markdown")
        elif is_minutes:
            await update.message.reply_text("❌ Ingresa los minutos como entero. Ej: `1440`", parse_mode="Markdown")
        else:
            await update.message.reply_text("❌ Ingresa los días como entero positivo. Ej: `7`", parse_mode="Markdown")
        return
    group    = get_group_by_id(group_id)
    settings = dict(group.get("settings", {}))
    settings[field] = value
    group["settings"] = settings
    await db.update_group_fields(group_id, {'settings': settings})
    context.user_data.pop('cfg_field', None)
    context.user_data.pop('cfg_group_id', None)
    labels = {
        "trial_minutes":    f"Trial: *{fmt_minutes(value)}*",
        "price_semanal":    f"Precio semanal base: *{fmt_price(value)}*",
        "price_mensual":    f"Precio mensual base: *{fmt_price(value)}*",
        "duration_semanal": f"Duración semanal: *{value} días*",
        "duration_mensual": f"Duración mensual: *{value} días*",
    }
    label = labels.get(field, f"{field}: *{value}*")
    await update.message.reply_text(
        f"✅ *Configuración actualizada*\n\n• {label}\n\nAplica en *{group['group_name']}*.",
        parse_mode="Markdown"
    )


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
        msg  += (f"{emoji} *{group['group_name']}*\n"
                 f"   🆔 ID: `{group['group_id']}`\n   👑 Admin: `{group['admin_id']}`\n\n")
    await update.message.reply_text(msg, parse_mode="Markdown")


async def handle_edit_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        return
    # Prioridades de input
    if context.user_data.get('broadcast_step') == 'message' and context.user_data.get('broadcast_group_id'):
        await handle_broadcast_input(update, context)
        return
    if context.user_data.get('warning_step') and context.user_data.get('warning_group_id'):
        await handle_warning_input(update, context)
        return
    if context.user_data.get('config_payment_step') and context.user_data.get('config_payment_group_id'):
        await handle_payment_config_input(update, context)
        return
    if context.user_data.get('cfg_field') and context.user_data.get('cfg_group_id'):
        await handle_cfg_input(update, context)
        return
    if update.effective_user.id != SUPER_ADMIN_ID:
        return
    text = update.message.text.strip()
    if text.lower() == 'cancelar':
        await update.message.reply_text("❌ Edición cancelada")
        for key in ('editing_field', 'editing_group_id', 'pending_changes', 'editing_mode'):
            context.user_data.pop(key, None)
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
        context.user_data.setdefault('pending_changes', {})['name'] = text
        await update.message.reply_text(f"✅ *Nombre guardado:* {text}", parse_mode="Markdown")
        context.user_data.pop('editing_field', None)
        await edit_group_multiple(update, context, group_id)
    elif field == 'multi_admin':
        try:
            new_admin = int(text)
            context.user_data.setdefault('pending_changes', {})['admin'] = new_admin
            await update.message.reply_text(f"✅ *Administrador guardado:* `{new_admin}`", parse_mode="Markdown")
            context.user_data.pop('editing_field', None)
            await edit_group_multiple(update, context, group_id)
        except ValueError:
            await update.message.reply_text("❌ El ID debe ser un número")


async def get_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in [SUPER_ADMIN_ID, 8682208062]:
        await update.message.reply_text("❌ No autorizado")
        return
    if len(context.args) < 1:
        await update.message.reply_text("❌ Usa: `/getlink @username` o `/getlink ID`", parse_mode="Markdown")
        return
    identifier = context.args[0].replace("@", "")
    try:
        if identifier.isdigit():
            user_id   = int(identifier)
            chat_link = f"tg://user?id={user_id}"
            await update.message.reply_text(f"🔗 Enlace:\n`{chat_link}`", parse_mode="Markdown")
        else:
            group_id = context.user_data.get('current_group')
            if not group_id:
                await update.message.reply_text("❌ Selecciona un grupo con /start")
                return
            user = await db.get_user_by_username(identifier, group_id)
            if user:
                chat_link    = f"tg://user?id={user['user_id']}"
                name         = user.get('first_name') or user.get('username', identifier)
                await update.message.reply_text(f"🔗 Chat con {name}:\n`{chat_link}`", parse_mode="Markdown")
            else:
                await update.message.reply_text(f"❌ No se encontró @{identifier}")
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")


async def sync_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "ℹ️ *Sincronización manual no disponible*\n\n"
        "Los usuarios se registran automáticamente al *entrar al grupo*.\n"
        "Para registrar uno manualmente: `/add @usuario plan` o `/add ID plan`",
        parse_mode="Markdown"
    )


async def sync_all_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != SUPER_ADMIN_ID:
        await update.message.reply_text("❌ Solo Super Admin")
        return
    msg = "📊 *Estado actual de grupos*\n\n"
    for group in GROUPS:
        users = await db.get_all_active_users(group["group_id"])
        emoji = "👑" if group.get("type", "VIP") == "VIP" else "📋"
        msg  += f"{emoji} *{group['group_name']}*: {len(users)} usuarios activos\n"
    await update.message.reply_text(msg, parse_mode="Markdown")


# ==================== PAGO ====================
async def pay_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id      = update.effective_user.id
    chat_id      = update.effective_chat.id
    target_group = None

    if update.effective_chat.type in ("group", "supergroup"):
        group = get_group_by_id(chat_id)
        if group and group.get("type") == "VIP":
            target_group = chat_id
        else:
            await update.message.reply_text("❌ Este grupo no está configurado como VIP.")
            return
    else:
        current_group = context.user_data.get('current_group')
        if context.args and context.args[0].isdigit():
            target_group = int(context.args[0])
        elif current_group:
            target_group = current_group
        else:
            await update.message.reply_text(
                "❌ *Uso del comando /pagar*\n\n"
                "• En un grupo VIP: escribe `/pagar`\n"
                "• En privado: `/pagar ID_DEL_GRUPO`",
                parse_mode="Markdown"
            )
            return

    group = get_group_by_id(target_group)
    if not group or group.get("type") != "VIP":
        await update.message.reply_text("❌ Grupo no encontrado o no es VIP")
        return

    success = await send_payment_info(context.bot, user_id, target_group, triggered_by="comando /pagar")
    if success:
        if update.effective_chat.type in ("group", "supergroup"):
            await update.message.reply_text(
                "💳 *Te he enviado los datos de pago por privado.*\nRevisa tu chat conmigo.",
                parse_mode="Markdown"
            )
    else:
        await update.message.reply_text(
            "❌ No se pudieron enviar los datos de pago. "
            "Asegúrate de haber iniciado chat privado conmigo primero (/start).",
            parse_mode="Markdown"
        )


async def config_payment_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not context.args:
        await update.message.reply_text(
            "❌ *Uso: `/configpago group_id`*\n\nEjemplo: `/configpago -1001234567890`",
            parse_mode="Markdown"
        )
        return
    try:
        group_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ El ID del grupo debe ser un número")
        return
    if not can_manage_group(user_id, group_id):
        await update.message.reply_text("❌ No autorizado para gestionar este grupo")
        return
    group = get_group_by_id(group_id)
    if not group or group.get("type") != "VIP":
        await update.message.reply_text("❌ Grupo no encontrado o no es VIP")
        return
    context.user_data['config_payment_group_id'] = group_id
    context.user_data['config_payment_step']     = 'bank_data'
    settings     = group.get("settings", {})
    current_bank = settings.get("bank_data", "")
    await update.message.reply_text(
        f"⚙️ *Configuración de Pago — {group['group_name']}*\n\n"
        f"Paso 1/3: *Datos bancarios*\n"
        f"Envía los datos de transferencia bancaria.\n\n"
        f"📋 *Actual:*\n`{current_bank or 'No configurado'}`\n\n"
        f"*Escribe 'saltar' para dejar el valor actual, o 'eliminar' para borrarlo.*",
        parse_mode="Markdown"
    )


async def handle_payment_config_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        return
    step     = context.user_data.get('config_payment_step')
    group_id = context.user_data.get('config_payment_group_id')
    if not step or not group_id:
        return
    if not can_manage_group(update.effective_user.id, group_id):
        return
    text  = update.message.text.strip()
    group = get_group_by_id(group_id)
    if not group:
        await update.message.reply_text("❌ Grupo no encontrado")
        context.user_data.pop('config_payment_step', None)
        context.user_data.pop('config_payment_group_id', None)
        return
    settings = dict(group.get("settings", {}))

    if step == 'bank_data':
        if text.lower() not in ('saltar',):
            if text.lower() == 'eliminar':
                settings.pop('bank_data', None)
            else:
                settings['bank_data'] = text
        group["settings"] = settings
        await db.update_group_fields(group_id, {'settings': settings})
        context.user_data['config_payment_step'] = 'payment_contact'
        current_contact = settings.get("payment_contact", "")
        await update.message.reply_text(
            f"✅ *Datos bancarios guardados.*\n\nPaso 2/3: *Contacto para comprobantes*\n"
            f"Envía el username de Telegram.\n\n"
            f"📋 *Actual:* `{current_contact or 'No configurado'}`\n\n"
            f"*Escribe 'saltar' para dejar el valor actual.*",
            parse_mode="Markdown"
        )
    elif step == 'payment_contact':
        if text.lower() != 'saltar':
            settings['payment_contact'] = text.lstrip('@').strip()
        group["settings"] = settings
        await db.update_group_fields(group_id, {'settings': settings})
        context.user_data['config_payment_step'] = 'paypal_data'
        current_paypal = settings.get("paypal_data", "")
        await update.message.reply_text(
            f"✅ *Contacto guardado.*\n\nPaso 3/3: *PayPal (opcional)*\n"
            f"Envía los datos de PayPal o escribe 'saltar' / 'eliminar'.\n\n"
            f"📋 *Actual:* `{current_paypal or 'No configurado'}`",
            parse_mode="Markdown"
        )
    elif step == 'paypal_data':
        if text.lower() not in ('saltar',):
            if text.lower() == 'eliminar':
                settings.pop('paypal_data', None)
            else:
                settings['paypal_data'] = text
        group["settings"] = settings
        await db.update_group_fields(group_id, {'settings': settings})
        context.user_data.pop('config_payment_step', None)
        context.user_data.pop('config_payment_group_id', None)
        await update.message.reply_text(
            f"✅ *Configuración de pago completada*\n\n"
            f"📌 *Grupo:* {group['group_name']}\n\n"
            f"🏦 *Bancaria:*\n`{settings.get('bank_data', 'No configurado')}`\n\n"
            f"📤 *Contacto:* @{settings.get('payment_contact', 'No configurado')}\n\n"
            f"🅿️ *PayPal:* `{settings.get('paypal_data', 'No configurado')}`",
            parse_mode="Markdown"
        )


async def config_payment_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query
    await query.answer()
    if not can_manage_group(query.from_user.id, group_id):
        await query.edit_message_text("❌ No autorizado")
        return
    group = get_group_by_id(group_id)
    if not group:
        await query.edit_message_text("❌ Grupo no encontrado")
        return
    context.user_data['config_payment_group_id'] = group_id
    context.user_data['config_payment_step']     = 'bank_data'
    settings     = group.get("settings", {})
    current_bank = settings.get("bank_data", "")
    await query.edit_message_text(
        f"⚙️ *Configuración de Pago — {group['group_name']}*\n\n"
        f"Paso 1/3: *Datos bancarios*\n"
        f"Envía los datos de transferencia bancaria.\n\n"
        f"📋 *Actual:*\n`{current_bank or 'No configurado'}`\n\n"
        f"*Escribe 'saltar' para dejar el valor actual, o 'eliminar' para borrarlo.*",
        parse_mode="Markdown"
    )


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data  = query.data
    logger.info(f"🔔 CALLBACK: {data}")
    try:
        if data == "add_user":
            await query.answer()
            await query.edit_message_text("📝 Usa: `/add @username plan` o `/add ID plan`", parse_mode="Markdown")
        elif data == "list_active":
            await query.answer()
            await list_active_users(update, context)
        elif data == "earnings":
            await query.answer()
            await show_earnings(update, context)
        elif data == "export_month":
            await query.answer()
            await export_report(update, context)
        elif data == "list_potential":
            await list_potential_clients(update, context)
        elif data == "export_clients":
            await export_clients(update, context)
        elif data in ("vip_groups", "view_vip_groups"):
            await query.answer()
            await view_vip_groups(update, context)
        elif data in ("free_groups", "view_free_groups"):
            await query.answer()
            await view_free_groups(update, context)
        elif data == "total_earnings":
            await query.answer()
            await total_earnings(update, context)
        elif data in ("all_groups", "groups"):
            await query.answer()
            await list_groups(update, context)
        elif data == "add_group":
            await query.answer()
            await query.edit_message_text(
                "📝 Usa: `/addgroup group_id TIPO \"nombre\" admin_id`", parse_mode="Markdown"
            )
        elif data.startswith("select_group_"):
            group_id = int(data.replace("select_group_", ""))
            await select_group(update, context, group_id)
        elif data == "back_to_admin":
            await query.answer()
            try:
                await query.message.delete()
            except Exception:
                pass
            await start(update, context)
        elif data == "menu_groups":
            await menu_groups(update, context)
        elif data == "menu_view_groups":
            await menu_view_groups(update, context)
        elif data == "menu_edit_group_select":
            await menu_edit_group_select(update, context)
        elif data == "menu_delete_group_select":
            await menu_delete_group_select(update, context)
        elif data.startswith("delete_confirm_"):
            await delete_group_confirm(update, context, int(data.replace("delete_confirm_", "")))
        elif data.startswith("delete_yes_"):
            await delete_group_execute(update, context, int(data.replace("delete_yes_", "")))
        elif data == "menu_commands":
            await menu_commands(update, context)
        elif data.startswith("multi_apply_"):
            await multi_apply_changes(update, context, int(data.replace("multi_apply_", "")))
        elif data.startswith("multi_name_"):
            await multi_name_request(update, context, int(data.replace("multi_name_", "")))
        elif data.startswith("multi_admin_"):
            await multi_admin_request(update, context, int(data.replace("multi_admin_", "")))
        elif data.startswith("multi_type_"):
            await multi_type_request(update, context, int(data.replace("multi_type_", "")))
        elif data.startswith("multi_set_type_"):
            parts = data.split("_")
            await multi_set_type(update, context, int(parts[3]), parts[4])
        elif data.startswith("edit_multiple_"):
            await edit_group_multiple(update, context, int(data.replace("edit_multiple_", "")))
        elif data.startswith("cfg_group_"):
            await menu_group_settings(update, context, int(data.replace("cfg_group_", "")))
        elif data.startswith("cfg_trial_"):
            await cfg_trial_request(update, context, int(data.replace("cfg_trial_", "")))
        elif data.startswith("cfg_price_semanal_"):
            await cfg_price_request(update, context, int(data.replace("cfg_price_semanal_", "")), "semanal")
        elif data.startswith("cfg_price_mensual_"):
            await cfg_price_request(update, context, int(data.replace("cfg_price_mensual_", "")), "mensual")
        elif data.startswith("cfg_dur_semanal_"):
            await cfg_duration_request(update, context, int(data.replace("cfg_dur_semanal_", "")), "semanal")
        elif data.startswith("cfg_dur_mensual_"):
            await cfg_duration_request(update, context, int(data.replace("cfg_dur_mensual_", "")), "mensual")
        elif data == "trial_stats":
            await trial_stats(update, context)
        elif data.startswith("pay_"):
            await query.answer("💳 Enviando datos de pago...")
            parts = data.split("_")
            if len(parts) >= 3:
                pay_group_id = int(parts[1])
                pay_user_id  = int(parts[2])
                if query.from_user.id == pay_user_id:
                    await send_payment_info(context.bot, pay_user_id, pay_group_id, triggered_by="botón Pagar")
                else:
                    await query.answer("❌ Este botón no es para ti", show_alert=True)
            else:
                await query.answer("❌ Error en datos de pago", show_alert=True)
        elif data.startswith("cfg_payment_"):
            await config_payment_callback(update, context, int(data.replace("cfg_payment_", "")))
        elif data.startswith("cfg_warnings_"):
            await menu_warning_settings(update, context, int(data.replace("cfg_warnings_", "")))
        elif data.startswith("warn_toggle_"):
            await toggle_warnings(update, context, int(data.replace("warn_toggle_", "")))
        elif data.startswith("warn_add_"):
            await add_warning_step1(update, context, int(data.replace("warn_add_", "")))
        elif data.startswith("warn_edit_"):
            parts = data.split("_")
            await edit_warning_step1(update, context, int(parts[2]), int(parts[3]))
        elif data.startswith("warn_delete_"):
            parts = data.split("_")
            await delete_warning(update, context, int(parts[2]), int(parts[3]))
        elif data.startswith("warn_test_"):
            await test_warning(update, context, int(data.replace("warn_test_", "")))
        elif data == "broadcast_menu":
            await broadcast_menu_callback(update, context)
        elif data.startswith("bc|"):
            await broadcast_callback_handler(update, context)
        elif data.startswith("spin_"):
            parts = data.split("_")
            if len(parts) >= 3:
                await spin_discount(update, context)
            else:
                await query.answer("❌ Error en datos", show_alert=True)
        elif data == "spin_used":
            await query.answer("🚫 Ya usaste tu ruleta. Si tienes dudas, contacta al admin.", show_alert=True)
            
            # Notificar al admin del intento
            group_id = None
            # Extraer group_id del mensaje anterior si es posible
            # O usar current_group del contexto
            if hasattr(query.message, 'reply_markup') and query.message.reply_markup:
                for row in query.message.reply_markup.inline_keyboard:
                    for btn in row:
                        if btn.callback_data and btn.callback_data.startswith("pay_"):
                            parts = btn.callback_data.split("_")
                            if len(parts) >= 2:
                                group_id = int(parts[1])
                                break
                    if group_id:
                        break
            
            if not group_id:
                group_id = context.user_data.get('current_group')
            
            if group_id:
                group = get_group_by_id(group_id)
                if group:
                    user = query.from_user
                    await _safe_send(
                        group["admin_id"],
                        f"⚠️ *Intento de ruleta repetida (botón usado)*\n\n"
                        f"👤 [{user.first_name or 'Usuario'}](tg://user?id={user.id})\n"
                        f"🆔 `{user.id}` | 📌 {group['group_name']}\n\n"
                        f"Este usuario presionó el botón 'Ruleta ya usada'.",
                        parse_mode="Markdown"
                    )
        else:
            await query.answer()
            logger.warning(f"Callback desconocido: {data}")
    except Exception as e:
        logger.error(f"❌ Error en callback '{data}': {e}", exc_info=True)
        try:
            await query.answer("❌ Ocurrió un error, intenta de nuevo", show_alert=True)
        except Exception:
            pass

# ==================== RULETA VIP ====================
async def spin_discount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    parts = query.data.split("_")
    if len(parts) < 3:
        await query.answer("❌ Error en datos", show_alert=True)
        return
    
    group_id = int(parts[1])
    user_id = query.from_user.id
    
    group = get_group_by_id(group_id)
    if not group or group.get("type", "VIP") != "VIP":
        await query.answer("❌ Grupo no válido", show_alert=True)
        return
    
    # ===== VERIFICACIÓN: ¿Ya existe registro? =====
    spin_data = await db.get_spin_data(user_id, group_id)
    
    # CASO 1: Ya tiene spin (cualquier estado) → BLOQUEADO
    if spin_data.get("spin_count", 0) >= 1:
        best = spin_data.get("best_discount", 0)
        
        # NOTIFICAR AL ADMIN del intento
        chat_link = f"tg://user?id={user_id}"
        await _safe_send(
            group["admin_id"],
            f"⚠️ *Intento de ruleta repetida*\n\n"
            f"👤 Usuario: [{query.from_user.first_name or 'Usuario'}]({chat_link})\n"
            f"🆔 ID: `{user_id}`\n"
            f"📌 Grupo: {group['group_name']}\n\n"
            f"🎰 Este usuario ya giró la ruleta ({best}% OFF).\n"
            f"{'✅ Descuento ya fue aplicado.' if spin_data.get('used') else '⏳ Descuento aún pendiente.'}\n\n"
            f"Verifica con: `/checkspin {user_id}`",
            parse_mode="Markdown"
        )
        
        if spin_data.get("used"):
            await query.edit_message_text(
                f"🎰 *Ruleta VIP — {group['group_name']}*\n\n"
                f"Ya usaste tu spin y tu descuento del *{best}%* fue aplicado.\n\n"
                f"🚫 *La ruleta es 1 vez por usuario.*\n"
                f"Para renovar, paga al precio normal:\n\n"
                f"• 📆 Mensual: {fmt_price(get_group_plan_config(group_id, 'mensual')['price'])}\n"
                f"• 📅 Semanal: {fmt_price(get_group_plan_config(group_id, 'semanal')['price'])}\n\n"
                f"💳 Presiona el botón para ver datos de pago:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("💳 Pagar Acceso", callback_data=f"pay_{group_id}_{user_id}")]
                ]),
                parse_mode="Markdown"
            )
        else:
            await query.edit_message_text(
                f"🎰 *Ruleta VIP — {group['group_name']}*\n\n"
                f"Ya giraste la ruleta. Tu descuento es: *{best}% OFF*\n\n"
                f"⏳ ¿Aún no lo usaste? Contacta a @{group.get('settings', {}).get('payment_contact', 'admin')}\n\n"
                f"💳 Presiona para pagar con tu descuento:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("💳 Pagar con Descuento", callback_data=f"pay_{group_id}_{user_id}")]
                ]),
                parse_mode="Markdown"
            )
        return
    
    # === PRIMER SPIN ===
    import random
    discounts = [10]*50 + [15]*30 + [20]*15 + [25]*3 + [30]*1 + [50]*1
    discount = random.choice(discounts)
    
    await db.record_spin(user_id, group_id, discount)
    
    cfg_m = get_group_plan_config(group_id, "mensual")
    cfg_s = get_group_plan_config(group_id, "semanal")
    price_m = cfg_m['price'] * (1 - discount/100)
    price_s = cfg_s['price'] * (1 - discount/100)
    
    # Animación de la ruleta
    for msg in ["🎰 Girando...", "🎰 Girando... 🎲", "🎰 Girando... 🎲 🎲"]:
        try:
            await query.edit_message_text(msg)
            await asyncio.sleep(0.8)
        except:
            pass
    
    await query.edit_message_text(
        f"🎰 *¡RULETA VIP — {group['group_name']}*\n\n"
        f"@{query.from_user.username or query.from_user.first_name} giró la ruleta...\n\n"
        f"🎉 *¡FELICIDADES!*\n\n"
        f"🏆 *Descuento: {discount}% OFF*\n\n"
        f"💰 *Precios con tu descuento:*\n"
        f"• 📆 Mensual: ~~{fmt_price(cfg_m['price'])}~~ → *{fmt_price(price_m)}*\n"
        f"• 📅 Semanal: ~~{fmt_price(cfg_s['price'])}~~ → *{fmt_price(price_s)}*\n\n"
        f"⏳ *Válido por 24 horas*\n"
        f"📤 Envía el comprobante + captura de este mensaje a:\n"
        f"@{group.get('settings', {}).get('payment_contact', 'admin')}\n\n"
        f"_Solo puedes usar la ruleta 1 vez. ¡No lo pierdas!_",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("💳 Pagar Mensual con Descuento", callback_data=f"pay_{group_id}_{user_id}")],
            [InlineKeyboardButton("📅 Pagar Semanal con Descuento", callback_data=f"pay_{group_id}_{user_id}")]
        ]),
        parse_mode="Markdown"
    )
    
# ==================== SISTEMA DE AVISOS ====================
async def send_trial_warnings():
    logger.info("⏰ Enviando avisos de expiración de trial...")
    for group in GROUPS:
        if group.get("type", "VIP") != "VIP":
            continue
        group_id = group["group_id"]
        try:
            warning_config = await db.get_warning_config(group_id)
        except Exception as e:
            logger.error(f"❌ Error obteniendo warning_config para grupo {group_id}: {e}")
            continue
        if not warning_config or not warning_config.get("enabled", False):
            continue
        for warning in warning_config.get("warnings", []):
            minutes_before    = warning.get("minutes_before", 15)
            message_template  = warning.get("message", "⏳ Tu trial expira pronto")
            try:
                users = await db.get_users_for_warning(group_id, minutes_before)
            except Exception as e:
                logger.error(f"❌ Error obteniendo usuarios para aviso en grupo {group_id}: {e}")
                continue
            if not users:
                continue
            warning_type = f"warning_{minutes_before}min"
            for user in users:
                user_id  = user['user_id']
                end_date = user['end_date']
                remaining = end_date - datetime.utcnow()
                mins_left = max(0, int(remaining.total_seconds() / 60))
                if mins_left <= 0:
                    continue
                expiry_str = fmt_dt(end_date)
                expiry_time = fmt_dt(end_date).split(' ')[1] if ' ' in fmt_dt(end_date) else fmt_dt(end_date)
                message = (message_template
                           .replace("{minutes_left}", str(mins_left))
                           .replace("{expiry_time}", expiry_time)
                           .replace("{expiry_datetime}", expiry_str)
                           .replace("{group_name}", group.get("group_name", "VIP"))
                           .replace("{user_name}", user.get("first_name", "Usuario") or "Usuario"))
                try:
                    await bot_app.bot.send_message(
                        user_id, message,
                        reply_markup=get_payment_keyboard(group_id, user_id),
                        parse_mode="Markdown"
                    )
                    await db.log_warning_sent(user_id, group_id, warning_type)
                    logger.info(f"⚠️ Aviso enviado a {user_id}: {mins_left} min restantes")
                except Exception as e:
                    logger.warning(f"No se pudo enviar aviso a {user_id}: {e}")


async def menu_warning_settings(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query
    await query.answer()
    group = get_group_by_id(group_id)
    if not group or not can_manage_group(query.from_user.id, group_id):
        await query.edit_message_text("❌ No autorizado o grupo no encontrado")
        return
    warning_config = await db.get_warning_config(group_id)
    enabled  = warning_config.get("enabled", False)
    warnings = warning_config.get("warnings", [])
    status_emoji = "🟢" if enabled else "🔴"
    status_text  = "ACTIVADOS" if enabled else "DESACTIVADOS"
    msg = (f"🔔 *Configuración de Avisos — {group['group_name']}*\n\n"
           f"Estado: {status_emoji} *{status_text}*\n\n"
           f"*Avisos configurados ({len(warnings)}):*\n")
    for i, w in enumerate(warnings, 1):
        msg += f"\n{i}. ⏰ {w.get('minutes_before', 15)} minutos antes de expirar"
    if not warnings:
        msg += "\n_Ninguno configurado_"
    msg += ("\n\n*Variables en mensajes:*\n"
            "• `{minutes_left}` — minutos restantes\n"
            "• `{expiry_time}` — hora de expiración (EC)\n"
            "• `{expiry_datetime}` — fecha y hora (EC)\n"
            "• `{group_name}` — nombre del grupo\n"
            "• `{user_name}` — nombre del usuario")
    keyboard = [
        [InlineKeyboardButton(f"{status_emoji} {'Desactivar' if enabled else 'Activar'} avisos",
                              callback_data=f"warn_toggle_{group_id}")],
        [InlineKeyboardButton("➕ Añadir aviso", callback_data=f"warn_add_{group_id}")],
    ]
    for i, w in enumerate(warnings):
        keyboard.append([
            InlineKeyboardButton(f"✏️ Editar {w.get('minutes_before', 15)}min", callback_data=f"warn_edit_{group_id}_{i}"),
            InlineKeyboardButton("🗑️ Eliminar", callback_data=f"warn_delete_{group_id}_{i}")
        ])
    keyboard.append([InlineKeyboardButton("🧪 Probar aviso", callback_data=f"warn_test_{group_id}")])
    keyboard.append([InlineKeyboardButton("🔙 Volver", callback_data=f"cfg_group_{group_id}")])
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


async def toggle_warnings(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query
    await query.answer()
    if not can_manage_group(query.from_user.id, group_id):
        await query.answer("❌ No autorizado", show_alert=True)
        return
    warning_config = await db.get_warning_config(group_id)
    warning_config["enabled"] = not warning_config.get("enabled", False)
    await db.save_warning_config(group_id, warning_config)
    await menu_warning_settings(update, context, group_id)


async def add_warning_step1(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query
    await query.answer()
    if not can_manage_group(query.from_user.id, group_id):
        await query.answer("❌ No autorizado", show_alert=True)
        return
    context.user_data.update({'warning_group_id': group_id, 'warning_step': 'minutes', 'warning_index': None})
    await query.edit_message_text(
        "➕ *Nuevo aviso de expiración*\n\n"
        "Envía cuántos minutos antes debe enviarse el aviso.\n\n"
        "*Ejemplos:* `15`, `60`, `5`\n\n*Escribe 'cancelar' para cancelar.*",
        parse_mode="Markdown"
    )


async def edit_warning_step1(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int, warning_index: int):
    query = update.callback_query
    await query.answer()
    if not can_manage_group(query.from_user.id, group_id):
        await query.answer("❌ No autorizado", show_alert=True)
        return
    warning_config = await db.get_warning_config(group_id)
    warnings = warning_config.get("warnings", [])
    if warning_index >= len(warnings):
        await query.edit_message_text("❌ Aviso no encontrado")
        return
    existing = warnings[warning_index]
    context.user_data.update({'warning_group_id': group_id, 'warning_step': 'edit_minutes', 'warning_index': warning_index})
    await query.edit_message_text(
        f"✏️ *Editar aviso*\n\nActual: {existing.get('minutes_before', 15)} minutos antes\n\n"
        f"Envía los nuevos minutos, o escribe 'saltar'.\n\n*Escribe 'cancelar' para cancelar.*",
        parse_mode="Markdown"
    )


async def delete_warning(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int, warning_index: int):
    query = update.callback_query
    await query.answer()
    if not can_manage_group(query.from_user.id, group_id):
        await query.answer("❌ No autorizado", show_alert=True)
        return
    warning_config = await db.get_warning_config(group_id)
    warnings = warning_config.get("warnings", [])
    if warning_index < len(warnings):
        removed = warnings.pop(warning_index)
        warning_config["warnings"] = warnings
        await db.save_warning_config(group_id, warning_config)
        await query.edit_message_text(f"✅ Aviso eliminado ({removed.get('minutes_before', 15)} min)")
        await asyncio.sleep(1)
    await menu_warning_settings(update, context, group_id)


async def test_warning(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query
    await query.answer()
    if not can_manage_group(query.from_user.id, group_id):
        await query.answer("❌ No autorizado", show_alert=True)
        return
    warning_config = await db.get_warning_config(group_id)
    warnings = warning_config.get("warnings", [])
    if not warnings:
        await query.answer("❌ No hay avisos configurados", show_alert=True)
        return
    test_w   = warnings[0]
    msg_tpl  = test_w.get("message", "⏳ Tu trial expira pronto")
    now_ec   = now_ec() if callable(globals().get('now_ec')) else datetime.now()
    group    = get_group_by_id(group_id)
    message  = (msg_tpl.replace("{minutes_left}", "15")
                .replace("{expiry_time}", "22:45")
                .replace("{expiry_datetime}", "15/06/2026 22:45")
                .replace("{group_name}", group.get("group_name", "VIP") if group else "VIP")
                .replace("{user_name}", query.from_user.first_name or "Usuario"))
    try:
        await context.bot.send_message(
            query.from_user.id,
            f"🧪 *AVISO DE PRUEBA*\n\n---\n{message}\n---\n\nSi te gusta, los avisos están listos.",
            reply_markup=get_payment_keyboard(group_id, query.from_user.id),
            parse_mode="Markdown"
        )
        await query.edit_message_text("✅ *Aviso de prueba enviado a tu chat privado*", parse_mode="Markdown")
    except Exception as e:
        logger.warning(f"Error enviando prueba: {e}")
        await query.edit_message_text("❌ Error enviando prueba. Inicia chat privado con el bot primero.")
    await asyncio.sleep(2)
    await menu_warning_settings(update, context, group_id)


WARN_MESSAGE_PROMPT = (
    "*Variables disponibles:*\n"
    "• `{minutes_left}` — minutos restantes\n"
    "• `{expiry_time}` — hora de expiración (EC)\n"
    "• `{expiry_datetime}` — fecha y hora (EC)\n"
    "• `{group_name}` — nombre del grupo\n"
    "• `{user_name}` — nombre del usuario\n\n"
    "*Escribe 'cancelar' para cancelar.*"
)

async def handle_warning_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        return
    step          = context.user_data.get('warning_step')
    group_id      = context.user_data.get('warning_group_id')
    warning_index = context.user_data.get('warning_index')
    if not step or not group_id:
        return
    if not can_manage_group(update.effective_user.id, group_id):
        return
    text = update.message.text.strip()
    if text.lower() == 'cancelar':
        for k in ('warning_step', 'warning_group_id', 'warning_index', 'warning_minutes'):
            context.user_data.pop(k, None)
        await update.message.reply_text("❌ Configuración de aviso cancelada")
        return
    warning_config = await db.get_warning_config(group_id)
    warnings       = warning_config.get("warnings", [])

    if step in ('minutes', 'edit_minutes'):
        if text.lower() == 'saltar' and step == 'edit_minutes':
            context.user_data['warning_step'] = 'edit_message'
            existing = warnings[warning_index] if warning_index is not None and warning_index < len(warnings) else {}
            await update.message.reply_text(
                f"✏️ *Editar mensaje*\n\nActual:\n`{existing.get('message', '')}`\n\n"
                f"Envía el nuevo mensaje, o 'saltar'.\n\n{WARN_MESSAGE_PROMPT}",
                parse_mode="Markdown"
            )
            return
        try:
            minutes = int(text)
            if not (1 <= minutes <= 1440):
                raise ValueError
        except ValueError:
            await update.message.reply_text("❌ Debe ser un número entre 1 y 1440.")
            return
        context.user_data['warning_minutes'] = minutes
        if step == 'edit_minutes':
            if warning_index is not None and warning_index < len(warnings):
                warnings[warning_index]["minutes_before"] = minutes
                warning_config["warnings"] = warnings
                await db.save_warning_config(group_id, warning_config)
            context.user_data['warning_step'] = 'edit_message'
            existing = warnings[warning_index] if warning_index is not None and warning_index < len(warnings) else {}
            await update.message.reply_text(
                f"✏️ *Editar mensaje*\n\nActual:\n`{existing.get('message', '')}`\n\n"
                f"Envía el nuevo mensaje, o 'saltar'.\n\n{WARN_MESSAGE_PROMPT}",
                parse_mode="Markdown"
            )
        else:
            context.user_data['warning_step'] = 'message'
            await update.message.reply_text(
                f"✅ Aviso a los *{minutes} minutos* configurado.\n\nAhora envía el mensaje para el usuario.\n\n{WARN_MESSAGE_PROMPT}",
                parse_mode="Markdown"
            )

    elif step in ('message', 'edit_message'):
        if text.lower() == 'saltar' and step == 'edit_message':
            pass  # Mantener mensaje actual
        else:
            msg_text = update.message.text.strip()
            if step == 'edit_message' and warning_index is not None and warning_index < len(warnings):
                warnings[warning_index]["message"] = msg_text
            else:
                minutes = context.user_data.get('warning_minutes', 15)
                warnings.append({"minutes_before": minutes, "message": msg_text})
                warnings.sort(key=lambda w: w.get("minutes_before", 0), reverse=True)
            warning_config["warnings"] = warnings
            await db.save_warning_config(group_id, warning_config)

        for k in ('warning_step', 'warning_group_id', 'warning_index', 'warning_minutes'):
            context.user_data.pop(k, None)
        keyboard = [[InlineKeyboardButton("🔙 Volver a avisos", callback_data=f"cfg_warnings_{group_id}")]]
        await update.message.reply_text("✅ *Aviso guardado correctamente*",
                                        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


# ==================== BROADCAST ====================
async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not context.args:
        await update.message.reply_text(
            "❌ *Uso: `/broadcast ID_DEL_GRUPO`*\n\nEjemplo: `/broadcast -1001234567890`",
            parse_mode="Markdown"
        )
        return
    try:
        group_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ El ID del grupo debe ser un número")
        return
    if not can_manage_group(user_id, group_id):
        await update.message.reply_text("❌ No autorizado")
        return
    group = get_group_by_id(group_id)
    if not group:
        await update.message.reply_text("❌ Grupo no encontrado")
        return
    context.user_data.update({'broadcast_group_id': group_id, 'broadcast_step': 'filter'})
    keyboard = [
        [InlineKeyboardButton("🆓 Solo trial SIN compra",       callback_data=f"bc|filter|{group_id}|trial_only")],
        [InlineKeyboardButton("💳 Solo con suscripción activa", callback_data=f"bc|filter|{group_id}|subscribed_only")],
        [InlineKeyboardButton("⏰ Solo expirados no renovados", callback_data=f"bc|filter|{group_id}|expired_only")],
        [InlineKeyboardButton("👥 TODOS los que usaron trial",  callback_data=f"bc|filter|{group_id}|all_trial")],
        [InlineKeyboardButton("❌ Cancelar",                    callback_data=f"bc|cancel|{group_id}")]
    ]
    await update.message.reply_text(
        f"📢 *Broadcast Masivo — {group['group_name']}*\n\nSelecciona a quién enviar el mensaje:",
        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
    )


async def broadcast_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data  = query.data
    await query.answer()

    # NUEVO PARSING ROBUSTO CON | COMO SEPARADOR
    if data.startswith("bc|"):
        parts = data.split("|")
        # parts[0] = "bc", parts[1] = acción, parts[2] = group_id, parts[3+] = resto
        if len(parts) < 3:
            await query.answer("❌ Error en datos", show_alert=True)
            return
            
        action = parts[1]
        
        try:
            group_id = int(parts[2])
        except ValueError:
            await query.answer("❌ ID de grupo inválido", show_alert=True)
            return
            
        # ---------- FILTRAR ----------
        if action == "filter":
            if len(parts) < 4:
                await query.answer("❌ Filtro no especificado", show_alert=True)
                return
                
            filter_type = parts[3]
            valid_filters = ['trial_only', 'subscribed_only', 'expired_only', 'all_trial']
            
            if filter_type not in valid_filters:
                await query.answer(f"❌ Filtro inválido: {filter_type}", show_alert=True)
                return
                
            context.user_data.update({
                'broadcast_group_id': group_id, 
                'broadcast_filter': filter_type,
                'broadcast_step': 'message'
            })
            
            targets = await db.get_broadcast_targets(group_id, filter_type)
            filter_names = {
                'trial_only': '🆓 Solo trial sin compra', 
                'subscribed_only': '💳 Solo con suscripción activa',
                'expired_only': '⏰ Solo expirados no renovados', 
                'all_trial': '👥 Todos los que usaron trial'
            }
            
            await query.edit_message_text(
                f"📢 *Broadcast — Configurar mensaje*\n\n"
                f"Filtro: {filter_names.get(filter_type, filter_type)}\n"
                f"Destinatarios estimados: *{len(targets)}* usuarios\n\n"
                f"Envía el mensaje a enviar.\n\n"
                f"*Variables:* `{{user_name}}`, `{{group_name}}`, `{{username}}`\n\n"
                f"*Escribe 'cancelar' para cancelar.*",
                parse_mode="Markdown"
            )
            
        # ---------- CANCELAR ----------
        elif action == "cancel":
            for k in ('broadcast_group_id', 'broadcast_step', 'broadcast_filter', 'broadcast_message'):
                context.user_data.pop(k, None)
            await query.edit_message_text("❌ Broadcast cancelado")
            
        # ---------- CONFIRMAR ----------
        elif action == "confirm":
            if len(parts) < 4:
                await query.answer("❌ Filtro no especificado", show_alert=True)
                return
                
            filter_type = parts[3]
            message_text = context.user_data.get('broadcast_message', '')
            
            if not message_text:
                await query.edit_message_text("❌ Error: No se encontró el mensaje")
                return
                
            await query.edit_message_text(
                "🚀 *Enviando broadcast...*\n\nEsto puede tomar varios minutos.\nTe notificaré cuando termine.",
                parse_mode="Markdown"
            )
            await execute_broadcast(context.bot, group_id, filter_type, message_text, query.from_user.id)
            
        # ---------- HISTORIAL ----------
        elif action == "history":
            await broadcast_history_callback(update, context, group_id)

        # ---------- VOLVER AL MENÚ ----------
        elif action == "menu":
            context.user_data['current_group'] = group_id
            await broadcast_menu_callback(update, context)
            
        else:
            await query.answer("❌ Acción desconocida", show_alert=True)

async def broadcast_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    group_id = context.user_data.get('current_group')
    if not group_id:
        await query.edit_message_text("❌ Selecciona un grupo primero")
        return
    group = get_group_by_id(group_id)
    if not group:
        await query.edit_message_text("❌ Grupo no encontrado")
        return
    trial_only  = await db.get_broadcast_targets(group_id, 'trial_only')
    subscribed  = await db.get_broadcast_targets(group_id, 'subscribed_only')
    expired     = await db.get_broadcast_targets(group_id, 'expired_only')
    all_trial   = await db.get_broadcast_targets(group_id, 'all_trial')
    
    keyboard = [
        [InlineKeyboardButton(f"🆓 Solo sin compra ({len(trial_only)})",   callback_data=f"bc|filter|{group_id}|trial_only")],
        [InlineKeyboardButton(f"💳 Solo suscriptos ({len(subscribed)})",   callback_data=f"bc|filter|{group_id}|subscribed_only")],
        [InlineKeyboardButton(f"⏰ Solo expirados ({len(expired)})",       callback_data=f"bc|filter|{group_id}|expired_only")],
        [InlineKeyboardButton(f"👥 Todos ({len(all_trial)})",              callback_data=f"bc|filter|{group_id}|all_trial")],
        [InlineKeyboardButton("📜 Historial",                              callback_data=f"bc|history|{group_id}")],
        [InlineKeyboardButton("🔙 Volver",                                 callback_data=f"select_group_{group_id}")]
    ]
    await query.edit_message_text(
        f"📢 *Broadcast Masivo — {group['group_name']}*\n\n"
        f"📊 *Destinatarios disponibles:*\n"
        f"• 🆓 Trial sin compra: *{len(trial_only)}*\n"
        f"• 💳 Con suscripción activa: *{len(subscribed)}*\n"
        f"• ⏰ Expirados no renovados: *{len(expired)}*\n"
        f"• 👥 Total que usaron trial: *{len(all_trial)}*\n\n"
        f"Selecciona el filtro:",
        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
    )

async def broadcast_history_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int = None):
    query = update.callback_query
    await query.answer()
    
    # Si viene de callback antiguo, extraer group_id
    if group_id is None:
        # Formato antiguo: bc_history_-1003807276197
        data = query.data
        if data.startswith("bc_history_"):
            try:
                group_id = int(data[len("bc_history_"):])
            except ValueError:
                await query.edit_message_text("❌ Error en ID de grupo")
                return
        else:
            await query.edit_message_text("❌ Error en callback")
            return
            
    history = await db.get_broadcast_history(group_id, limit=10)
    if not history:
        msg = "📭 *No hay broadcasts enviados aún*"
    else:
        filter_names = {
            'trial_only': '🆓 Sin compra', 
            'subscribed_only': '💳 Suscriptos',
            'expired_only': '⏰ Expirados', 
            'all_trial': '👥 Todos'
        }
        msg = "📜 *Historial de Broadcasts*\n\n"
        for i, h in enumerate(history, 1):
            f_name   = filter_names.get(h['filter_type'], h['filter_type'])
            date_str = fmt_dt(h['sent_at'])
            msg += (f"{i}. *{f_name}* | {date_str}\n"
                    f"   ✅ {h['success_count']} enviados | ❌ {h['fail_count']} fallidos\n"
                    f"   📝 {h['message_preview'] or 'Sin preview'}...\n\n")
    
    # Volver al menú de broadcast del grupo actual
    keyboard = [[InlineKeyboardButton("🔙 Volver", callback_data=f"bc|menu|{group_id}")]]
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


async def handle_broadcast_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        return
    step        = context.user_data.get('broadcast_step')
    group_id    = context.user_data.get('broadcast_group_id')
    filter_type = context.user_data.get('broadcast_filter')
    if step != 'message' or not group_id or not filter_type:
        return
    text = update.message.text.strip()
    if text.lower() == 'cancelar':
        for k in ('broadcast_group_id', 'broadcast_step', 'broadcast_filter'):
            context.user_data.pop(k, None)
        await update.message.reply_text("❌ Broadcast cancelado")
        return
    context.user_data['broadcast_message'] = text
    context.user_data['broadcast_step']    = 'confirm'
    targets     = await db.get_broadcast_targets(group_id, filter_type)
    preview_msg = (text.replace("{user_name}", "Juan")
                       .replace("{group_name}", "VIP")
                       .replace("{username}", "@juan"))
    filter_names = {
        'trial_only': '🆓 Solo trial sin compra', 
        'subscribed_only': '💳 Solo con suscripción activa',
        'expired_only': '⏰ Solo expirados no renovados', 
        'all_trial': '👥 Todos los que usaron trial'
    }
    
    # NUEVOS CALLBACKS CON | COMO SEPARADOR
    keyboard = [
        [InlineKeyboardButton("✅ Sí, enviar ahora", callback_data=f"bc|confirm|{group_id}|{filter_type}")],
        [InlineKeyboardButton("✏️ Editar mensaje",   callback_data=f"bc|filter|{group_id}|{filter_type}")],
        [InlineKeyboardButton("❌ Cancelar",          callback_data=f"bc|cancel|{group_id}")]
    ]
    await update.message.reply_text(
        f"📢 *Confirmar Broadcast*\n\n"
        f"Filtro: {filter_names.get(filter_type)}\n"
        f"Destinatarios: *{len(targets)}* usuarios\n\n"
        f"*Vista previa:*\n---\n{preview_msg}\n---\n\n"
        f"⚠️ ¿Estás seguro?",
        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
    )


async def execute_broadcast(bot, group_id: int, filter_type: str, message_text: str, sent_by: int):
    targets = await db.get_broadcast_targets(group_id, filter_type)
    logger.info(f"DEBUG execute_broadcast START: targets={len(targets)}, filter={filter_type}")
    
    if not targets:
        # Usar bot.send_message directamente o el _safe_send arreglado
        await bot.send_message(
            sent_by,
            "📭 *Broadcast completado*\n\nNo hay usuarios con el filtro seleccionado.",
            parse_mode="Markdown"
        )
        return
    
    group      = get_group_by_id(group_id)
    group_name = group.get('group_name', 'VIP') if group else 'VIP'
    total      = len(targets)
    success_count = 0
    fail_count    = 0
    delay = 0.12  # ~8 msg/segundo

    # Enviar mensaje de progreso INICIAL y GUARDAR el objeto Message
    try:
        progress_msg = await bot.send_message(
            sent_by,
            f"🚀 *Broadcast en progreso...*\n\n• Total: {total} usuarios\n• Enviados: 0\n• Fallidos: 0",
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"❌ No se pudo enviar mensaje de progreso: {e}")
        progress_msg = None

    last_update = datetime.utcnow()

    for i, user in enumerate(targets, 1):
        user_id    = user['user_id']
        first_name = user.get('first_name', 'Usuario') or 'Usuario'
        username   = user.get('username', '') or ''
        personalized = (message_text
                        .replace("{user_name}", first_name)
                        .replace("{group_name}", group_name)
                        .replace("{username}", f"@{username}" if username and not username.startswith('user_') else first_name))
        try:
            await bot.send_message(user_id, personalized, parse_mode="Markdown", disable_web_page_preview=True)
            success_count += 1
        except Exception as e:
            fail_count += 1
            logger.warning(f"❌ Fallo broadcast a {user_id}: {e}")
        
        await asyncio.sleep(delay)
        
        # Actualizar progreso cada 5 segundos o cada 10 mensajes
        now = datetime.utcnow()
        if progress_msg and ((now - last_update).seconds >= 5 or i % 10 == 0):
            try:
                await bot.edit_message_text(
                    f"🚀 *Broadcast en progreso...*\n\n"
                    f"• Total: {total}\n• Enviados: {success_count}\n"
                    f"• Fallidos: {fail_count}\n• Progreso: {int(i/total*100)}%",
                    chat_id=sent_by,
                    message_id=progress_msg.message_id,
                    parse_mode="Markdown"
                )
                last_update = now
            except Exception as e:
                logger.warning(f"⚠️ No se pudo actualizar progreso: {e}")
                # Si falla la edición, intentar enviar nuevo mensaje
                try:
                    progress_msg = await bot.send_message(
                        sent_by,
                        f"🚀 *Broadcast en progreso...* {int(i/total*100)}%\n"
                        f"• Enviados: {success_count} | Fallidos: {fail_count}",
                        parse_mode="Markdown"
                    )
                    last_update = now
                except Exception:
                    pass

    # Guardar log en base de datos
    await db.log_broadcast_sent(group_id, filter_type, message_text, total, success_count, fail_count, sent_by)
    
    # Mensaje FINAL de completado
    try:
        # Editar el mensaje de progreso a mensaje final
        if progress_msg:
            await bot.edit_message_text(
                f"✅ *Broadcast completado*\n\n"
                f"• Total: {total}\n• ✅ Enviados: {success_count}\n• ❌ Fallidos: {fail_count}\n"
                f"• 📈 Éxito: {(success_count/total*100):.1f}%\n\n"
                f"Filtro: {filter_type} | Grupo: {group_name}",
                chat_id=sent_by,
                message_id=progress_msg.message_id,
                parse_mode="Markdown"
            )
        else:
            await bot.send_message(
                sent_by,
                f"✅ *Broadcast completado*\n\n"
                f"• Total: {total}\n• ✅ Enviados: {success_count}\n• ❌ Fallidos: {fail_count}\n"
                f"• 📈 Éxito: {(success_count/total*100):.1f}%\n\n"
                f"Filtro: {filter_type} | Grupo: {group_name}",
                parse_mode="Markdown"
            )
    except Exception as e:
        logger.error(f"❌ Error enviando mensaje final: {e}")
        # Fallback: intentar enviar de cualquier forma
        try:
            await bot.send_message(
                sent_by,
                f"✅ *Broadcast completado* — {success_count}/{total} enviados",
                parse_mode="Markdown"
            )
        except Exception as e2:
            logger.error(f"❌ Fallback también falló: {e2}")


# ==================== TAREAS PROGRAMADAS ====================
async def check_expired_subscriptions():
    logger.info("🔍 Verificando suscripciones expiradas...")

    async def _process_group(group: dict):
        group_id      = group["group_id"]
        expired_users = await db.get_expired_users(group_id)
        if not expired_users:
            return
        logger.info(f"Usuarios expirados en {group['group_name']}: {len(expired_users)}")
        for user in expired_users:
            user_id    = user['user_id']
            username   = user.get('username') or ''
            first_name = user.get('first_name') or ''
            display    = (first_name or
                          (f"@{username}" if username and not username.startswith('user_') else f"ID:{user_id}"))
            plan       = user.get('plan', 'desconocido')
            end_date   = user.get('end_date')
            expiry_str = fmt_dt(end_date) if end_date else "N/A"
            chat_link  = f"tg://user?id={user_id}"

            was_updated = await db.expire_user(user_id, group_id)
            if not was_updated:
                continue

            kick_success = await _kick_user_with_retry(group_id, user_id)
            status_emoji = "🚫" if kick_success else "⚠️"
            action_text  = ("expulsado del grupo" if kick_success
                            else "marcado como expirado (expulsión fallida — acción manual requerida)")

            await _safe_send(
                group["admin_id"],
                f"{status_emoji} *Suscripción expirada*\n\n"
                f"👤 *Usuario:* [{display}]({chat_link})\n"
                f"📋 *Plan:* {plan}\n"
                f"📅 *Expiró:* {expiry_str} (hora Ecuador)\n"
                f"📌 *Grupo:* {group['group_name']}\n\n"
                f"✅ Usuario {action_text}.",
                parse_mode="Markdown"
            )
            await _safe_send(
                user_id,
                f"⏰ *Tu acceso ha expirado*\n\n"
                f"Tu plan *{plan}* en *{group['group_name']}* ha vencido.\n\n"
                f"💳 ¿Quieres renovar? Presiona el botón:",
                reply_markup=get_payment_keyboard(group_id, user_id), parse_mode="Markdown"
            )

    vip_groups = [g for g in GROUPS if g.get("type", "VIP") == "VIP"]
    results    = await asyncio.gather(*[_process_group(g) for g in vip_groups], return_exceptions=True)
    for group, result in zip(vip_groups, results):
        if isinstance(result, Exception):
            logger.error(f"❌ Error procesando grupo {group['group_name']}: {result}", exc_info=result)


# ==================== DIAGNÓSTICO ====================
async def diagnose_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    if update.effective_chat.type in ("group", "supergroup"):
        target_chat = chat_id
    else:
        target_chat = context.user_data.get('current_group')
        if not target_chat:
            await update.message.reply_text("❌ Usa este comando dentro del grupo, o selecciona uno con /start.")
            return
    if not can_manage_group(user_id, target_chat):
        await update.message.reply_text("❌ No autorizado")
        return
    group = get_group_by_id(target_chat)
    if not group:
        await update.message.reply_text(f"❌ El grupo `{target_chat}` no está registrado.", parse_mode="Markdown")
        return
    lines = [f"🔍 *Diagnóstico — {group['group_name']}*\n",
             f"🆔 ID: `{target_chat}`",
             f"📋 Tipo: {group.get('type', 'VIP')}",
             f"👑 Admin registrado: `{group['admin_id']}`\n"]
    try:
        bot_member = await context.bot.get_chat_member(target_chat, context.bot.id)
        status = bot_member.status
        lines.append(f"🤖 *Estado del bot:* `{status}`")
        if status == "administrator":
            lines.append("✅ Bot es administrador")
            can_ban = getattr(bot_member, 'can_restrict_members', False)
            lines.append(f"   • Puede banear: {'✅' if can_ban else '❌ (necesario para expulsión)'}")
        elif status == "member":
            lines.append("⚠️ Bot es miembro normal — expulsión automática no funcionará")
        else:
            lines.append(f"❌ Estado inesperado: {status}")
    except Exception as e:
        lines.append(f"❌ Error al verificar permisos: `{e}`")
    lines += ["", "📡 *Handlers activos:*",
              "• ChatMemberHandler (v20+) ✅", "• NEW_CHAT_MEMBERS (fallback) ✅",
              "• Verificación de expirados: cada 5 min ✅"]
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


# ==================== FIX TRIAL ====================
async def fix_trial_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != SUPER_ADMIN_ID:
        await update.message.reply_text("❌ Solo Super Admin")
        return
    group_id = context.user_data.get('current_group')
    if not group_id and context.args:
        try:
            group_id = int(context.args[0])
        except ValueError:
            pass
    if not group_id:
        await update.message.reply_text("❌ Usa /fixtrial o /fixtrial GROUP_ID")
        return

    def _diag(conn):
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            counts = {}
            for col, q in [
                ('total',         "SELECT COUNT(*) FROM users WHERE group_id=%s"),
                ('trial_true',    "SELECT COUNT(*) FROM users WHERE group_id=%s AND trial_used=TRUE"),
                ('trial_false',   "SELECT COUNT(*) FROM users WHERE group_id=%s AND trial_used=FALSE"),
                ('trial_null',    "SELECT COUNT(*) FROM users WHERE group_id=%s AND trial_used IS NULL"),
                ('plan_trial',    "SELECT COUNT(*) FROM users WHERE group_id=%s AND plan='trial'"),
            ]:
                cur.execute(q, (group_id,))
                counts[col] = cur.fetchone()[0]
            cur.execute("""
                SELECT COUNT(DISTINCT user_id) FROM payments WHERE group_id=%s AND plan='trial'
            """, (group_id,))
            counts['payments_trial'] = cur.fetchone()[0]
            cur.execute("""
                SELECT COUNT(*) FROM users u
                WHERE u.group_id=%s AND (u.trial_used=FALSE OR u.trial_used IS NULL)
                  AND EXISTS (SELECT 1 FROM payments p WHERE p.user_id=u.user_id AND p.group_id=u.group_id AND p.plan='trial')
            """, (group_id,))
            counts['need_fix'] = cur.fetchone()[0]
            return counts

    diag = await db._run(_diag)
    msg  = (f"🔍 *Diagnóstico trial_used — Grupo {group_id}*\n\n"
            f"• Total usuarios: {diag['total']}\n"
            f"• trial_used=TRUE: {diag['trial_true']}\n"
            f"• trial_used=FALSE: {diag['trial_false']}\n"
            f"• trial_used=NULL: {diag['trial_null']}\n"
            f"• plan='trial': {diag['plan_trial']}\n"
            f"• Con pago de trial: {diag['payments_trial']}\n"
            f"• *Necesitan corrección:* {diag['need_fix']}\n\n")

    if diag['need_fix'] > 0:
        def _fix(conn):
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE users u SET trial_used=TRUE, updated_at=NOW()
                    WHERE u.group_id=%s AND (u.trial_used=FALSE OR u.trial_used IS NULL)
                      AND EXISTS (SELECT 1 FROM payments p WHERE p.user_id=u.user_id AND p.group_id=u.group_id AND p.plan='trial')
                """, (group_id,))
                return cur.rowcount
        fixed = await db._run(_fix)
        msg  += f"✅ *Corregidos: {fixed} usuarios*"
    else:
        msg += "✅ No hay usuarios que corregir"
    await update.message.reply_text(msg, parse_mode="Markdown")

# ==================== CHECKSPIN (Ruleta VIP) ====================
async def check_spin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin verifica si un usuario tiene descuento de ruleta."""
    user_id = update.effective_user.id
    current_group = context.user_data.get('current_group')
    
    if not current_group or not can_manage_group(user_id, current_group):
        await update.message.reply_text("❌ No autorizado. Selecciona un grupo primero con /start")
        return
    
    if not context.args:
        await update.message.reply_text(
            "❌ *Uso: `/checkspin @usuario` o `/checkspin ID`*\n\n"
            "Verifica si un usuario tiene descuento de ruleta.",
            parse_mode="Markdown"
        )
        return
    
    identifier = context.args[0].replace("@", "")
    
    if identifier.isdigit():
        target_id = int(identifier)
        user = await db.get_user_by_id_full(target_id, current_group)
    else:
        user = await db.get_user_by_username(identifier, current_group)
        target_id = user['user_id'] if user else None
    
    if not user:
        await update.message.reply_text(f"❌ Usuario no encontrado en este grupo")
        return
    
    spin_data = await db.get_spin_data(target_id, current_group)
    
    if spin_data.get("spin_count", 0) == 0:
        await update.message.reply_text(
            f"👤 *{user.get('first_name', 'Usuario')}*\n\n"
            f"🎰 *No ha usado la ruleta.*\n"
            f"Precio normal aplicable.",
            parse_mode="Markdown"
        )
        return
    
    best = spin_data.get("best_discount", 0)
    used = spin_data.get("used", False)
    
    cfg_m = get_group_plan_config(current_group, "mensual")
    cfg_s = get_group_plan_config(current_group, "semanal")
    
    msg = (
        f"👤 *{user.get('first_name', 'Usuario')}* (`{target_id}`)\n\n"
        f"🎰 *Ruleta:* {'✅ Usado' if used else '⏳ Pendiente'}\n"
        f"🏆 Descuento: *{best}% OFF*\n\n"
    )
    
    if not used:
        price_m = cfg_m['price'] * (1 - best/100)
        price_s = cfg_s['price'] * (1 - best/100)
        msg += (
            f"💰 *Precios con descuento:*\n"
            f"• 📆 Mensual: ~~{fmt_price(cfg_m['price'])}~~ → *{fmt_price(price_m)}*\n"
            f"• 📅 Semanal: ~~{fmt_price(cfg_s['price'])}~~ → *{fmt_price(price_s)}*\n\n"
            f"✅ *Puedes aplicar este descuento al activar con:*\n"
            f"`/add {target_id} mensual {price_m:.2f}`\n"
            f"`/add {target_id} semanal {price_s:.2f}`"
        )
    else:
        msg += (
            f"🚫 *Descuento ya fue aplicado a:* {spin_data.get('applied_to_plan', 'N/A')}\n"
            f"💰 Precio normal para renovación."
        )
    
    await update.message.reply_text(msg, parse_mode="Markdown")

# ==================== MAIN ====================
async def main():
    global bot_app
    await db.init_tables()
    await db.load_groups_from_db()
    logger.info(f"📦 {len(GROUPS)} grupos disponibles")

    bot_app = ApplicationBuilder().token(TOKEN).build()

    # Comandos
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
    bot_app.add_handler(CommandHandler("pagar",       pay_command))
    bot_app.add_handler(CommandHandler("configpago",  config_payment_command))
    bot_app.add_handler(CommandHandler("test",        test))
    bot_app.add_handler(CommandHandler("diaggrupo",   diagnose_group))
    bot_app.add_handler(CommandHandler("broadcast",   broadcast_command))
    bot_app.add_handler(CommandHandler("fixtrial",    fix_trial_command))
    bot_app.add_handler(CommandHandler("checkspin", check_spin_command))

    # Callbacks
    bot_app.add_handler(CallbackQueryHandler(handle_callback))

    # Detección de miembros
    bot_app.add_handler(ChatMemberHandler(track_chat_member, ChatMemberHandler.CHAT_MEMBER))
    bot_app.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, detect_new_member))

    # Mensajes en grupos (detección de usuarios activos)
    bot_app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.ChatType.GROUPS,
        detect_active_member
    ))

    # Input de texto en privado
    bot_app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE,
        handle_edit_input
    ))

    # Scheduler
    scheduler.add_job(check_expired_subscriptions, 'interval', minutes=5,   id='check_expired',  replace_existing=True)
    scheduler.add_job(auto_backup,                 'interval', hours=24,    id='auto_backup',     replace_existing=True)
    scheduler.add_job(send_trial_warnings,         'interval', minutes=1,   id='trial_warnings',  replace_existing=True)
    scheduler.start()

    logger.info("🤖 Bot iniciado")
    await bot_app.initialize()
    await bot_app.start()
    await bot_app.updater.start_polling(
        drop_pending_updates=True,
        allowed_updates=["message", "callback_query", "chat_member", "my_chat_member"]
    )
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
