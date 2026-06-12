import os
import csv
import asyncio
import logging
import re
from io import StringIO
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import json

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import TelegramError, BadRequest
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    ContextTypes, Defaults, MessageHandler, filters, ChatMemberHandler
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
def get_payment_keyboard(group_id: int, user_id: int) -> InlineKeyboardMarkup:
    """Genera el teclado con botón de pago para un grupo específico."""
    keyboard = [
        [InlineKeyboardButton("💳 Pagar Acceso", callback_data="pay_{}_{}".format(group_id, user_id))]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_payment_info_text(group_id: int) -> str:
    """Genera el mensaje completo de información de pago para un grupo."""
    group = get_group_by_id(group_id)
    if not group:
        return "Grupo no encontrado"

    settings = group.get("settings", {})
    cfg_s = get_group_plan_config(group_id, "semanal")
    cfg_m = get_group_plan_config(group_id, "mensual")

    bank_data = settings.get("bank_data", "").strip()
    payment_contact = settings.get("payment_contact", "ZonaEcFuego").strip()
    paypal_data = settings.get("paypal_data", "").strip()

    group_name = group.get("group_name", "VIP")
    semanal_price = fmt_price(cfg_s.get("price", 10))
    mensual_price = fmt_price(cfg_m.get("price", 20))
    semanal_days = cfg_s.get("days", 7)
    mensual_days = cfg_m.get("days", 30)

    lines = []
    lines.append("💰 *INFORMACIÓN DE PAGO — " + group_name + "*")
    lines.append("")
    lines.append("📋 *Planes disponibles:*")
    lines.append("• 📅 Semanal (" + str(semanal_days) + " días): *" + semanal_price + "*")
    lines.append("• 📆 Mensual (" + str(mensual_days) + " días): *" + mensual_price + "*")
    lines.append("")

    if bank_data:
        lines.append("🏦 *Transferencia Bancaria:*")
        lines.append(bank_data)
        lines.append("")

    if paypal_data:
        lines.append("🅿️ *PayPal:*")
        lines.append(paypal_data)
        lines.append("")

    if not bank_data and not paypal_data:
        lines.append("⚠️ *Métodos de pago no configurados aún.*")
        lines.append("Contacta al administrador para más información.")
        lines.append("")

    lines.append("📤 *Después de pagar:*")
    lines.append("Envía el comprobante a @" + payment_contact)
    lines.append("")
    lines.append("⏱ *Tiempo de activación:*")
    lines.append("Tu acceso se activará cuando un administrador valide la transferencia.")
    lines.append("")
    lines.append("🔄 ¿No recibiste los datos? Presiona el botón de nuevo.")

    return chr(10).join(lines)

async def send_payment_info(bot, user_id: int, group_id: int, triggered_by: str = "comando"):
    """Envía la información de pago por privado al usuario."""
    group = get_group_by_id(group_id)
    if not group:
        return False

    try:
        await bot.send_message(
            user_id,
            get_payment_info_text(group_id),
            parse_mode="Markdown",
            reply_markup=get_payment_keyboard(group_id, user_id)
        )

        display = "Usuario " + str(user_id)
        chat_link = "tg://user?id=" + str(user_id)
        group_name = group.get("group_name", "VIP")
        await _safe_send(
            group["admin_id"],
            "💳 *Nuevo lead de pago*" + chr(10) + chr(10) +
            "👤 Usuario: [" + display + "](" + chat_link + ")" + chr(10) +
            "🆔 ID: `" + str(user_id) + "`" + chr(10) +
            "📌 Grupo: " + group_name + chr(10) +
            "🔔 Acción: *" + triggered_by + "*" + chr(10) + chr(10) +
            "_Esperando comprobante en el contacto configurado._",
            parse_mode="Markdown"
        )
        return True
    except Exception as e:
        logger.warning("No se pudo enviar info de pago a " + str(user_id) + ": " + str(e))
        return False


# ==================== BASE DE DATOS ====================
class Database:
    def __init__(self, db_url: str):
        self.db_url = db_url
        self._pool: pool.ThreadedConnectionPool = None

    def _get_pool(self) -> pool.ThreadedConnectionPool:
        if self._pool is None or self._pool.closed:
            self._pool = pool.ThreadedConnectionPool(
                minconn=2,
                maxconn=10,
                dsn=self.db_url
            )
            logger.info("✅ Connection pool creado (min=2, max=10)")
        return self._pool

    def _execute(self, func):
        p = self._get_pool()
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
                """);
                # NUEVA TABLA: warning_logs (registro de avisos enviados)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS warning_logs (
                        id           SERIAL PRIMARY KEY,
                        user_id      BIGINT NOT NULL,
                        group_id     BIGINT NOT NULL,
                        warning_type TEXT NOT NULL,
                        sent_at      TIMESTAMP DEFAULT NOW(),
                        UNIQUE(user_id, group_id, warning_type)
                    )
                """);
                # Índices para warning_logs
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_warning_logs_lookup
                    ON warning_logs(user_id, group_id, warning_type)
                """);
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_warning_logs_group
                    ON warning_logs(group_id, sent_at)
                """);
                # Migraciones seguras
                cur.execute("""
                    DO $$
                    BEGIN
                        IF EXISTS (
                            SELECT 1 FROM information_schema.columns
                            WHERE table_name='payments' AND column_name='amount'
                            AND data_type='integer'
                        ) THEN
                            ALTER TABLE payments ALTER COLUMN amount TYPE NUMERIC(10,2);
                        END IF;
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.columns
                            WHERE table_name='payments' AND column_name='duration_minutes'
                        ) THEN
                            ALTER TABLE payments ADD COLUMN duration_minutes INTEGER;
                        END IF;
                    END $$;
                """)
                cur.execute("""
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.columns
                            WHERE table_name='users' AND column_name='kicked_at'
                        ) THEN
                            ALTER TABLE users ADD COLUMN kicked_at TIMESTAMP DEFAULT NULL;
                        END IF;
                    END $$;
                """)
                # ============================================================
                # MIGRACIÓN: Corregir trial_used para usuarios antiguos
                # Usuarios con plan='trial' pero trial_used=FALSE/NULL
                # deben tener trial_used=TRUE porque obviamente usaron el trial
                # ============================================================
                cur.execute("""
                    UPDATE users 
                    SET trial_used = TRUE, updated_at = NOW()
                    WHERE plan = 'trial' 
                      AND (trial_used = FALSE OR trial_used IS NULL)
                """)
                fixed_count = cur.rowcount
                if fixed_count > 0:
                    logger.info(f"🔧 Migración: {fixed_count} usuarios con plan='trial' corregidos a trial_used=TRUE")
                # Índices
                cur.execute("CREATE INDEX IF NOT EXISTS idx_users_group    ON users(group_id, status)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_users_end_date ON users(group_id, end_date)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_users_uid_gid  ON users(user_id, group_id)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_payments_group ON payments(group_id, payment_date)")
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_users_expired
                    ON users(group_id, status, end_date)
                    WHERE status = 'active'
                """)
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
        """Obtiene todos los datos del usuario por ID (para uso en comandos)"""
        def _get(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM users WHERE user_id=%s AND group_id=%s LIMIT 1",
                    (user_id, group_id)
                )
                return cur.fetchone()
        return await self._run(_get)

    async def register_user_auto(self, group_id: int, user_id: int,
                                 username: str, first_name: str):
        """
        Registra o evalúa a un usuario que acaba de entrar al grupo VIP.

        Retorna una tupla (allow_access: bool, result_code: str, end_date: datetime | None)

        Códigos de resultado:
          - "trial_nuevo"  → usuario nuevo, se le asignó trial → PERMITIR + bienvenida
          - "activo"       → usuario con plan vigente (trial o pago) → PERMITIR, sin bienvenida
          - "expirado"     → plan vencido → EXPULSAR
          - "sin_trial"    → ya usó su trial y no tiene plan activo → EXPULSAR

        FIX CRÍTICO: La versión anterior solo permitía acceso con "trial_nuevo",
        expulsando incorrectamente a usuarios con suscripciones de pago vigentes
        que simplemente salían y volvían a entrar al grupo.
        """
        now = datetime.now()
        trial_cfg = get_group_plan_config(group_id, "trial")
        trial_minutes = trial_cfg.get("minutes", 1440)

        def _register(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # FOR UPDATE evita condiciones de carrera en entradas simultáneas
                cur.execute(
                    "SELECT user_id, trial_used, status, end_date, plan "
                    "FROM users WHERE user_id=%s AND group_id=%s FOR UPDATE",
                    (user_id, group_id)
                )
                existing = cur.fetchone()

                # ── CASO 1: Usuario NUEVO (nunca estuvo en este grupo) ──────────────
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

                # Actualizar username/first_name siempre (puede haber cambiado)
                cur.execute("""
                    UPDATE users SET username=%s, first_name=%s, updated_at=NOW()
                    WHERE user_id=%s AND group_id=%s
                """, (username, first_name, user_id, group_id))

                # ── CASO 2: Usuario con suscripción VIGENTE ──────────────────────────
                # Cubre: trial activo, plan semanal/mensual activo, o cualquier plan
                # que haya sido renovado por el admin. Siempre permitir el acceso.
                if existing["status"] == "active" and existing["end_date"] > now:
                    # Si estaba marcado como kicked (admin lo reactivó manualmente),
                    # limpiar la marca para que el scheduler no lo re-procese.
                    cur.execute("""
                        UPDATE users SET kicked_at=NULL, updated_at=NOW()
                        WHERE user_id=%s AND group_id=%s AND kicked_at IS NOT NULL
                    """, (user_id, group_id))
                    return True, "activo", existing["end_date"]

                # ── CASO 3: Usuario EXPIRADO (plan vencido, cualquier tipo) ──────────
                if existing["status"] in ("expired", "active") and existing["end_date"] <= now:
                    return False, "expirado", None

                # ── CASO 4: Ya usó trial y está en estado no-activo sin plan ─────────
                # (status='potencial', 'cancelled', u otro estado no estándar)
                if existing["trial_used"]:
                    return False, "sin_trial", None

                # ── CASO 5: Nunca usó trial, pero tiene registro (ej: potencial) ─────
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

    async def register_free_user(self, chat_id: int, user_id: int,
                                 username: str, first_name: str) -> bool:
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
                                 plan: str, first_name: str = "",
                                 custom_price: float = None,
                                 custom_days: int = None):
        now = datetime.now()
        if plan not in PLANS:
            return False, "❌ Plan inválido"
        config = get_group_plan_config(group_id, plan)

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
                if existing:
                    if plan == "trial" and existing['trial_used']:
                        return False, "❌ Este usuario ya usó su prueba gratuita"
                    if plan == "trial":
                        end_date    = now + timedelta(minutes=effective_mins)
                        dur_minutes = effective_mins
                        expiry_str  = end_date.strftime('%d/%m/%Y %H:%M')
                    else:
                        end_date    = now + timedelta(days=effective_days)
                        dur_minutes = effective_days * 1440
                        expiry_str  = end_date.strftime('%d/%m/%Y')
                    fn = first_name or existing.get('first_name', '')
                    cur.execute("""
                        UPDATE users SET
                            plan=%s, start_date=%s, end_date=%s, status='active',
                            updated_at=NOW(), username=%s, first_name=%s,
                            trial_used = trial_used OR %s,
                            kicked_at  = NULL
                        WHERE user_id=%s AND group_id=%s
                    """, (plan, now, end_date, username, fn,
                          plan == "trial", existing['user_id'], group_id))
                    cur.execute("""
                        INSERT INTO payments
                            (user_id, group_id, username, first_name, plan,
                             amount, duration_minutes, payment_date)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (existing['user_id'], group_id, username, fn,
                          plan, effective_price, dur_minutes, now))
                    price_str = fmt_price(effective_price) if effective_price > 0 else "gratis"
                    return True, (
                        f"✅ *@{username}* activado\n"
                        f"📋 Plan: {plan} | 💰 {price_str}\n"
                        f"📅 Expira: {expiry_str}"
                    )
                return False, f"❌ No tengo registro de @{username}. Pídele que envíe un mensaje al bot."

        return await self._run(_add)

    async def add_or_update_user_by_id(self, group_id: int, user_id: int,
                                       plan: str, first_name: str = "",
                                       custom_price: float = None,
                                       custom_days: int = None):
        """Versión de add_or_update_user que trabaja directamente con user_id numérico"""
        now = datetime.now()
        if plan not in PLANS:
            return False, "❌ Plan inválido"
        config = get_group_plan_config(group_id, plan)

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
                if existing:
                    if plan == "trial" and existing['trial_used']:
                        return False, "❌ Este usuario ya usó su prueba gratuita"
                    if plan == "trial":
                        end_date    = now + timedelta(minutes=effective_mins)
                        dur_minutes = effective_mins
                        expiry_str  = end_date.strftime('%d/%m/%Y %H:%M')
                    else:
                        end_date    = now + timedelta(days=effective_days)
                        dur_minutes = effective_days * 1440
                        expiry_str  = end_date.strftime('%d/%m/%Y')
                    fn = first_name or existing.get('first_name', '')
                    username = existing.get('username', f"user_{user_id}")
                    cur.execute("""
                        UPDATE users SET
                            plan=%s, start_date=%s, end_date=%s, status='active',
                            updated_at=NOW(), first_name=%s,
                            trial_used = trial_used OR %s,
                            kicked_at  = NULL
                        WHERE user_id=%s AND group_id=%s
                    """, (plan, now, end_date, fn, plan == "trial", user_id, group_id))
                    cur.execute("""
                        INSERT INTO payments
                            (user_id, group_id, username, first_name, plan,
                             amount, duration_minutes, payment_date)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (user_id, group_id, username, fn,
                          plan, effective_price, dur_minutes, now))
                    price_str = fmt_price(effective_price) if effective_price > 0 else "gratis"
                    display_name = fn if fn else username
                    return True, (
                        f"✅ *{display_name}* activado\n"
                        f"📋 Plan: {plan} | 💰 {price_str}\n"
                        f"📅 Expira: {expiry_str}"
                    )
                return False, f"❌ No tengo registro del usuario con ID {user_id}. Pídele que envíe un mensaje al bot."

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
                cur.execute("""
                    SELECT user_id, username, first_name, plan, amount,
                           duration_minutes, payment_date
                    FROM payments
                    WHERE group_id=%s AND payment_date >= date_trunc('month', NOW())
                    ORDER BY payment_date DESC
                    LIMIT 10
                """, (group_id,))
                recent = cur.fetchall()
                return {"summary": summary, "total": total,
                        "new_users": new_users, "recent": recent}

        

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
        """
        Retorna solo usuarios cuyo plan venció y que aún no han sido procesados
        (kicked_at IS NULL). La marca kicked_at se setea atómicamente en expire_user()
        con RETURNING, lo que garantiza que ningún ciclo concurrente los re-procese.
        """
        def _get(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT user_id, username, first_name, plan, end_date
                    FROM users
                    WHERE group_id=%s
                      AND status='active'
                      AND end_date < NOW()
                      AND kicked_at IS NULL
                """, (group_id,))
                return cur.fetchall()
        return await self._run(_get)

    async def expire_user(self, user_id: int, group_id: int):
        """
        Marca al usuario como expirado y registra kicked_at en una sola operación
        atómica. RETURNING garantiza que solo un proceso por ciclo lo procese.
        """
        def _expire(conn):
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE users
                    SET status='expired', kicked_at=NOW(), updated_at=NOW()
                    WHERE user_id=%s AND group_id=%s
                      AND status='active'
                      AND kicked_at IS NULL
                    RETURNING user_id
                """, (user_id, group_id))
                return cur.fetchone() is not None
        return await self._run(_expire)

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
                    SELECT user_id, username, first_name, plan, amount,
                           duration_minutes, payment_date
                    FROM payments
                    WHERE group_id=%s AND payment_date>=%s
                    ORDER BY payment_date DESC
                """, (group_id, start_date))
                return cur.fetchall()

        return await self._run(_get)

    async def update_group_fields(self, group_id: int, changes: dict):
        if not changes:
            return

        import json  # ← MOVER AQUÍ
        
        def _upd(conn):
            with conn.cursor() as cur:
                ...
                if 'settings' in changes:
                    cur.execute(
                        "UPDATE groups SET settings=%s WHERE group_id=%s",
                        (json.dumps(changes['settings']), group_id)
                    )
                if 'admin' in changes:
                    cur.execute("UPDATE groups SET admin_id=%s WHERE group_id=%s",
                                (changes['admin'], group_id))
                if 'type' in changes:
                    cur.execute("UPDATE groups SET group_type=%s WHERE group_id=%s",
                                (changes['type'], group_id))
                if 'settings' in changes:
                    import json
                    cur.execute(
                        "UPDATE groups SET settings=%s WHERE group_id=%s",
                        (json.dumps(changes['settings']), group_id)
                    )

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
        

    async def get_trial_stats(self, group_id: int) -> dict:
        def _get(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT COUNT(*) as total_trial_users
                    FROM users
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
                    SELECT COUNT(*) as expired_from_trial
                    FROM users
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

        # ============================================================
    # NUEVOS MÉTODOS PARA BROADCAST MASIVO A USUARIOS TRIAL
    # ============================================================

    async def get_broadcast_targets(self, group_id: int, filter_type: str) -> list:
        """
        Obtiene usuarios según el filtro de broadcast.
        
        Filtros:
        - 'trial_only': Solo usuarios que usaron trial pero NUNCA compraron
        - 'subscribed_only': Solo usuarios que tienen suscripción activa (pagada)
        - 'expired_only': Solo usuarios con suscripción expirada no renovada
        - 'all_trial': Todos los que usaron trial (incluye comprados y no comprados)
        """
        def _get(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # ============================================================
                # FIX CRÍTICO: Usar (plan='trial' OR trial_used=TRUE) para identificar
                # usuarios que usaron trial, ya que muchos usuarios antiguos tienen
                # plan='trial' pero trial_used=FALSE/NULL por migraciones o registros
                # manuales anteriores.
                # ============================================================
                
                # Subquery base: usuarios que usaron trial (plan='trial' O trial_used=TRUE)
                trial_base = """
                    SELECT user_id, username, first_name, group_id, plan, status, end_date
                    FROM users
                    WHERE group_id = %s
                      AND (plan = 'trial' OR trial_used = TRUE OR trial_used IS NULL)
                """
                
                if filter_type == 'trial_only':
                    # Usaron trial (plan='trial' o trial_used=TRUE) pero NUNCA pagaron
                    # FIX: Incluimos trial_used IS NULL porque usuarios antiguos pueden tener NULL
                    cur.execute("""
                        SELECT DISTINCT u.user_id, u.username, u.first_name, u.group_id
                        FROM users u
                        WHERE u.group_id = %s
                          AND (u.plan = 'trial' OR u.trial_used = TRUE OR u.trial_used IS NULL)
                          AND NOT EXISTS (
                              SELECT 1 FROM payments p 
                              WHERE p.user_id = u.user_id 
                                AND p.group_id = u.group_id
                                AND p.amount > 0
                          )
                        ORDER BY u.user_id
                    """, (group_id,))
                    
                elif filter_type == 'subscribed_only':
                    # Tienen suscripción activa pagada (plan != 'trial' y status='active')
                    # FIX: Requiere que hayan usado trial (trial_used=TRUE) o tengan plan pago
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
                    # Usaron trial, compraron alguna vez, pero ahora están expirados
                    cur.execute("""
                        SELECT DISTINCT u.user_id, u.username, u.first_name, u.group_id
                        FROM users u
                        WHERE u.group_id = %s
                          AND (u.trial_used = TRUE OR u.trial_used IS NULL)
                          AND u.status = 'expired'
                          AND EXISTS (
                              SELECT 1 FROM payments p 
                              WHERE p.user_id = u.user_id 
                                AND p.group_id = u.group_id
                                AND p.amount > 0
                          )
                        ORDER BY u.user_id
                    """, (group_id,))
                    
                elif filter_type == 'all_trial':
                    # Todos los que usaron trial o tienen plan='trial' (cualquier estado)
                    # FIX: Incluimos plan='trial' para capturar usuarios antiguos
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
    
    # ============================================================
    # NUEVOS MÉTODOS PARA SISTEMA DE AVISOS DE EXPIRACIÓN
    # ============================================================

    async def get_warning_config(self, group_id: int) -> dict:
        """
        Obtiene la configuración de avisos para un grupo.
        Si no existe, retorna valores por defecto.
        """
        def _get(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT settings 
                    FROM groups 
                    WHERE group_id = %s
                """, (group_id,))
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
        
        # Configuración por defecto
        return {
            "enabled": False,
            "warnings": [
                {"minutes_before": 15, "message": "⏳ *Tu trial expira en {minutes_left} minutos*\n\n📅 Expira: {expiry_time}\n\n💳 ¿Quieres seguir disfrutando del contenido?\nPresiona el botón para ver los datos de pago:"}
            ]
        }

    async def save_warning_config(self, group_id: int, config: dict):
        """Guarda la configuración de avisos para un grupo."""
        import json
        
        def _save(conn):
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT settings FROM groups WHERE group_id = %s
                """, (group_id,))
                row = cur.fetchone()
                settings = row[0] if row and row[0] else {}
                if isinstance(settings, str):
                    settings = json.loads(settings)
                settings['warning_config'] = config
                cur.execute("""
                    UPDATE groups 
                    SET settings = %s
                    WHERE group_id = %s
                """, (json.dumps(settings), group_id))
        await self._run(_save)
        
        # Actualizar en memoria también
        group = get_group_by_id(group_id)
        if group:
            if 'settings' not in group:
                group['settings'] = {}
            group['settings']['warning_config'] = config

    async def get_users_for_warning(self, group_id: int, minutes_before: int) -> list:
        """
        Obtiene usuarios que necesitan recibir un aviso específico.
        Busca usuarios cuyo trial expire dentro de 'minutes_before' minutos
        y que no hayan recibido ya este tipo de aviso.
        """
        warning_threshold = datetime.now() + timedelta(minutes=minutes_before)
        warning_type = f"warning_{minutes_before}min"
        
        def _get(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT u.user_id, u.username, u.first_name, u.end_date, u.plan
                    FROM users u
                    LEFT JOIN warning_logs w 
                        ON u.user_id = w.user_id 
                        AND u.group_id = w.group_id 
                        AND w.warning_type = %s
                    WHERE u.group_id = %s
                      AND u.status = 'active'
                      AND u.plan = 'trial'
                      AND u.end_date <= %s
                      AND u.end_date > NOW()
                      AND w.id IS NULL
                    ORDER BY u.end_date ASC
                """, (warning_type, group_id, warning_threshold))
                return cur.fetchall()
        return await self._run(_get)

    async def log_warning_sent(self, user_id: int, group_id: int, warning_type: str):
        """Registra que un aviso fue enviado para evitar duplicados."""
        def _log(conn):
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO warning_logs (user_id, group_id, warning_type, sent_at)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (user_id, group_id, warning_type) DO UPDATE
                    SET sent_at = NOW()
                """, (user_id, group_id, warning_type))
        await self._run(_log)

    async def get_warning_stats(self, group_id: int) -> list:
        """Estadísticas de avisos enviados para un grupo."""
        def _get(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT warning_type, COUNT(*) as count
                    FROM warning_logs
                    WHERE group_id = %s
                    GROUP BY warning_type
                """, (group_id,))
                return cur.fetchall()
        return await self._run(_get)

    async def clear_warning_logs(self, group_id: int = None):
        """Limpia registros de avisos (útil para testing o reset)."""
        def _clear(conn):
            with conn.cursor() as cur:
                if group_id:
                    cur.execute("DELETE FROM warning_logs WHERE group_id = %s", (group_id,))
                else:
                    cur.execute("DELETE FROM warning_logs")
        await self._run(_clear)


# ==================== INSTANCIAS GLOBALES ====================
db = Database(DATABASE_URL)
scheduler = AsyncIOScheduler()
bot_app = None


# ==================== HELPERS INTERNOS ====================
async def _safe_send(chat_id: int, text: str, **kwargs):
    try:
        await bot_app.bot.send_message(chat_id, text, **kwargs)
    except Exception as e:
        logger.warning(f"No se pudo enviar mensaje a {chat_id}: {e}")


# ==================== EXPULSIÓN CON REINTENTOS ====================
async def _kick_user_with_retry(group_id: int, user_id: int, retries: int = 3) -> bool:
    """
    Expulsa al usuario con reintentos exponenciales y verificación previa de membresía.

    Flujo Telegram para "kick sin ban permanente":
      1. ban_chat_member   → expulsa e impide re-entrada
      2. asyncio.sleep(1)  → espera que Telegram procese el ban
      3. unban_chat_member → levanta el ban (puede volver si el admin lo reactiva)
    """
    for attempt in range(retries):
        try:
            # Verificar membresía antes de intentar expulsar
            try:
                member = await bot_app.bot.get_chat_member(group_id, user_id)
                if member.status in ("left", "kicked", "banned"):
                    logger.info(
                        f"Usuario {user_id} ya no está en el grupo {group_id} "
                        f"(status: {member.status})"
                    )
                    return True  # Ya no está: objetivo cumplido
            except BadRequest as e:
                err = str(e).lower()
                if "user not found" in err or "member_id_invalid" in err:
                    logger.info(f"Usuario {user_id} no encontrado en grupo {group_id}: {e}")
                    return True
                raise

            await bot_app.bot.ban_chat_member(group_id, user_id)
            await asyncio.sleep(1)  # Necesario para que el ban se procese antes del unban
            await bot_app.bot.unban_chat_member(group_id, user_id)

            logger.info(f"✅ Usuario {user_id} expulsado del grupo {group_id}")
            return True

        except TelegramError as e:
            error_str = str(e).lower()
            if any(phrase in error_str for phrase in [
                "not enough rights",
                "bot is not a member",
                "chat not found",
                "user not found",
                "participant_id_invalid",
                "member_id_invalid"
            ]):
                logger.error(f"❌ Error no recuperable expulsando {user_id} de {group_id}: {e}")
                return False

            wait = 2 ** attempt  # 1s → 2s → 4s
            logger.warning(
                f"⚠️ Intento {attempt + 1}/{retries} fallido para expulsar {user_id} "
                f"de {group_id}: {e}. Reintentando en {wait}s..."
            )
            await asyncio.sleep(wait)

    logger.error(f"❌ No se pudo expulsar al usuario {user_id} del grupo {group_id} tras {retries} intentos")
    return False


# ==================== HANDLERS ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if update.callback_query:
        send = update.callback_query.message.reply_text
    elif update.message:
        send = update.message.reply_text
    else:
        return

    if user_id == SUPER_ADMIN_ID:
        vip_count      = len([g for g in GROUPS if g.get("type", "VIP") == "VIP"])
        free_count     = len([g for g in GROUPS if g.get("type", "VIP") == "FREE"])
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
            cfg_t = get_group_plan_config(group["group_id"], "trial")
            cfg_s = get_group_plan_config(group["group_id"], "semanal")
            cfg_m = get_group_plan_config(group["group_id"], "mensual")
            keyboard = [
                [InlineKeyboardButton("📊 Usuarios activos",   callback_data="list_active")],
                [InlineKeyboardButton("💰 Ganancias",          callback_data="earnings")],
                [InlineKeyboardButton("📥 Exportar mes",       callback_data="export_month")],
                [InlineKeyboardButton("📊 Estadísticas Trial", callback_data="trial_stats")],
                [InlineKeyboardButton("⚙️ Precios y Trial",    callback_data=f"cfg_group_{group['group_id']}")],
                [InlineKeyboardButton("📢 Broadcast Trial",    callback_data="broadcast_menu")]
            ]
            await send(
                f"👑 *Panel VIP - {group['group_name']}*\n\n"
                f"🆔 ID del grupo: `{group['group_id']}`\n\n"
                f"💡 *Tarifas actuales:*\n"
                f"• ⏱ Trial: {fmt_minutes(cfg_t.get('minutes', 1440))}\n"
                f"• 📅 Semanal: ${cfg_s['price']}\n"
                f"• 📆 Mensual: ${cfg_m['price']}\n\n"
                f"Comandos disponibles:\n"
                f"• `/add @usuario plan` - Agregar suscripción\n"
                f"• También puedes RESPONDER al mensaje de registro con `/add plan`\n"
                f"• O usar `/add ID plan` (ej: `/add 123456789 mensual`)\n"
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
        cfg   = get_group_plan_config(group_id, "trial")
        cfg_s = get_group_plan_config(group_id, "semanal")
        cfg_m = get_group_plan_config(group_id, "mensual")
        keyboard = [
            [InlineKeyboardButton("➕ Agregar usuario",     callback_data="add_user")],
            [InlineKeyboardButton("📊 Usuarios activos",    callback_data="list_active")],
            [InlineKeyboardButton("💰 Ganancias",           callback_data="earnings")],
            [InlineKeyboardButton("📥 Exportar mes",        callback_data="export_month")],
            [InlineKeyboardButton("📊 Estadísticas Trial",  callback_data="trial_stats")],
            [InlineKeyboardButton("⚙️ Precios y Trial",     callback_data=f"cfg_group_{group_id}")],
            [InlineKeyboardButton("📢 Broadcast Trial",     callback_data="broadcast_menu")],
        ]
        await query.edit_message_text(
            f"👑 *Panel VIP - {group['group_name']}*\n\n"
            f"🆔 ID: `{group['group_id']}`\n"
            f"👑 Admin: `{group['admin_id']}`\n\n"
            f"💡 *Tarifas actuales:*\n"
            f"• ⏱ Trial: {fmt_minutes(cfg.get('minutes', 1440))}\n"
            f"• 📅 Semanal: ${cfg_s['price']}\n"
            f"• 📆 Mensual: ${cfg_m['price']}",
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


# ==================== NUEVA FUNCIÓN: HANDLER PARA REPLY CON /add ====================
async def handle_reply_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Maneja respuestas con /add a mensajes de registro de trial.
    Extrae automáticamente el ID del usuario del mensaje al que se responde.
    """
    if not update.message or not update.message.reply_to_message:
        return
    
    # Verificar que el comando sea /add
    if not update.message.text or not update.message.text.startswith('/add'):
        return
    
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    
    # Obtener el mensaje al que se está respondiendo
    replied_msg = update.message.reply_to_message
    
    # Intentar extraer el user_id del mensaje de registro
    target_user_id = None
    target_username = None
    target_first_name = None
    
    # Patrones comunes en los mensajes de registro
    text = replied_msg.text or replied_msg.caption or ""
    
    logger.info(f"📝 Procesando reply a mensaje: {text[:100]}")
    
    # Buscar el ID en el mensaje (formato: 🆔 ID: 123456789)
    id_match = re.search(r'🆔 ID:\s*`?(\d+)`?', text)
    if id_match:
        target_user_id = int(id_match.group(1))
        logger.info(f"✅ ID encontrado en mensaje: {target_user_id}")
    
    # Buscar el nombre (formato: 👤 Nombre: E)
    name_match = re.search(r'👤 Nombre:\s*(.+?)(?:\n|$)', text)
    if name_match:
        target_first_name = name_match.group(1).strip()
        logger.info(f"✅ Nombre encontrado: {target_first_name}")
    
    # Buscar username si existe
    username_match = re.search(r'@(\w+)', text)
    if username_match:
        target_username = username_match.group(1)
        logger.info(f"✅ Username encontrado: @{target_username}")
    
    if not target_user_id:
        await update.message.reply_text(
            "❌ No pude encontrar el ID del usuario en el mensaje.\n"
            "Asegúrate de responder al mensaje de registro del trial.\n\n"
            "También puedes usar:\n"
            "• `/add 123456789 mensual` (por ID)\n"
            "• `/add @usuario mensual` (por username)",
            parse_mode="Markdown"
        )
        return
    
    # Obtener el grupo actual
    current_group = context.user_data.get('current_group')
    if not current_group:
        # Intentar obtener del chat actual si es un grupo
        if update.effective_chat.type in ("group", "supergroup"):
            current_group = chat_id
        else:
            await update.message.reply_text("❌ Usa /start primero para seleccionar un grupo")
            return
    
    # Verificar permisos
    if not can_manage_group(user_id, current_group) and user_id != SUPER_ADMIN_ID:
        await update.message.reply_text("❌ No autorizado para gestionar este grupo")
        return
    
    # Parsear el comando: /add [plan] [precio] [días]
    args = update.message.text.split()
    
    if len(args) < 2:
        await update.message.reply_text(
            "❌ *Uso respondiendo al mensaje:*\n"
            "`/add plan [precio] [días]`\n\n"
            "*Ejemplos:*\n"
            "• `/add mensual`\n"
            "• `/add semanal 5.99`\n"
            "• `/add mensual 15.99 45`",
            parse_mode="Markdown"
        )
        return
    
    plan = args[1].lower()
    if plan not in PLANS:
        await update.message.reply_text("❌ Plan inválido. Usa: trial, semanal, mensual")
        return
    
    custom_price = None
    custom_days = None
    
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
    
    # Obtener información completa del usuario
    username = target_username or f"user_{target_user_id}"
    first_name = target_first_name or ""
    
    # Verificar si existe en BD, si no, crearlo
    existing = await db.get_user_by_id(target_user_id, current_group)
    if not existing:
        logger.info(f"Usuario {target_user_id} no existe en BD, registrando...")
        await db.register_free_user(current_group, target_user_id, username, first_name)
    
    # Activar la suscripción usando el método por ID
    logger.info(f"🎯 Activando {plan} para usuario {target_user_id} via reply")
    
    success, msg = await db.add_or_update_user_by_id(
        current_group, target_user_id, plan, first_name, custom_price, custom_days
    )
    
    # Enviar confirmación
    await update.message.reply_text(
        f"✅ *Activación por respuesta*\n\n{msg}",
        parse_mode="Markdown"
    )
    
    # Notificar al usuario activado
    try:
        await context.bot.send_message(
            target_user_id,
            f"🎉 *¡Suscripción activada!*\n\n"
            f"Tu plan *{plan}* en *{get_group_by_id(current_group)['group_name']}* ha sido activado.\n"
            f"¡Gracias por tu compra!",
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.warning(f"No se pudo notificar al usuario {target_user_id}: {e}")


# ==================== FUNCIÓN MODIFICADA: add_user_command con soporte para ID numérico ====================
async def add_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # 🔥 NUEVO: Verificar si es una respuesta a un mensaje
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
            "*Planes disponibles:*\n"
            f"• `trial`   — {cfg_t['name']}\n"
            f"• `semanal` — {cfg_s['name']}\n"
            f"• `mensual` — {cfg_m['name']}\n\n"
            "*Ejemplos:*\n"
            "`/add @juan semanal`           → por username\n"
            "`/add 123456789 mensual`       → por ID numérico\n"
            "`/add @juan semanal 5.99`      → precio custom\n"
            "`/add @juan semanal 5.99 10`   → precio y días custom\n\n"
            "*También puedes RESPONDER al mensaje de registro con:*\n"
            "`/add mensual`",
            parse_mode="Markdown"
        )
        return

    identifier = context.args[0].replace("@", "")
    plan     = context.args[1].lower()
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
            await update.message.reply_text(
                "❌ Precio inválido. Usa punto o coma decimal (ej: `5.99`)",
                parse_mode="Markdown"
            )
            return
    if len(context.args) >= 4:
        try:
            custom_days = int(context.args[3])
            if custom_days <= 0:
                raise ValueError
        except ValueError:
            await update.message.reply_text(
                "❌ Días inválidos. Debe ser un número entero positivo.",
                parse_mode="Markdown"
            )
            return

    # 🔥 NUEVO: Detectar si es ID numérico o username
    if identifier.isdigit():
        # Es un ID numérico - usar el método especial
        target_user_id = int(identifier)
        first_name = ""
        username = f"user_{target_user_id}"
        
        # Intentar obtener información del usuario desde el grupo
        try:
            member = await context.bot.get_chat_member(current_group, target_user_id)
            if member and member.user:
                first_name = member.user.first_name or ""
                username = member.user.username or f"user_{target_user_id}"
        except Exception as e:
            logger.warning(f"No se pudo obtener info del usuario {target_user_id}: {e}")
        
        # Verificar si existe en BD, si no, crearlo
        existing = await db.get_user_by_id(target_user_id, current_group)
        if not existing:
            logger.info(f"Usuario {target_user_id} no existe en BD, registrando...")
            await db.register_free_user(current_group, target_user_id, username, first_name)
        
        # Activar usando el método por ID
        success, msg = await db.add_or_update_user_by_id(
            current_group, target_user_id, plan, first_name, custom_price, custom_days
        )
        await update.message.reply_text(msg, parse_mode="Markdown")
        return
    
    # Si no es ID numérico, continuar con el flujo normal (por username)
    username = identifier
    first_name = ""
    try:
        member = await context.bot.get_chat_member(current_group, username)
        if member and member.user:
            first_name = member.user.first_name or ""
    except Exception:
        pass

    success, msg = await db.add_or_update_user(
        current_group, username, plan, first_name, custom_price, custom_days
    )
    await update.message.reply_text(msg, parse_mode="Markdown")


async def list_active_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
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
    now = datetime.now()
    for user in users[:30]:
        end_date   = user['end_date']
        remaining  = end_date - now
        total_mins = int(remaining.total_seconds() / 60)
        if total_mins < 0:
            total_mins = 0
        days_left = total_mins // 1440
        emoji = "🟢" if days_left > 7 else "🟡" if days_left > 1 else "🔴"
        if total_mins < 60:
            expiry_text = f"{total_mins} min restantes"
            expiry_date = end_date.strftime('%d/%m/%Y %H:%M')
        elif total_mins < 1440:
            h = total_mins // 60; m = total_mins % 60
            expiry_text = f"{h}h {m}m restantes" if m else f"{h}h restantes"
            expiry_date = end_date.strftime('%d/%m/%Y %H:%M')
        else:
            expiry_text = f"{days_left} días restantes"
            expiry_date = end_date.strftime('%d/%m/%Y')
        first_name = user.get('first_name', '') or 'Sin nombre'
        username   = user.get('username', '')
        display    = (
            f"{first_name} (@{username})"
            if username and not username.startswith('user_')
            else f"{first_name} (ID: `{user['user_id']}`)"
        )
        chat_link = f"tg://user?id={user['user_id']}"
        msg += (
            f"{emoji} {display}\n"
            f"   📅 Expira: {expiry_date} ({expiry_text})\n"
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
    writer.writerow(['Fecha', 'User ID', 'Username', 'Nombre', 'Plan', 'Duración', 'Monto USD', 'Link de contacto'])
    for t in transactions:
        dur = fmt_minutes(t['duration_minutes']) if t.get('duration_minutes') else ""
        writer.writerow([
            t['payment_date'].strftime('%Y-%m-%d %H:%M:%S'),
            t['user_id'],
            t['username'] or 'Sin username',
            t['first_name'] or 'Sin nombre',
            t['plan'].upper(),
            dur,
            f"{float(t['amount']):.2f}",
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
        await update.message.reply_text("❌ Envía el archivo CSV de backup junto con el comando.")
        return
    await update.message.reply_text("🔄 Restaurando configuración...")
    try:
        import io
        file         = await update.message.document.get_file()
        file_content = await file.download_as_bytearray()
        content      = file_content.decode('utf-8')
        reader       = csv.reader(io.StringIO(content))
        next(reader)
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
                        "type": group_type,   "admin_id": admin_id,
                        "settings": {}
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
        "✏️ *Selecciona el grupo que deseas editar*",
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
        "❌ *Eliminar Grupo*\n\n⚠️ Esta acción es irreversible.\nSelecciona el grupo:",
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
        f"📌 *{group['group_name']}*\n"
        f"🆔 ID: `{group['group_id']}`\n\n"
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
    global GROUPS
    GROUPS = [g for g in GROUPS if g["group_id"] != group_id]
    await db.delete_group_from_db(group_id)
    await query.edit_message_text(
        f"✅ *Grupo eliminado*\n\n📌 {group_name}\n🆔 ID: `{group_id}`",
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
        "• `/add @usuario|ID plan [precio] [días]` - Agregar usuario\n"
        "• También puedes RESPONDER al mensaje de registro con `/add plan`\n"
        "• `/configpago group_id` - Configurar datos de pago\n"
        "• `/broadcast group_id` - Enviar mensaje masivo a usuarios trial\n\n"
        "*Usuarios (VIP):*\n"
        "• `/pagar` - Ver datos de pago para renovar acceso\n"
        "• Presiona 💳 Pagar Acceso en los mensajes del bot\n\n"
        "*Planes disponibles:*\n"
        "• `trial`   - duración configurada por grupo\n"
        "• `semanal` - precio y días configurables\n"
        "• `mensual` - precio y días configurables\n\n"
        "*Ejemplos:*\n"
        "• `/add @juan semanal`\n"
        "• `/add 123456789 mensual`\n"
        "• `/add @juan semanal 5.99`\n"
        "• `/add @juan semanal 5.99 10`\n"
        "• Responder a mensaje de registro: `/add mensual`"
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


async def trial_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

    group = get_group_by_id(group_id)
    if not group or group.get("type") != "VIP":
        await message.reply_text("❌ Solo disponible en grupos VIP")
        return

    stats = await db.get_trial_stats(group_id)

    msg = f"📊 *ESTADÍSTICAS DE TRIAL — {group['group_name']}*\n\n"
    msg += f"• 🆓 *Usaron trial:* {stats['total_trial_users']} personas\n"
    msg += f"• 🟢 *Trial activos ahora:* {stats['active_count']} personas\n"
    msg += f"• 🔴 *Expirados desde trial:* {stats['expired_from_trial']} personas\n"
    msg += f"• 💰 *Convertidos a pago:* {stats['converted_users']} personas\n"

    if stats['active_trials']:
        msg += f"\n⏳ *TIEMPO RESTANTE POR USUARIO:*\n"
        for trial in stats['active_trials']:
            total_hours = float(trial['hours_left']) if trial['hours_left'] else 0
            days  = int(total_hours // 24)
            hours = int(total_hours % 24)
            mins  = int((total_hours * 60) % 60)
            name  = trial['first_name'] or trial['username'] or f"ID:{trial['user_id']}"
            username_str = (
                f"@{trial['username']}"
                if trial['username'] and not str(trial['username']).startswith('user_')
                else f"ID:{trial['user_id']}"
            )
            if days > 0:
                time_left = f"{days}d {hours}h {mins}m"
            elif hours > 0:
                time_left = f"{hours}h {mins}m"
            else:
                time_left = f"{mins}m"

            expiry_str = trial['end_date'].strftime('%d/%m/%Y %H:%M')
            chat_link  = f"tg://user?id={trial['user_id']}"
            msg += (
                f"\n👤 [{name}]({chat_link}) ({username_str})\n"
                f"   ⏳ Tiempo restante: *{time_left}*\n"
                f"   📅 Expira: {expiry_str}\n"
            )

    if stats['total_trial_users'] > 0:
        conversion = (stats['converted_users'] / stats['total_trial_users']) * 100
        msg += f"\n\n📈 *Tasa de conversión:* {conversion:.1f}%"

    keyboard = [[InlineKeyboardButton("🔙 Volver", callback_data=f"select_group_{group_id}")]]

    if query:
        await query.edit_message_text(msg, parse_mode="Markdown",
                                      reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await message.reply_text(msg, parse_mode="Markdown",
                                 reply_markup=InlineKeyboardMarkup(keyboard))


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
        "✏️ *Cambiar nombre*\n\nEnvía el nuevo nombre en el chat.\n*Escribe 'cancelar' para cancelar.*",
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
    context.user_data.pop('pending_changes', None)
    context.user_data.pop('editing_mode', None)
    context.user_data.pop('editing_group_id', None)
    await query.edit_message_text(
        f"✅ *Cambios aplicados*\n\n" + "\n".join(changes_made),
        parse_mode="Markdown"
    )
    await asyncio.sleep(2)
    await menu_edit_group_select(update, context)


# ==================== DETECCIÓN DE MIEMBROS ====================

async def _process_new_vip_member(chat_id: int, user_id: int, username: str,
                                  first_name: str, group: dict):
    """
    Evalúa a un usuario que acaba de entrar al grupo VIP y decide si permitir
    o expulsar su acceso según el estado de su suscripción.

    Resultado de register_user_auto:
      - "trial_nuevo" → permitir + mensaje de bienvenida
      - "activo"      → permitir silenciosamente (suscripción vigente)
      - "expirado"    → expulsar inmediatamente
      - "sin_trial"   → expulsar inmediatamente (ya usó trial, no tiene plan)
    """
    display   = first_name if first_name else (
        f"@{username}" if not username.startswith('user_') else f"Usuario {user_id}"
    )
    chat_link = f"tg://user?id={user_id}"

    logger.info(f"🔔 Nuevo miembro VIP: user={user_id} ({display}) en {group['group_name']}")

    allow_access, result_code, end_date = await db.register_user_auto(
        chat_id, user_id, username, first_name
    )

    logger.info(
        f"register_user_auto → allow={allow_access}, code={result_code}, "
        f"end_date={end_date}, user={display}"
    )

    # ── ACCESO PERMITIDO ─────────────────────────────────────────────────────
    if allow_access:
        if result_code == "trial_nuevo":
            cfg_t      = get_group_plan_config(chat_id, "trial")
            trial_str  = fmt_minutes(cfg_t.get('minutes', 1440))
            expiry_str = end_date.strftime('%d/%m/%Y %H:%M') if end_date else "N/A"

            await _safe_send(
                user_id,
                f"🎉 *¡Bienvenido al grupo VIP!*\n\n"
                f"✨ Tu período de prueba de *{trial_str}* ha comenzado.\n"
                f"📅 Tu acceso expira el: *{expiry_str}*\n\n"
                f"💳 ¿Quieres continuar después del trial? Presiona el botón para ver los datos de pago:",
                reply_markup=get_payment_keyboard(chat_id, user_id),
                parse_mode="Markdown"
            )
            await _safe_send(
                group["admin_id"],
                f"🆕 *Nuevo usuario en TRIAL*\n\n"
                f"👤 *Nombre:* [{display}]({chat_link})\n"
                f"🆔 *ID:* `{user_id}`\n"
                f"📌 *Grupo:* {group['group_name']}\n"
                f"⏱ *Duración:* {trial_str}\n"
                f"📅 *Expira:* {expiry_str}\n\n"
                f"_Se registró automáticamente al entrar._\n\n"
                f"💡 *Para activar suscripción:* RESPONDE a este mensaje con:\n"
                f"• `/add mensual`\n"
                f"• `/add semanal 5.99`\n"
                f"• O usa: `/add {user_id} mensual`",
                parse_mode="Markdown"
            )
            logger.info(f"🆕 Trial activado: {display} en {group['group_name']} hasta {expiry_str}")

        elif result_code == "activo":
            # Usuario con plan vigente que volvió a entrar — no hacer nada.
            # Solo registrar en logs para trazabilidad.
            logger.info(
                f"✅ Reingreso permitido (plan activo): {display} "
                f"en {group['group_name']} — expira {end_date}"
            )

        return True

    # ── ACCESO DENEGADO: expulsar ─────────────────────────────────────────────
    if result_code == "expirado":
        motivo_usuario = "Tu período de prueba o suscripción ha expirado."
        motivo_admin   = "Plan vencido"
    else:  # "sin_trial"
        motivo_usuario = "Ya usaste tu prueba gratuita y no tienes suscripción activa."
        motivo_admin   = "Trial ya utilizado"

    logger.info(f"🚫 Acceso denegado ({result_code}): expulsando a {display}")

    # ===== ANTI-DUPLICADO: Verificar si usuario ya está fuera =====
    # Si ya fue expulsado por otro handler/proceso, no enviar notificaciones duplicadas
    try:
        member_check = await bot_app.bot.get_chat_member(chat_id, user_id)
        if member_check.status in ("left", "kicked", "banned"):
            logger.info(f"⏭️ Usuario {display} ya está fuera del grupo {group['group_name']} "
                       f"(status: {member_check.status}) — omitiendo notificaciones duplicadas")
            return False
    except Exception as e:
        # Si falla la verificación, continuar normalmente
        logger.debug(f"No se pudo verificar estado previo de {user_id}: {e}")
    # ================================================================

    kick_success = await _kick_user_with_retry(chat_id, user_id)

    cfg_s = get_group_plan_config(chat_id, "semanal")
    cfg_m = get_group_plan_config(chat_id, "mensual")

    if kick_success:
        await _safe_send(
            user_id,
            f"🚫 *Acceso denegado — {group['group_name']}*\n\n"
            f"{motivo_usuario}\n\n"
            f"💳 ¿Quieres renovar tu acceso? Presiona el botón para ver los datos de pago:",
            reply_markup=get_payment_keyboard(chat_id, user_id),
            
            parse_mode="Markdown"
        )
        await _safe_send(
            group["admin_id"],
            f"🚫 *Reingreso denegado y expulsado*\n\n"
            f"👤 *Usuario:* [{display}]({chat_link})\n"
            f"🆔 *ID:* `{user_id}`\n"
            f"📌 *Grupo:* {group['group_name']}\n"
            f"⚠️ *Motivo:* {motivo_admin}\n\n"
            f"_El usuario fue expulsado automáticamente._",
            parse_mode="Markdown"
        )
        logger.info(f"✅ {display} expulsado correctamente ({result_code})")
    else:
        await _safe_send(
            group["admin_id"],
            f"⚠️ *No se pudo expulsar automáticamente*\n\n"
            f"👤 Usuario: [{display}]({chat_link})\n"
            f"📌 Grupo: {group['group_name']}\n"
            f"⚠️ Motivo: {motivo_admin}\n\n"
            f"🔧 *Acción manual requerida:* expulsa a este usuario desde Telegram.",
            parse_mode="Markdown"
        )
        logger.error(f"❌ No se pudo expulsar a {display} — ¿el bot tiene permiso de banear?")

    return False


async def _process_new_free_member(chat_id: int, user_id: int, username: str,
                                   first_name: str, group: dict):
    display   = first_name if first_name else (
        f"@{username}" if not username.startswith('user_') else f"Usuario {user_id}"
    )
    chat_link = f"tg://user?id={user_id}"

    is_new = await db.register_free_user(chat_id, user_id, username, first_name)
    if is_new:
        await _safe_send(
            group["admin_id"],
            f"📋 *Nuevo cliente potencial*\n\n"
            f"👤 *Nombre:* {display}\n"
            f"🆔 *ID:* `{user_id}`\n"
            f"📌 *Grupo:* {group['group_name']}\n"
            f"🔗 [Abrir chat]({chat_link})",
            parse_mode="Markdown"
        )
    return is_new


async def track_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handler principal para detectar entradas al grupo (API v20+).

    FIX CRÍTICO: La versión original tenía:
      1. Un bloque `if new_member.is_bot / get_group_by_id` FUERA de lugar (antes de `joined`)
         con indentación incorrecta (2 espacios en vez de 4) → SyntaxError silencioso
      2. Chequeos de is_bot y get_group_by_id duplicados
      3. La variable `joined` tenía indentación incorrecta

    Esta versión tiene el flujo correcto y sin duplicados.
    """
    result = update.chat_member
    if not result:
        return

    new_member = result.new_chat_member.user

    # Ignorar al propio bot
    if new_member.is_bot:
        return

    chat_id    = result.chat.id
    old_status = result.old_chat_member.status
    new_status = result.new_chat_member.status

    # Solo procesar cuando alguien *entra* al grupo
    joined = (
        old_status in ("left", "kicked", "restricted", "banned") and
        new_status in ("member", "administrator", "creator")
    )
    if not joined:
        return

    group = get_group_by_id(chat_id)
    if not group:
        logger.info(f"Chat {chat_id} no está en GROUPS configurados — ignorado")
        return

    user_id    = new_member.id
    username   = new_member.username or f"user_{user_id}"
    first_name = new_member.first_name or ""

    if group["type"] == "VIP":
        await _process_new_vip_member(chat_id, user_id, username, first_name, group)
    else:
        await _process_new_free_member(chat_id, user_id, username, first_name, group)


async def detect_new_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Fallback para grupos legacy que emiten new_chat_members en lugar de chat_member."""
    if not update.message or not update.message.new_chat_members:
        return
    chat_id = update.message.chat_id
    group   = get_group_by_id(chat_id)
    if not group:
        return

    logger.info(f"📨 new_chat_members evento en chat {chat_id} ({group['group_name']})")

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
    """
    Registra usuarios que escriben sin haber pasado por el evento de entrada.
    Usa caché en bot_data para evitar consultar la BD en cada mensaje.

    FIX: El cache_key se elimina cuando el usuario expira, para que pueda
    ser detectado correctamente si vuelve a escribir tras su expiración.
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

    cache_key = f"known_{chat_id}_{user_id}"
    if context.bot_data.get(cache_key):
        return

    existing = await db.get_user_by_id(user_id, chat_id)

    display   = first_name if first_name else (
        f"@{username}" if not username.startswith("user_") else f"Usuario {user_id}"
    )
    chat_link = f"tg://user?id={user_id}"

    if group["type"] == "FREE":
        is_new = await db.register_free_user(chat_id, user_id, username, first_name)
        if is_new:
            logger.info(f"📋 Potencial detectado en FREE {group['group_name']}: {display}")
            await _safe_send(
                group["admin_id"],
                f"📋 *Nuevo cliente potencial detectado*\n\n"
                f"👤 *Nombre:* {display}\n"
                f"🆔 *ID:* `{user_id}`\n"
                f"📌 *Grupo:* {group['group_name']}\n"
                f"🔗 [Abrir chat]({chat_link})\n\n"
                f"_Detectado al escribir en el grupo._",
                parse_mode="Markdown"
            )
        # Solo cachear si está registrado con plan activo (no expirado)
        # para no perder de vista a usuarios que escriben tras expirar
        if existing and existing.get("status") == "active":
            context.bot_data[cache_key] = True
        elif not existing:
            context.bot_data[cache_key] = True

    else:  # VIP
        if not existing:
            # No tiene registro: avisar al admin
            logger.info(f"⚠️ Sin registro en VIP {group['group_name']}: {display}")
            await _safe_send(
                group["admin_id"],
                f"⚠️ *Usuario sin registro en grupo VIP*\n\n"
                f"👤 *Nombre:* {display}\n"
                f"🆔 *ID:* `{user_id}`\n"
                f"📌 *Grupo:* {group['group_name']}\n"
                f"🔗 [Abrir chat]({chat_link})\n\n"
                f"Para activarlo: `/add {user_id} semanal`\n"
                f"Para expulsarlo: hazlo desde Telegram directamente.",
                parse_mode="Markdown"
            )
        elif existing.get("status") == "active":
            # Cachear solo si está activo
            context.bot_data[cache_key] = True
        # Si está expirado, NO cachear (para que el scheduler lo procese)


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
        [InlineKeyboardButton(f"⏱ Trial: {trial_str}",                              callback_data=f"cfg_trial_{group_id}")],
        [InlineKeyboardButton(f"📅 Días semanal: {cfg_semanal['days']}d",            callback_data=f"cfg_dur_semanal_{group_id}")],
        [InlineKeyboardButton(f"💲 Precio semanal: {fmt_price(cfg_semanal['price'])}", callback_data=f"cfg_price_semanal_{group_id}")],
        [InlineKeyboardButton(f"📆 Días mensual: {cfg_mensual['days']}d",            callback_data=f"cfg_dur_mensual_{group_id}")],
        [InlineKeyboardButton(f"💲 Precio mensual: {fmt_price(cfg_mensual['price'])}", callback_data=f"cfg_price_mensual_{group_id}")],
        [InlineKeyboardButton("💳 Configurar Pago",                                 callback_data=f"cfg_payment_{group_id}")],
        [InlineKeyboardButton("🔔 Configurar Avisos",                               callback_data=f"cfg_warnings_{group_id}")],
        [InlineKeyboardButton("🔙 Volver",                                            callback_data=f"select_group_{group_id}")]
    ]
    await query.edit_message_text(
        f"⚙️ *Configuración - {group['group_name']}*\n\n"
        f"• ⏱ *Trial:* {trial_str}\n"
        f"• 📅 *Semanal:* {cfg_semanal['days']} días | {fmt_price(cfg_semanal['price'])} base\n"
        f"• 📆 *Mensual:* {cfg_mensual['days']} días | {fmt_price(cfg_mensual['price'])} base\n\n"
        f"_Con `/add` puedes sobreescribir precio y días por usuario._",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


async def cfg_trial_request(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query
    await query.answer()
    context.user_data['cfg_field']    = 'trial_minutes'
    context.user_data['cfg_group_id'] = group_id
    cfg         = get_group_plan_config(group_id, "trial")
    current_str = fmt_minutes(cfg.get('minutes', 1440))
    await query.edit_message_text(
        f"⏱ *Cambiar duración del Trial*\n\n"
        f"Valor actual: *{current_str}*\n\n"
        f"Envía el número de *minutos*:\n"
        f"• 20 min = 20\n• 1 hora = 60\n• 1 día = 1440\n• 7 días = 10080\n\n"
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
        f"💲 *Precio base - {plan_name}*\n\n"
        f"Valor actual: *{fmt_price(cfg['price'])}*\n\n"
        f"Envía el precio (ej: `10`, `5.99`, `3.50`)\n\n"
        f"*Escribe 'cancelar' para cancelar.*",
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
        f"Envía el número de días (ej: `7`, `14`, `30`)\n\n"
        f"*Escribe 'cancelar' para cancelar.*",
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
            await update.message.reply_text("❌ Ingresa los minutos como entero. Ej: `20`, `1440`", parse_mode="Markdown")
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

    if is_minutes:
        label = f"Trial: *{fmt_minutes(value)}*"
    elif field == "price_semanal":
        label = f"Precio semanal base: *{fmt_price(value)}*"
    elif field == "price_mensual":
        label = f"Precio mensual base: *{fmt_price(value)}*"
    elif field == "duration_semanal":
        label = f"Duración semanal: *{value} días*"
    elif field == "duration_mensual":
        label = f"Duración mensual: *{value} días*"
    else:
        label = f"{field}: *{value}*"

    await update.message.reply_text(
        f"✅ *Configuración actualizada*\n\n• {label}\n\nAplica como valor por defecto en *{group['group_name']}*.",
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
        msg += (
            f"{emoji} *{group['group_name']}*\n"
            f"   🆔 ID: `{group['group_id']}`\n"
            f"   👑 Admin: `{group['admin_id']}`\n\n"
        )
    await update.message.reply_text(msg, parse_mode="Markdown")


async def handle_edit_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        return

    # 🔥 PRIORIDAD: input de broadcast (AGREGAR ESTO)
    if context.user_data.get('broadcast_step') == 'message' and context.user_data.get('broadcast_group_id'):
        await handle_broadcast_input(update, context)
        return

    # Prioridad: input de configuración de avisos
    if context.user_data.get('warning_step') and context.user_data.get('warning_group_id'):
        await handle_warning_input(update, context)
        return

    # Prioridad: input de configuración de precios/trial
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
        await update.message.reply_text(f"✅ *Nombre guardado:* {text}", parse_mode="Markdown")
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
                f"✅ *Administrador guardado:* `{new_admin}`", parse_mode="Markdown"
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
                chat_link = f"tg://user?id={user['user_id']}"
                name      = user.get('first_name', 'Usuario') or user.get('username', identifier)
                await update.message.reply_text(f"🔗 Chat con {name}:\n`{chat_link}`", parse_mode="Markdown")
            else:
                await update.message.reply_text(f"❌ No se encontró @{identifier}")
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")


async def sync_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "ℹ️ *Sincronización manual no disponible*\n\n"
        "La API de Telegram no permite a los bots listar miembros de un grupo.\n\n"
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
        msg += f"{emoji} *{group['group_name']}*: {len(users)} usuarios activos\n"
    msg += "\n\nℹ️ Los usuarios se registran al *entrar al grupo* automáticamente."
    await update.message.reply_text(msg, parse_mode="Markdown")


# ==================== CALLBACK HANDLER ====================

async def pay_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Comando /pagar - Envía información de pago al usuario por privado.
    """
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id

    target_group = None

    if update.effective_chat.type in ("group", "supergroup"):
        group = get_group_by_id(chat_id)
        if group and group.get("type") == "VIP":
            target_group = chat_id
        else:
            await update.message.reply_text(
                "❌ Este grupo no está configurado como VIP.",
                parse_mode="Markdown"
            )
            return
    else:
        current_group = context.user_data.get('current_group')
        if context.args and context.args[0].isdigit():
            target_group = int(context.args[0])
        elif current_group:
            target_group = current_group
        else:
            await update.message.reply_text(
                "❌ *Uso del comando /pagar*" + chr(10) + chr(10) +
                "• En un grupo VIP: simplemente escribe `/pagar`" + chr(10) +
                "• En privado: `/pagar ID_DEL_GRUPO`" + chr(10) +
                "• O selecciona un grupo primero con /start",
                parse_mode="Markdown"
            )
            return

    group = get_group_by_id(target_group)
    if not group:
        await update.message.reply_text("❌ Grupo no encontrado")
        return

    if group.get("type") != "VIP":
        await update.message.reply_text("❌ Este comando solo está disponible para grupos VIP")
        return

    success = await send_payment_info(context.bot, user_id, target_group, triggered_by="comando /pagar")

    if success:
        if update.effective_chat.type in ("group", "supergroup"):
            await update.message.reply_text(
                "💳 *Te he enviado los datos de pago por privado.*" + chr(10) +
                "Revisa tu chat conmigo para ver la información.",
                parse_mode="Markdown"
            )
        else:
            await update.message.reply_text(
                "💳 *Datos de pago enviados.*" + chr(10) +
                "Revisa el mensaje anterior para ver la información de pago.",
                parse_mode="Markdown"
            )
    else:
        await update.message.reply_text(
            "❌ No se pudieron enviar los datos de pago. "
            "Asegúrate de haber iniciado una conversación privada conmigo primero (/start).",
            parse_mode="Markdown"
        )


async def config_payment_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Comando /configpago - Configura los datos de pago para un grupo VIP.
    """
    user_id = update.effective_user.id

    if not context.args:
        await update.message.reply_text(
            "❌ *Uso: `/configpago group_id`*" + chr(10) + chr(10) +
            "Ejemplo: `/configpago -1001234567890`" + chr(10) + chr(10) +
            "Después te pediré los datos bancarios y el contacto para comprobantes.",
            parse_mode="Markdown"
        )
        return

    try:
        group_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ El ID del grupo debe ser un número")
        return

    if not can_manage_group(user_id, group_id) and user_id != SUPER_ADMIN_ID:
        await update.message.reply_text("❌ No autorizado para gestionar este grupo")
        return

    group = get_group_by_id(group_id)
    if not group:
        await update.message.reply_text("❌ Grupo no encontrado")
        return

    if group.get("type") != "VIP":
        await update.message.reply_text("❌ Este comando solo es para grupos VIP")
        return

    context.user_data['config_payment_group_id'] = group_id
    context.user_data['config_payment_step'] = 'bank_data'

    settings = group.get("settings", {})
    current_bank = settings.get("bank_data", "")

    await update.message.reply_text(
        "⚙️ *Configuración de Pago — " + group['group_name'] + "*" + chr(10) + chr(10) +
        "Paso 1/3: *Datos bancarios*" + chr(10) +
        "Envía los datos de transferencia bancaria (texto libre)." + chr(10) +
        "Puedes incluir: banco, tipo de cuenta, número de cuenta, nombre del titular, cédula, etc." + chr(10) + chr(10) +
        "📋 *Actual:*" + chr(10) + "`" + (current_bank or "No configurado") + "`" + chr(10) + chr(10) +
        "*Escribe 'saltar' para dejar el valor actual, o 'eliminar' para borrarlo.*",
        parse_mode="Markdown"
    )


async def handle_payment_config_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler para recibir los pasos de configuración de pago."""
    if update.effective_chat.type != "private":
        return

    step = context.user_data.get('config_payment_step')
    group_id = context.user_data.get('config_payment_group_id')

    if not step or not group_id:
        return

    if not can_manage_group(update.effective_user.id, group_id) and update.effective_user.id != SUPER_ADMIN_ID:
        return

    text = update.message.text.strip()
    group = get_group_by_id(group_id)
    if not group:
        await update.message.reply_text("❌ Grupo no encontrado")
        context.user_data.pop('config_payment_step', None)
        context.user_data.pop('config_payment_group_id', None)
        return

    settings = dict(group.get("settings", {}))

    if step == 'bank_data':
        if text.lower() == 'saltar':
            pass
        elif text.lower() == 'eliminar':
            settings.pop('bank_data', None)
        else:
            settings['bank_data'] = text

        # GUARDAR EN BD INMEDIATAMENTE
        group["settings"] = settings
        await db.update_group_fields(group_id, {'settings': settings})

        context.user_data['config_payment_step'] = 'payment_contact'
        current_contact = settings.get("payment_contact", "")
        await update.message.reply_text(
            "✅ *Datos bancarios guardados.*" + chr(10) + chr(10) +
            "Paso 2/3: *Contacto para comprobantes*" + chr(10) +
            "Envía el username de Telegram donde se enviarán los comprobantes." + chr(10) +
            "Ejemplo: `@ZonaEcFuego` o `ZonaEcFuego`" + chr(10) + chr(10) +
            "📋 *Actual:* `" + (current_contact or "No configurado") + "`" + chr(10) + chr(10) +
            "*Escribe 'saltar' para dejar el valor actual.*",
            parse_mode="Markdown"
        )
        return

    elif step == 'payment_contact':
        if text.lower() == 'saltar':
            pass
        else:
            contact = text.lstrip('@').strip()
            settings['payment_contact'] = contact

        # GUARDAR EN BD INMEDIATAMENTE
        group["settings"] = settings
        await db.update_group_fields(group_id, {'settings': settings})

        context.user_data['config_payment_step'] = 'paypal_data'
        current_paypal = settings.get("paypal_data", "")
        await update.message.reply_text(
            "✅ *Contacto guardado.*" + chr(10) + chr(10) +
            "Paso 3/3: *PayPal (opcional)*" + chr(10) +
            "Envía los datos de PayPal si deseas ofrecerlo como método de pago." + chr(10) +
            "Ejemplo: email de PayPal o link de pago." + chr(10) + chr(10) +
            "📋 *Actual:* `" + (current_paypal or "No configurado") + "`" + chr(10) + chr(10) +
            "*Escribe 'saltar' para omitir, o 'eliminar' para borrar.*",
            parse_mode="Markdown"
        )
        return

    elif step == 'paypal_data':
        if text.lower() == 'saltar':
            pass
        elif text.lower() == 'eliminar':
            settings.pop('paypal_data', None)
        else:
            settings['paypal_data'] = text

        group["settings"] = settings
        await db.update_group_fields(group_id, {'settings': settings})

        context.user_data.pop('config_payment_step', None)
        context.user_data.pop('config_payment_group_id', None)

        bank = settings.get('bank_data', 'No configurado')
        contact = settings.get('payment_contact', 'No configurado')
        paypal = settings.get('paypal_data', 'No configurado')

        await update.message.reply_text(
            "✅ *Configuración de pago completada*" + chr(10) + chr(10) +
            "📌 *Grupo:* " + group['group_name'] + chr(10) + chr(10) +
            "🏦 *Bancaria:*" + chr(10) + "`" + bank + "`" + chr(10) + chr(10) +
            "📤 *Contacto:* @" + contact + chr(10) + chr(10) +
            "🅿️ *PayPal:* `" + paypal + "`" + chr(10) + chr(10) +
            "Los usuarios ahora podrán usar `/pagar` o el botón 💳 para ver esta información.",
            parse_mode="Markdown"
        )
        return


async def config_payment_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    """Callback para iniciar configuración de pago desde el menú de ajustes."""
    query = update.callback_query
    await query.answer()

    if not can_manage_group(query.from_user.id, group_id) and query.from_user.id != SUPER_ADMIN_ID:
        await query.edit_message_text("❌ No autorizado")
        return

    group = get_group_by_id(group_id)
    if not group:
        await query.edit_message_text("❌ Grupo no encontrado")
        return

    context.user_data['config_payment_group_id'] = group_id
    context.user_data['config_payment_step'] = 'bank_data'

    settings = group.get("settings", {})
    current_bank = settings.get("bank_data", "")

    await query.edit_message_text(
        "⚙️ *Configuración de Pago — " + group['group_name'] + "*" + chr(10) + chr(10) +
        "Paso 1/3: *Datos bancarios*" + chr(10) +
        "Envía los datos de transferencia bancaria (texto libre)." + chr(10) +
        "Puedes incluir: banco, tipo de cuenta, número de cuenta, nombre del titular, cédula, etc." + chr(10) + chr(10) +
        "📋 *Actual:*" + chr(10) + "`" + (current_bank or "No configurado") + "`" + chr(10) + chr(10) +
        "*Escribe 'saltar' para dejar el valor actual, o 'eliminar' para borrarlo.*",
        parse_mode="Markdown"
    )

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data  = query.data
    logger.info(f"🔔 CALLBACK: {data}")
    try:
        if data == "add_user":
            await query.answer()
            await query.edit_message_text("📝 Usa el comando: `/add @username plan` o `/add ID plan`", parse_mode="Markdown")
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
        elif data == "vip_groups":
            await query.answer()
            await show_groups_by_type(update, context, "VIP")
        elif data == "free_groups":
            await query.answer()
            await show_groups_by_type(update, context, "FREE")
        elif data == "total_earnings":
            await query.answer()
            await total_earnings(update, context)
        elif data == "all_groups":
            await query.answer()
            await list_groups(update, context)
        elif data == "add_group":
            await query.answer()
            await query.edit_message_text(
                "📝 Usa: `/addgroup group_id TIPO \"nombre\" admin_id`",
                parse_mode="Markdown"
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
        elif data == "view_vip_groups":
            await query.answer()
            await view_vip_groups(update, context)
        elif data == "view_free_groups":
            await query.answer()
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
        elif data.startswith("cfg_group_"):
            group_id = int(data.replace("cfg_group_", ""))
            await menu_group_settings(update, context, group_id)
        elif data.startswith("cfg_trial_"):
            group_id = int(data.replace("cfg_trial_", ""))
            await cfg_trial_request(update, context, group_id)
        elif data.startswith("cfg_price_semanal_"):
            group_id = int(data.replace("cfg_price_semanal_", ""))
            await cfg_price_request(update, context, group_id, "semanal")
        elif data.startswith("cfg_price_mensual_"):
            group_id = int(data.replace("cfg_price_mensual_", ""))
            await cfg_price_request(update, context, group_id, "mensual")
        elif data.startswith("cfg_dur_semanal_"):
            group_id = int(data.replace("cfg_dur_semanal_", ""))
            await cfg_duration_request(update, context, group_id, "semanal")
        elif data.startswith("cfg_dur_mensual_"):
            group_id = int(data.replace("cfg_dur_mensual_", ""))
            await cfg_duration_request(update, context, group_id, "mensual")
        elif data == "trial_stats":
            await trial_stats(update, context)
        elif data.startswith("pay_"):
            await query.answer("💳 Enviando datos de pago...")
            parts = data.split("_")
            if len(parts) >= 3:
                pay_group_id = int(parts[1])
                pay_user_id = int(parts[2])
                if query.from_user.id == pay_user_id:
                    await send_payment_info(context.bot, pay_user_id, pay_group_id, triggered_by="botón Pagar")
                else:
                    await query.answer("❌ Este botón no es para ti", show_alert=True)
            else:
                await query.answer("❌ Error en datos de pago", show_alert=True)
        elif data.startswith("cfg_payment_"):
            group_id = int(data.replace("cfg_payment_", ""))
            await config_payment_callback(update, context, group_id)
        elif data == "cfg_warnings":
            group_id = context.user_data.get('current_group')
            if group_id:
                await menu_warning_settings(update, context, group_id)
            else:
                await query.answer("❌ Selecciona un grupo primero", show_alert=True)
        elif data.startswith("cfg_warnings_"):
            group_id = int(data.replace("cfg_warnings_", ""))
            await menu_warning_settings(update, context, group_id)
        elif data.startswith("warn_toggle_"):
            group_id = int(data.replace("warn_toggle_", ""))
            await toggle_warnings(update, context, group_id)
        elif data.startswith("warn_add_"):
            group_id = int(data.replace("warn_add_", ""))
            await add_warning_step1(update, context, group_id)
        elif data.startswith("warn_edit_"):
            parts = data.split("_")
            group_id = int(parts[2])
            warning_index = int(parts[3])
            await edit_warning_step1(update, context, group_id, warning_index)
        elif data.startswith("warn_delete_"):
            parts = data.split("_")
            group_id = int(parts[2])
            warning_index = int(parts[3])
            await delete_warning(update, context, group_id, warning_index)
        elif data.startswith("warn_test_"):
            group_id = int(data.replace("warn_test_", ""))
            await test_warning(update, context, group_id)
        elif data.startswith("bc_filter_"):
            await broadcast_callback_handler(update, context)
        elif data.startswith("bc_cancel_"):
            await broadcast_callback_handler(update, context)
        elif data.startswith("bc_confirm_"):
            await broadcast_callback_handler(update, context)
        elif data == "broadcast_menu":
            await broadcast_menu_callback(update, context)
        elif data.startswith("bc_history_"):
            await broadcast_history_callback(update, context)
        else:
            await query.answer()
            logger.warning(f"Callback desconocido: {data}")

    except Exception as e:
        logger.error(f"❌ Error en callback '{data}': {e}", exc_info=True)
        try:
            await query.answer("❌ Ocurrió un error, intenta de nuevo", show_alert=True)
        except Exception:
            pass


# ==================== SISTEMA DE AVISOS DE EXPIRACIÓN ====================

async def send_trial_warnings():
    """
    Envía avisos de expiración a usuarios con trial por vencer.
    Revisa cada grupo VIP configurado y envía avisos según la configuración
    personalizada de cada grupo.
    """
    logger.info("⏰ Enviando avisos de expiración de trial...")
    
    for group in GROUPS:
        if group.get("type", "VIP") != "VIP":
            continue
        
        group_id = group["group_id"]
        
        # Obtener configuración de avisos para este grupo
        try:
            warning_config = await db.get_warning_config(group_id)
        except Exception as e:
            logger.error(f"❌ Error obteniendo warning_config para grupo {group_id}: {e}")
            continue
        
        if not warning_config or not warning_config.get("enabled", False):
            logger.debug(f"⏭️ Avisos deshabilitados para grupo {group_id}")
            continue
        
        warnings = warning_config.get("warnings", [])
        if not warnings:
            continue
        
        for warning in warnings:
            minutes_before = warning.get("minutes_before", 15)
            message_template = warning.get("message", "⏳ Tu trial expira pronto")
            
            # Obtener usuarios que necesitan este aviso
            try:
                users = await db.get_users_for_warning(group_id, minutes_before)
            except Exception as e:
                logger.error(f"❌ Error obteniendo usuarios para aviso en grupo {group_id}: {e}")
                continue
            
            if not users:
                continue
            
            warning_type = f"warning_{minutes_before}min"
            
            for user in users:
                user_id = user['user_id']
                end_date = user['end_date']
                remaining = end_date - datetime.now()
                mins_left = max(0, int(remaining.total_seconds() / 60))
                
                if mins_left <= 0:
                    continue
                
                # Formatear el mensaje
                expiry_time = end_date.strftime('%H:%M')
                expiry_datetime = end_date.strftime('%d/%m/%Y %H:%M')
                
                # Reemplazar variables en el mensaje
                message = message_template.replace(
                    "{minutes_left}", str(mins_left)
                ).replace(
                    "{expiry_time}", expiry_time
                ).replace(
                    "{expiry_datetime}", expiry_datetime
                ).replace(
                    "{group_name}", group.get("group_name", "VIP")
                ).replace(
                    "{user_name}", user.get("first_name", "Usuario") or "Usuario"
                )
                
                # Enviar aviso
                try:
                    await bot_app.bot.send_message(
                        user_id,
                        message,
                        reply_markup=get_payment_keyboard(group_id, user_id),
                        parse_mode="Markdown"
                    )
                    await db.log_warning_sent(user_id, group_id, warning_type)
                    logger.info(f"⚠️ Aviso enviado a {user_id}: {mins_left} min restantes (grupo {group_id})")
                except Exception as e:
                    logger.warning(f"No se pudo enviar aviso a {user_id}: {e}")


# ==================== TAREAS PROGRAMADAS ====================

async def check_expired_subscriptions():
    """
    Verifica y expulsa usuarios con suscripciones vencidas.

    Garantías de correctitud:
      - expire_user() usa RETURNING + condición `kicked_at IS NULL` para garantizar
        que solo UN ciclo procese a cada usuario (operación atómica en BD).
      - _kick_user_with_retry() verifica membresía antes de intentar banear.
      - Los grupos se procesan en paralelo para mayor velocidad.
      - Los errores por grupo se loggean individualmente sin detener los demás.
    """
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
            display    = (
                first_name or
                (f"@{username}" if username and not username.startswith('user_') else f"ID:{user_id}")
            )
            plan       = user.get('plan', 'desconocido')
            end_date   = user.get('end_date')
            expiry_str = end_date.strftime('%d/%m/%Y %H:%M') if end_date else "N/A"
            chat_link  = f"tg://user?id={user_id}"

            # Operación atómica: solo continúa si esta instancia "ganó" la carrera
            was_updated = await db.expire_user(user_id, group_id)
            if not was_updated:
                logger.info(f"Usuario {user_id} ya fue procesado por otro ciclo — saltando")
                continue

            kick_success = await _kick_user_with_retry(group_id, user_id)

            status_emoji = "🚫" if kick_success else "⚠️"
            action_text  = (
                "expulsado del grupo" if kick_success
                else "marcado como expirado (expulsión automática fallida — acción manual requerida)"
            )

            await _safe_send(
                group["admin_id"],
                f"{status_emoji} *Suscripción expirada*\n\n"
                f"👤 *Usuario:* [{display}]({chat_link})\n"
                f"📋 *Plan:* {plan}\n"
                f"📅 *Expiró:* {expiry_str}\n"
                f"📌 *Grupo:* {group['group_name']}\n\n"
                f"✅ Usuario {action_text}.",
                parse_mode="Markdown"
            )

            await _safe_send(
                user_id,
                f"⏰ *Tu acceso ha expirado*\n\n"
                f"Tu plan *{plan}* en el grupo *{group['group_name']}* ha vencido.\n\n"
                f"💳 ¿Quieres renovar? Presiona el botón para ver los datos de pago:",
                reply_markup=get_payment_keyboard(group_id, user_id),
                parse_mode="Markdown"
            )

            logger.info(
                f"{'✅' if kick_success else '⚠️'} {display} ({plan}) "
                f"procesado en {group['group_name']} — kick_success={kick_success}"
            )

    vip_groups = [g for g in GROUPS if g.get("type", "VIP") == "VIP"]
    results    = await asyncio.gather(
        *[_process_group(g) for g in vip_groups],
        return_exceptions=True
    )
    # Loggear excepciones sin silenciarlas
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
            await update.message.reply_text(
                "❌ Usa este comando dentro del grupo, o selecciona uno con /start primero."
            )
            return

    if not can_manage_group(user_id, target_chat) and user_id != SUPER_ADMIN_ID:
        await update.message.reply_text("❌ No autorizado")
        return

    group = get_group_by_id(target_chat)
    if not group:
        await update.message.reply_text(
            f"❌ El grupo `{target_chat}` no está registrado en el bot.", parse_mode="Markdown"
        )
        return

    lines = [f"🔍 *Diagnóstico — {group['group_name']}*\n"]
    lines.append(f"🆔 ID: `{target_chat}`")
    lines.append(f"📋 Tipo: {group.get('type', 'VIP')}")
    lines.append(f"👑 Admin registrado: `{group['admin_id']}`\n")

    try:
        bot_member = await context.bot.get_chat_member(target_chat, context.bot.id)
        status = bot_member.status
        lines.append(f"🤖 *Estado del bot:* `{status}`")
        if status == "administrator":
            lines.append("✅ Bot es administrador — detección y expulsión completas")
            if hasattr(bot_member, 'can_restrict_members'):
                can_ban = getattr(bot_member, 'can_restrict_members', False)
                lines.append(
                    f"   • Puede banear/expulsar: "
                    f"{'✅' if can_ban else '❌ (necesario para expulsión automática)'}"
                )
        elif status == "member":
            lines.append("⚠️ Bot es miembro normal")
            lines.append("   → Detección de nuevos miembros: ✅ funciona")
            lines.append("   → Expulsión automática: ❌ no funcionará")
            lines.append("   → Solución: promover el bot a admin con permiso 'Banear usuarios'")
        else:
            lines.append(f"❌ Estado inesperado: {status}")
    except Exception as e:
        lines.append(f"❌ Error al verificar permisos: `{e}`")

    lines.append("")
    lines.append("📡 *Handlers activos:*")
    lines.append("• `ChatMemberHandler` (método principal v20+) ✅")
    lines.append("• `NEW_CHAT_MEMBERS` (fallback legacy) ✅")
    lines.append("• Verificación de expirados: cada 5 minutos ✅")
    lines.append("")
    lines.append("💡 *Si los miembros no se detectan:*")
    lines.append("1. Verifica que el bot esté dentro del grupo")
    lines.append("2. Entra al grupo con una cuenta de prueba y revisa los logs")
    lines.append("3. Usa `/add @usuario trial` para registrar manualmente")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


# ==================== CONFIGURACIÓN DE AVISOS (HANDLERS) ====================

async def menu_warning_settings(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    """Muestra el menú de configuración de avisos para un grupo."""
    query = update.callback_query
    await query.answer()
    
    group = get_group_by_id(group_id)
    if not group:
        await query.edit_message_text("❌ Grupo no encontrado")
        return
    
    if not can_manage_group(query.from_user.id, group_id) and query.from_user.id != SUPER_ADMIN_ID:
        await query.edit_message_text("❌ No autorizado")
        return
    
    warning_config = await db.get_warning_config(group_id)
    enabled = warning_config.get("enabled", False)
    warnings = warning_config.get("warnings", [])
    
    status_emoji = "🟢" if enabled else "🔴"
    status_text = "ACTIVADOS" if enabled else "DESACTIVADOS"
    
    msg = (
        f"🔔 *Configuración de Avisos — {group['group_name']}*\n\n"
        f"Estado: {status_emoji} *{status_text}*\n\n"
        f"*Avisos configurados ({len(warnings)}):*\n"
    )
    
    for i, w in enumerate(warnings, 1):
        mins = w.get("minutes_before", 15)
        msg += f"\n{i}. ⏰ {mins} minutos antes de expirar"
    
    if not warnings:
        msg += "\n_Ninguno configurado_"
    
    msg += (
        f"\n\n*Variables disponibles en mensajes:*\n"
        f"• `{{minutes_left}}` — minutos restantes\n"
        f"• `{{expiry_time}}` — hora de expiración (HH:MM)\n"
        f"• `{{expiry_datetime}}` — fecha y hora (DD/MM/YYYY HH:MM)\n"
        f"• `{{group_name}}` — nombre del grupo\n"
        f"• `{{user_name}}` — nombre del usuario"
    )
    
    keyboard = [
        [InlineKeyboardButton(f"{status_emoji} {'Desactivar' if enabled else 'Activar'} avisos", 
                              callback_data=f"warn_toggle_{group_id}")],
        [InlineKeyboardButton("➕ Añadir aviso", callback_data=f"warn_add_{group_id}")],
    ]
    
    # Botones para editar/eliminar cada aviso
    for i, w in enumerate(warnings):
        mins = w.get("minutes_before", 15)
        keyboard.append([
            InlineKeyboardButton(f"✏️ Editar {mins}min", callback_data=f"warn_edit_{group_id}_{i}"),
            InlineKeyboardButton("🗑️ Eliminar", callback_data=f"warn_delete_{group_id}_{i}")
        ])
    
    keyboard.append([InlineKeyboardButton("🧪 Probar aviso", callback_data=f"warn_test_{group_id}")])
    keyboard.append([InlineKeyboardButton("🔙 Volver", callback_data=f"cfg_group_{group_id}")])
    
    await query.edit_message_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


async def toggle_warnings(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    """Activa o desactiva los avisos para un grupo."""
    query = update.callback_query
    await query.answer()
    
    if not can_manage_group(query.from_user.id, group_id) and query.from_user.id != SUPER_ADMIN_ID:
        await query.answer("❌ No autorizado", show_alert=True)
        return
    
    warning_config = await db.get_warning_config(group_id)
    warning_config["enabled"] = not warning_config.get("enabled", False)
    
    await db.save_warning_config(group_id, warning_config)
    
    await menu_warning_settings(update, context, group_id)


async def add_warning_step1(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    """Paso 1: Pedir minutos antes para el nuevo aviso."""
    query = update.callback_query
    await query.answer()
    
    if not can_manage_group(query.from_user.id, group_id) and query.from_user.id != SUPER_ADMIN_ID:
        await query.answer("❌ No autorizado", show_alert=True)
        return
    
    context.user_data['warning_group_id'] = group_id
    context.user_data['warning_step'] = 'minutes'
    context.user_data['warning_index'] = None  # Nuevo aviso
    
    await query.edit_message_text(
        "➕ *Nuevo aviso de expiración*\n\n"
        "Envía cuántos minutos antes de expirar debe enviarse el aviso.\n\n"
        "*Ejemplos:*\n"
        "• `15` — 15 minutos antes\n"
        "• `60` — 1 hora antes\n"
        "• `5` — 5 minutos antes (último aviso)\n\n"
        "*Escribe 'cancelar' para cancelar.*",
        parse_mode="Markdown"
    )


async def edit_warning_step1(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int, warning_index: int):
    """Paso 1 de edición: Pedir nuevos minutos o saltar."""
    query = update.callback_query
    await query.answer()
    
    if not can_manage_group(query.from_user.id, group_id) and query.from_user.id != SUPER_ADMIN_ID:
        await query.answer("❌ No autorizado", show_alert=True)
        return
    
    warning_config = await db.get_warning_config(group_id)
    warnings = warning_config.get("warnings", [])
    
    if warning_index >= len(warnings):
        await query.edit_message_text("❌ Aviso no encontrado")
        return
    
    existing = warnings[warning_index]
    
    context.user_data['warning_group_id'] = group_id
    context.user_data['warning_step'] = 'edit_minutes'
    context.user_data['warning_index'] = warning_index
    
    await query.edit_message_text(
        f"✏️ *Editar aviso*\n\n"
        f"Actual: {existing.get('minutes_before', 15)} minutos antes\n\n"
        f"Envía los nuevos minutos, o escribe 'saltar' para mantener el actual.\n\n"
        f"*Escribe 'cancelar' para cancelar.*",
        parse_mode="Markdown"
    )


async def delete_warning(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int, warning_index: int):
    """Elimina un aviso de la configuración."""
    query = update.callback_query
    await query.answer()
    
    if not can_manage_group(query.from_user.id, group_id) and query.from_user.id != SUPER_ADMIN_ID:
        await query.answer("❌ No autorizado", show_alert=True)
        return
    
    warning_config = await db.get_warning_config(group_id)
    warnings = warning_config.get("warnings", [])
    
    if warning_index < len(warnings):
        removed = warnings.pop(warning_index)
        warning_config["warnings"] = warnings
        await db.save_warning_config(group_id, warning_config)
        await query.edit_message_text(
            f"✅ Aviso eliminado ({removed.get('minutes_before', 15)} min)",
            parse_mode="Markdown"
        )
        await asyncio.sleep(1)
    
    await menu_warning_settings(update, context, group_id)


async def test_warning(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    """Envía un aviso de prueba al admin."""
    query = update.callback_query
    await query.answer()
    
    if not can_manage_group(query.from_user.id, group_id) and query.from_user.id != SUPER_ADMIN_ID:
        await query.answer("❌ No autorizado", show_alert=True)
        return
    
    warning_config = await db.get_warning_config(group_id)
    warnings = warning_config.get("warnings", [])
    
    if not warnings:
        await query.answer("❌ No hay avisos configurados", show_alert=True)
        return
    
    # Usar el primer aviso como prueba
    test_warning = warnings[0]
    message_template = test_warning.get("message", "⏳ Tu trial expira pronto")
    
    # Simular valores
    message = message_template.replace(
        "{minutes_left}", "15"
    ).replace(
        "{expiry_time}", "22:45"
    ).replace(
        "{expiry_datetime}", "15/06/2026 22:45"
    ).replace(
        "{group_name}", get_group_by_id(group_id).get("group_name", "VIP")
    ).replace(
        "{user_name}", query.from_user.first_name or "Usuario"
    )
    
    try:
        await context.bot.send_message(
            query.from_user.id,
            f"🧪 *AVISO DE PRUEBA*\n\n"
            f"Este es cómo se vería el aviso para un usuario:\n\n"
            f"---\n{message}\n---\n\n"
            f"Si te gusta, los avisos están listos para enviarse.",
            reply_markup=get_payment_keyboard(group_id, query.from_user.id),
            parse_mode="Markdown"
        )
        await query.edit_message_text(
            "✅ *Aviso de prueba enviado a tu chat privado*\n\n"
            "Revisa cómo se ve y decide si quieres ajustar el mensaje.",
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.warning(f"Error enviando prueba: {e}")
        await query.edit_message_text(
            "❌ Error enviando prueba. Asegúrate de haber iniciado chat privado con el bot.",
            parse_mode="Markdown"
        )
    
    await asyncio.sleep(2)
    await menu_warning_settings(update, context, group_id)


async def handle_warning_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Procesa input de texto para configuración de avisos."""
    if update.effective_chat.type != "private":
        return
    
    step = context.user_data.get('warning_step')
    group_id = context.user_data.get('warning_group_id')
    
    if not step or not group_id:
        return
    
    if not can_manage_group(update.effective_user.id, group_id) and update.effective_user.id != SUPER_ADMIN_ID:
        await update.message.reply_text("❌ No autorizado")
        return
    
    text = update.message.text.strip().lower()
    
    if text == 'cancelar':
        context.user_data.pop('warning_step', None)
        context.user_data.pop('warning_group_id', None)
        context.user_data.pop('warning_index', None)
        context.user_data.pop('warning_minutes', None)
        await update.message.reply_text("❌ Configuración de aviso cancelada")
        return
    
    warning_config = await db.get_warning_config(group_id)
    warnings = warning_config.get("warnings", [])
    warning_index = context.user_data.get('warning_index')
    
    if step == 'minutes' or step == 'edit_minutes':
        # Validar minutos
        if text == 'saltar' and step == 'edit_minutes':
            # Mantener minutos actuales, pasar al mensaje
            context.user_data['warning_step'] = 'edit_message'
            existing = warnings[warning_index] if warning_index is not None and warning_index < len(warnings) else {}
            current_msg = existing.get("message", "⏳ Tu trial expira en {minutes_left} minutos")
            await update.message.reply_text(
                f"✏️ *Editar mensaje del aviso*\n\n"
                f"Mensaje actual:\n`{current_msg}`\n\n"
                f"Envía el nuevo mensaje, o escribe 'saltar' para mantenerlo.\n\n"
                f"*Variables disponibles:*\n"
                f"• `{{minutes_left}}` — minutos restantes\n"
                f"• `{{expiry_time}}` — hora de expiración\n"
                f"• `{{expiry_datetime}}` — fecha y hora\n"
                f"• `{{group_name}}` — nombre del grupo\n"
                f"• `{{user_name}}` — nombre del usuario\n\n"
                f"*Escribe 'cancelar' para cancelar.*",
                parse_mode="Markdown"
            )
            return
        
        try:
            minutes = int(text)
            if minutes <= 0 or minutes > 1440:
                raise ValueError
        except ValueError:
            await update.message.reply_text(
                "❌ Debe ser un número entre 1 y 1440 (24 horas).\n"
                "Ejemplos: 5, 15, 60, 120",
                parse_mode="Markdown"
            )
            return
        
        context.user_data['warning_minutes'] = minutes
        
        if step == 'edit_minutes':
            # Guardar minutos y pasar a editar mensaje
            if warning_index is not None and warning_index < len(warnings):
                warnings[warning_index]["minutes_before"] = minutes
                warning_config["warnings"] = warnings
                await db.save_warning_config(group_id, warning_config)
            
            context.user_data['warning_step'] = 'edit_message'
            existing = warnings[warning_index] if warning_index is not None and warning_index < len(warnings) else {}
            current_msg = existing.get("message", "⏳ Tu trial expira en {minutes_left} minutos")
            await update.message.reply_text(
                f"✏️ *Editar mensaje del aviso*\n\n"
                f"Mensaje actual:\n`{current_msg}`\n\n"
                f"Envía el nuevo mensaje, o escribe 'saltar' para mantenerlo.\n\n"
                f"*Variables disponibles:*\n"
                f"• `{{minutes_left}}` — minutos restantes\n"
                f"• `{{expiry_time}}` — hora de expiración\n"
                f"• `{{expiry_datetime}}` — fecha y hora\n"
                f"• `{{group_name}}` — nombre del grupo\n"
                f"• `{{user_name}}` — nombre del usuario\n\n"
                f"*Escribe 'cancelar' para cancelar.*",
                parse_mode="Markdown"
            )
            return
        
        # Nuevo aviso: paso 2 (mensaje)
        context.user_data['warning_step'] = 'message'
        await update.message.reply_text(
            f"✅ *Aviso a los {minutes} minutos* configurado.\n\n"
            f"Ahora envía el mensaje que recibirá el usuario.\n\n"
            f"*Variables disponibles:*\n"
            f"• `{{minutes_left}}` — minutos restantes\n"
            f"• `{{expiry_time}}` — hora de expiración (HH:MM)\n"
            f"• `{{expiry_datetime}}` — fecha y hora (DD/MM/YYYY HH:MM)\n"
            f"• `{{group_name}}` — nombre del grupo\n"
            f"• `{{user_name}}` — nombre del usuario\n\n"
            f"*Ejemplo:*\n"
            f"`⏳ Tu trial expira en {{minutes_left}} minutos!\n\n"
            f"📅 Expira: {{expiry_time}}\n\n"
            f"💳 ¿Quieres seguir? Presiona el botón:`\n\n"
            f"*Escribe 'cancelar' para cancelar.*",
            parse_mode="Markdown"
        )
        return
    
    elif step == 'message' or step == 'edit_message':
        if text == 'saltar' and step == 'edit_message':
            # Mantener mensaje actual
            context.user_data.pop('warning_step', None)
            context.user_data.pop('warning_group_id', None)
            context.user_data.pop('warning_index', None)
            context.user_data.pop('warning_minutes', None)
            await update.message.reply_text("✅ Aviso actualizado correctamente")
            # Botón para volver
            keyboard = [[InlineKeyboardButton("🔙 Volver a configuración de avisos", 
                                              callback_data=f"cfg_warnings_{group_id}")]]
            await update.message.reply_text(
                "🔔 Configuración guardada.",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return
        
        # Guardar mensaje
        message_text = update.message.text.strip()
        
        if step == 'edit_message' and warning_index is not None:
            # Editar existente
            if warning_index < len(warnings):
                warnings[warning_index]["message"] = message_text
                warning_config["warnings"] = warnings
                await db.save_warning_config(group_id, warning_config)
        else:
            # Nuevo aviso
            minutes = context.user_data.get('warning_minutes', 15)
            warnings.append({
                "minutes_before": minutes,
                "message": message_text
            })
            # Ordenar por minutos (mayor a menor para que el más lejano vaya primero)
            warnings.sort(key=lambda w: w.get("minutes_before", 0), reverse=True)
            warning_config["warnings"] = warnings
            await db.save_warning_config(group_id, warning_config)
        
        # Limpiar estado
        context.user_data.pop('warning_step', None)
        context.user_data.pop('warning_group_id', None)
        context.user_data.pop('warning_index', None)
        context.user_data.pop('warning_minutes', None)
        
        await update.message.reply_text("✅ *Aviso guardado correctamente*")
        # Botón para volver
        keyboard = [[InlineKeyboardButton("🔙 Volver a configuración de avisos", 
                                          callback_data=f"cfg_warnings_{group_id}")]]
        await update.message.reply_text(
            "🔔 Configuración completada.",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

# ==================== BROADCAST MASIVO A USUARIOS TRIAL ====================

async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Comando /broadcast - Inicia el flujo de envío masivo a usuarios trial.
    
    Uso: /broadcast group_id
    """
    user_id = update.effective_user.id
    
    if not context.args:
        await update.message.reply_text(
            "❌ *Uso: `/broadcast ID_DEL_GRUPO`*\n\n"
            "Ejemplo: `/broadcast -1001234567890`\n\n"
            "Después te pediré el mensaje y el filtro de destinatarios.",
            parse_mode="Markdown"
        )
        return
    
    try:
        group_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ El ID del grupo debe ser un número")
        return
    
    if not can_manage_group(user_id, group_id) and user_id != SUPER_ADMIN_ID:
        await update.message.reply_text("❌ No autorizado para gestionar este grupo")
        return
    
    group = get_group_by_id(group_id)
    if not group:
        await update.message.reply_text("❌ Grupo no encontrado")
        return
    
    # Iniciar flujo de configuración
    context.user_data['broadcast_group_id'] = group_id
    context.user_data['broadcast_step'] = 'filter'
    
    keyboard = [
        [InlineKeyboardButton("🆓 Solo trial SIN compra", callback_data=f"bc_filter_{group_id}_trial_only")],
        [InlineKeyboardButton("💳 Solo con suscripción activa", callback_data=f"bc_filter_{group_id}_subscribed_only")],
        [InlineKeyboardButton("⏰ Solo expirados no renovados", callback_data=f"bc_filter_{group_id}_expired_only")],
        [InlineKeyboardButton("👥 TODOS los que usaron trial", callback_data=f"bc_filter_{group_id}_all_trial")],
        [InlineKeyboardButton("❌ Cancelar", callback_data=f"bc_cancel_{group_id}")]
    ]
    
    await update.message.reply_text(
        f"📢 *Broadcast Masivo — {group['group_name']}*\n\n"
        f"Selecciona a quién quieres enviar el mensaje:\n\n"
        f"• 🆓 *Trial sin compra* — Usaron trial pero nunca pagaron\n"
        f"• 💳 *Con suscripción* — Usaron trial y tienen plan activo\n"
        f"• ⏰ *Expirados* — Compraron antes pero ahora están expirados\n"
        f"• 👥 *Todos* — Cualquiera que haya usado trial\n\n"
        f"_Puedes personalizar el mensaje en el siguiente paso._",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


async def broadcast_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja callbacks del menú de broadcast."""
    query = update.callback_query
    data = query.data
    await query.answer()
    
    if data.startswith("bc_filter_"):
        parts = data.split("_")
        group_id = int(parts[2])
        filter_type = parts[3]
        
        # Guardar filtro y pedir mensaje
        context.user_data['broadcast_group_id'] = group_id
        context.user_data['broadcast_filter'] = filter_type
        context.user_data['broadcast_step'] = 'message'
        
        filter_names = {
            'trial_only': '🆓 Solo trial sin compra',
            'subscribed_only': '💳 Solo con suscripción activa',
            'expired_only': '⏰ Solo expirados no renovados',
            'all_trial': '👥 Todos los que usaron trial'
        }
        
        # Obtener preview de cuántos usuarios
        targets = await db.get_broadcast_targets(group_id, filter_type)
        
        await query.edit_message_text(
            f"📢 *Broadcast — Configurar mensaje*\n\n"
            f"Filtro: {filter_names.get(filter_type, filter_type)}\n"
            f"Destinatarios estimados: *{len(targets)}* usuarios\n\n"
            f"Envía el mensaje que quieres enviar.\n\n"
            f"*Variables disponibles:*\n"
            f"• `{{user_name}}` — nombre del usuario\n"
            f"• `{{group_name}}` — nombre del grupo\n"
            f"• `{{username}}` — @username del usuario\n\n"
            f"*Puedes usar Markdown:* negritas, emojis, links, etc.\n\n"
            f"*Escribe 'cancelar' para cancelar.*",
            parse_mode="Markdown"
        )
        
    elif data.startswith("bc_cancel_"):
        context.user_data.pop('broadcast_group_id', None)
        context.user_data.pop('broadcast_step', None)
        context.user_data.pop('broadcast_filter', None)
        await query.edit_message_text("❌ Broadcast cancelado")
        
    elif data.startswith("bc_confirm_"):
        parts = data.split("_")
        group_id = int(parts[2])
        filter_type = parts[3]
        message_text = context.user_data.get('broadcast_message', '')
        
        if not message_text:
            await query.edit_message_text("❌ Error: No se encontró el mensaje")
            return
        
        # Iniciar envío
        await query.edit_message_text(
            "🚀 *Enviando broadcast...*\n\n"
            "Esto puede tomar varios minutos dependiendo de la cantidad de usuarios.\n"
            "Te notificaré cuando termine.",
            parse_mode="Markdown"
        )
        
        await execute_broadcast(context.bot, group_id, filter_type, message_text, query.from_user.id)
        
    elif data == "broadcast_menu":
        await broadcast_menu_callback(update, context)


async def broadcast_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra el menú de broadcast desde el panel."""
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
    
    # FIX: Obtener conteos reales de cada filtro para mostrar números exactos
    trial_only = await db.get_broadcast_targets(group_id, 'trial_only')
    subscribed = await db.get_broadcast_targets(group_id, 'subscribed_only')
    expired = await db.get_broadcast_targets(group_id, 'expired_only')
    all_trial = await db.get_broadcast_targets(group_id, 'all_trial')
    
    keyboard = [
        [InlineKeyboardButton(f"🆓 Solo sin compra ({len(trial_only)})", callback_data=f"bc_filter_{group_id}_trial_only")],
        [InlineKeyboardButton(f"💳 Solo suscriptos ({len(subscribed)})", callback_data=f"bc_filter_{group_id}_subscribed_only")],
        [InlineKeyboardButton(f"⏰ Solo expirados ({len(expired)})", callback_data=f"bc_filter_{group_id}_expired_only")],
        [InlineKeyboardButton(f"👥 Todos ({len(all_trial)})", callback_data=f"bc_filter_{group_id}_all_trial")],
        [InlineKeyboardButton("📜 Historial", callback_data=f"bc_history_{group_id}")],
        [InlineKeyboardButton("🔙 Volver", callback_data=f"select_group_{group_id}")]
    ]
    
    await query.edit_message_text(
        f"📢 *Broadcast Masivo — {group['group_name']}*\n\n"
        f"📊 *Destinatarios disponibles:*\n"
        f"• 🆓 Trial sin compra: *{len(trial_only)}*\n"
        f"• 💳 Con suscripción activa: *{len(subscribed)}*\n"
        f"• ⏰ Expirados no renovados: *{len(expired)}*\n"
        f"• 👥 Total que usaron trial: *{len(all_trial)}*\n\n"
        f"Selecciona el filtro de destinatarios:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


async def broadcast_history_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra historial de broadcasts."""
    query = update.callback_query
    await query.answer()
    
    parts = query.data.split("_")
    group_id = int(parts[2])
    
    history = await db.get_broadcast_history(group_id, limit=10)
    
    if not history:
        msg = "📭 *No hay broadcasts enviados aún*"
    else:
        msg = "📜 *Historial de Broadcasts*\n\n"
        for i, h in enumerate(history, 1):
            filter_names = {
                'trial_only': '🆓 Sin compra',
                'subscribed_only': '💳 Suscriptos',
                'expired_only': '⏰ Expirados',
                'all_trial': '👥 Todos'
            }
            f_name = filter_names.get(h['filter_type'], h['filter_type'])
            date_str = h['sent_at'].strftime('%d/%m/%Y %H:%M')
            msg += (
                f"{i}. *{f_name}* | {date_str}\n"
                f"   ✅ {h['success_count']} enviados | ❌ {h['fail_count']} fallidos\n"
                f"   📝 {h['message_preview'] or 'Sin preview'}...\n\n"
            )
    
    keyboard = [[InlineKeyboardButton("🔙 Volver", callback_data=f"cfg_warnings_{group_id}")]]
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


async def handle_broadcast_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Procesa el mensaje de broadcast ingresado por el admin."""
    if update.effective_chat.type != "private":
        return
    
    step = context.user_data.get('broadcast_step')
    group_id = context.user_data.get('broadcast_group_id')
    filter_type = context.user_data.get('broadcast_filter')
    
    if step != 'message' or not group_id or not filter_type:
        return
    
    text = update.message.text.strip()
    
    if text.lower() == 'cancelar':
        context.user_data.pop('broadcast_group_id', None)
        context.user_data.pop('broadcast_step', None)
        context.user_data.pop('broadcast_filter', None)
        await update.message.reply_text("❌ Broadcast cancelado")
        return
    
    # Guardar mensaje y pedir confirmación
    context.user_data['broadcast_message'] = text
    context.user_data['broadcast_step'] = 'confirm'
    
    # Preview de destinatarios
    targets = await db.get_broadcast_targets(group_id, filter_type)
    preview_msg = text.replace("{user_name}", "Juan").replace("{group_name}", "VIP").replace("{username}", "@juan")
    
    filter_names = {
        'trial_only': '🆓 Solo trial sin compra',
        'subscribed_only': '💳 Solo con suscripción activa',
        'expired_only': '⏰ Solo expirados no renovados',
        'all_trial': '👥 Todos los que usaron trial'
    }
    
    keyboard = [
        [InlineKeyboardButton("✅ Sí, enviar ahora", callback_data=f"bc_confirm_{group_id}_{filter_type}")],
        [InlineKeyboardButton("✏️ Editar mensaje", callback_data=f"bc_filter_{group_id}_{filter_type}")],
        [InlineKeyboardButton("❌ Cancelar", callback_data=f"bc_cancel_{group_id}")]
    ]
    
    await update.message.reply_text(
        f"📢 *Confirmar Broadcast*\n\n"
        f"Filtro: {filter_names.get(filter_type)}\n"
        f"Destinatarios: *{len(targets)}* usuarios\n\n"
        f"*Vista previa del mensaje:*\n"
        f"---\n{preview_msg}\n---\n\n"
        f"⚠️ *Este mensaje se enviará a todos los usuarios seleccionados.*\n"
        f"¿Estás seguro?",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


async def execute_broadcast(bot, group_id: int, filter_type: str, message_text: str, sent_by: int):
    """
    Ejecuta el envío masivo con rate limiting.
    Telegram limita a ~30 mensajes/segundo. Usamos un delay conservador.
    """
    targets = await db.get_broadcast_targets(group_id, filter_type)
    
    if not targets:
        await _safe_send(
            sent_by,
            f"📭 *Broadcast completado*\n\nNo hay usuarios que coincidan con el filtro seleccionado.",
            parse_mode="Markdown"
        )
        return
    
    group = get_group_by_id(group_id)
    group_name = group.get('group_name', 'VIP') if group else 'VIP'
    
    success_count = 0
    fail_count = 0
    total = len(targets)
    
    # Rate limiting: 25 mensajes por segundo máximo (conservador)
    # Para evitar FloodWait, usamos 1 mensaje cada 0.1s = 10 msg/segundo
    delay = 0.12  # 120ms entre mensajes = ~8 msg/segundo (muy seguro)
    
    # Enviar mensaje de progreso al admin
    progress_msg = await _safe_send(
        sent_by,
        f"🚀 *Broadcast en progreso...*\n\n"
        f"• Total: {total} usuarios\n"
        f"• Enviados: 0\n"
        f"• Fallidos: 0\n"
        f"• Progreso: 0%",
        parse_mode="Markdown"
    )
    
    last_progress_update = datetime.now()
    
    for i, user in enumerate(targets, 1):
        user_id = user['user_id']
        first_name = user.get('first_name', 'Usuario') or 'Usuario'
        username = user.get('username', '') or ''
        
        # Personalizar mensaje
        personalized = message_text.replace(
            "{user_name}", first_name
        ).replace(
            "{group_name}", group_name
        ).replace(
            "{username}", f"@{username}" if username and not username.startswith('user_') else first_name
        )
        
        try:
            await bot.send_message(
                user_id,
                personalized,
                parse_mode="Markdown",
                disable_web_page_preview=True
            )
            success_count += 1
            logger.info(f"📢 Broadcast enviado a {user_id} ({first_name})")
            
        except Exception as e:
            fail_count += 1
            logger.warning(f"❌ Fallo broadcast a {user_id}: {e}")
        
        # Rate limiting
        await asyncio.sleep(delay)
        
        # Actualizar progreso cada 5 segundos o cada 10 usuarios
        now = datetime.now()
        if (now - last_progress_update).seconds >= 5 or i % 10 == 0:
            progress_pct = int((i / total) * 100)
            try:
                if progress_msg and hasattr(progress_msg, 'message_id'):
                    await bot.edit_message_text(
                        f"🚀 *Broadcast en progreso...*\n\n"
                        f"• Total: {total} usuarios\n"
                        f"• Enviados: {success_count}\n"
                        f"• Fallidos: {fail_count}\n"
                        f"• Progreso: {progress_pct}%",
                        chat_id=sent_by,
                        message_id=progress_msg.message_id,
                        parse_mode="Markdown"
                    )
            except Exception:
                pass
            last_progress_update = now
    
    # Guardar en historial
    await db.log_broadcast_sent(
        group_id, filter_type, message_text,
        total, success_count, fail_count, sent_by
    )
    
    # Notificar finalización
    await _safe_send(
        sent_by,
        f"✅ *Broadcast completado*\n\n"
        f"📊 *Resultados:*\n"
        f"• Total destinatarios: {total}\n"
        f"• ✅ Enviados: {success_count}\n"
        f"• ❌ Fallidos: {fail_count}\n"
        f"• 📈 Tasa de éxito: {(success_count/total*100):.1f}%\n\n"
        f"Filtro usado: {filter_type}\n"
        f"Grupo: {group_name}",
        parse_mode="Markdown"
    )
# ==================== MAIN ====================
async def main():
    global bot_app
    await db.init_tables()
    await db.load_groups_from_db()
    logger.info(f"📦 {len(GROUPS)} grupos disponibles")

    # FIX: Eliminar Defaults(parse_mode="HTML") porque todos los mensajes usan
    # parse_mode="Markdown" explícitamente. El default HTML causaba que los mensajes
    # sin parse_mode explícito fallaran si contenían <, > o & sin escapar.
    bot_app = ApplicationBuilder().token(TOKEN).build()

    # ── Comandos ──────────────────────────────────────────────────────────────
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

    # ── Callbacks ─────────────────────────────────────────────────────────────
    bot_app.add_handler(CallbackQueryHandler(handle_callback))

    # ── Detección de miembros ─────────────────────────────────────────────────
    bot_app.add_handler(ChatMemberHandler(track_chat_member, ChatMemberHandler.CHAT_MEMBER))
    bot_app.add_handler(MessageHandler(
        filters.StatusUpdate.NEW_CHAT_MEMBERS,
        detect_new_member
    ))

    # Detectar mensajes de usuarios no registrados en grupos
    bot_app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.ChatType.GROUPS,
        detect_active_member
    ))

    # Input de texto en privado (edición de grupos, configuración de precios)
    bot_app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE,
        handle_edit_input
    ))

    # ── Scheduler ────────────────────────────────────────────────────────────
    scheduler.add_job(
        check_expired_subscriptions,
        'interval',
        minutes=5,
        id='check_expired',
        replace_existing=True
    )
    scheduler.add_job(
        auto_backup,
        'interval',
        hours=24,
        id='auto_backup',
        replace_existing=True
    )
    # NUEVO: Scheduler para avisos de expiración
    scheduler.add_job(
        send_trial_warnings,
        'interval',
        minutes=1,
        id='trial_warnings',
        replace_existing=True
    )
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
