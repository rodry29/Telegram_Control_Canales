"""
Bot de gestión de grupos VIP/FREE para Telegram.
Versión refactorizada: bugs corregidos, código optimizado.
"""
import os
import csv
import asyncio
import logging
import re
import json
import threading
from io import StringIO
from datetime import datetime, timedelta
from typing import Optional

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
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==================== CONFIGURACIÓN ====================
DATABASE_URL = os.getenv("DATABASE_URL")
SUPER_ADMIN_ID = 5054216496
TOKEN = os.getenv("TELEGRAM_TOKEN")

for _var, _name in ((TOKEN, "TELEGRAM_TOKEN"), (DATABASE_URL, "DATABASE_URL")):
    if not _var:
        logger.critical("❌ ERROR: No se encontró %s", _name)
        raise SystemExit(1)

PLANS = {
    "trial":   {"minutes": 1440, "price": 0,  "name": "🎁 Trial (1 día)"},
    "semanal": {"days": 7,       "price": 10, "name": "📅 Semanal (7 días)"},
    "mensual": {"days": 30,      "price": 20, "name": "📆 Mensual (30 días)"},
}

GROUPS: list[dict] = []

# ==================== FUNCIONES DE UTILIDAD ====================

def get_group_by_id(group_id: int) -> Optional[dict]:
    return next((g for g in GROUPS if g["group_id"] == group_id), None)


def get_groups_by_admin(admin_id: int, group_type: str = None) -> list:
    groups = GROUPS if admin_id == SUPER_ADMIN_ID else [g for g in GROUPS if g["admin_id"] == admin_id]
    if group_type:
        groups = [g for g in groups if g.get("type") == group_type]
    return groups


def can_manage_group(user_id: int, group_id: int) -> bool:
    if user_id == SUPER_ADMIN_ID:
        return True
    g = get_group_by_id(group_id)
    return g is not None and g["admin_id"] == user_id


def get_group_plan_config(group_id: int, plan: str) -> dict:
    base = dict(PLANS.get(plan, {}))
    group = get_group_by_id(group_id)
    if not group or not base:
        return base
    s = group.get("settings", {})

    if plan == "trial":
        mins = s.get("trial_minutes", base.get("minutes", 1440))
        base["minutes"] = mins
        h, m = divmod(mins, 60)
        if h >= 24:
            d = h // 24
            base["name"] = f"🎁 Trial ({d} día{'s' if d != 1 else ''})"
        elif h > 0:
            base["name"] = f"🎁 Trial ({h}h {m}m)" if m else f"🎁 Trial ({h}h)"
        else:
            base["name"] = f"🎁 Trial ({m} min)"
    elif plan == "semanal":
        base["days"]  = s.get("duration_semanal", base.get("days", 7))
        base["price"] = float(s.get("price_semanal", base.get("price", 10)))
        base["name"]  = f"📅 Semanal ({base['days']} días) - ${base['price']:.2f}"
    elif plan == "mensual":
        base["days"]  = s.get("duration_mensual", base.get("days", 30))
        base["price"] = float(s.get("price_mensual", base.get("price", 20)))
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
    if h > 0:
        return f"{h}h {m}m" if m else f"{h}h"
    return f"{m} min"


# ==================== HELPERS DE PAGO ====================

def get_payment_keyboard(group_id: int, user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("💳 Pagar Acceso", callback_data=f"pay_{group_id}_{user_id}")
    ]])


def get_payment_info_text(group_id: int) -> str:
    group = get_group_by_id(group_id)
    if not group:
        return "Grupo no encontrado"
    s = group.get("settings", {})
    cfg_s = get_group_plan_config(group_id, "semanal")
    cfg_m = get_group_plan_config(group_id, "mensual")

    bank    = s.get("bank_data", "").strip()
    contact = s.get("payment_contact", "ZonaEcFuego").strip()
    paypal  = s.get("paypal_data", "").strip()
    name    = group.get("group_name", "VIP")

    lines = [
        f"💰 *INFORMACIÓN DE PAGO — {name}*\n",
        "📋 *Planes disponibles:*",
        f"• 📅 Semanal ({cfg_s.get('days', 7)} días): *{fmt_price(cfg_s.get('price', 10))}*",
        f"• 📆 Mensual ({cfg_m.get('days', 30)} días): *{fmt_price(cfg_m.get('price', 20))}*\n",
    ]
    if bank:
        lines += [f"🏦 *Transferencia Bancaria:*\n{bank}\n"]
    if paypal:
        lines += [f"🅿️ *PayPal:*\n{paypal}\n"]
    if not bank and not paypal:
        lines += ["⚠️ *Métodos de pago no configurados aún.*\nContacta al administrador.\n"]
    lines += [
        f"📤 *Después de pagar:*\nEnvía el comprobante a @{contact}\n",
        "⏱ *Tiempo de activación:*\nTu acceso se activará cuando un administrador valide la transferencia.\n",
        "🔄 ¿No recibiste los datos? Presiona el botón de nuevo.",
    ]
    return "\n".join(lines)


async def send_payment_info(bot, user_id: int, group_id: int, triggered_by: str = "comando") -> bool:
    group = get_group_by_id(group_id)
    if not group:
        return False
    try:
        await bot.send_message(
            user_id,
            get_payment_info_text(group_id),
            parse_mode="Markdown",
            reply_markup=get_payment_keyboard(group_id, user_id),
        )
        await _safe_send(
            group["admin_id"],
            f"💳 *Nuevo lead de pago*\n\n"
            f"👤 Usuario: [ID {user_id}](tg://user?id={user_id})\n"
            f"🆔 ID: `{user_id}`\n"
            f"📌 Grupo: {group.get('group_name', 'VIP')}\n"
            f"🔔 Acción: *{triggered_by}*\n\n"
            f"_Esperando comprobante en el contacto configurado._",
            parse_mode="Markdown",
        )
        return True
    except Exception as e:
        logger.warning("No se pudo enviar info de pago a %s: %s", user_id, e)
        return False


# ==================== BASE DE DATOS ====================

class Database:
    def __init__(self, db_url: str):
        self.db_url = db_url
        self._pool: pool.ThreadedConnectionPool | None = None
        self._pool_lock = threading.Lock()  # FIX: thread-safety para _get_pool

    def _get_pool(self) -> pool.ThreadedConnectionPool:
        with self._pool_lock:
            if self._pool is None or self._pool.closed:
                self._pool = pool.ThreadedConnectionPool(2, 10, dsn=self.db_url)
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

    # ── Inicialización ────────────────────────────────────────────────────────

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
                        kicked_at   TIMESTAMP DEFAULT NULL,
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
                # Migraciones seguras
                cur.execute("""
                    DO $$ BEGIN
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
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.columns
                            WHERE table_name='users' AND column_name='kicked_at'
                        ) THEN
                            ALTER TABLE users ADD COLUMN kicked_at TIMESTAMP DEFAULT NULL;
                        END IF;
                    END $$;
                """)
                # Índices
                for stmt in [
                    "CREATE INDEX IF NOT EXISTS idx_users_group    ON users(group_id, status)",
                    "CREATE INDEX IF NOT EXISTS idx_users_end_date ON users(group_id, end_date)",
                    "CREATE INDEX IF NOT EXISTS idx_users_uid_gid  ON users(user_id, group_id)",
                    "CREATE INDEX IF NOT EXISTS idx_payments_group ON payments(group_id, payment_date)",
                    "CREATE INDEX IF NOT EXISTS idx_users_expired  ON users(group_id, status, end_date) WHERE status = 'active'",
                    "CREATE INDEX IF NOT EXISTS idx_warning_logs_lookup ON warning_logs(user_id, group_id, warning_type)",
                    "CREATE INDEX IF NOT EXISTS idx_warning_logs_group  ON warning_logs(group_id, sent_at)",
                ]:
                    cur.execute(stmt)
        await self._run(_init)
        logger.info("✅ Base de datos inicializada")

    # ── Grupos ────────────────────────────────────────────────────────────────

    async def load_groups_from_db(self):
        def _load(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT group_id, group_name, admin_id,
                           COALESCE(group_type,'VIP') as group_type,
                           COALESCE(settings,'{}') as settings
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
                    "settings":   settings,
                })
            logger.info("📦 %d grupos cargados desde BD", len(GROUPS))
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

    async def update_group_fields(self, group_id: int, changes: dict):
        if not changes:
            return
        def _upd(conn):
            with conn.cursor() as cur:
                if "name"     in changes: cur.execute("UPDATE groups SET group_name=%s WHERE group_id=%s", (changes["name"],  group_id))
                if "admin"    in changes: cur.execute("UPDATE groups SET admin_id=%s   WHERE group_id=%s", (changes["admin"], group_id))
                if "type"     in changes: cur.execute("UPDATE groups SET group_type=%s WHERE group_id=%s", (changes["type"],  group_id))
                if "settings" in changes: cur.execute("UPDATE groups SET settings=%s   WHERE group_id=%s", (json.dumps(changes["settings"]), group_id))
        await self._run(_upd)

    async def delete_group_from_db(self, group_id: int):
        def _del(conn):
            with conn.cursor() as cur:
                cur.execute("DELETE FROM groups WHERE group_id=%s", (group_id,))
        await self._run(_del)

    # ── Usuarios ──────────────────────────────────────────────────────────────

    async def get_user_by_username(self, username: str, group_id: int = None):
        def _get(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if group_id:
                    cur.execute("SELECT * FROM users WHERE LOWER(username)=LOWER(%s) AND group_id=%s", (username, group_id))
                else:
                    cur.execute("SELECT * FROM users WHERE LOWER(username)=LOWER(%s)", (username,))
                return cur.fetchone()
        return await self._run(_get)

    async def get_user_by_id(self, user_id: int, group_id: int):
        def _get(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT user_id, status, end_date, plan, trial_used FROM users WHERE user_id=%s AND group_id=%s LIMIT 1",
                    (user_id, group_id)
                )
                return cur.fetchone()
        return await self._run(_get)

    async def register_free_user(self, chat_id: int, user_id: int, username: str, first_name: str) -> bool:
        def _reg(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM users WHERE user_id=%s AND group_id=%s", (user_id, chat_id))
                if cur.fetchone():
                    return False
                cur.execute("""
                    INSERT INTO users (user_id, group_id, username, first_name, plan, start_date, end_date, status, trial_used)
                    VALUES (%s,%s,%s,%s,'FREE',%s,%s,'potencial',FALSE)
                    ON CONFLICT (user_id, group_id) DO NOTHING
                """, (user_id, chat_id, username, first_name, datetime.now(), datetime.now() + timedelta(days=365)))
                return True
        return await self._run(_reg)

    async def register_user_auto(self, group_id: int, user_id: int, username: str, first_name: str):
        """
        Registra/evalúa un usuario al entrar al grupo VIP.
        Retorna (allow_access: bool, result_code: str, end_date: datetime | None)

        Códigos:
          "trial_nuevo" → usuario nuevo, trial asignado → PERMITIR + bienvenida
          "activo"      → plan vigente                  → PERMITIR
          "expirado"    → plan vencido                  → EXPULSAR
          "sin_trial"   → ya usó trial, sin plan activo → EXPULSAR
        """
        now = datetime.now()
        trial_mins = get_group_plan_config(group_id, "trial").get("minutes", 1440)

        def _register(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT user_id, trial_used, status, end_date, plan FROM users "
                    "WHERE user_id=%s AND group_id=%s FOR UPDATE",
                    (user_id, group_id)
                )
                ex = cur.fetchone()

                if not ex:
                    # CASO 1: Usuario completamente nuevo
                    end = now + timedelta(minutes=trial_mins)
                    cur.execute("""
                        INSERT INTO users (user_id, group_id, username, first_name, plan, start_date, end_date, trial_used, status)
                        VALUES (%s,%s,%s,%s,'trial',%s,%s,TRUE,'active')
                    """, (user_id, group_id, username, first_name, now, end))
                    cur.execute("""
                        INSERT INTO payments (user_id, group_id, username, first_name, plan, amount, duration_minutes, payment_date)
                        VALUES (%s,%s,%s,%s,'trial',0,%s,%s)
                    """, (user_id, group_id, username, first_name, trial_mins, now))
                    return True, "trial_nuevo", end

                # Actualizar nombre siempre
                cur.execute(
                    "UPDATE users SET username=%s, first_name=%s, updated_at=NOW() WHERE user_id=%s AND group_id=%s",
                    (username, first_name, user_id, group_id)
                )

                # CASO 2: Plan vigente (cualquier plan activo)
                if ex["status"] == "active" and ex["end_date"] > now:
                    cur.execute(
                        "UPDATE users SET kicked_at=NULL, updated_at=NOW() WHERE user_id=%s AND group_id=%s AND kicked_at IS NOT NULL",
                        (user_id, group_id)
                    )
                    return True, "activo", ex["end_date"]

                # CASO 3: Plan expirado
                if ex["end_date"] <= now:
                    return False, "expirado", None

                # CASO 4: Ya usó trial, sin plan activo → FIX: marcar como expirado
                if ex["trial_used"]:
                    cur.execute(
                        "UPDATE users SET status='expired', updated_at=NOW() WHERE user_id=%s AND group_id=%s",
                        (user_id, group_id)
                    )
                    return False, "sin_trial", None

                # CASO 5: Tiene registro pero nunca usó trial
                end = now + timedelta(minutes=trial_mins)
                cur.execute("""
                    UPDATE users SET plan='trial', start_date=%s, end_date=%s,
                        trial_used=TRUE, status='active', kicked_at=NULL, updated_at=NOW()
                    WHERE user_id=%s AND group_id=%s
                """, (now, end, user_id, group_id))
                cur.execute("""
                    INSERT INTO payments (user_id, group_id, username, first_name, plan, amount, duration_minutes, payment_date)
                    VALUES (%s,%s,%s,%s,'trial',0,%s,%s)
                """, (user_id, group_id, username, first_name, trial_mins, now))
                return True, "trial_nuevo", end

        return await self._run(_register)

    def _build_activate_result(self, existing, plan, now, effective_price, effective_days, effective_mins) -> tuple:
        """Construye end_date, dur_minutes, expiry_str según plan."""
        if plan == "trial":
            end_date    = now + timedelta(minutes=effective_mins)
            dur_minutes = effective_mins
            expiry_str  = end_date.strftime("%d/%m/%Y %H:%M")
        else:
            end_date    = now + timedelta(days=effective_days)
            dur_minutes = effective_days * 1440
            expiry_str  = end_date.strftime("%d/%m/%Y")
        return end_date, dur_minutes, expiry_str

    async def _activate_user(self, group_id: int, plan: str,
                             effective_price: float, effective_days: int, effective_mins: int,
                             lookup_field: str, lookup_value) -> tuple[bool, str]:
        """
        Activa o renueva un plan para un usuario identificado por username o user_id.
        FIX: lógica de activación unificada (antes duplicada en 2 métodos).
        """
        now = datetime.now()

        def _add(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if lookup_field == "username":
                    cur.execute(
                        "SELECT user_id, trial_used, first_name, username FROM users "
                        "WHERE LOWER(username)=LOWER(%s) AND group_id=%s FOR UPDATE",
                        (lookup_value, group_id)
                    )
                else:
                    cur.execute(
                        "SELECT user_id, trial_used, first_name, username FROM users "
                        "WHERE user_id=%s AND group_id=%s FOR UPDATE",
                        (lookup_value, group_id)
                    )
                ex = cur.fetchone()
                if not ex:
                    hint = f"@{lookup_value}" if lookup_field == "username" else f"ID {lookup_value}"
                    return False, f"❌ No tengo registro de {hint}. Pídele que envíe un mensaje al bot."

                if plan == "trial" and ex["trial_used"]:
                    return False, "❌ Este usuario ya usó su prueba gratuita"

                end_date, dur_minutes, expiry_str = self._build_activate_result(
                    ex, plan, now, effective_price, effective_days, effective_mins
                )
                fn       = ex.get("first_name") or ""
                username = ex.get("username") or f"user_{ex['user_id']}"

                cur.execute("""
                    UPDATE users SET plan=%s, start_date=%s, end_date=%s, status='active',
                        updated_at=NOW(), trial_used = trial_used OR %s, kicked_at=NULL
                    WHERE user_id=%s AND group_id=%s
                """, (plan, now, end_date, plan == "trial", ex["user_id"], group_id))
                cur.execute("""
                    INSERT INTO payments (user_id, group_id, username, first_name, plan, amount, duration_minutes, payment_date)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """, (ex["user_id"], group_id, username, fn, plan, effective_price, dur_minutes, now))

                price_str = fmt_price(effective_price) if effective_price > 0 else "gratis"
                display   = fn or f"@{username}"
                return True, f"✅ *{display}* activado\n📋 Plan: {plan} | 💰 {price_str}\n📅 Expira: {expiry_str}"

        return await self._run(_add)

    def _resolve_plan_params(self, group_id: int, plan: str, custom_price, custom_days) -> tuple:
        config         = get_group_plan_config(group_id, plan)
        effective_price = custom_price if custom_price is not None else config.get("price", 0)
        effective_days  = custom_days  if custom_days  is not None else config.get("days", 7)
        effective_mins  = config.get("minutes", effective_days * 1440)
        return float(effective_price), int(effective_days), int(effective_mins)

    async def add_or_update_user(self, group_id: int, username: str, plan: str,
                                 first_name: str = "", custom_price=None, custom_days=None):
        if plan not in PLANS:
            return False, "❌ Plan inválido"
        ep, ed, em = self._resolve_plan_params(group_id, plan, custom_price, custom_days)
        return await self._activate_user(group_id, plan, ep, ed, em, "username", username)

    async def add_or_update_user_by_id(self, group_id: int, user_id: int, plan: str,
                                        first_name: str = "", custom_price=None, custom_days=None):
        if plan not in PLANS:
            return False, "❌ Plan inválido"
        ep, ed, em = self._resolve_plan_params(group_id, plan, custom_price, custom_days)
        return await self._activate_user(group_id, plan, ep, ed, em, "user_id", user_id)

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

    async def get_expired_users(self, group_id: int):
        """Usuarios vencidos aún no procesados. LIMIT 50 para evitar bloqueos largos."""
        def _get(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT user_id, username, first_name, plan, end_date
                    FROM users
                    WHERE group_id=%s AND status='active' AND end_date < NOW() AND kicked_at IS NULL
                    LIMIT 50
                """, (group_id,))
                return cur.fetchall()
        return await self._run(_get)

    async def expire_user(self, user_id: int, group_id: int) -> bool:
        """Atómico: solo un ciclo procesa al usuario gracias a RETURNING + kicked_at IS NULL."""
        def _expire(conn):
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE users SET status='expired', kicked_at=NOW(), updated_at=NOW()
                    WHERE user_id=%s AND group_id=%s AND status='active' AND kicked_at IS NULL
                    RETURNING user_id
                """, (user_id, group_id))
                return cur.fetchone() is not None
        return await self._run(_expire)

    # ── Estadísticas ──────────────────────────────────────────────────────────

    async def get_monthly_earnings(self, group_id: int):
        now = datetime.now()
        start = datetime(now.year, now.month, 1)
        def _get(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT plan, COUNT(*) AS count, COALESCE(SUM(amount),0) AS total FROM payments WHERE group_id=%s AND payment_date>=%s GROUP BY plan", (group_id, start))
                summary = cur.fetchall()
                cur.execute("SELECT COUNT(*) AS c FROM users WHERE group_id=%s AND created_at>=%s", (group_id, start))
                new_users = cur.fetchone()["c"]
                cur.execute("""
                    SELECT user_id, username, first_name, plan, amount, duration_minutes, payment_date
                    FROM payments WHERE group_id=%s AND payment_date >= date_trunc('month',NOW())
                    ORDER BY payment_date DESC LIMIT 10
                """, (group_id,))
                recent = cur.fetchall()
                return {"summary": summary, "total": sum(r["total"] for r in summary), "new_users": new_users, "recent": recent}
        return await self._run(_get)

    async def get_total_monthly_earnings(self):
        now = datetime.now()
        def _get(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT COALESCE(SUM(amount),0) FROM payments WHERE payment_date>=%s", (datetime(now.year, now.month, 1),))
                return cur.fetchone()[0]
        return await self._run(_get)

    async def get_total_users_count(self):
        def _get(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM users")
                return cur.fetchone()[0]
        return await self._run(_get)

    async def get_trial_stats(self, group_id: int) -> dict:
        def _get(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT COUNT(*) as n FROM users WHERE group_id=%s AND (plan='trial' OR trial_used=TRUE)", (group_id,))
                total = cur.fetchone()["n"]
                cur.execute("""
                    SELECT user_id, username, first_name, end_date,
                           EXTRACT(EPOCH FROM (end_date - NOW())) / 3600 as hours_left
                    FROM users WHERE group_id=%s AND plan='trial' AND status='active' AND end_date > NOW()
                    ORDER BY end_date ASC
                """, (group_id,))
                active = cur.fetchall()
                cur.execute("SELECT COUNT(*) as n FROM users WHERE group_id=%s AND plan='trial' AND status='expired'", (group_id,))
                expired = cur.fetchone()["n"]
                cur.execute("""
                    SELECT COUNT(DISTINCT u.user_id) as n FROM users u
                    JOIN payments p ON u.user_id=p.user_id AND u.group_id=p.group_id
                    WHERE u.group_id=%s AND u.trial_used=TRUE AND p.amount > 0
                """, (group_id,))
                converted = cur.fetchone()["n"]
                return {"total_trial_users": total, "active_trials": list(active),
                        "active_count": len(active), "expired_from_trial": expired, "converted_users": converted}
        return await self._run(_get)

    async def get_potential_clients_stats(self, group_id: int):
        now = datetime.now()
        som = datetime(now.year, now.month, 1)
        slm = datetime(now.year - 1 if now.month == 1 else now.year, 12 if now.month == 1 else now.month - 1, 1)
        elm = som
        def _get(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM users WHERE group_id=%s AND status='potencial' AND created_at>=%s", (group_id, som))
                cm = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM users WHERE group_id=%s AND status='potencial' AND created_at>=%s AND created_at<%s", (group_id, slm, elm))
                clm = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM users WHERE group_id=%s AND status='potencial'", (group_id,))
                total = cur.fetchone()[0]
                return cm, clm, total
        return await self._run(_get)

    async def get_potential_clients_list(self, group_id: int):
        def _get(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT user_id, username, first_name, created_at FROM users WHERE group_id=%s AND status='potencial' ORDER BY created_at DESC", (group_id,))
                return cur.fetchall()
        return await self._run(_get)

    async def get_export_data(self, group_id: int):
        now = datetime.now()
        def _get(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT user_id, username, first_name, plan, amount, duration_minutes, payment_date
                    FROM payments WHERE group_id=%s AND payment_date>=%s ORDER BY payment_date DESC
                """, (group_id, datetime(now.year, now.month, 1)))
                return cur.fetchall()
        return await self._run(_get)

    # ── Avisos ────────────────────────────────────────────────────────────────

    async def get_warning_config(self, group_id: int) -> dict:
        def _get(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT settings FROM groups WHERE group_id=%s", (group_id,))
                row = cur.fetchone()
                if row and row["settings"]:
                    s = row["settings"] if isinstance(row["settings"], dict) else {}
                    wc = s.get("warning_config")
                    if wc:
                        return wc
                return {}
        result = await self._run(_get)
        return result or {
            "enabled": False,
            "warnings": [{
                "minutes_before": 15,
                "message": "⏳ *Tu trial expira en {minutes_left} minutos*\n\n📅 Expira: {expiry_time}\n\n💳 ¿Quieres seguir?\nPresiona el botón para ver los datos de pago:"
            }]
        }

    async def save_warning_config(self, group_id: int, config: dict):
        def _save(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT settings FROM groups WHERE group_id=%s", (group_id,))
                row = cur.fetchone()
                s = row[0] if row and row[0] else {}
                if not isinstance(s, dict):
                    s = json.loads(s)
                s["warning_config"] = config
                cur.execute("UPDATE groups SET settings=%s WHERE group_id=%s", (json.dumps(s), group_id))
        await self._run(_save)
        g = get_group_by_id(group_id)
        if g:
            g.setdefault("settings", {})["warning_config"] = config

    async def get_users_for_warning(self, group_id: int, minutes_before: int) -> list:
        threshold = datetime.now() + timedelta(minutes=minutes_before)
        warning_type = f"warning_{minutes_before}min"
        def _get(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT u.user_id, u.username, u.first_name, u.end_date, u.plan
                    FROM users u
                    LEFT JOIN warning_logs w ON u.user_id=w.user_id AND u.group_id=w.group_id AND w.warning_type=%s
                    WHERE u.group_id=%s AND u.status='active' AND u.plan='trial'
                      AND u.end_date <= %s AND u.end_date > NOW() AND w.id IS NULL
                    ORDER BY u.end_date ASC
                """, (warning_type, group_id, threshold))
                return cur.fetchall()
        return await self._run(_get)

    async def log_warning_sent(self, user_id: int, group_id: int, warning_type: str):
        """
        FIX: ON CONFLICT DO NOTHING en lugar de DO UPDATE.
        DO UPDATE re-activaba avisos ya enviados en cada ciclo del scheduler.
        """
        def _log(conn):
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO warning_logs (user_id, group_id, warning_type, sent_at)
                    VALUES (%s,%s,%s,NOW())
                    ON CONFLICT (user_id, group_id, warning_type) DO NOTHING
                """, (user_id, group_id, warning_type))
        await self._run(_log)

    async def get_warning_stats(self, group_id: int) -> list:
        def _get(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT warning_type, COUNT(*) as count FROM warning_logs WHERE group_id=%s GROUP BY warning_type", (group_id,))
                return cur.fetchall()
        return await self._run(_get)

    async def clear_warning_logs(self, group_id: int = None):
        def _clear(conn):
            with conn.cursor() as cur:
                cur.execute("DELETE FROM warning_logs WHERE group_id=%s" if group_id else "DELETE FROM warning_logs",
                            (group_id,) if group_id else ())
        await self._run(_clear)


# ==================== INSTANCIAS GLOBALES ====================
db = Database(DATABASE_URL)
scheduler = AsyncIOScheduler()
bot_app = None


async def _safe_send(chat_id: int, text: str, **kwargs):
    try:
        await bot_app.bot.send_message(chat_id, text, **kwargs)
    except Exception as e:
        logger.warning("No se pudo enviar mensaje a %s: %s", chat_id, e)


# ==================== EXPULSIÓN CON REINTENTOS ====================

async def _kick_user_with_retry(group_id: int, user_id: int, retries: int = 3) -> bool:
    for attempt in range(retries):
        try:
            member = await bot_app.bot.get_chat_member(group_id, user_id)
            if member.status in ("left", "kicked", "banned"):
                return True
        except BadRequest as e:
            err = str(e).lower()
            if "user not found" in err or "member_id_invalid" in err:
                return True
            raise

        try:
            await bot_app.bot.ban_chat_member(group_id, user_id)
            await asyncio.sleep(1)
            await bot_app.bot.unban_chat_member(group_id, user_id)
            logger.info("✅ Usuario %s expulsado del grupo %s", user_id, group_id)
            return True
        except TelegramError as e:
            err = str(e).lower()
            if any(p in err for p in ["not enough rights", "bot is not a member", "chat not found",
                                       "user not found", "participant_id_invalid", "member_id_invalid"]):
                logger.error("❌ Error no recuperable expulsando %s de %s: %s", user_id, group_id, e)
                return False
            wait = 2 ** attempt
            logger.warning("⚠️ Intento %d/%d fallido para expulsar %s: %s. Reintentando en %ds...", attempt + 1, retries, user_id, e, wait)
            await asyncio.sleep(wait)

    logger.error("❌ No se pudo expulsar al usuario %s del grupo %s tras %d intentos", user_id, group_id, retries)
    return False


# ==================== DETECCIÓN DE MIEMBROS ====================

async def track_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Método principal (v20+): detecta entradas/salidas mediante chat_member updates."""
    result = update.chat_member
    if not result:
        return
    new_status  = result.new_chat_member.status
    old_status  = result.old_chat_member.status
    user        = result.new_chat_member.user
    group_id    = result.chat.id

    # Solo procesar entradas (de fuera → miembro)
    if old_status in ("left", "kicked", "banned") and new_status == "member":
        await _handle_new_member(context, user, group_id)


async def detect_new_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Fallback legacy para grupos que no emiten chat_member updates."""
    for user in update.message.new_chat_members:
        if not user.is_bot:
            await _handle_new_member(context, user, update.effective_chat.id)


async def _handle_new_member(context, user, group_id: int):
    """Lógica centralizada de registro/evaluación al entrar al grupo."""
    group = get_group_by_id(group_id)
    if not group or group.get("type") != "VIP":
        return

    username   = user.username   or f"user_{user.id}"
    first_name = user.first_name or ""

    allow, code, end_date = await db.register_user_auto(group_id, user.id, username, first_name)

    if allow:
        if code == "trial_nuevo":
            expiry_str = end_date.strftime("%d/%m/%Y %H:%M")
            trial_cfg  = get_group_plan_config(group_id, "trial")
            trial_str  = fmt_minutes(trial_cfg.get("minutes", 1440))
            await _safe_send(
                user.id,
                f"👋 *¡Bienvenido/a a {group['group_name']}!*\n\n"
                f"🎁 *Trial activado:* {trial_str}\n"
                f"📅 *Expira:* {expiry_str}\n\n"
                f"Para renovar tu acceso después del trial:",
                reply_markup=get_payment_keyboard(group_id, user.id),
                parse_mode="Markdown",
            )
            await _safe_send(
                group["admin_id"],
                f"🆕 *Nuevo usuario en trial*\n\n"
                f"👤 [{first_name or username}](tg://user?id={user.id})\n"
                f"🆔 ID: `{user.id}`\n"
                f"📌 Grupo: {group['group_name']}\n"
                f"📅 Trial expira: {expiry_str}",
                parse_mode="Markdown",
            )
    else:
        reason = (
            "Tu acceso expiró. Renueva para seguir disfrutando del contenido."
            if code == "expirado"
            else "Ya usaste tu período de prueba gratuita. Adquiere un plan para acceder."
        )
        await _safe_send(
            user.id,
            f"⛔ *Acceso denegado — {group['group_name']}*\n\n{reason}",
            reply_markup=get_payment_keyboard(group_id, user.id),
            parse_mode="Markdown",
        )
        await _kick_user_with_retry(group_id, user.id)


async def detect_active_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Registra usuarios FREE que escriben en grupos FREE."""
    if not update.message or not update.effective_user:
        return
    user     = update.effective_user
    group_id = update.effective_chat.id
    group    = get_group_by_id(group_id)
    if not group or group.get("type") != "FREE":
        return
    await db.register_free_user(group_id, user.id, user.username or "", user.first_name or "")


# ==================== HANDLERS ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    send = (update.callback_query.message.reply_text if update.callback_query
            else update.message.reply_text if update.message else None)
    if not send:
        return

    if user_id == SUPER_ADMIN_ID:
        vip_count  = sum(1 for g in GROUPS if g.get("type") == "VIP")
        free_count = sum(1 for g in GROUPS if g.get("type") == "FREE")
        total_earn = await db.get_total_monthly_earnings()
        total_usr  = await db.get_total_users_count()
        keyboard   = [
            [InlineKeyboardButton("📋 Grupos",    callback_data="menu_groups")],
            [InlineKeyboardButton("💰 Ganancias", callback_data="total_earnings")],
            [InlineKeyboardButton("📟 Comandos",  callback_data="menu_commands")],
        ]
        await send(
            f"👑 *Panel Super Administrador*\n\n"
            f"• 👑 Grupos VIP: {vip_count}\n• 📋 Grupos FREE: {free_count}\n"
            f"• 👥 Total usuarios: {total_usr}\n• 💰 Ganancias del mes: ${total_earn}\n\nSelecciona una opción:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown",
        )
        return

    user_groups = get_groups_by_admin(user_id)
    if not user_groups:
        await send("❌ No tienes grupos asignados. Contacta al Super Administrador.")
        return

    if len(user_groups) == 1:
        group = user_groups[0]
        context.user_data["current_group"] = group["group_id"]
        await _send_group_panel(send, group)
    else:
        keyboard = []
        for g in user_groups:
            emoji = "👑" if g.get("type") == "VIP" else "📋"
            keyboard.append([InlineKeyboardButton(f"{emoji} {g['group_name']}", callback_data=f"select_group_{g['group_id']}")])
        await send("📋 *Selecciona el grupo que quieres gestionar*", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


async def _send_group_panel(send_func, group: dict, edit: bool = False):
    """Genera el panel de control de un grupo (VIP o FREE)."""
    gid = group["group_id"]
    if group.get("type") == "VIP":
        cfg_t = get_group_plan_config(gid, "trial")
        cfg_s = get_group_plan_config(gid, "semanal")
        cfg_m = get_group_plan_config(gid, "mensual")
        keyboard = [
            [InlineKeyboardButton("📊 Usuarios activos",   callback_data="list_active")],
            [InlineKeyboardButton("💰 Ganancias",          callback_data="earnings")],
            [InlineKeyboardButton("📥 Exportar mes",       callback_data="export_month")],
            [InlineKeyboardButton("📊 Estadísticas Trial", callback_data="trial_stats")],
            [InlineKeyboardButton("⚙️ Precios y Trial",    callback_data=f"cfg_group_{gid}")],
        ]
        text = (
            f"👑 *Panel VIP - {group['group_name']}*\n\n"
            f"🆔 ID: `{gid}`\n\n"
            f"💡 *Tarifas actuales:*\n"
            f"• ⏱ Trial: {fmt_minutes(cfg_t.get('minutes', 1440))}\n"
            f"• 📅 Semanal: {fmt_price(cfg_s['price'])}\n"
            f"• 📆 Mensual: {fmt_price(cfg_m['price'])}"
        )
    else:
        keyboard = [
            [InlineKeyboardButton("📋 Clientes potenciales", callback_data="list_potential")],
            [InlineKeyboardButton("📥 Exportar clientes",    callback_data="export_clients")],
        ]
        text = f"📋 *Panel FREE - {group['group_name']}*\n\n🆔 ID: `{gid}`"

    markup = InlineKeyboardMarkup(keyboard)
    if edit:
        await send_func(text, reply_markup=markup, parse_mode="Markdown")
    else:
        await send_func(text, reply_markup=markup, parse_mode="Markdown")


async def test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("✅ El bot funciona!")


# ── Grupos ────────────────────────────────────────────────────────────────────

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
    await query.edit_message_text("📋 *Gestión de Grupos*\n\nSelecciona una opción:",
                                   reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


async def menu_view_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    keyboard = [
        [InlineKeyboardButton("👑 Grupos VIP",  callback_data="view_vip_groups")],
        [InlineKeyboardButton("📋 Grupos FREE", callback_data="view_free_groups")],
        [InlineKeyboardButton("🔙 Volver",      callback_data="menu_groups")],
    ]
    await query.edit_message_text("👁️ *Ver Grupos*\n\nSelecciona el tipo:",
                                   reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


async def show_groups_by_type(update: Update, context: ContextTypes.DEFAULT_TYPE,
                               group_type: str, back_cb: str = "menu_view_groups"):
    query  = update.callback_query
    groups = [g for g in GROUPS if g.get("type") == group_type]
    if not groups:
        await query.edit_message_text(f"📭 No hay grupos {group_type}")
        return
    keyboard = [[InlineKeyboardButton(f"📌 {g['group_name']}", callback_data=f"select_group_{g['group_id']}")] for g in groups]
    keyboard.append([InlineKeyboardButton("🔙 Volver", callback_data=back_cb)])
    await query.edit_message_text(f"📋 *Grupos {group_type}*", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


async def select_group(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query
    await query.answer()
    group = get_group_by_id(group_id)
    if not group:
        await query.edit_message_text("❌ Grupo no encontrado")
        return
    context.user_data["current_group"] = group_id
    await _send_group_panel(query.edit_message_text, group, edit=True)


async def total_earnings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query:
        await query.answer()
    message = query.message if query else update.message
    total = await db.get_total_monthly_earnings()
    now   = datetime.now()
    await message.reply_text(
        f"💰 *GANANCIAS TOTALES DEL MES*\n\n📅 {now.strftime('%B %Y')}\n💵 Total: *${total}*\n📊 Incluye todos los grupos.",
        parse_mode="Markdown",
    )


async def list_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    message = query.message if query else update.message
    if update.effective_user.id != SUPER_ADMIN_ID:
        await message.reply_text("❌ Solo Super Admin")
        return
    if not GROUPS:
        await message.reply_text("📭 No hay grupos")
        return
    msg = "📊 *GRUPOS CONFIGURADOS*\n\n"
    for g in GROUPS:
        msg += f"📌 *{g['group_name']}*\n   🆔 ID: `{g['group_id']}`\n   👑 Admin: `{g['admin_id']}`\n   📋 Tipo: {g.get('type','VIP')}\n\n"
    await message.reply_text(msg, parse_mode="Markdown")


async def add_group_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != SUPER_ADMIN_ID:
        await update.message.reply_text("❌ Solo Super Admin")
        return
    if len(context.args) < 4:
        await update.message.reply_text('❌ Formato: `/addgroup group_id TIPO "nombre" admin_id`', parse_mode="Markdown")
        return
    try:
        group_id   = int(context.args[0])
        group_type = context.args[1].upper()
        group_name = " ".join(context.args[2:-1]).strip('"')
        admin_id   = int(context.args[-1])
        if group_type not in ("VIP", "FREE"):
            await update.message.reply_text("❌ TIPO debe ser VIP o FREE")
            return
        GROUPS.append({"group_id": group_id, "type": group_type, "group_name": group_name, "admin_id": admin_id, "settings": {}})
        await db.save_group(group_id, group_name, admin_id, group_type)
        await update.message.reply_text(f"✅ Grupo {group_name} agregado")
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")


# ── Usuarios ──────────────────────────────────────────────────────────────────

async def _parse_add_args(args: list) -> tuple:
    """Parsea args de /add: (identifier, plan, custom_price, custom_days) o None si error."""
    if len(args) < 2:
        return None
    identifier = args[0].lstrip("@")
    plan = args[1].lower()
    if plan not in PLANS:
        return None
    custom_price = custom_days = None
    if len(args) >= 3:
        try:
            custom_price = float(args[2].replace(",", "."))
            if custom_price < 0:
                raise ValueError
        except ValueError:
            return None
    if len(args) >= 4:
        try:
            custom_days = int(args[3])
            if custom_days <= 0:
                raise ValueError
        except ValueError:
            return None
    return identifier, plan, custom_price, custom_days


async def _ensure_user_registered(group_id: int, user_id: int, username: str, first_name: str):
    existing = await db.get_user_by_id(user_id, group_id)
    if not existing:
        await db.register_free_user(group_id, user_id, username, first_name)


async def add_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Reply a mensaje de registro
    if update.message and update.message.reply_to_message:
        await _handle_reply_add(update, context)
        return

    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    current_group = context.user_data.get("current_group") or (
        chat_id if get_group_by_id(chat_id) and can_manage_group(user_id, chat_id) else None
    )
    if not current_group:
        await update.message.reply_text("❌ Usa /start primero")
        return
    if not can_manage_group(user_id, current_group):
        await update.message.reply_text("❌ No autorizado")
        return

    parsed = await _parse_add_args(context.args)
    if not parsed:
        cfg_t = get_group_plan_config(current_group, "trial")
        cfg_s = get_group_plan_config(current_group, "semanal")
        cfg_m = get_group_plan_config(current_group, "mensual")
        await update.message.reply_text(
            f"❌ *Uso:* `/add @username|ID plan [precio] [días]`\n\n"
            f"*Planes:*\n• `trial`   — {cfg_t['name']}\n• `semanal` — {cfg_s['name']}\n• `mensual` — {cfg_m['name']}\n\n"
            f"*Ejemplos:*\n`/add @juan semanal`\n`/add 123456789 mensual`\n`/add @juan semanal 5.99`\n\n"
            f"También puedes *responder al mensaje de registro* con `/add plan`",
            parse_mode="Markdown",
        )
        return

    identifier, plan, custom_price, custom_days = parsed

    if identifier.isdigit():
        target_id = int(identifier)
        username = first_name = ""
        try:
            m = await context.bot.get_chat_member(current_group, target_id)
            if m and m.user:
                first_name = m.user.first_name or ""
                username   = m.user.username   or f"user_{target_id}"
        except Exception:
            username = f"user_{target_id}"
        await _ensure_user_registered(current_group, target_id, username, first_name)
        success, msg = await db.add_or_update_user_by_id(current_group, target_id, plan, first_name, custom_price, custom_days)
    else:
        first_name = ""
        try:
            m = await context.bot.get_chat_member(current_group, identifier)
            if m and m.user:
                first_name = m.user.first_name or ""
        except Exception:
            pass
        success, msg = await db.add_or_update_user(current_group, identifier, plan, first_name, custom_price, custom_days)

    await update.message.reply_text(msg, parse_mode="Markdown")


async def _handle_reply_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Activa plan respondiendo a un mensaje de registro de trial."""
    replied = update.message.reply_to_message
    text    = replied.text or replied.caption or ""

    target_user_id  = None
    target_username = None
    target_name     = None

    id_match = re.search(r"🆔 ID:\s*`?(\d+)`?", text)
    if id_match:
        target_user_id = int(id_match.group(1))
    nm_match = re.search(r"👤 Nombre:\s*(.+?)(?:\n|$)", text)
    if nm_match:
        target_name = nm_match.group(1).strip()
    un_match = re.search(r"@(\w+)", text)
    if un_match:
        target_username = un_match.group(1)

    if not target_user_id:
        await update.message.reply_text(
            "❌ No pude encontrar el ID del usuario.\n"
            "Responde al mensaje de registro de trial, o usa `/add ID plan`.",
            parse_mode="Markdown",
        )
        return

    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    current_group = context.user_data.get("current_group") or (
        chat_id if update.effective_chat.type in ("group", "supergroup") else None
    )
    if not current_group:
        await update.message.reply_text("❌ Usa /start primero para seleccionar un grupo")
        return
    if not can_manage_group(user_id, current_group):
        await update.message.reply_text("❌ No autorizado")
        return

    parsed = await _parse_add_args(update.message.text.split()[1:])  # quitar '/add'
    if not parsed:
        await update.message.reply_text(
            "❌ *Uso respondiendo:*\n`/add plan [precio] [días]`\n\nEjemplos:\n`/add mensual`\n`/add semanal 5.99`",
            parse_mode="Markdown",
        )
        return

    _, plan, custom_price, custom_days = parsed
    username   = target_username or f"user_{target_user_id}"
    first_name = target_name or ""

    await _ensure_user_registered(current_group, target_user_id, username, first_name)
    success, msg = await db.add_or_update_user_by_id(current_group, target_user_id, plan, first_name, custom_price, custom_days)
    await update.message.reply_text(f"✅ *Activación por respuesta*\n\n{msg}", parse_mode="Markdown")

    if success:
        try:
            g = get_group_by_id(current_group)
            await context.bot.send_message(
                target_user_id,
                f"🎉 *¡Suscripción activada!*\n\nTu plan *{plan}* en *{g['group_name']}* ha sido activado. ¡Gracias!",
                parse_mode="Markdown",
            )
        except Exception as e:
            logger.warning("No se pudo notificar al usuario %s: %s", target_user_id, e)


async def list_active_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    message = query.message if query else update.message
    group_id = context.user_data.get("current_group")
    if not group_id:
        await message.reply_text("❌ Selecciona un grupo con /start")
        return
    users = await db.get_all_active_users(group_id)
    if not users:
        await message.reply_text("📭 No hay usuarios activos")
        return
    now = datetime.now()
    msg = f"📊 *USUARIOS ACTIVOS* ({len(users)})\n\n"
    for user in users[:30]:
        end_date  = user["end_date"]
        remaining = end_date - now
        mins_left = max(0, int(remaining.total_seconds() / 60))
        days_left = mins_left // 1440
        emoji = "🟢" if days_left > 7 else "🟡" if days_left > 1 else "🔴"

        if mins_left < 60:
            time_text = f"{mins_left} min"
            date_text = end_date.strftime("%d/%m/%Y %H:%M")
        elif mins_left < 1440:
            h, m = divmod(mins_left, 60)
            time_text = f"{h}h {m}m" if m else f"{h}h"
            date_text = end_date.strftime("%d/%m/%Y %H:%M")
        else:
            time_text = f"{days_left} días"
            date_text = end_date.strftime("%d/%m/%Y")

        fn  = user.get("first_name") or "Sin nombre"
        un  = user.get("username") or ""
        display = f"{fn} (@{un})" if un and not un.startswith("user_") else f"{fn} (ID: `{user['user_id']}`)"
        msg += f"{emoji} {display}\n   📅 Expira: {date_text} ({time_text} restantes)\n   📋 Plan: {user['plan']}\n   🔗 [Abrir chat](tg://user?id={user['user_id']})\n\n"

    await message.reply_text(msg, parse_mode="Markdown")


async def show_earnings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    message = query.message if query else update.message
    group_id = context.user_data.get("current_group")
    if not group_id:
        await message.reply_text("❌ Selecciona un grupo con /start")
        return
    earnings = await db.get_monthly_earnings(group_id)
    now = datetime.now()
    msg = f"💰 *GANANCIAS DE {now.strftime('%B %Y').upper()}*\n\n"
    if not earnings["summary"]:
        msg += "📭 No hay ventas"
    else:
        for p in earnings["summary"]:
            msg += f"• {PLANS.get(p['plan'], {}).get('name', p['plan'])}: {p['count']} ventas - {fmt_price(p['total'])}\n"
        msg += f"\n💵 *TOTAL*: {fmt_price(earnings['total'])}\n👥 *Nuevos*: {earnings['new_users']}"
    if earnings.get("recent"):
        msg += "\n\n📋 *Últimos pagos:*\n"
        for p in earnings["recent"]:
            name    = p["first_name"] or p["username"] or f"ID:{p['user_id']}"
            dur_str = f" ({fmt_minutes(p['duration_minutes'])})" if p.get("duration_minutes") else ""
            msg += f"• {name} - {p['plan']}{dur_str} - {fmt_price(p['amount'])}\n"
    await message.reply_text(msg, parse_mode="Markdown")


async def export_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    message = query.message if query else update.message
    group_id = context.user_data.get("current_group")
    if not group_id:
        await message.reply_text("❌ Selecciona un grupo con /start")
        return
    now = datetime.now()
    transactions = await db.get_export_data(group_id)
    if not transactions:
        await message.reply_text(f"📭 No hay transacciones en {now.strftime('%B %Y')}")
        return
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(["Fecha", "User ID", "Username", "Nombre", "Plan", "Duración", "Monto USD", "Link"])
    for t in transactions:
        dur = fmt_minutes(t["duration_minutes"]) if t.get("duration_minutes") else ""
        writer.writerow([t["payment_date"].strftime("%Y-%m-%d %H:%M:%S"), t["user_id"],
                         t["username"] or "", t["first_name"] or "", t["plan"].upper(),
                         dur, f"{float(t['amount']):.2f}", f"tg://user?id={t['user_id']}"])
    output.seek(0)
    await message.reply_document(
        document=output.getvalue().encode("utf-8-sig"),
        filename=f"reporte_{now.year}_{now.month:02d}.csv",
        caption=f"📊 Reporte de {now.strftime('%B %Y')}",
    )
    output.close()


async def trial_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query:
        await query.answer()
    message = query.message if query else update.message
    group_id = context.user_data.get("current_group")
    if not group_id:
        await message.reply_text("❌ Selecciona un grupo con /start")
        return
    group = get_group_by_id(group_id)
    if not group or group.get("type") != "VIP":
        await message.reply_text("❌ Solo disponible en grupos VIP")
        return
    stats = await db.get_trial_stats(group_id)
    msg = (
        f"📊 *ESTADÍSTICAS DE TRIAL — {group['group_name']}*\n\n"
        f"• 🆓 Usaron trial: {stats['total_trial_users']}\n"
        f"• 🟢 Activos ahora: {stats['active_count']}\n"
        f"• 🔴 Expirados: {stats['expired_from_trial']}\n"
        f"• 💰 Convertidos a pago: {stats['converted_users']}\n"
    )
    if stats["active_trials"]:
        msg += "\n⏳ *TIEMPO RESTANTE:*\n"
        for t in stats["active_trials"]:
            total_h = float(t["hours_left"] or 0)
            d, h, m = int(total_h // 24), int(total_h % 24), int((total_h * 60) % 60)
            time_left = f"{d}d {h}h {m}m" if d > 0 else (f"{h}h {m}m" if h > 0 else f"{m}m")
            name = t["first_name"] or t["username"] or f"ID:{t['user_id']}"
            un   = f"@{t['username']}" if t["username"] and not str(t["username"]).startswith("user_") else f"ID:{t['user_id']}"
            msg += f"\n👤 [{name}](tg://user?id={t['user_id']}) ({un})\n   ⏳ {time_left} | 📅 {t['end_date'].strftime('%d/%m/%Y %H:%M')}\n"
    if stats["total_trial_users"] > 0:
        msg += f"\n📈 *Conversión:* {stats['converted_users']/stats['total_trial_users']*100:.1f}%"
    keyboard = [[InlineKeyboardButton("🔙 Volver", callback_data=f"select_group_{group_id}")]]
    if query:
        await query.edit_message_text(msg, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await message.reply_text(msg, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))


async def list_potential_clients(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    group_id = context.user_data.get("current_group")
    if not group_id:
        await query.edit_message_text("❌ Selecciona un grupo con /start")
        return
    group = get_group_by_id(group_id)
    if not group or group.get("type") != "FREE":
        await query.edit_message_text("❌ Solo funciona en grupos FREE")
        return
    cm, clm, total = await db.get_potential_clients_stats(group_id)
    now = datetime.now()
    if clm > 0:
        g = (cm - clm) / clm * 100
        growth_text = f"{'📈' if g > 0 else '📉' if g < 0 else '➖'} {g:+.1f}% vs mes anterior"
    else:
        growth_text = "📊 Primer mes con registros" if cm > 0 else "📊 Sin registros este mes"
    await query.edit_message_text(
        f"📋 *CLIENTES POTENCIALES — {group['group_name']}*\n\n"
        f"📅 *{now.strftime('%B %Y')}:* {cm} nuevos\n📆 *Mes anterior:* {clm}\n📊 *Total:* {total}\n\n{growth_text}",
        parse_mode="Markdown",
    )


async def export_clients(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    group_id = context.user_data.get("current_group")
    if not group_id:
        await query.edit_message_text("❌ Selecciona un grupo con /start")
        return
    clients = await db.get_potential_clients_list(group_id)
    if not clients:
        await query.edit_message_text("📭 No hay clientes")
        return
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(["User ID", "Username", "Nombre", "Fecha Registro"])
    for c in clients:
        writer.writerow([c["user_id"], c["username"] or "", c["first_name"] or "", c["created_at"].strftime("%Y-%m-%d %H:%M:%S")])
    output.seek(0)
    await query.message.reply_document(
        document=output.getvalue().encode("utf-8-sig"),
        filename=f"clientes_{datetime.now().strftime('%Y%m%d')}.csv",
        caption="📋 Clientes potenciales",
    )
    output.close()


# ── Pagos ─────────────────────────────────────────────────────────────────────

async def pay_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id  = update.effective_user.id
    chat_id  = update.effective_chat.id
    is_group = update.effective_chat.type in ("group", "supergroup")

    if is_group:
        target_group = chat_id
    elif context.args and context.args[0].isdigit():
        target_group = int(context.args[0])
    elif context.user_data.get("current_group"):
        target_group = context.user_data["current_group"]
    else:
        await update.message.reply_text(
            "❌ *Uso de /pagar*\n\n"
            "• En un grupo VIP: `/pagar`\n"
            "• En privado: `/pagar ID_DEL_GRUPO`\n"
            "• O selecciona un grupo con /start",
            parse_mode="Markdown",
        )
        return

    group = get_group_by_id(target_group)
    if not group or group.get("type") != "VIP":
        await update.message.reply_text("❌ Grupo no encontrado o no es VIP")
        return

    success = await send_payment_info(context.bot, user_id, target_group, triggered_by="comando /pagar")
    if success:
        if is_group:
            await update.message.reply_text("💳 *Te envié los datos de pago por privado.*", parse_mode="Markdown")
    else:
        await update.message.reply_text(
            "❌ No se pudieron enviar los datos. Inicia chat privado conmigo primero (/start).",
            parse_mode="Markdown",
        )


async def config_payment_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not context.args:
        await update.message.reply_text("❌ *Uso: `/configpago group_id`*", parse_mode="Markdown")
        return
    try:
        group_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ El ID debe ser un número")
        return
    if not can_manage_group(user_id, group_id):
        await update.message.reply_text("❌ No autorizado")
        return
    group = get_group_by_id(group_id)
    if not group or group.get("type") != "VIP":
        await update.message.reply_text("❌ Grupo no encontrado o no es VIP")
        return
    await _start_payment_config(update.message.reply_text, context, group, edit=False)


async def _start_payment_config(send_func, context, group: dict, edit: bool = False):
    gid = group["group_id"]
    context.user_data["config_payment_group_id"] = gid
    context.user_data["config_payment_step"]     = "bank_data"
    current = group.get("settings", {}).get("bank_data", "No configurado")
    text = (
        f"⚙️ *Configuración de Pago — {group['group_name']}*\n\n"
        f"Paso 1/3: *Datos bancarios*\n"
        f"Envía los datos de transferencia (banco, cuenta, nombre, cédula...)\n\n"
        f"📋 *Actual:*\n`{current}`\n\n"
        f"*Escribe 'saltar' para mantener o 'eliminar' para borrar.*"
    )
    if edit:
        await send_func(text, parse_mode="Markdown")
    else:
        await send_func(text, parse_mode="Markdown")


# ── Edición de grupos ─────────────────────────────────────────────────────────

async def menu_edit_group_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not GROUPS:
        await query.edit_message_text("📭 No hay grupos configurados")
        return
    keyboard = []
    for g in GROUPS:
        emoji = "👑" if g.get("type") == "VIP" else "📋"
        keyboard.append([InlineKeyboardButton(f"{emoji} {g['group_name']}", callback_data=f"edit_multiple_{g['group_id']}")])
    keyboard.append([InlineKeyboardButton("🔙 Volver", callback_data="menu_groups")])
    await query.edit_message_text("✏️ *Selecciona el grupo a editar*", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


async def menu_delete_group_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not GROUPS:
        await query.edit_message_text("📭 No hay grupos configurados")
        return
    keyboard = []
    for g in GROUPS:
        emoji = "👑" if g.get("type") == "VIP" else "📋"
        keyboard.append([InlineKeyboardButton(f"{emoji} {g['group_name']}", callback_data=f"delete_confirm_{g['group_id']}")])
    keyboard.append([InlineKeyboardButton("🔙 Volver", callback_data="menu_groups")])
    await query.edit_message_text("❌ *Eliminar Grupo*\n\n⚠️ Irreversible. Selecciona:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


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
        f"⚠️ *Confirmar eliminación*\n\n📌 *{group['group_name']}*\n🆔 `{group_id}`\n\n*Esta acción no se puede deshacer.*",
        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown",
    )


async def delete_group_execute(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query
    await query.answer()
    group = get_group_by_id(group_id)
    if not group:
        await query.edit_message_text("❌ Grupo no encontrado")
        return
    group_name = group["group_name"]
    # FIX: modificar la lista in-place en vez de reasignar la variable global
    GROUPS[:] = [g for g in GROUPS if g["group_id"] != group_id]
    await db.delete_group_from_db(group_id)
    await query.edit_message_text(f"✅ *Grupo eliminado*\n\n📌 {group_name}\n🆔 `{group_id}`", parse_mode="Markdown")
    await asyncio.sleep(2)
    await menu_groups(update, context)


async def edit_group_multiple(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query
    await query.answer()
    group = get_group_by_id(group_id)
    if not group:
        await query.edit_message_text("❌ Grupo no encontrado")
        return
    context.user_data["editing_group_id"] = group_id
    context.user_data["editing_mode"]     = "multiple"
    context.user_data.setdefault("pending_changes", {})
    pending = context.user_data.get("pending_changes", {})
    pending_text = ""
    if pending:
        pending_text = "\n\n📝 *Cambios pendientes:*\n"
        if "name"  in pending: pending_text += f"• Nombre: `{pending['name']}`\n"
        if "admin" in pending: pending_text += f"• Admin: `{pending['admin']}`\n"
        if "type"  in pending: pending_text += f"• Tipo: `{pending['type']}`\n"
    keyboard = [
        [InlineKeyboardButton("📝 Cambiar nombre", callback_data=f"multi_name_{group_id}")],
        [InlineKeyboardButton("👤 Cambiar admin",  callback_data=f"multi_admin_{group_id}")],
        [InlineKeyboardButton("🔄 Cambiar tipo",   callback_data=f"multi_type_{group_id}")],
        [InlineKeyboardButton("✅ Aplicar cambios", callback_data=f"multi_apply_{group_id}")],
        [InlineKeyboardButton("🔙 Volver",          callback_data="menu_edit_group_select")],
    ]
    await query.edit_message_text(
        f"✏️ *Edición — {group['group_name']}*\n\n📋 Tipo: {group.get('type','VIP')}\n👑 Admin: `{group['admin_id']}`{pending_text}\n\n_Escribe 'cancelar' para cancelar._",
        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown",
    )


async def multi_apply_changes(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query
    await query.answer()
    pending = context.user_data.get("pending_changes", {})
    if not pending:
        await query.edit_message_text("❌ No hay cambios pendientes")
        return
    group = get_group_by_id(group_id)
    if not group:
        await query.edit_message_text("❌ Grupo no encontrado")
        return
    changes_made = []
    for g in GROUPS:
        if g["group_id"] == group_id:
            if "name"  in pending: g["group_name"] = pending["name"];  changes_made.append(f"📝 Nombre → {pending['name']}")
            if "admin" in pending: g["admin_id"]   = pending["admin"]; changes_made.append(f"👤 Admin → {pending['admin']}")
            if "type"  in pending: g["type"]        = pending["type"];  changes_made.append(f"🔄 Tipo → {pending['type']}")
            break
    await db.update_group_fields(group_id, pending)
    for k in ("pending_changes", "editing_mode", "editing_group_id"):
        context.user_data.pop(k, None)
    await query.edit_message_text(f"✅ *Cambios aplicados*\n\n" + "\n".join(changes_made), parse_mode="Markdown")
    await asyncio.sleep(2)
    await menu_edit_group_select(update, context)


# ── Configuración de precios ──────────────────────────────────────────────────

async def menu_group_settings(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query
    await query.answer()
    group = get_group_by_id(group_id)
    if not group:
        await query.edit_message_text("❌ Grupo no encontrado")
        return
    ct = get_group_plan_config(group_id, "trial")
    cs = get_group_plan_config(group_id, "semanal")
    cm = get_group_plan_config(group_id, "mensual")
    keyboard = [
        [InlineKeyboardButton(f"⏱ Trial: {fmt_minutes(ct.get('minutes',1440))}",      callback_data=f"cfg_trial_{group_id}")],
        [InlineKeyboardButton(f"📅 Días semanal: {cs['days']}d",                        callback_data=f"cfg_dur_semanal_{group_id}")],
        [InlineKeyboardButton(f"💲 Precio semanal: {fmt_price(cs['price'])}",           callback_data=f"cfg_price_semanal_{group_id}")],
        [InlineKeyboardButton(f"📆 Días mensual: {cm['days']}d",                        callback_data=f"cfg_dur_mensual_{group_id}")],
        [InlineKeyboardButton(f"💲 Precio mensual: {fmt_price(cm['price'])}",           callback_data=f"cfg_price_mensual_{group_id}")],
        [InlineKeyboardButton("💳 Configurar Pago",                                     callback_data=f"cfg_payment_{group_id}")],
        [InlineKeyboardButton("🔔 Configurar Avisos",                                   callback_data=f"cfg_warnings_{group_id}")],
        [InlineKeyboardButton("🔙 Volver",                                               callback_data=f"select_group_{group_id}")],
    ]
    await query.edit_message_text(
        f"⚙️ *Configuración — {group['group_name']}*\n\n"
        f"• Trial: {fmt_minutes(ct.get('minutes',1440))}\n"
        f"• Semanal: {cs['days']}d | {fmt_price(cs['price'])}\n"
        f"• Mensual: {cm['days']}d | {fmt_price(cm['price'])}\n\n"
        f"_Con `/add` puedes sobreescribir precio y días por usuario._",
        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown",
    )


async def _cfg_request(update: Update, context: ContextTypes.DEFAULT_TYPE,
                        group_id: int, field: str, prompt: str):
    """Helper genérico para iniciar input de configuración."""
    query = update.callback_query
    await query.answer()
    context.user_data["cfg_field"]    = field
    context.user_data["cfg_group_id"] = group_id
    await query.edit_message_text(prompt, parse_mode="Markdown")


async def handle_cfg_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        return
    field    = context.user_data.get("cfg_field")
    group_id = context.user_data.get("cfg_group_id")
    if not field or not group_id or not can_manage_group(update.effective_user.id, group_id):
        return

    text = update.message.text.strip().replace(",", ".")
    if text.lower() == "cancelar":
        context.user_data.pop("cfg_field", None)
        context.user_data.pop("cfg_group_id", None)
        await update.message.reply_text("❌ Configuración cancelada")
        return

    is_price   = field.startswith("price_")
    is_minutes = field == "trial_minutes"
    try:
        value = round(float(text), 2) if is_price else int(float(text))
        if value <= 0:
            raise ValueError
    except ValueError:
        tip = "Ej: `5.99`" if is_price else "Ej: `1440`" if is_minutes else "Ej: `7`"
        await update.message.reply_text(f"❌ Valor inválido. {tip}", parse_mode="Markdown")
        return

    group = get_group_by_id(group_id)
    s = dict(group.get("settings", {}))
    s[field] = value
    group["settings"] = s
    await db.update_group_fields(group_id, {"settings": s})
    context.user_data.pop("cfg_field", None)
    context.user_data.pop("cfg_group_id", None)

    labels = {
        "trial_minutes":   f"Trial: *{fmt_minutes(value)}*",
        "price_semanal":   f"Precio semanal: *{fmt_price(value)}*",
        "price_mensual":   f"Precio mensual: *{fmt_price(value)}*",
        "duration_semanal": f"Días semanal: *{value}d*",
        "duration_mensual": f"Días mensual: *{value}d*",
    }
    await update.message.reply_text(
        f"✅ *Configuración actualizada*\n\n• {labels.get(field, f'{field}: {value}')}\n\nAplica en *{group['group_name']}*.",
        parse_mode="Markdown",
    )


# ── Pagos (configuración) ─────────────────────────────────────────────────────

async def handle_payment_config_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        return
    step     = context.user_data.get("config_payment_step")
    group_id = context.user_data.get("config_payment_group_id")
    if not step or not group_id or not can_manage_group(update.effective_user.id, group_id):
        return

    text  = update.message.text.strip()
    group = get_group_by_id(group_id)
    if not group:
        await update.message.reply_text("❌ Grupo no encontrado")
        context.user_data.pop("config_payment_step", None)
        context.user_data.pop("config_payment_group_id", None)
        return

    s = dict(group.get("settings", {}))

    if step == "bank_data":
        if text.lower() != "saltar":
            if text.lower() == "eliminar":
                s.pop("bank_data", None)
            else:
                s["bank_data"] = text
        group["settings"] = s
        await db.update_group_fields(group_id, {"settings": s})
        context.user_data["config_payment_step"] = "payment_contact"
        await update.message.reply_text(
            f"✅ Guardado.\n\nPaso 2/3: *Contacto para comprobantes*\n"
            f"Username de Telegram (ej: `ZonaEcFuego`)\n\n"
            f"📋 *Actual:* `{s.get('payment_contact', 'No configurado')}`\n\n*'saltar' para mantener.*",
            parse_mode="Markdown",
        )

    elif step == "payment_contact":
        if text.lower() != "saltar":
            s["payment_contact"] = text.lstrip("@").strip()
        group["settings"] = s
        await db.update_group_fields(group_id, {"settings": s})
        context.user_data["config_payment_step"] = "paypal_data"
        await update.message.reply_text(
            f"✅ Guardado.\n\nPaso 3/3: *PayPal (opcional)*\n"
            f"Email o link de PayPal\n\n"
            f"📋 *Actual:* `{s.get('paypal_data', 'No configurado')}`\n\n*'saltar' u 'eliminar'.*",
            parse_mode="Markdown",
        )

    elif step == "paypal_data":
        if text.lower() != "saltar":
            if text.lower() == "eliminar":
                s.pop("paypal_data", None)
            else:
                s["paypal_data"] = text
        group["settings"] = s
        await db.update_group_fields(group_id, {"settings": s})
        context.user_data.pop("config_payment_step", None)
        context.user_data.pop("config_payment_group_id", None)
        await update.message.reply_text(
            f"✅ *Configuración de pago completada*\n\n"
            f"🏦 Bancaria: `{s.get('bank_data','No configurado')}`\n"
            f"📤 Contacto: @{s.get('payment_contact','No configurado')}\n"
            f"🅿️ PayPal: `{s.get('paypal_data','No configurado')}`",
            parse_mode="Markdown",
        )


# ── Avisos ────────────────────────────────────────────────────────────────────

async def menu_warning_settings(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query
    await query.answer()
    group = get_group_by_id(group_id)
    if not group or not can_manage_group(query.from_user.id, group_id):
        await query.edit_message_text("❌ No autorizado o grupo no encontrado")
        return
    wc       = await db.get_warning_config(group_id)
    enabled  = wc.get("enabled", False)
    warnings = wc.get("warnings", [])
    status   = "🟢 ACTIVADOS" if enabled else "🔴 DESACTIVADOS"
    msg = (
        f"🔔 *Avisos — {group['group_name']}*\n\nEstado: *{status}*\n\n"
        f"*Avisos configurados ({len(warnings)}):*\n"
    )
    for i, w in enumerate(warnings, 1):
        msg += f"\n{i}. ⏰ {w.get('minutes_before', 15)} min antes"
    if not warnings:
        msg += "\n_Ninguno_"
    msg += (
        "\n\n*Variables para mensajes:*\n"
        "`{minutes_left}` `{expiry_time}` `{expiry_datetime}` `{group_name}` `{user_name}`"
    )
    keyboard = [
        [InlineKeyboardButton(f"{'🔴 Desactivar' if enabled else '🟢 Activar'} avisos", callback_data=f"warn_toggle_{group_id}")],
        [InlineKeyboardButton("➕ Añadir aviso",  callback_data=f"warn_add_{group_id}")],
    ]
    for i, w in enumerate(warnings):
        keyboard.append([
            InlineKeyboardButton(f"✏️ Editar {w.get('minutes_before',15)}min", callback_data=f"warn_edit_{group_id}_{i}"),
            InlineKeyboardButton("🗑️ Eliminar", callback_data=f"warn_delete_{group_id}_{i}"),
        ])
    keyboard += [
        [InlineKeyboardButton("🧪 Probar aviso", callback_data=f"warn_test_{group_id}")],
        [InlineKeyboardButton("🔙 Volver",       callback_data=f"cfg_group_{group_id}")],
    ]
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


async def toggle_warnings(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query
    await query.answer()
    if not can_manage_group(query.from_user.id, group_id):
        await query.answer("❌ No autorizado", show_alert=True)
        return
    wc = await db.get_warning_config(group_id)
    wc["enabled"] = not wc.get("enabled", False)
    await db.save_warning_config(group_id, wc)
    await menu_warning_settings(update, context, group_id)


async def _warning_input_request(update: Update, context: ContextTypes.DEFAULT_TYPE,
                                  group_id: int, step: str, warning_index: int | None, prompt: str):
    query = update.callback_query
    await query.answer()
    context.user_data.update({"warning_group_id": group_id, "warning_step": step, "warning_index": warning_index})
    await query.edit_message_text(prompt, parse_mode="Markdown")


async def delete_warning_entry(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int, idx: int):
    query = update.callback_query
    await query.answer()
    if not can_manage_group(query.from_user.id, group_id):
        await query.answer("❌ No autorizado", show_alert=True)
        return
    wc = await db.get_warning_config(group_id)
    warnings = wc.get("warnings", [])
    if idx < len(warnings):
        removed = warnings.pop(idx)
        wc["warnings"] = warnings
        await db.save_warning_config(group_id, wc)
        await query.edit_message_text(f"✅ Aviso de {removed.get('minutes_before',15)} min eliminado")
        await asyncio.sleep(1)
    await menu_warning_settings(update, context, group_id)


async def test_warning(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query
    await query.answer()
    if not can_manage_group(query.from_user.id, group_id):
        await query.answer("❌ No autorizado", show_alert=True)
        return
    wc = await db.get_warning_config(group_id)
    warnings = wc.get("warnings", [])
    if not warnings:
        await query.answer("❌ No hay avisos configurados", show_alert=True)
        return
    tmpl = warnings[0].get("message", "⏳ Tu trial expira pronto")
    g    = get_group_by_id(group_id)
    msg  = (tmpl
            .replace("{minutes_left}", "15")
            .replace("{expiry_time}", "22:45")
            .replace("{expiry_datetime}", "15/06/2026 22:45")
            .replace("{group_name}", g.get("group_name", "VIP") if g else "VIP")
            .replace("{user_name}", query.from_user.first_name or "Usuario"))
    try:
        await context.bot.send_message(
            query.from_user.id,
            f"🧪 *AVISO DE PRUEBA*\n\n---\n{msg}\n---",
            reply_markup=get_payment_keyboard(group_id, query.from_user.id),
            parse_mode="Markdown",
        )
        await query.edit_message_text("✅ *Aviso de prueba enviado a tu chat privado*", parse_mode="Markdown")
    except Exception as e:
        await query.edit_message_text(f"❌ Error: {e}")
    await asyncio.sleep(2)
    await menu_warning_settings(update, context, group_id)


async def handle_warning_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        return
    step     = context.user_data.get("warning_step")
    group_id = context.user_data.get("warning_group_id")
    if not step or not group_id or not can_manage_group(update.effective_user.id, group_id):
        return

    text = update.message.text.strip()
    if text.lower() == "cancelar":
        for k in ("warning_step", "warning_group_id", "warning_index", "warning_minutes"):
            context.user_data.pop(k, None)
        await update.message.reply_text("❌ Configuración de aviso cancelada")
        return

    wc       = await db.get_warning_config(group_id)
    warnings = wc.get("warnings", [])
    idx      = context.user_data.get("warning_index")

    if step in ("minutes", "edit_minutes"):
        if text.lower() == "saltar" and step == "edit_minutes":
            context.user_data["warning_step"] = "edit_message"
            existing_msg = (warnings[idx].get("message", "") if idx is not None and idx < len(warnings) else "")
            await update.message.reply_text(
                f"✏️ *Editar mensaje*\n\nActual:\n`{existing_msg}`\n\nEnvía el nuevo o 'saltar'.\n\n*Variables:* `{{minutes_left}}` `{{expiry_time}}` `{{expiry_datetime}}` `{{group_name}}` `{{user_name}}`\n\n_Escribe 'cancelar' para cancelar._",
                parse_mode="Markdown",
            )
            return
        try:
            minutes = int(text)
            if not 1 <= minutes <= 1440:
                raise ValueError
        except ValueError:
            await update.message.reply_text("❌ Ingresa un número entre 1 y 1440 minutos")
            return
        context.user_data["warning_minutes"] = minutes
        if step == "edit_minutes" and idx is not None and idx < len(warnings):
            warnings[idx]["minutes_before"] = minutes
            wc["warnings"] = warnings
            await db.save_warning_config(group_id, wc)
        context.user_data["warning_step"] = "edit_message" if step == "edit_minutes" else "message"
        existing_msg = (warnings[idx].get("message", "") if step == "edit_minutes" and idx is not None and idx < len(warnings) else "")
        prompt = (
            f"✏️ *Editar mensaje*\n\nActual:\n`{existing_msg}`\n\nEnvía el nuevo o 'saltar'."
            if step == "edit_minutes"
            else f"✅ Aviso a los {minutes} min.\n\nAhora envía el mensaje para el usuario.\n\n*Variables:* `{{minutes_left}}` `{{expiry_time}}` `{{expiry_datetime}}` `{{group_name}}` `{{user_name}}`"
        )
        await update.message.reply_text(prompt + "\n\n_'cancelar' para cancelar._", parse_mode="Markdown")

    elif step in ("message", "edit_message"):
        if text.lower() == "saltar" and step == "edit_message":
            for k in ("warning_step", "warning_group_id", "warning_index", "warning_minutes"):
                context.user_data.pop(k, None)
            await update.message.reply_text("✅ Aviso sin cambios en el mensaje")
            await update.message.reply_text(
                "🔔 Configuración guardada.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Volver", callback_data=f"cfg_warnings_{group_id}")]]),
            )
            return

        message_text = update.message.text.strip()
        if step == "edit_message" and idx is not None and idx < len(warnings):
            warnings[idx]["message"] = message_text
        else:
            mins = context.user_data.get("warning_minutes", 15)
            warnings.append({"minutes_before": mins, "message": message_text})
            warnings.sort(key=lambda w: w.get("minutes_before", 0), reverse=True)
        wc["warnings"] = warnings
        await db.save_warning_config(group_id, wc)
        for k in ("warning_step", "warning_group_id", "warning_index", "warning_minutes"):
            context.user_data.pop(k, None)
        await update.message.reply_text("✅ *Aviso guardado correctamente*", parse_mode="Markdown")
        await update.message.reply_text(
            "🔔 Configuración completada.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Volver", callback_data=f"cfg_warnings_{group_id}")]]),
        )


# ── Input de texto en privado (router) ───────────────────────────────────────

async def handle_edit_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Router de input privado. Orden de prioridad:
    1. Configuración de avisos
    2. Configuración de precios/trial
    3. Configuración de pago
    4. Edición múltiple de grupo (solo SUPER_ADMIN)

    FIX: la guarda SUPER_ADMIN_ID ahora solo aplica al bloque 4, 
    no bloquea a admins normales que usan cfg_field o config_payment.
    """
    if update.effective_chat.type != "private":
        return

    # 1. Avisos
    if context.user_data.get("warning_step") and context.user_data.get("warning_group_id"):
        await handle_warning_input(update, context)
        return

    # 2. Precios/trial
    if context.user_data.get("cfg_field") and context.user_data.get("cfg_group_id"):
        await handle_cfg_input(update, context)
        return

    # 3. Configuración de pago
    if context.user_data.get("config_payment_step") and context.user_data.get("config_payment_group_id"):
        await handle_payment_config_input(update, context)
        return

    # 4. Edición de grupo (solo SUPER_ADMIN)
    if update.effective_user.id != SUPER_ADMIN_ID:
        return

    text     = update.message.text.strip()
    field    = context.user_data.get("editing_field")
    group_id = context.user_data.get("editing_group_id")
    if not field or not group_id:
        return

    if text.lower() == "cancelar":
        for k in ("editing_field", "editing_group_id", "pending_changes", "editing_mode"):
            context.user_data.pop(k, None)
        await update.message.reply_text("❌ Edición cancelada")
        return

    group = get_group_by_id(group_id)
    if not group:
        await update.message.reply_text("❌ Grupo no encontrado")
        return

    if field == "multi_name":
        context.user_data.setdefault("pending_changes", {})["name"] = text
        await update.message.reply_text(f"✅ *Nombre guardado:* {text}", parse_mode="Markdown")
        context.user_data.pop("editing_field", None)
        await edit_group_multiple(update, context, group_id)
    elif field == "multi_admin":
        try:
            new_admin = int(text)
            context.user_data.setdefault("pending_changes", {})["admin"] = new_admin
            await update.message.reply_text(f"✅ *Admin guardado:* `{new_admin}`", parse_mode="Markdown")
            context.user_data.pop("editing_field", None)
            await edit_group_multiple(update, context, group_id)
        except ValueError:
            await update.message.reply_text("❌ El ID debe ser un número")


# ── Comandos auxiliares ───────────────────────────────────────────────────────

async def search_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != SUPER_ADMIN_ID:
        await update.message.reply_text("❌ Solo Super Admin")
        return
    if not context.args:
        await update.message.reply_text("❌ Usa: `/searchgrupo nombre`", parse_mode="Markdown")
        return
    term    = " ".join(context.args).lower()
    results = [g for g in GROUPS if term in g["group_name"].lower()]
    if not results:
        await update.message.reply_text(f"📭 No se encontraron grupos con '{term}'")
        return
    msg = f"🔍 *Resultados para '{term}'*\n\n"
    for g in results:
        emoji = "👑" if g.get("type") == "VIP" else "📋"
        msg += f"{emoji} *{g['group_name']}*\n   🆔 `{g['group_id']}`\n   👑 Admin: `{g['admin_id']}`\n\n"
    await update.message.reply_text(msg, parse_mode="Markdown")


async def get_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in (SUPER_ADMIN_ID, 8682208062):
        await update.message.reply_text("❌ No autorizado")
        return
    if not context.args:
        await update.message.reply_text("❌ Usa: `/getlink @username` o `/getlink ID`", parse_mode="Markdown")
        return
    identifier = context.args[0].lstrip("@")
    try:
        if identifier.isdigit():
            await update.message.reply_text(f"🔗 `tg://user?id={identifier}`", parse_mode="Markdown")
        else:
            group_id = context.user_data.get("current_group")
            if not group_id:
                await update.message.reply_text("❌ Selecciona un grupo con /start")
                return
            user = await db.get_user_by_username(identifier, group_id)
            if user:
                name = user.get("first_name") or user.get("username") or identifier
                await update.message.reply_text(f"🔗 Chat con {name}:\n`tg://user?id={user['user_id']}`", parse_mode="Markdown")
            else:
                await update.message.reply_text(f"❌ No se encontró @{identifier}")
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")


async def sync_all_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != SUPER_ADMIN_ID:
        await update.message.reply_text("❌ Solo Super Admin")
        return
    msg = "📊 *Estado actual de grupos*\n\n"
    for g in GROUPS:
        users = await db.get_all_active_users(g["group_id"])
        emoji = "👑" if g.get("type") == "VIP" else "📋"
        msg += f"{emoji} *{g['group_name']}*: {len(users)} activos\n"
    msg += "\n\nℹ️ Los usuarios se registran al entrar al grupo automáticamente."
    await update.message.reply_text(msg, parse_mode="Markdown")


async def diagnose_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id  = update.effective_user.id
    chat_id  = update.effective_chat.id
    is_group = update.effective_chat.type in ("group", "supergroup")
    target   = chat_id if is_group else context.user_data.get("current_group")
    if not target:
        await update.message.reply_text("❌ Usa en el grupo o selecciona uno con /start.")
        return
    if not can_manage_group(user_id, target):
        await update.message.reply_text("❌ No autorizado")
        return
    group = get_group_by_id(target)
    if not group:
        await update.message.reply_text(f"❌ Grupo `{target}` no registrado.", parse_mode="Markdown")
        return
    lines = [f"🔍 *Diagnóstico — {group['group_name']}*\n", f"🆔 `{target}`", f"📋 Tipo: {group.get('type','VIP')}", f"👑 Admin: `{group['admin_id']}`\n"]
    try:
        bot_member = await context.bot.get_chat_member(target, context.bot.id)
        status = bot_member.status
        lines.append(f"🤖 Estado del bot: `{status}`")
        if status == "administrator":
            can_ban = getattr(bot_member, "can_restrict_members", False)
            lines.append(f"✅ Admin — expulsión: {'✅' if can_ban else '❌ (habilita puede banear)'}")
        elif status == "member":
            lines += ["⚠️ Miembro normal — detección OK, expulsión ❌", "→ Promover a admin con permiso 'Banear usuarios'"]
        else:
            lines.append(f"❌ Estado inesperado: {status}")
    except Exception as e:
        lines.append(f"❌ Error al verificar: `{e}`")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def manual_backup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != SUPER_ADMIN_ID:
        await update.message.reply_text("❌ Solo Super Admin")
        return
    output = StringIO()
    writer = csv.writer(output)
    now = datetime.now()
    writer.writerow(["group_id", "group_name", "group_type", "admin_id", "backup_date"])
    for g in GROUPS:
        writer.writerow([g["group_id"], g["group_name"], g.get("type","VIP"), g["admin_id"], now.strftime("%Y-%m-%d %H:%M:%S")])
    output.seek(0)
    await update.message.reply_document(
        document=output.getvalue().encode("utf-8-sig"),
        filename=f"backup_manual_{now.strftime('%Y%m%d_%H%M%S')}.csv",
        caption=f"📦 *Backup Manual*\n\n📅 {now.strftime('%d/%m/%Y %H:%M:%S')}\n📊 Grupos: {len(GROUPS)}",
        parse_mode="Markdown",
    )
    output.close()


async def restore_backup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != SUPER_ADMIN_ID:
        await update.message.reply_text("❌ Solo Super Admin")
        return
    if not update.message.document:
        await update.message.reply_text("❌ Envía el archivo CSV junto con el comando.")
        return
    await update.message.reply_text("🔄 Restaurando...")
    try:
        import io
        file_bytes = await (await update.message.document.get_file()).download_as_bytearray()
        reader = csv.reader(io.StringIO(file_bytes.decode("utf-8")))
        next(reader)
        count = 0
        for row in reader:
            if len(row) < 4:
                continue
            gid, gname, gtype, aid = int(row[0]), row[1], row[2], int(row[3])
            existing = get_group_by_id(gid)
            if existing:
                for g in GROUPS:
                    if g["group_id"] == gid:
                        g.update({"group_name": gname, "type": gtype, "admin_id": aid})
                        break
                await db.update_group_fields(gid, {"name": gname, "admin": aid, "type": gtype})
            else:
                GROUPS.append({"group_id": gid, "group_name": gname, "type": gtype, "admin_id": aid, "settings": {}})
                await db.save_group(gid, gname, aid, gtype)
            count += 1
        await update.message.reply_text(f"✅ *Restauración completa*\n\n📊 Grupos restaurados: {count}", parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"❌ Error al restaurar: {e}")


async def auto_backup():
    last_file = "last_backup.txt"
    try:
        last = datetime.fromisoformat(open(last_file).read().strip()) if os.path.exists(last_file) else None
    except Exception:
        last = None
    now = datetime.now()
    if last and (now - last).days < 15:
        return
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(["group_id", "group_name", "group_type", "admin_id", "backup_date"])
    for g in GROUPS:
        writer.writerow([g["group_id"], g["group_name"], g.get("type","VIP"), g["admin_id"], now.strftime("%Y-%m-%d %H:%M:%S")])
    output.seek(0)
    try:
        await bot_app.bot.send_document(
            SUPER_ADMIN_ID,
            document=output.getvalue().encode("utf-8-sig"),
            filename=f"backup_{now.strftime('%Y%m%d')}.csv",
            caption=f"📦 *Backup Automático*\n📅 {now.strftime('%d/%m/%Y %H:%M')}\n📊 Grupos: {len(GROUPS)}",
            parse_mode="Markdown",
        )
        output.close()
        open(last_file, "w").write(now.isoformat())
        logger.info("✅ Backup automático enviado")
    except Exception as e:
        logger.error("❌ Error en backup automático: %s", e)


# ── Comandos de menú ──────────────────────────────────────────────────────────

async def menu_commands(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    text = (
        "📟 *Comandos Disponibles*\n\n"
        "*Super Admin:* `/start` `/addgroup` `/groups` `/backup` `/searchgrupo`\n\n"
        "*Admin de Grupo:*\n"
        "• `/start` — Panel\n"
        "• `/add @usuario|ID plan [precio] [días]`\n"
        "• Responder a registro: `/add plan`\n"
        "• `/configpago group_id`\n\n"
        "*Usuarios VIP:* `/pagar` o botón 💳\n\n"
        "*Planes:* `trial` | `semanal` | `mensual`\n\n"
        "*Ejemplos:*\n`/add @juan semanal`\n`/add 123456789 mensual`\n`/add @juan semanal 5.99 10`"
    )
    keyboard = [[InlineKeyboardButton("🔙 Volver", callback_data="back_to_admin")]]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


# ── Callback principal ────────────────────────────────────────────────────────

# Mapa estático de callbacks simples (callback_data → función)
_SIMPLE_CALLBACKS = {
    "list_active":            list_active_users,
    "earnings":               show_earnings,
    "export_month":           export_report,
    "list_potential":         list_potential_clients,
    "export_clients":         export_clients,
    "total_earnings":         total_earnings,
    "trial_stats":            trial_stats,
    "menu_groups":            menu_groups,
    "menu_view_groups":       menu_view_groups,
    "menu_edit_group_select": menu_edit_group_select,
    "menu_delete_group_select": menu_delete_group_select,
    "menu_commands":          menu_commands,
}

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data  = query.data
    logger.info("🔔 CALLBACK: %s", data)

    try:
        # Callbacks simples
        if data in _SIMPLE_CALLBACKS:
            await query.answer()
            await _SIMPLE_CALLBACKS[data](update, context)
            return

        if data == "add_user":
            await query.answer()
            await query.edit_message_text("📝 Usa: `/add @username plan` o `/add ID plan`", parse_mode="Markdown")
        elif data == "back_to_admin":
            await query.answer()
            try:
                await query.message.delete()
            except Exception:
                pass
            await start(update, context)
        elif data == "view_vip_groups":
            await query.answer()
            await show_groups_by_type(update, context, "VIP")
        elif data == "view_free_groups":
            await query.answer()
            await show_groups_by_type(update, context, "FREE")

        # Prefijos con group_id
        elif data.startswith("select_group_"):
            await select_group(update, context, int(data[13:]))
        elif data.startswith("delete_confirm_"):
            await delete_group_confirm(update, context, int(data[15:]))
        elif data.startswith("delete_yes_"):
            await delete_group_execute(update, context, int(data[11:]))
        elif data.startswith("edit_multiple_"):
            await edit_group_multiple(update, context, int(data[14:]))
        elif data.startswith("multi_apply_"):
            await multi_apply_changes(update, context, int(data[12:]))
        elif data.startswith("multi_name_"):
            gid = int(data[11:])
            context.user_data["editing_field"] = "multi_name"
            context.user_data["editing_group_id"] = gid
            await query.answer()
            await query.edit_message_text("✏️ *Nuevo nombre:*\n\n_Escribe 'cancelar' para cancelar._", parse_mode="Markdown")
        elif data.startswith("multi_admin_"):
            gid = int(data[12:])
            context.user_data["editing_field"] = "multi_admin"
            context.user_data["editing_group_id"] = gid
            await query.answer()
            await query.edit_message_text("👤 *Nuevo admin ID:*\n\n_Escribe 'cancelar' para cancelar._", parse_mode="Markdown")
        elif data.startswith("multi_type_"):
            gid = int(data[11:])
            await query.answer()
            keyboard = [
                [InlineKeyboardButton("👑 VIP",  callback_data=f"multi_set_type_{gid}_VIP")],
                [InlineKeyboardButton("📋 FREE", callback_data=f"multi_set_type_{gid}_FREE")],
                [InlineKeyboardButton("🔙 Volver", callback_data=f"edit_multiple_{gid}")],
            ]
            await query.edit_message_text("🔄 *Selecciona el nuevo tipo*", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
        elif data.startswith("multi_set_type_"):
            parts = data.split("_")
            gid, new_type = int(parts[3]), parts[4]
            context.user_data.setdefault("pending_changes", {})["type"] = new_type
            await query.answer(f"Tipo {new_type} guardado")
            await edit_group_multiple(update, context, gid)
        elif data.startswith("cfg_group_"):
            await menu_group_settings(update, context, int(data[10:]))
        elif data.startswith("cfg_trial_"):
            gid = int(data[10:])
            cfg = get_group_plan_config(gid, "trial")
            await _cfg_request(update, context, gid, "trial_minutes",
                f"⏱ *Trial actual:* {fmt_minutes(cfg.get('minutes',1440))}\n\nEnvía los nuevos minutos (ej: 1440 = 1 día)\n\n_'cancelar' para cancelar._")
        elif data.startswith("cfg_price_semanal_"):
            gid = int(data[18:])
            cfg = get_group_plan_config(gid, "semanal")
            await _cfg_request(update, context, gid, "price_semanal",
                f"💲 *Precio semanal actual:* {fmt_price(cfg['price'])}\n\nEnvía el nuevo precio (ej: 9.99)\n\n_'cancelar' para cancelar._")
        elif data.startswith("cfg_price_mensual_"):
            gid = int(data[18:])
            cfg = get_group_plan_config(gid, "mensual")
            await _cfg_request(update, context, gid, "price_mensual",
                f"💲 *Precio mensual actual:* {fmt_price(cfg['price'])}\n\nEnvía el nuevo precio\n\n_'cancelar' para cancelar._")
        elif data.startswith("cfg_dur_semanal_"):
            gid = int(data[16:])
            cfg = get_group_plan_config(gid, "semanal")
            await _cfg_request(update, context, gid, "duration_semanal",
                f"📅 *Días semanal actual:* {cfg.get('days',7)}d\n\nEnvía los nuevos días (ej: 7)\n\n_'cancelar' para cancelar._")
        elif data.startswith("cfg_dur_mensual_"):
            gid = int(data[16:])
            cfg = get_group_plan_config(gid, "mensual")
            await _cfg_request(update, context, gid, "duration_mensual",
                f"📆 *Días mensual actual:* {cfg.get('days',30)}d\n\nEnvía los nuevos días (ej: 30)\n\n_'cancelar' para cancelar._")
        elif data.startswith("cfg_payment_"):
            gid   = int(data[12:])
            group = get_group_by_id(gid)
            if not group:
                await query.edit_message_text("❌ Grupo no encontrado")
                return
            await _start_payment_config(query.edit_message_text, context, group, edit=True)
        elif data.startswith("cfg_warnings_"):
            await menu_warning_settings(update, context, int(data[13:]))
        elif data.startswith("warn_toggle_"):
            await toggle_warnings(update, context, int(data[12:]))
        elif data.startswith("warn_add_"):
            gid = int(data[9:])
            await _warning_input_request(update, context, gid, "minutes", None,
                "➕ *Nuevo aviso*\n\nMinutos antes de expirar (ej: 15, 60)\n\n_'cancelar' para cancelar._")
        elif data.startswith("warn_edit_"):
            parts = data.split("_")
            await _warning_input_request(update, context, int(parts[2]), "edit_minutes", int(parts[3]),
                f"✏️ *Editar aviso*\n\nEnvía los nuevos minutos o 'saltar'.\n\n_'cancelar' para cancelar._")
        elif data.startswith("warn_delete_"):
            parts = data.split("_")
            await delete_warning_entry(update, context, int(parts[2]), int(parts[3]))
        elif data.startswith("warn_test_"):
            await test_warning(update, context, int(data[10:]))
        elif data.startswith("pay_"):
            parts = data.split("_")
            if len(parts) >= 3:
                pay_gid = int(parts[1])
                pay_uid = int(parts[2])
                if query.from_user.id == pay_uid:
                    await query.answer("💳 Enviando datos...")
                    await send_payment_info(context.bot, pay_uid, pay_gid, triggered_by="botón Pagar")
                else:
                    await query.answer("❌ Este botón no es para ti", show_alert=True)
        elif data == "add_group":
            await query.answer()
            await query.edit_message_text('📝 Usa: `/addgroup group_id TIPO "nombre" admin_id`', parse_mode="Markdown")
        else:
            await query.answer()
            logger.warning("Callback desconocido: %s", data)

    except Exception as e:
        logger.error("❌ Error en callback '%s': %s", data, e, exc_info=True)
        try:
            await query.answer("❌ Ocurrió un error, intenta de nuevo", show_alert=True)
        except Exception:
            pass


# ==================== TAREAS PROGRAMADAS ====================

async def check_expired_subscriptions():
    logger.info("🔍 Verificando suscripciones expiradas...")

    async def _process_group(group: dict):
        group_id      = group["group_id"]
        expired_users = await db.get_expired_users(group_id)
        if not expired_users:
            return
        logger.info("Usuarios expirados en %s: %d", group["group_name"], len(expired_users))

        for user in expired_users:
            user_id  = user["user_id"]
            fn       = user.get("first_name") or ""
            un       = user.get("username")   or ""
            display  = fn or (f"@{un}" if un and not un.startswith("user_") else f"ID:{user_id}")
            plan     = user.get("plan", "desconocido")
            end_date = user.get("end_date")
            exp_str  = end_date.strftime("%d/%m/%Y %H:%M") if end_date else "N/A"
            chat_lnk = f"tg://user?id={user_id}"

            was_updated = await db.expire_user(user_id, group_id)
            if not was_updated:
                continue  # Otro ciclo ya lo procesó

            kick_ok = await _kick_user_with_retry(group_id, user_id)
            action  = "expulsado" if kick_ok else "marcado como expirado (expulsión manual requerida)"

            await _safe_send(
                group["admin_id"],
                f"{'🚫' if kick_ok else '⚠️'} *Suscripción expirada*\n\n"
                f"👤 [{display}]({chat_lnk})\n📋 Plan: {plan}\n📅 Expiró: {exp_str}\n📌 {group['group_name']}\n\n✅ {action}.",
                parse_mode="Markdown",
            )
            await _safe_send(
                user_id,
                f"⏰ *Tu acceso ha expirado*\n\nTu plan *{plan}* en *{group['group_name']}* ha vencido.\n\n💳 ¿Quieres renovar?",
                reply_markup=get_payment_keyboard(group_id, user_id),
                parse_mode="Markdown",
            )

    vip_groups = [g for g in GROUPS if g.get("type") == "VIP"]
    results    = await asyncio.gather(*[_process_group(g) for g in vip_groups], return_exceptions=True)
    for g, r in zip(vip_groups, results):
        if isinstance(r, Exception):
            logger.error("❌ Error procesando %s: %s", g["group_name"], r, exc_info=r)


async def send_trial_warnings():
    logger.debug("⏰ Verificando avisos de expiración...")

    for group in GROUPS:
        if group.get("type") != "VIP":
            continue
        group_id = group["group_id"]
        try:
            wc = await db.get_warning_config(group_id)
        except Exception as e:
            logger.error("❌ Warning config error grupo %d: %s", group_id, e)
            continue

        if not wc.get("enabled", False):
            continue

        for warning in wc.get("warnings", []):
            mins_before = warning.get("minutes_before", 15)
            tmpl        = warning.get("message", "⏳ Tu trial expira en {minutes_left} minutos")
            warning_type = f"warning_{mins_before}min"

            try:
                users = await db.get_users_for_warning(group_id, mins_before)
            except Exception as e:
                logger.error("❌ get_users_for_warning error: %s", e)
                continue

            for user in users:
                end_date  = user["end_date"]
                mins_left = max(0, int((end_date - datetime.now()).total_seconds() / 60))
                if mins_left <= 0:
                    continue
                msg = (tmpl
                       .replace("{minutes_left}", str(mins_left))
                       .replace("{expiry_time}", end_date.strftime("%H:%M"))
                       .replace("{expiry_datetime}", end_date.strftime("%d/%m/%Y %H:%M"))
                       .replace("{group_name}", group.get("group_name", "VIP"))
                       .replace("{user_name}", user.get("first_name") or "Usuario"))
                try:
                    await bot_app.bot.send_message(
                        user["user_id"], msg,
                        reply_markup=get_payment_keyboard(group_id, user["user_id"]),
                        parse_mode="Markdown",
                    )
                    await db.log_warning_sent(user["user_id"], group_id, warning_type)
                    logger.info("⚠️ Aviso %s → user %d (%d min restantes)", warning_type, user["user_id"], mins_left)
                except Exception as e:
                    logger.warning("No se pudo enviar aviso a %d: %s", user["user_id"], e)


# ==================== MAIN ====================

async def main():
    global bot_app
    await db.init_tables()
    await db.load_groups_from_db()
    logger.info("📦 %d grupos disponibles", len(GROUPS))

    bot_app = ApplicationBuilder().token(TOKEN).build()

    # Comandos
    for cmd, handler in [
        ("start",       start),
        ("add",         add_user_command),
        ("groups",      list_groups),
        ("addgroup",    add_group_command),
        ("backup",      manual_backup),
        ("restore",     restore_backup),
        ("getlink",     get_link),
        ("syncall",     sync_all_groups),
        ("searchgrupo", search_group),
        ("pagar",       pay_command),
        ("configpago",  config_payment_command),
        ("test",        test),
        ("diaggrupo",   diagnose_group),
    ]:
        bot_app.add_handler(CommandHandler(cmd, handler))

    # Callbacks
    bot_app.add_handler(CallbackQueryHandler(handle_callback))

    # Detección de miembros (v20+ primero, legacy como fallback)
    bot_app.add_handler(ChatMemberHandler(track_chat_member, ChatMemberHandler.CHAT_MEMBER))
    bot_app.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, detect_new_member))

    # Mensajes en grupos FREE y privado
    bot_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.GROUPS, detect_active_member))
    bot_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, handle_edit_input))

    # Scheduler
    scheduler.add_job(check_expired_subscriptions, "interval", minutes=5,  id="check_expired",  replace_existing=True)
    scheduler.add_job(auto_backup,                 "interval", hours=24,   id="auto_backup",    replace_existing=True)
    scheduler.add_job(send_trial_warnings,         "interval", minutes=1,  id="trial_warnings", replace_existing=True)
    scheduler.start()

    logger.info("🤖 Bot iniciado")
    await bot_app.initialize()
    await bot_app.start()
    await bot_app.updater.start_polling(
        drop_pending_updates=True,
        allowed_updates=["message", "callback_query", "chat_member", "my_chat_member"],
    )
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
