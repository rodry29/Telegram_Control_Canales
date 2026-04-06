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
    ContextTypes, Defaults, ChatMemberHandler
)
from telegram.ext import MessageHandler, filters

# ---------- CONFIGURACIÓN MULTI-GRUPO CON ROLES ----------
SUPER_ADMIN_ID = 5054216496

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

def get_group_by_id(group_id: int) -> dict:
    """Obtiene la configuración de un grupo por su ID"""
    for group in GROUPS:
        if group["group_id"] == group_id:
            return group
    return None

def get_groups_by_admin(admin_id: int) -> list:
    """Obtiene todos los grupos donde un usuario es admin"""
    if admin_id == SUPER_ADMIN_ID:
        # Super admin ve todos los grupos
        return GROUPS
    else:
        # Admin normal solo ve su grupo
        return [g for g in GROUPS if g["admin_id"] == admin_id]

def can_manage_group(user_id: int, group_id: int) -> bool:
    """Verifica si un usuario puede gestionar un grupo"""
    if user_id == SUPER_ADMIN_ID:
        return True
    group = get_group_by_id(group_id)
    return group and group["admin_id"] == user_id

def get_group_name(group_id: int) -> str:
    """Obtiene el nombre del grupo"""
    group = get_group_by_id(group_id)
    return group["group_name"] if group else f"Grupo {group_id}"

async def detect_new_member_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Detecta cuando alguien entra al grupo mediante el mensaje de Telegram"""
    if not update.message:
        return
    
    # Verificar que es el grupo correcto
    if update.message.chat_id != VIP_GROUP_ID:
        return
    
    # Verificar si hay nuevos miembros en el mensaje
    if update.message.new_chat_members:
        for new_member in update.message.new_chat_members:
            # Ignorar si es el bot mismo
            if new_member.id == context.bot.id:
                continue
            
            user_id = new_member.id
            username = new_member.username or f"user_{user_id}"
            first_name = new_member.first_name or ""
            
            logger.info(f"📥 Nuevo miembro detectado por mensaje: @{username} ({user_id})")
            
            # Registrar automáticamente
            registered, result = await db.register_user_auto(user_id, username, first_name)
            
            if registered:
                if result == "trial_nuevo":
                    welcome_msg = (
                        f"🎉 ¡Bienvenido @{username}!\n\n"
                        f"✨ Has recibido un **TRIAL GRATIS de 1 día**\n"
                        f"📅 Expira: {(datetime.now() + timedelta(days=1)).strftime('%d/%m/%Y')}\n\n"
                        f"Para continuar después del trial, contacta al administrador.\n\n"
                        f"Planes disponibles:\n"
                        f"• 📅 Semanal (7 días) - $10\n"
                        f"• 📆 Mensual (30 días) - $20"
                    )
                    await context.bot.send_message(user_id, welcome_msg, parse_mode="Markdown")
                    await context.bot.send_message(
                        ADMIN_ID,
                        f"🆕 *Nuevo usuario registrado automáticamente*\n"
                        f"👤 @{username}\n"
                        f"🎁 Trial activado por 1 día",
                        parse_mode="Markdown"
                    )
                elif result == "activo":
                    user_data = await db.get_user_by_id(user_id)
                    if user_data:
                        days_left = (user_data['end_date'] - datetime.now()).days
                        await context.bot.send_message(
                            user_id,
                            f"🎉 ¡Bienvenido de vuelta @{username}!\n\n"
                            f"✅ Tu suscripción está activa\n"
                            f"📅 Expira en {days_left} días"
                        )
            else:
                if result == "expirado":
                    # Expulsar inmediatamente
                    await context.bot.ban_chat_member(VIP_GROUP_ID, user_id)
                    await context.bot.send_message(
                        ADMIN_ID,
                        f"🚫 *ACCESO DENEGADO*\n"
                        f"👤 @{username}\n"
                        f"❌ Usuario expirado intentó reingresar - Expulsado",
                        parse_mode="Markdown"
                    )
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ---------- CONFIGURACIÓN ----------
TOKEN = "8782944509:AAFqTBOCPwJdhRgt2Qxx4Usj45DNF83Y86s"
VIP_GROUP_ID = -1003842587095
ADMIN_ID = 8682208062
DATABASE_URL = os.getenv("DATABASE_URL")

# Planes y precios
PLANS = {
    "trial": {"days": 1, "price": 0, "name": "🎁 Trial (1 día)"},
    "semanal": {"days": 7, "price": 10, "name": "📅 Semanal (7 días)"},
    "mensual": {"days": 30, "price": 20, "name": "📆 Mensual (30 días)"}
}

# Logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ---------- BASE DE DATOS ----------
class Database:
    def __init__(self, db_url: str):
        self.db_url = db_url
        
    def get_connection(self):
        conn = psycopg2.connect(self.db_url)
        conn.autocommit = True
        return conn

    async def init_tables(self):
        """Inicializa las tablas con soporte multi-grupo y roles"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Tabla de grupos
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
                
                # Tabla de usuarios (con group_id)
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
                
                # Tabla de pagos (con group_id)
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
                
                # Índices
                cur.execute("CREATE INDEX IF NOT EXISTS idx_users_group ON users(group_id, status)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_users_end_date ON users(group_id, end_date)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_payments_group ON payments(group_id, payment_date)")
                
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
        
        logger.info(f"✅ Base de datos inicializada con {len(GROUPS)} grupos")
    
    async def init_tables(self):
        """Inicializa las tablas"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Tabla de usuarios
                cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    plan TEXT NOT NULL,
                    start_date TIMESTAMP NOT NULL,
                    end_date TIMESTAMP NOT NULL,
                    status TEXT DEFAULT 'active',
                    trial_used BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
                """)
                
                # Tabla de pagos
                cur.execute("""
                CREATE TABLE IF NOT EXISTS payments (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    username TEXT,
                    plan TEXT NOT NULL,
                    amount INTEGER NOT NULL,
                    payment_date TIMESTAMP DEFAULT NOW()
                )
                """)
                
                # Índices
                cur.execute("CREATE INDEX IF NOT EXISTS idx_users_end_date ON users(end_date)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_users_status ON users(status)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_payments_date ON payments(payment_date)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_payments_user_id ON payments(user_id)")
                
                conn.commit()
        logger.info("✅ Base de datos inicializada")
    
    async def get_user_by_username(self, username: str) -> Optional[Dict]:
        """Busca usuario por username"""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM users WHERE username = %s", (username,))
                return cur.fetchone()
    
    async def get_user_by_id(self, user_id: int) -> Optional[Dict]:
        """Busca usuario por user_id"""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM users WHERE user_id = %s", (user_id,))
                return cur.fetchone()
    
    async def register_user_auto(self, user_id: int, username: str, first_name: str) -> Tuple[bool, str]:
        """Registra usuario automáticamente al entrar al grupo"""
        now = datetime.now()
        
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT user_id, trial_used, status, end_date FROM users WHERE user_id = %s", (user_id,))
                existing = cur.fetchone()
                
                if not existing:
                    # Nuevo usuario - dar trial
                    end_date = now + timedelta(days=PLANS["trial"]["days"])
                    cur.execute("""
                    INSERT INTO users (user_id, username, first_name, plan, start_date, end_date, trial_used, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, 'active')
                    """, (user_id, username, first_name, "trial", now, end_date, True))
                    cur.execute("INSERT INTO payments (user_id, username, plan, amount, payment_date) VALUES (%s, %s, %s, %s, %s)", (user_id, username, "trial", 0, now))
                    conn.commit()
                    return True, "trial_nuevo"
                
                elif existing['status'] == 'active' and existing['end_date'] > now:
                    # Usuario activo - actualizar username
                    cur.execute("UPDATE users SET username = %s, first_name = %s WHERE user_id = %s", (username, first_name, user_id))
                    conn.commit()
                    return True, "activo"
                
                elif existing['status'] == 'active' and existing['end_date'] <= now:
                    # Usuario con suscripción vencida PERO que podría haber renovado recientemente
                    # Dar un margen de 1 hora para permitir renovación
                    time_since_expiry = now - existing['end_date']
                    if time_since_expiry.total_seconds() < 3600:  # 1 hora de gracia
                        logger.info(f"Usuario {username} en periodo de gracia - permitiendo reingreso")
                        # Extender un poco más (1 hora) para dar tiempo a renovar
                        new_end = now + timedelta(hours=1)
                        cur.execute("UPDATE users SET end_date = %s, updated_at = NOW() WHERE user_id = %s", (new_end, user_id))
                        conn.commit()
                        return True, "periodo_gracia"
                    else:
                        return False, "expirado"
                else:
                    return False, "expirado"
    
    async def add_or_update_user(self, username: str, plan: str, user_id: int = None) -> Tuple[bool, str]:
        """Agrega o renueva usuario - NO expulsa durante renovación"""
        now = datetime.now()
        
        if plan not in PLANS:
            return False, "❌ Plan inválido"
        
        config = PLANS[plan]
        
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                existing = await self.get_user_by_username(username)
                
                if existing:
                    user_id = existing['user_id']
                    
                    # Verificar si ya usó trial
                    if plan == "trial" and existing['trial_used']:
                        return False, "❌ Este usuario ya usó su prueba gratuita"
                    
                    # ✅ IMPORTANTE: Siempre reactivar, incluso si estaba expirado
                    end_date = now + timedelta(days=config['days'])
                    
                    cur.execute("""
                    UPDATE users 
                    SET plan = %s, start_date = %s, end_date = %s, status = 'active',
                        updated_at = NOW(), username = %s,
                        trial_used = trial_used OR %s
                    WHERE user_id = %s
                    """, (plan, now, end_date, username, plan == "trial", user_id))
                    
                else:
                    if not user_id:
                        return False, f"❌ No tengo el ID de @{username}. Pídele que envíe un mensaje al bot."
                    
                    end_date = now + timedelta(days=config['days'])
                    trial_used = (plan == "trial")
                    
                    cur.execute("""
                    INSERT INTO users (user_id, username, first_name, plan, start_date, end_date, trial_used, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, 'active')
                    """, (user_id, username, "", plan, now, end_date, trial_used))
                
                # Registrar pago
                cur.execute("""
                INSERT INTO payments (user_id, username, plan, amount, payment_date)
                VALUES (%s, %s, %s, %s, %s)
                """, (user_id, username, plan, config['price'], now))
                
                conn.commit()
                
                message = f"✅ @{username} activado con {config['name']}\n📅 Expira: {end_date.strftime('%d/%m/%Y')}"
                return True, message
    
    async def get_expiring_users(self, days_before: int) -> List[Dict]:
        """Obtiene usuarios que expiran en X días"""
        target_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=days_before)
        target_end = target_date + timedelta(days=1)
        
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                SELECT user_id, username, plan, end_date
                FROM users
                WHERE status = 'active'
                AND end_date >= %s AND end_date < %s
                """, (target_date, target_end))
                return cur.fetchall()
    
    async def get_expired_users(self) -> List[Dict]:
        """Obtiene usuarios ya expirados"""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                SELECT user_id, username, plan, end_date
                FROM users
                WHERE status = 'active'
                AND end_date < NOW()
                """)
                return cur.fetchall()
    
    async def expire_user(self, user_id: int):
        """Marca usuario como expirado"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                UPDATE users SET status = 'expired', updated_at = NOW()
                WHERE user_id = %s
                """, (user_id,))
                conn.commit()
                logger.info(f"⏰ Usuario {user_id} marcado como expirado")
    
    async def get_monthly_report(self, year: int, month: int) -> Dict:
        """Genera reporte mensual"""
        start_date = datetime(year, month, 1)
        if month == 12:
            end_date = datetime(year + 1, 1, 1)
        else:
            end_date = datetime(year, month + 1, 1)
        
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                SELECT user_id, username, plan, amount, payment_date
                FROM payments
                WHERE payment_date >= %s AND payment_date < %s
                ORDER BY payment_date DESC
                """, (start_date, end_date))
                transactions = cur.fetchall()
                
                cur.execute("""
                SELECT plan, COUNT(*) as count, SUM(amount) as total
                FROM payments
                WHERE payment_date >= %s AND payment_date < %s
                GROUP BY plan
                """, (start_date, end_date))
                summary = cur.fetchall()
                
                total = sum(row['total'] for row in summary)
                
                cur.execute("""
                SELECT COUNT(*) as new_users
                FROM users
                WHERE created_at >= %s AND created_at < %s
                """, (start_date, end_date))
                new_users = cur.fetchone()['new_users']
                
                return {
                    "year": year,
                    "month": month,
                    "transactions": transactions,
                    "summary": summary,
                    "total": total,
                    "new_users": new_users
                }
    
    async def get_active_users_count(self) -> int:
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM users WHERE status = 'active' AND end_date > NOW()")
                return cur.fetchone()[0]
    
    async def get_all_active_users(self) -> List[Dict]:
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                SELECT user_id, username, first_name, plan, start_date, end_date,
                       EXTRACT(DAY FROM (end_date - NOW())) as days_left
                FROM users
                WHERE status = 'active' AND end_date > NOW()
                ORDER BY end_date ASC
                """)
                return cur.fetchall()

    async def reactivate_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/reactivate @username plan - Reactiva usuario expulsado/expirado"""
        if update.effective_user.id != ADMIN_ID:
            return
        
        if len(context.args) < 2:
            await update.message.reply_text(
                "❌ *Formato:* `/reactivate @username plan`\n\n"
                "Ejemplo: `/reactivate @juan semanal`\n\n"
                "⚠️ Esto reactivará al usuario aunque haya sido expulsado.",
                parse_mode="Markdown"
            )
            return
        
        username = context.args[0].replace("@", "")
        plan = context.args[1].lower()
        
        if plan not in PLANS:
            await update.message.reply_text("❌ Plan inválido. Usa: semanal o mensual")
            return
        
        # Buscar el usuario
        user = await db.get_user_by_username(username)
        
        if not user:
            await update.message.reply_text(f"❌ No hay registro de @{username}")
            return
        
        now = datetime.now()
        plan_config = PLANS[plan]
        end_date = now + timedelta(days=plan_config['days'])
        
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                # Reactivar usuario
                cur.execute("""
                UPDATE users 
                SET plan = %s, start_date = %s, end_date = %s, status = 'active',
                    updated_at = NOW()
                WHERE user_id = %s
                """, (plan, now, end_date, user['user_id']))
                
                # Registrar pago
                cur.execute("""
                INSERT INTO payments (user_id, username, plan, amount, payment_date)
                VALUES (%s, %s, %s, %s, %s)
                """, (user['user_id'], username, plan, plan_config['price'], now))
                
                conn.commit()
        
        # Desbanear del grupo
        try:
            await context.bot.unban_chat_member(VIP_GROUP_ID, user['user_id'])
            await update.message.reply_text(
                f"✅ @{username} reactivado con {plan_config['name']}\n"
                f"📅 Expira: {end_date.strftime('%d/%m/%Y')}\n"
                f"✅ Usuario desbaneado del grupo"
            )
            
            # Notificar al usuario
            await context.bot.send_message(
                user['user_id'],
                f"🎉 ¡Tu suscripción ha sido reactivada!\n\n"
                f"📋 Plan: {plan_config['name']}\n"
                f"📅 Expira: {end_date.strftime('%d/%m/%Y')}\n\n"
                f"¡Bienvenido de nuevo!"
            )
        except Exception as e:
            await update.message.reply_text(
                f"✅ Usuario reactivado en BD pero hubo error al desbanear: {e}"
            )

    async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Maneja los callbacks del teclado inline"""
        query = update.callback_query
        await query.answer()
        
        if query.data == "add_user":
            await query.message.reply_text(
                "📝 *Agregar usuario*\n\n"
                "Usa el comando:\n"
                "`/add @username plan`\n\n"
                "Planes: trial, semanal, mensual",
                parse_mode="Markdown"
            )
        elif query.data == "list_active":
            await list_active_users(update, context)
        elif query.data == "earnings":
            await show_earnings(update, context)
        elif query.data == "stats":
            await show_stats(update, context)
        elif query.data == "export_month":
            await export_report(update, context)  # ✅ Esto debe llamar a export_report
   
    # ---------- MANEJADOR DE NUEVOS MIEMBROS ----------
    async def handle_new_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Detecta cuando un usuario entra al grupo - MODO ESTRICTO"""
        chat_member = update.chat_member
        
        if not chat_member or chat_member.chat.id != VIP_GROUP_ID:
            return
        
        # Verificar que es un nuevo miembro
        if chat_member.new_chat_member.status == "member" and chat_member.old_chat_member.status in ["left", "kicked"]:
            user = chat_member.new_chat_member.user
            user_id = user.id
            username = user.username or f"user_{user_id}"
            first_name = user.first_name or ""
            
            logger.info(f"📥 Nuevo miembro detectado: @{username} ({user_id})")
            
            registered, result = await db.register_user_auto(user_id, username, first_name)
            
            if registered:
                if result == "trial_nuevo":
                    welcome_msg = (
                        f"🎉 ¡Bienvenido @{username}!\n\n"
                        f"✨ Has recibido un **TRIAL GRATIS de 1 día**\n"
                        f"📅 Expira: {(datetime.now() + timedelta(days=1)).strftime('%d/%m/%Y')}\n\n"
                        f"Para continuar después del trial, contacta al administrador.\n\n"
                        f"Planes disponibles:\n"
                        f"• 📅 Semanal (7 días) - $10\n"
                        f"• 📆 Mensual (30 días) - $20"
                    )
                    await context.bot.send_message(user_id, welcome_msg, parse_mode="Markdown")
                    await context.bot.send_message(
                        ADMIN_ID,
                        f"🆕 *Nuevo usuario registrado*\n"
                        f"👤 @{username}\n"
                        f"🎁 Trial activado por 1 día\n"
                        f"📅 Expira: {(datetime.now() + timedelta(days=1)).strftime('%d/%m/%Y')}",
                        parse_mode="Markdown"
                    )
                
                elif result == "activo":
                    # Usuario ya activo, mensaje de bienvenida de vuelta
                    user_data = await db.get_user_by_id(user_id)
                    if user_data:
                        days_left = (user_data['end_date'] - datetime.now()).days
                        await context.bot.send_message(
                            user_id,
                            f"🎉 ¡Bienvenido de vuelta @{username}!\n\n"
                            f"✅ Tu suscripción está activa\n"
                            f"📅 Expira en {days_left} días\n"
                            f"📋 Plan: {user_data['plan']}"
                        )
            
            else:
                # Usuario expirado o no válido - Expulsar inmediatamente
                if result == "expirado":
                    try:
                        await context.bot.ban_chat_member(VIP_GROUP_ID, user_id)
                        await context.bot.send_message(
                            ADMIN_ID,
                            f"🚫 *ACCESO DENEGADO*\n"
                            f"👤 @{username}\n"
                            f"❌ Intento de reingreso de usuario expirado\n"
                            f"🛡️ Expulsado automáticamente",
                            parse_mode="Markdown"
                        )
                        logger.info(f"🚫 Usuario expirado {username} expulsado automáticamente")
                    except Exception as e:
                        logger.error(f"Error expulsando usuario {username}: {e}")

# ---------- INSTANCIA GLOBAL ----------
db = Database(DATABASE_URL)
scheduler = AsyncIOScheduler()
bot_app = None

# ---------- COMANDOS ----------
async def list_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /groups - Lista todos los grupos (solo Super Admin)"""
    if update.effective_user.id != SUPER_ADMIN_ID:
        await update.message.reply_text("❌ Solo el Super Admin puede ver esta información")
        return
    
    if not GROUPS:
        await update.message.reply_text("📭 No hay grupos configurados")
        return
    
    msg = "📊 *GRUPOS CONFIGURADOS*\n\n"
    
    for group in GROUPS:
        # Obtener estadísticas del grupo
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM users WHERE group_id = %s AND status = 'active' AND end_date > NOW()", (group["group_id"],))
                active = cur.fetchone()[0]
                
                cur.execute("SELECT COUNT(*) FROM users WHERE group_id = %s", (group["group_id"],))
                total = cur.fetchone()[0]
                
                cur.execute("SELECT COALESCE(SUM(amount), 0) FROM payments WHERE group_id = %s AND payment_date >= date_trunc('month', NOW())", (group["group_id"],))
                monthly = cur.fetchone()[0]
        
        msg += f"📌 *{group['group_name']}*\n"
        msg += f"   🆔 ID: `{group['group_id']}`\n"
        msg += f"   👑 Admin: `{group['admin_id']}`\n"
        msg += f"   👥 Activos: {active}/{total}\n"
        msg += f"   💰 Mes: ${monthly}\n\n"
    
    await update.message.reply_text(msg, parse_mode="Markdown")

async def add_group_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/addgroup group_id group_name admin_id - Agrega un nuevo grupo (solo Super Admin)"""
    if update.effective_user.id != SUPER_ADMIN_ID:
        await update.message.reply_text("❌ Solo el Super Admin puede agregar grupos")
        return
    
    if len(context.args) < 3:
        await update.message.reply_text(
            "❌ *Formato:* `/addgroup group_id \"nombre_grupo\" admin_id`\n\n"
            "Ejemplo: `/addgroup -1001234567890 \"VIP Club\" 123456789`\n\n"
            "Para obtener el group_id, agrega el bot al grupo y usa /getId",
            parse_mode="Markdown"
        )
        return
    
    group_id = int(context.args[0])
    group_name = context.args[1].strip('"')
    admin_id = int(context.args[2])
    
    # Verificar que el grupo no exista ya
    if get_group_by_id(group_id):
        await update.message.reply_text(f"❌ El grupo {group_id} ya está configurado")
        return
    
    # Agregar a la lista
    GROUPS.append({
        "group_id": group_id,
        "group_name": group_name,
        "admin_id": admin_id
    })
    
    # Registrar en base de datos
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO groups (group_id, group_name, admin_id, super_admin_id)
            VALUES (%s, %s, %s, %s)
            """, (group_id, group_name, admin_id, SUPER_ADMIN_ID))
            conn.commit()
    
    await update.message.reply_text(
        f"✅ *Grupo agregado exitosamente*\n\n"
        f"📌 Nombre: {group_name}\n"
        f"🆔 ID: `{group_id}`\n"
        f"👑 Admin: `{admin_id}`\n\n"
        f"⚠️ Asegúrate de que el bot sea administrador del grupo",
        parse_mode="Markdown"
    )

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Panel de control - Solo admin"""
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ No autorizado")
        return
    
    keyboard = [
        [InlineKeyboardButton("➕ Agregar usuario", callback_data="add_user")],
        [InlineKeyboardButton("📊 Usuarios activos", callback_data="list_active")],
        [InlineKeyboardButton("💰 Ganancias", callback_data="earnings")],
        [InlineKeyboardButton("📈 Estadísticas", callback_data="stats")],
        [InlineKeyboardButton("📥 Exportar mes", callback_data="export_month")]
    ]
    await update.message.reply_text(
        "🤖 *Panel de Control - Suscripciones VIP*\n\n"
        "Gestiona las suscripciones de los usuarios.\n\n"
        "Planes: `trial`, `semanal`, `mensual`",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Panel de control - detecta automáticamente el grupo y rol"""
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    
    # Verificar si es Super Admin (puede usar en cualquier chat)
    if user_id == SUPER_ADMIN_ID:
        keyboard = [
            [InlineKeyboardButton("📊 Todos los grupos", callback_data="all_groups")],
            [InlineKeyboardButton("➕ Agregar grupo", callback_data="add_group")],
            [InlineKeyboardButton("📈 Estadísticas globales", callback_data="global_stats")],
            [InlineKeyboardButton("📥 Reporte consolidado", callback_data="consolidated_report")]
        ]
        await update.message.reply_text(
            "👑 *Panel de Super Administrador*\n\n"
            "Tienes control sobre todos los grupos configurados.\n\n"
            "Usa los botones para ver estadísticas globales.",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
        return
    
    # Verificar si es admin de algún grupo
    user_groups = get_groups_by_admin(user_id)
    
    if not user_groups:
        await update.message.reply_text("❌ No tienes grupos asignados")
        return
    
    # Si es admin de un solo grupo, mostrar panel de ese grupo
    if len(user_groups) == 1:
        group = user_groups[0]
        context.user_data['current_group'] = group['group_id']
        
        keyboard = [
            [InlineKeyboardButton("➕ Agregar usuario", callback_data="add_user")],
            [InlineKeyboardButton("📊 Usuarios activos", callback_data="list_active")],
            [InlineKeyboardButton("💰 Ganancias", callback_data="earnings")],
            [InlineKeyboardButton("📈 Estadísticas", callback_data="stats")],
            [InlineKeyboardButton("📥 Exportar mes", callback_data="export_month")]
        ]
        await update.message.reply_text(
            f"🤖 *Panel de Control - {group['group_name']}*\n\n"
            f"Gestiona las suscripciones de tu grupo.",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
    else:
        # Es admin de múltiples grupos, mostrar selector
        keyboard = []
        for group in user_groups:
            keyboard.append([InlineKeyboardButton(f"📌 {group['group_name']}", callback_data=f"select_group_{group['group_id']}")])
        
        await update.message.reply_text(
            "📋 *Selecciona el grupo que quieres gestionar*\n\n"
            f"Eres administrador de {len(user_groups)} grupos.",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
        
async def add_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/add @username plan - Agrega o renueva usuario"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    if len(context.args) < 2:
        await update.message.reply_text(
            "❌ *Formato correcto:*\n"
            "`/add @username plan`\n\n"
            "Ejemplo: `/add @juan semanal`\n\n"
            "Planes: `trial`, `semanal`, `mensual`",
            parse_mode="Markdown"
        )
        return
    
    username = context.args[0].replace("@", "")
    plan = context.args[1].lower()
    
    if plan not in PLANS:
        await update.message.reply_text("❌ Plan inválido. Usa: trial, semanal o mensual")
        return
    
    # Verificar si el usuario existe
    existing = await db.get_user_by_username(username)
    
    if existing:
        success, message = await db.add_or_update_user(username, plan, existing['user_id'])
        await update.message.reply_text(message)
        
        if success:
            # Agregar/Desbanear del grupo
            try:
                await context.bot.unban_chat_member(VIP_GROUP_ID, existing['user_id'])
                # Enviar mensaje de confirmación al usuario
                await context.bot.send_message(
                    existing['user_id'],
                    f"✅ ¡Tu suscripción ha sido { 'renovada' if plan != 'trial' else 'activada'}!\n"
                    f"📋 Plan: {PLANS[plan]['name']}\n"
                    f"📅 Expira: {(datetime.now() + timedelta(days=PLANS[plan]['days'])).strftime('%d/%m/%Y')}"
                )
            except Exception as e:
                logger.warning(f"No se pudo notificar al usuario: {e}")
    else:
        await update.message.reply_text(
            f"⚠️ No tengo registro de @{username}\n\n"
            f"Pídele a @{username} que envíe cualquier mensaje a este bot.\n"
            f"Una vez que lo haga, vuelve a ejecutar el comando."
        )

async def remove_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/remove @username - Expulsa usuario manualmente"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    if len(context.args) < 1:
        await update.message.reply_text("❌ Usa: `/remove @username`", parse_mode="Markdown")
        return
    
    username = context.args[0].replace("@", "")
    user = await db.get_user_by_username(username)
    
    if not user:
        await update.message.reply_text(f"❌ No se encontró al usuario @{username}")
        return
    
    try:
        await context.bot.ban_chat_member(VIP_GROUP_ID, user['user_id'])
        await db.expire_user(user['user_id'])
        await update.message.reply_text(f"✅ Usuario @{username} expulsado del grupo")
        await context.bot.send_message(
            user['user_id'],
            "🚫 Tu suscripción ha sido cancelada. Contacta al administrador para más información."
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")

async def export_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/export - Exporta reporte del mes actual a CSV (funciona con comando y botón)"""
    
    # Determinar si es callback (botón) o comando directo
    query = update.callback_query
    
    if query:
        # Es un clic en botón
        await query.answer()  # Esto es IMPORTANTE para botones
        message = query.message
        user_id = query.from_user.id
    else:
        # Es un comando /export
        message = update.message
        user_id = update.effective_user.id
    
    # Verificar autorización
    if user_id != ADMIN_ID:
        if query:
            await query.edit_message_text("❌ No autorizado")
        else:
            await message.reply_text("❌ No autorizado")
        return
    
    now = datetime.now()
    year = now.year
    month = now.month
    
    # Mostrar mensaje de "procesando"
    processing_msg = await message.reply_text("📊 Generando reporte... Por favor espera.")
    
    try:
        # Obtener datos del mes
        with db.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                start_date = datetime(year, month, 1)
                if month == 12:
                    end_date = datetime(year + 1, 1, 1)
                else:
                    end_date = datetime(year, month + 1, 1)
                
                # Transacciones
                cur.execute("""
                SELECT user_id, username, plan, amount, payment_date
                FROM payments
                WHERE payment_date >= %s AND payment_date < %s
                ORDER BY payment_date DESC
                """, (start_date, end_date))
                transactions = cur.fetchall()
                
                # Resumen por plan
                cur.execute("""
                SELECT plan, COUNT(*) as count, SUM(amount) as total
                FROM payments
                WHERE payment_date >= %s AND payment_date < %s
                GROUP BY plan
                """, (start_date, end_date))
                summary = cur.fetchall()
                
                total = sum(row['total'] for row in summary) if summary else 0
                
                # Usuarios nuevos
                cur.execute("""
                SELECT COUNT(*) as new_users
                FROM users
                WHERE created_at >= %s AND created_at < %s
                """, (start_date, end_date))
                new_users = cur.fetchone()['new_users']
        
        if not transactions:
            await processing_msg.edit_text(f"📭 No hay transacciones en {now.strftime('%B %Y')}")
            return
        
        # Crear CSV
        output = StringIO()
        writer = csv.writer(output)
        
        writer.writerow(['Fecha', 'User ID', 'Username', 'Plan', 'Monto (USD)'])
        writer.writerow(['='*50, '='*10, '='*20, '='*10, '='*15])
        
        for t in transactions:
            writer.writerow([
                t['payment_date'].strftime('%Y-%m-%d %H:%M:%S'),
                t['user_id'],
                t['username'] or 'Sin username',
                t['plan'].upper(),
                f"${t['amount']}"
            ])
        
        writer.writerow([])
        writer.writerow(['RESUMEN DEL MES', '', '', '', ''])
        writer.writerow(['-'*30, '', '', '', ''])
        
        for s in summary:
            plan_name = PLANS.get(s['plan'], {}).get('name', s['plan'])
            writer.writerow([f'{plan_name}:', f"{s['count']} ventas", f"${s['total']}", '', ''])
        
        writer.writerow([])
        writer.writerow([f'TOTAL DEL MES:', '', '', '', f"${total}"])
        writer.writerow([f'NUEVOS USUARIOS:', '', '', '', new_users])
        
        output.seek(0)
        
        # Eliminar mensaje de "procesando"
        await processing_msg.delete()
        
        # Enviar el archivo
        await message.reply_document(
            document=output.getvalue().encode('utf-8-sig'),
            filename=f"reporte_{year}_{month:02d}.csv",
            caption=f"📊 *Reporte de {now.strftime('%B %Y')}*\n💰 Total: ${total}\n👥 Nuevos usuarios: {new_users}",
            parse_mode="Markdown"
        )
        
        output.close()
        logger.info(f"Reporte exportado: {year}-{month:02d} - Total: ${total}")
        
    except Exception as e:
        logger.error(f"Error exportando reporte: {e}")
        await processing_msg.edit_text(f"❌ Error al generar el reporte: {str(e)}")

async def list_active_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Lista usuarios activos"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    query = update.callback_query
    if query:
        await query.answer()
        message = query.message
    else:
        message = update.message
    
    users = await db.get_all_active_users()
    
    if not users:
        await message.reply_text("📭 No hay usuarios activos")
        return
    
    msg = f"📊 *USUARIOS ACTIVOS* ({len(users)})\n\n"
    
    for user in users[:30]:
        days_left = int(user['days_left']) if user['days_left'] else 0
        emoji = "🟢" if days_left > 7 else "🟡" if days_left > 2 else "🔴"
        msg += f"{emoji} @{user['username'] or user['user_id']}\n"
        msg += f"   📅 Expira: {user['end_date'].strftime('%d/%m/%Y')}\n"
        msg += f"   ⏳ Días: {days_left}\n"
        msg += f"   📋 Plan: {user['plan']}\n\n"
    
    if len(users) > 30:
        msg += f"\n... y {len(users) - 30} más"
    
    await message.reply_text(msg, parse_mode="Markdown")

async def show_earnings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra ganancias del mes"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    query = update.callback_query
    if query:
        await query.answer()
        message = query.message
    else:
        message = update.message
    
    now = datetime.now()
    report = await db.get_monthly_report(now.year, now.month)
    
    msg = f"💰 *GANANCIAS DE {now.strftime('%B %Y').upper()}*\n\n"
    
    if not report['summary']:
        msg += "📭 No hay ventas registradas este mes"
    else:
        for plan_data in report['summary']:
            plan_name = PLANS.get(plan_data['plan'], {}).get('name', plan_data['plan'])
            msg += f"• {plan_name}: {plan_data['count']} ventas - ${plan_data['total']}\n"
        msg += f"\n💵 *TOTAL MES*: ${report['total']}\n"
        msg += f"👥 *Nuevos usuarios*: {report['new_users']}"
    
    await message.reply_text(msg, parse_mode="Markdown")

async def show_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra estadísticas generales"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    query = update.callback_query
    if query:
        await query.answer()
        message = query.message
    else:
        message = update.message
    
    with db.get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT COUNT(*) as total FROM users")
            total = cur.fetchone()['total']
            
            cur.execute("SELECT COUNT(*) as active FROM users WHERE status = 'active' AND end_date > NOW()")
            active = cur.fetchone()['active']
            
            cur.execute("""
            SELECT COUNT(*) as expired
            FROM users
            WHERE status = 'expired' OR end_date < NOW()
            """)
            expired = cur.fetchone()['expired']
            
            cur.execute("""
            SELECT COUNT(*) as expiring
            FROM users
            WHERE status = 'active' AND end_date > NOW() 
            AND end_date < NOW() + INTERVAL '7 days'
            """)
            expiring = cur.fetchone()['expiring']
    
    msg = "📈 *ESTADÍSTICAS*\n\n"
    msg += f"👥 Total usuarios registrados: {total}\n"
    msg += f"🟢 Activos: {active}\n"
    msg += f"🔴 Expirados/Expulsados: {expired}\n"
    msg += f"⚠️ Expiran en 7 días: {expiring}"
    
    await message.reply_text(msg, parse_mode="Markdown")

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja los callbacks del teclado inline"""
    query = update.callback_query
    
    # Log para depuración
    logger.info(f"Callback recibido: {query.data} de usuario {query.from_user.id}")
    
    if query.data == "add_user":
        await query.answer()
        await query.message.reply_text(
            "📝 *Agregar usuario*\n\n"
            "Usa el comando:\n"
            "`/add @username plan`\n\n"
            "Planes: trial, semanal, mensual",
            parse_mode="Markdown"
        )
    elif query.data == "list_active":
        await query.answer()
        await list_active_users(update, context)
    elif query.data == "earnings":
        await query.answer()
        await show_earnings(update, context)
    elif query.data == "stats":
        await query.answer()
        await show_stats(update, context)
    elif query.data == "export_month":
        # ✅ IMPORTANTE: No llamar a query.answer() dos veces
        # export_report ya llama a query.answer()
        await export_report(update, context)
    else:
        await query.answer(f"Opción no implementada: {query.data}")

async def register_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/register @username - Registra manualmente un usuario que ya está en el grupo"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    if len(context.args) < 1:
        await update.message.reply_text("❌ Usa: `/register @username`", parse_mode="Markdown")
        return
    
    username = context.args[0].replace("@", "")
    
    # Buscar el usuario en el grupo
    try:
        # Intentar obtener info del miembro del grupo
        member = await context.bot.get_chat_member(VIP_GROUP_ID, username)
        user_id = member.user.id
        username = member.user.username or username
        first_name = member.user.first_name or ""
        
        # Registrar manualmente
        registered, result = await db.register_user_auto(user_id, username, first_name)
        
        if registered:
            await update.message.reply_text(f"✅ Usuario @{username} registrado correctamente con TRIAL")
            
            # Notificar al usuario
            await context.bot.send_message(
                user_id,
                f"🎉 ¡Tu suscripción TRIAL ha sido activada!\n"
                f"📅 Expira en 1 día\n\n"
                f"Para renovar, contacta al administrador."
            )
        else:
            if result == "expirado":
                await update.message.reply_text(f"❌ Usuario @{username} ya expiró y no puede ser registrado nuevamente")
            else:
                await update.message.reply_text(f"⚠️ No se pudo registrar a @{username}")
                
    except Exception as e:
        await update.message.reply_text(f"❌ Error: No pude encontrar a @{username} en el grupo.\nAsegúrate de que esté en el grupo y que el bot sea admin.")
        logger.error(f"Error en register: {e}")

async def check_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/check @username - Verifica el estado de un usuario"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    if len(context.args) < 1:
        await update.message.reply_text("❌ Usa: `/check @username`", parse_mode="Markdown")
        return
    
    username = context.args[0].replace("@", "")
    user = await db.get_user_by_username(username)
    
    if not user:
        await update.message.reply_text(f"❌ No hay registro de @{username}")
        return
    
    status_emoji = "🟢" if user['status'] == 'active' and user['end_date'] > datetime.now() else "🔴"
    days_left = (user['end_date'] - datetime.now()).days if user['end_date'] > datetime.now() else 0
    
    msg = f"📊 *Estado de @{username}*\n\n"
    msg += f"{status_emoji} Estado: {user['status']}\n"
    msg += f"📋 Plan: {user['plan']}\n"
    msg += f"📅 Inicio: {user['start_date'].strftime('%d/%m/%Y')}\n"
    msg += f"📅 Expira: {user['end_date'].strftime('%d/%m/%Y')}\n"
    msg += f"⏳ Días restantes: {days_left}\n"
    msg += f"🎁 Trial usado: {'✅' if user['trial_used'] else '❌'}"
    
    await update.message.reply_text(msg, parse_mode="Markdown")

# ---------- TAREAS PROGRAMADAS ----------
async def check_expiring_subscriptions():
    """Verifica suscripciones próximas a expirar (7 AM)"""
    global bot_app
    
    now = datetime.now()
    if now.hour != 7 or now.minute > 30:
        return
    
    if not bot_app:
        return
    
    for days in [3, 2, 1]:
        users = await db.get_expiring_users(days)
        for user in users:
            try:
                await bot_app.bot.send_message(
                    ADMIN_ID,
                    f"⏰ *RECORDATORIO DE EXPIRACIÓN*\n\n"
                    f"👤 @{user['username']}\n"
                    f"📅 Expira en {days} día(s)\n"
                    f"📆 Fecha: {user['end_date'].strftime('%d/%m/%Y')}\n"
                    f"📋 Plan: {user['plan']}\n\n"
                    f"Para renovar: `/add @{user['username']} {user['plan']}`",
                    parse_mode="Markdown"
                )
                logger.info(f"Recordatorio enviado para {user['username']} - expira en {days} días")
            except Exception as e:
                logger.error(f"Error en recordatorio: {e}")

async def check_expired_subscriptions():
    """Expulsa usuarios con suscripción vencida"""
    global bot_app
    
    if not bot_app:
        return
    
    expired_users = await db.get_expired_users()
    
    for user in expired_users:
        await db.expire_user(user['user_id'])
        try:
            await bot_app.bot.ban_chat_member(VIP_GROUP_ID, user['user_id'])
            await bot_app.bot.send_message(
                ADMIN_ID,
                f"🚫 *USUARIO EXPULSADO AUTOMÁTICAMENTE*\n\n"
                f"👤 @{user['username']}\n"
                f"📅 Suscripción expirada el {user['end_date'].strftime('%d/%m/%Y')}\n"
                f"📋 Plan: {user['plan']}",
                parse_mode="Markdown"
            )
            logger.info(f"Usuario {user['username']} expulsado por vencimiento")
        except Exception as e:
            logger.error(f"Error expulsando {user['username']}: {e}")

async def send_monthly_report():
    """Envía reporte automático al inicio del mes"""
    global bot_app
    
    now = datetime.now()
    if now.day == 1 and now.hour == 8:
        if not bot_app:
            return
        
        last_month = now.replace(day=1) - timedelta(days=1)
        report = await db.get_monthly_report(last_month.year, last_month.month)
        
        if report['transactions']:
            output = StringIO()
            writer = csv.writer(output)
            writer.writerow(['Fecha', 'Username', 'Plan', 'Monto'])
            for t in report['transactions']:
                writer.writerow([
                    t['payment_date'].strftime('%Y-%m-%d'),
                    t['username'] or t['user_id'],
                    t['plan'],
                    f"${t['amount']}"
                ])
            output.seek(0)
            
            await bot_app.bot.send_document(
                ADMIN_ID,
                document=output.getvalue().encode('utf-8'),
                filename=f"reporte_{last_month.year}_{last_month.month:02d}.csv",
                caption=f"📊 *REPORTE {last_month.strftime('%B %Y').upper()}*\n💰 Total: ${report['total']}",
                parse_mode="Markdown"
            )
            output.close()
        else:
            await bot_app.bot.send_message(
                ADMIN_ID,
                f"📊 *REPORTE {last_month.strftime('%B %Y').upper()}*\n📭 Sin transacciones",
                parse_mode="Markdown"
            )

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja los callbacks del teclado inline"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "add_user":
        await query.message.reply_text(
            "📝 *Agregar usuario*\n\n"
            "Usa el comando:\n"
            "`/add @username plan`\n\n"
            "Planes: trial, semanal, mensual",
            parse_mode="Markdown"
        )
    elif query.data == "list_active":
        await list_active_users(update, context)
    elif query.data == "earnings":
        await show_earnings(update, context)
    elif query.data == "stats":
        await show_stats(update, context)
    elif query.data == "export_month":
        await export_report(update, context)  # ✅ Esto debe llamar a export_report

async def select_group(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    """Selecciona un grupo para gestionar"""
    query = update.callback_query
    user_id = query.from_user.id
    
    group = get_group_by_id(group_id)
    if not group or (user_id != SUPER_ADMIN_ID and group["admin_id"] != user_id):
        await query.edit_message_text("❌ No tienes permiso para gestionar este grupo")
        return

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja los callbacks del teclado inline"""
    query = update.callback_query
    
    # Log para ver qué callback está llegando
    logger.info(f"📱 Callback recibido: {query.data} de usuario {query.from_user.id}")
    
    # Siempre responder al callback primero
    await query.answer()
    
    # Verificar según el callback
    if query.data == "add_user":
        await query.edit_message_text(
            "📝 *Agregar usuario*\n\n"
            "Usa el comando:\n"
            "`/add @username plan`\n\n"
            "Planes: trial, semanal, mensual",
            parse_mode="Markdown"
        )
    
    elif query.data == "list_active":
        await list_active_users(update, context)
    
    elif query.data == "earnings":
        await show_earnings(update, context)
    
    elif query.data == "stats":
        await show_stats(update, context)
    
    elif query.data == "export_month":
        await export_report(update, context)
    
    elif query.data.startswith("select_group_"):
        group_id = int(query.data.replace("select_group_", ""))
        await select_group(update, context, group_id)
    
    elif query.data == "all_groups":
        await list_groups(update, context)
    
    elif query.data == "add_group":
        await query.edit_message_text(
            "📝 *Agregar nuevo grupo*\n\n"
            "Usa el comando:\n"
            "`/addgroup group_id \"nombre\" admin_id`\n\n"
            "Ejemplo: `/addgroup -1001234567890 \"Mi Grupo\" 123456789`",
            parse_mode="Markdown"
        )
    
    elif query.data == "global_stats":
        await global_stats(update, context)
    
    elif query.data == "consolidated_report":
        await consolidated_report(update, context)
    
    else:
        logger.warning(f"⚠️ Callback no reconocido: {query.data}")
        await query.edit_message_text(f"❌ Opción no implementada: {query.data}")
    
    context.user_data['current_group'] = group_id

    #pruebas
    async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja los callbacks del teclado inline"""
    query = update.callback_query
    
    # ✅ Esto imprimirá en los logs de Railway
    print(f"🔔 CALLBACK RECIBIDO: {query.data}")
    logger.info(f"🔔 CALLBACK RECIBIDO: {query.data}")
    
    await query.answer()
    
   # En la función start() para Super Admin:
    keyboard = [
        [InlineKeyboardButton("📊 Todos los grupos", callback_data="all_groups")],
        [InlineKeyboardButton("➕ Agregar grupo", callback_data="add_group")],
        [InlineKeyboardButton("📈 Estadísticas globales", callback_data="global_stats")],
        [InlineKeyboardButton("📥 Reporte consolidado", callback_data="consolidated_report")]
    ]
    
    # Para admin de grupo:
    keyboard = [
        [InlineKeyboardButton("➕ Agregar usuario", callback_data="add_user")],
        [InlineKeyboardButton("📊 Usuarios activos", callback_data="list_active")],
        [InlineKeyboardButton("💰 Ganancias", callback_data="earnings")],
        [InlineKeyboardButton("📈 Estadísticas", callback_data="stats")],
        [InlineKeyboardButton("📥 Exportar mes", callback_data="export_month")]
    ]
    
    await query.edit_message_text(
        f"🤖 *Panel de Control - {group['group_name']}*\n\n"
        f"Gestiona las suscripciones de este grupo.",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def add_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/add @username plan - Agrega usuario al grupo actual"""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    
    # Obtener grupo actual del contexto o del chat
    current_group = context.user_data.get('current_group')
    if not current_group:
        # Intentar obtener del chat actual
        group = get_group_by_id(chat_id)
        if group and can_manage_group(user_id, chat_id):
            current_group = chat_id
        else:
            await update.message.reply_text("❌ No se ha seleccionado un grupo. Usa /start primero.")
            return
    
    if not can_manage_group(user_id, current_group):
        await update.message.reply_text("❌ No tienes permiso para gestionar este grupo")
        return
    
    if len(context.args) < 2:
        await update.message.reply_text("❌ Usa: `/add @username plan`", parse_mode="Markdown")
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

async def global_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra estadísticas de todos los grupos (solo Super Admin)"""
    if update.effective_user.id != SUPER_ADMIN_ID:
        await update.message.reply_text("❌ Solo el Super Admin puede ver estadísticas globales")
        return
    
    query = update.callback_query
    if query:
        await query.answer()
        message = query.message
    else:
        message = update.message
    
    total_users = 0
    total_active = 0
    total_monthly = 0
    
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
        
        msg += f"📌 *{group['group_name']}*\n"
        msg += f"   👥 Usuarios: {active}/{total}\n"
        msg += f"   💰 Ganancias mes: ${monthly}\n\n"
    
    msg += f"━━━━━━━━━━━━━━━━━━\n"
    msg += f"📊 *TOTALES*\n"
    msg += f"👥 Usuarios: {total_active}/{total_users}\n"
    msg += f"💰 Ganancias totales mes: ${total_monthly}\n"
    
    await message.reply_text(msg, parse_mode="Markdown")
        
# ---------- MAIN ----------
async def main():
    global bot_app
    
    await db.init_tables()
    logger.info("📦 Base de datos lista")
    
    defaults = Defaults(parse_mode="HTML")
    bot_app = ApplicationBuilder().token(TOKEN).defaults(defaults).build()
    
    # Handlers de comandos
    bot_app.add_handler(CommandHandler("start", start))
    bot_app.add_handler(CommandHandler("add", add_user_command))
    bot_app.add_handler(CommandHandler("groups", list_groups))
    bot_app.add_handler(CommandHandler("addgroup", add_group_command))
    bot_app.add_handler(CommandHandler("global", global_stats))
    
    # ✅ IMPORTANTE: El handler de callbacks debe estar registrado
    bot_app.add_handler(CallbackQueryHandler(handle_callback))
    
    # Detectar nuevos miembros
    bot_app.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, detect_new_member_message))
    
    # Tareas programadas
    scheduler.add_job(check_expired_subscriptions, 'interval', hours=6)
    scheduler.start()
    
    logger.info("🤖 Bot iniciado")
    
    await bot_app.initialize()
    await bot_app.start()
    await bot_app.updater.start_polling()
    
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
