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
        "📌 *Comandos:*\n"
        "`/add @username plan` - Agregar/Renovar usuario\n"
        "`/remove @username` - Expulsar usuario\n"
        "`/export` - Exportar reporte del mes\n\n"
        "Planes: `trial`, `semanal`, `mensual`",
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
    """/export - Exporta reporte del mes"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    now = datetime.now()
    report = await db.get_monthly_report(now.year, now.month)
    
    if not report['transactions']:
        await update.message.reply_text(f"📭 No hay transacciones en {now.strftime('%B %Y')}")
        return
    
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(['Fecha', 'User ID', 'Username', 'Plan', 'Monto'])
    
    for t in report['transactions']:
        writer.writerow([
            t['payment_date'].strftime('%Y-%m-%d %H:%M:%S'),
            t['user_id'],
            t['username'] or '',
            t['plan'],
            f"${t['amount']}"
        ])
    
    output.seek(0)
    
    await update.message.reply_document(
        document=output.getvalue().encode('utf-8'),
        filename=f"reporte_{now.year}_{now.month:02d}.csv",
        caption=f"📊 Reporte de {now.strftime('%B %Y')}\n💰 Total: ${report['total']}"
    )
    output.close()

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
    """Maneja callbacks del teclado"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "add_user":
        await query.message.reply_text("📝 Usa: `/add @username plan`", parse_mode="Markdown")
    elif query.data == "list_active":
        await list_active_users(update, context)
    elif query.data == "earnings":
        await show_earnings(update, context)
    elif query.data == "stats":
        await show_stats(update, context)
    elif query.data == "export_month":
        await export_report(update, context)

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

# ---------- MAIN ----------
async def main():
    global bot_app
    
    await db.init_tables()
    logger.info("📦 Base de datos lista - Modo ESTRICTO activado")
    
    defaults = Defaults(parse_mode="HTML")
    bot_app = ApplicationBuilder().token(TOKEN).defaults(defaults).build()
    
    # Handlers
    bot_app.add_handler(CommandHandler("start", start))
    bot_app.add_handler(CommandHandler("add", add_user_command))
    bot_app.add_handler(CommandHandler("remove", remove_user))
    bot_app.add_handler(CommandHandler("export", export_report))
    bot_app.add_handler(CallbackQueryHandler(handle_callback))
    bot_app.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, detect_new_member_message))
    bot_app.add_handler(CommandHandler("register", register_user_command))
    bot_app.add_handler(CommandHandler("check", check_user_command))
    
    # Detectar nuevos miembros
    bot_app.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, detect_new_member_message))
    
    # Tareas programadas
    scheduler.add_job(check_expiring_subscriptions, 'interval', hours=1)
    scheduler.add_job(check_expired_subscriptions, 'interval', hours=6)
    scheduler.add_job(send_monthly_report, 'interval', hours=1)
    scheduler.start()
    
    logger.info("🤖 Bot iniciado en MODO ESTRICTO")
    logger.info("✅ Usuarios expirados NO pueden reingresar")
    logger.info("✅ Nuevos miembros reciben TRIAL automático (1 día)")
    logger.info("✅ Expulsión automática al vencer la suscripción")
    
    await bot_app.initialize()
    await bot_app.start()
    await bot_app.updater.start_polling()
    
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
