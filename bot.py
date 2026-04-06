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
    ContextTypes, Defaults
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ---------- CONFIGURACIÓN ----------
TOKEN = os.getenv("TELEGRAM_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
VIP_GROUP_ID = int(os.getenv("VIP_GROUP_ID", 0))

# Diagnóstico de variables
print("=== INICIO DEL BOT ===")
print(f"TOKEN desde variable: {TOKEN}")
print(f"TOKEN existe: {bool(TOKEN)}")
print(f"Longitud: {len(TOKEN) if TOKEN else 0}")
print("======================")

# Planes y precios (días, precio, nombre)
PLANS = {
    "trial": {"days": 1, "price": 0, "name": "🎁 Trial (1 día)"},
    "semanal": {"days": 7, "price": 10, "name": "📅 Semanal (7 días)"},
    "mensual": {"days": 30, "price": 20, "name": "📆 Mensual (30 días)"}
}

# Configuración de logs
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
        """Obtiene una conexión a PostgreSQL con autocommit"""
        conn = psycopg2.connect(self.db_url)
        conn.autocommit = True
        return conn
    
    async def init_tables(self):
        """Inicializa las tablas necesarias"""
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
                
                # Tabla de pagos/transacciones
                cur.execute("""
                CREATE TABLE IF NOT EXISTS payments (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT REFERENCES users(user_id),
                    username TEXT,
                    plan TEXT NOT NULL,
                    amount INTEGER NOT NULL,
                    payment_date TIMESTAMP DEFAULT NOW()
                )
                """)
                
                # Índices para escalabilidad
                cur.execute("CREATE INDEX IF NOT EXISTS idx_users_end_date ON users(end_date)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_users_status ON users(status)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_payments_date ON payments(payment_date)")
                
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
    
    async def add_or_update_user(self, user_id: int, username: str, first_name: str, 
                                   plan: str, days: int, amount: int) -> Tuple[bool, str]:
        """
        Agrega o actualiza un usuario (renovación)
        Retorna: (éxito, mensaje)
        """
        now = datetime.now()
        
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Verificar si el usuario existe
                cur.execute("SELECT end_date, trial_used FROM users WHERE user_id = %s", (user_id,))
                existing = cur.fetchone()
                
                if existing:
                    # Verificar si ya usó trial
                    if plan == "trial" and existing['trial_used']:
                        return False, "❌ Este usuario ya usó su prueba gratuita"
                    
                    # RENOVACIÓN: empieza desde cero (fecha actual)
                    start_date = now
                    end_date = now + timedelta(days=days)
                    
                    cur.execute("""
                    UPDATE users 
                    SET plan = %s, start_date = %s, end_date = %s, status = 'active',
                        updated_at = NOW(), username = %s, first_name = %s,
                        trial_used = trial_used OR %s
                    WHERE user_id = %s
                    """, (plan, start_date, end_date, username, first_name, plan == "trial", user_id))
                else:
                    # NUEVO USUARIO
                    start_date = now
                    end_date = now + timedelta(days=days)
                    trial_used = (plan == "trial")
                    
                    cur.execute("""
                    INSERT INTO users (user_id, username, first_name, plan, start_date, end_date, trial_used)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (user_id, username, first_name, plan, start_date, end_date, trial_used))
                
                # Registrar el pago
                cur.execute("""
                INSERT INTO payments (user_id, username, plan, amount, payment_date)
                VALUES (%s, %s, %s, %s, %s)
                """, (user_id, username, plan, amount, now))
                
                conn.commit()
                
                message = f"✅ {username or user_id} activado con {PLANS[plan]['name']}\n📅 Expira: {end_date.strftime('%d/%m/%Y')}"
                return True, message
    
    async def get_expiring_users(self, days_before: int) -> List[Dict]:
        """Obtiene usuarios que expiran exactamente en X días"""
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
        """Marca un usuario como expirado"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                UPDATE users SET status = 'expired', updated_at = NOW()
                WHERE user_id = %s
                """, (user_id,))
                conn.commit()
        logger.info(f"⏰ Usuario {user_id} marcado como expirado")
    
    async def get_monthly_report(self, year: int, month: int) -> Dict:
        """Genera reporte mensual completo para exportar"""
        start_date = datetime(year, month, 1)
        if month == 12:
            end_date = datetime(year + 1, 1, 1)
        else:
            end_date = datetime(year, month + 1, 1)
        
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Todas las transacciones del mes
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
                
                # Total general
                total = sum(row['total'] for row in summary)
                
                # Usuarios nuevos del mes
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
                    "new_users": new_users,
                    "start_date": start_date,
                    "end_date": end_date
                }
    
    async def get_active_users_count(self) -> int:
        """Obtiene cantidad de usuarios activos"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM users WHERE status = 'active' AND end_date > NOW()")
                return cur.fetchone()[0]
    
    async def get_all_active_users(self) -> List[Dict]:
        """Obtiene todos los usuarios activos con días restantes"""
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

# ---------- INSTANCIA GLOBAL ----------
db = Database(DATABASE_URL)
scheduler = AsyncIOScheduler()
bot_app = None  # Se asignará en main

# ---------- FUNCIONES DEL BOT ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /start - Solo para admin"""
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
        "Gestiona las suscripciones de los usuarios del canal VIP.\n\n"
        "📌 *Comandos rápidos:*\n"
        "`/add @username plan` - Agregar usuario\n"
        "`/renew @username plan` - Renovar usuario\n"
        "`/remove @username` - Expulsar usuario\n"
        "`/export` - Exportar reporte del mes actual",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

# ---------- COMANDOS DE ADMIN ----------
async def add_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /add @username plan - Agrega un usuario manualmente"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    try:
        if len(context.args) < 2:
            await update.message.reply_text(
                "❌ *Formato incorrecto*\n\n"
                "Usa: `/add @username plan`\n\n"
                "Planes disponibles:\n"
                "• `trial` - 1 día ($0)\n"
                "• `semanal` - 7 días ($10)\n"
                "• `mensual` - 30 días ($20)\n\n"
                "Ejemplo: `/add @juan semanal`",
                parse_mode="Markdown"
            )
            return
        
        username = context.args[0].replace("@", "")
        plan = context.args[1].lower()
        
        if plan not in PLANS:
            await update.message.reply_text("❌ Plan inválido. Usa: trial, semanal o mensual")
            return
        
        plan_config = PLANS[plan]
        
        # Buscar si el usuario ya existe en la DB
        existing = await db.get_user_by_username(username)
        
        if existing:
            user_id = existing['user_id']
        else:
            # Si no existe, necesitamos que el admin pase el user_id
            await update.message.reply_text(
                f"⚠️ No tengo registro de @{username}\n\n"
                "Por favor, usa el formato completo:\n"
                "`/add user_id @username plan`\n\n"
                "Ejemplo: `/add 123456789 @juan semanal`\n\n"
                "Para obtener el user_id, pídele al usuario que envíe un mensaje al bot o usa @userinfobot",
                parse_mode="Markdown"
            )
            return
        
        success, message = await db.add_or_update_user(
            user_id=user_id,
            username=username,
            first_name="",
            plan=plan,
            days=plan_config['days'],
            amount=plan_config['price']
        )
        
        if success:
            # Agregar al grupo VIP (unban por si estaba baneado)
            try:
                await context.bot.unban_chat_member(VIP_GROUP_ID, user_id)
                await context.bot.send_message(
                    VIP_GROUP_ID,
                    f"🎉 ¡Bienvenido @{username}! Tu suscripción {plan_config['name']} está activa."
                )
            except:
                pass
        
        await update.message.reply_text(message)
        
    except Exception as e:
        logger.error(f"Error en add_user: {e}")
        await update.message.reply_text("❌ Error al agregar usuario")

async def renew_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /renew @username plan - Renueva un usuario"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    # Misma lógica que add_user pero con mensaje diferente
    await add_user_command(update, context)

async def remove_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /remove @username - Expulsa un usuario manualmente"""
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
        await update.message.reply_text(f"✅ Usuario @{username} expulsado del canal VIP")
    except Exception as e:
        logger.error(f"Error removiendo usuario: {e}")
        await update.message.reply_text("❌ Error al expulsar usuario")

async def export_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /export - Exporta reporte del mes actual a CSV"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    now = datetime.now()
    report = await db.get_monthly_report(now.year, now.month)
    
    if not report['transactions']:
        await update.message.reply_text(f"📭 No hay transacciones en {now.strftime('%B %Y')}")
        return
    
    # Crear CSV en memoria
    output = StringIO()
    writer = csv.writer(output)
    
    # Encabezados
    writer.writerow(['Fecha', 'User ID', 'Username', 'Plan', 'Monto'])
    
    for t in report['transactions']:
        writer.writerow([
            t['payment_date'].strftime('%Y-%m-%d %H:%M:%S'),
            t['user_id'],
            t['username'] or '',
            t['plan'],
            f"${t['amount']}"
        ])
    
    # Agregar resumen al final
    writer.writerow([])
    writer.writerow(['RESUMEN', '', '', '', ''])
    writer.writerow(['Total del mes', '', '', '', f"${report['total']}"])
    writer.writerow(['Nuevos usuarios', '', '', '', report['new_users']])
    
    for s in report['summary']:
        writer.writerow([f"  {s['plan']}", '', f"{s['count']} ventas", '', f"${s['total']}"])
    
    output.seek(0)
    
    await update.message.reply_document(
        document=output.getvalue().encode('utf-8'),
        filename=f"reporte_{now.year}_{now.month:02d}.csv",
        caption=f"📊 Reporte de {now.strftime('%B %Y')}\n💰 Total: ${report['total']}"
    )
    
    output.close()

async def list_active_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra lista de usuarios activos con días restantes"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    query = update.callback_query
    if query:
        await query.answer()
        message = query.message
    else:
        message = update.message
    
    active_count = await db.get_active_users_count()
    users = await db.get_all_active_users()
    
    if not users:
        await message.reply_text("📭 No hay usuarios activos")
        return
    
    msg = f"📊 *USUARIOS ACTIVOS* ({len(users)})\n\n"
    
    for user in users[:30]:  # Límite de 30 por mensaje
        days_left = user['days_left']
        if days_left is not None:
            days_left = int(days_left)
        else:
            # Calcular manualmente si no vino de la consulta
            days_left = (user['end_date'] - datetime.now()).days
        
        # Emoji según días restantes
        if days_left > 7:
            emoji = "🟢"
        elif days_left > 2:
            emoji = "🟡"
        elif days_left > 0:
            emoji = "🔴"
        else:
            emoji = "⚫"
        
        username_display = f"@{user['username']}" if user['username'] else str(user['user_id'])
        
        msg += f"{emoji} {username_display}\n"
        msg += f"   📅 Expira: {user['end_date'].strftime('%d/%m/%Y')}\n"
        msg += f"   ⏳ Días restantes: {days_left}\n"
        msg += f"   📋 Plan: {user['plan']}\n\n"
    
    if len(users) > 30:
        msg += f"\n... y {len(users) - 30} más"
    
    # Agregar botón para exportar la lista completa
    keyboard = [[InlineKeyboardButton("📥 Exportar lista completa", callback_data="export_active_list")]]
    
    await message.reply_text(msg, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))

async def export_active_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Exporta la lista de usuarios activos a CSV"""
    query = update.callback_query
    await query.answer()
    
    users = await db.get_all_active_users()
    
    if not users:
        await query.message.reply_text("📭 No hay usuarios activos")
        return
    
    output = StringIO()
    writer = csv.writer(output)
    
    writer.writerow(['User ID', 'Username', 'Plan', 'Fecha Inicio', 'Fecha Expiración', 'Días Restantes'])
    
    for user in users:
        days_left = int(user['days_left']) if user['days_left'] else (user['end_date'] - datetime.now()).days
        writer.writerow([
            user['user_id'],
            user['username'] or '',
            user['plan'],
            user['start_date'].strftime('%Y-%m-%d'),
            user['end_date'].strftime('%Y-%m-%d'),
            days_left
        ])
    
    output.seek(0)
    
    await query.message.reply_document(
        document=output.getvalue().encode('utf-8'),
        filename=f"usuarios_activos_{datetime.now().strftime('%Y%m%d')}.csv",
        caption=f"📊 Lista de usuarios activos\nTotal: {len(users)}"
    )
    output.close()

async def show_earnings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra reporte de ganancias del mes actual"""
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
            # Total usuarios
            cur.execute("SELECT COUNT(*) as total FROM users")
            total = cur.fetchone()['total']
            
            # Usuarios activos
            cur.execute("SELECT COUNT(*) as active FROM users WHERE status = 'active' AND end_date > NOW()")
            active = cur.fetchone()['active']
            
            # Próximos a expirar (7 días)
            cur.execute("""
            SELECT COUNT(*) as expiring
            FROM users
            WHERE status = 'active' 
            AND end_date > NOW() 
            AND end_date < NOW() + INTERVAL '7 days'
            """)
            expiring = cur.fetchone()['expiring']
            
            # Próximos a expirar (1 día)
            cur.execute("""
            SELECT COUNT(*) as expiring_tomorrow
            FROM users
            WHERE status = 'active' 
            AND end_date >= DATE(NOW() + INTERVAL '1 day')
            AND end_date < DATE(NOW() + INTERVAL '2 days')
            """)
            expiring_tomorrow = cur.fetchone()['expiring_tomorrow']
            
            # Por plan
            cur.execute("""
            SELECT plan, COUNT(*) as count
            FROM users
            WHERE status = 'active' AND end_date > NOW()
            GROUP BY plan
            """)
            plans = cur.fetchall()
    
    msg = "📈 *ESTADÍSTICAS*\n\n"
    msg += f"👥 Total usuarios registrados: {total}\n"
    msg += f"🟢 Activos actualmente: {active}\n"
    msg += f"⚠️ Expiran mañana: {expiring_tomorrow}\n"
    msg += f"⚠️ Expiran en 7 días: {expiring}\n\n"
    msg += "*Distribución de activos:*\n"
    
    for plan in plans:
        plan_name = PLANS.get(plan['plan'], {}).get('name', plan['plan'])
        msg += f"• {plan_name}: {plan['count']}\n"
    
    await message.reply_text(msg, parse_mode="Markdown")

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja los callbacks del teclado inline"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "add_user":
        await query.message.reply_text(
            "📝 *Agregar usuario*\n\n"
            "Usa el comando:\n"
            "`/add @username plan`\n\n"
            "Planes: trial, semanal, mensual\n\n"
            "Ejemplo: `/add @juan semanal`",
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
    elif query.data == "export_active_list":
        await export_active_list(update, context)

# ---------- TAREAS PROGRAMADAS ----------
async def check_expiring_subscriptions():
    """Verifica suscripciones próximas a expirar (3, 2, 1 días antes a las 7 AM)"""
    global bot_app
    
    now = datetime.now()
    
    # Solo ejecutar cerca de las 7 AM (margen de 30 minutos)
    if now.hour != 7 or now.minute > 30:
        return
    
    if not bot_app:
        logger.warning("Bot app no disponible para enviar recordatorios")
        return
    
    for days in [3, 2, 1]:
        users = await db.get_expiring_users(days)
        
        for user in users:
            try:
                await bot_app.bot.send_message(
                    ADMIN_ID,
                    f"⏰ *RECORDATORIO DE EXPIRACIÓN*\n\n"
                    f"👤 Usuario: @{user['username'] or user['user_id']}\n"
                    f"📅 Expira en {days} día(s)\n"
                    f"📆 Fecha: {user['end_date'].strftime('%d/%m/%Y')}\n"
                    f"📋 Plan: {user['plan']}\n\n"
                    f"Para renovar: `/renew @{user['username']} {user['plan']}`",
                    parse_mode="Markdown"
                )
                logger.info(f"Recordatorio enviado para {user['user_id']} (expira en {days} días)")
            except Exception as e:
                logger.error(f"Error enviando recordatorio: {e}")

async def check_expired_subscriptions():
    """Verifica y expulsa usuarios con suscripción vencida"""
    global bot_app
    
    expired_users = await db.get_expired_users()
    
    if not bot_app:
        logger.warning("Bot app no disponible para expulsar usuarios")
        return
    
    for user in expired_users:
        # Marcar como expirado en BD
        await db.expire_user(user['user_id'])
        
        # Expulsar del grupo VIP
        try:
            await bot_app.bot.ban_chat_member(VIP_GROUP_ID, user['user_id'])
            await bot_app.bot.send_message(
                ADMIN_ID,
                f"🚫 *USUARIO EXPULSADO*\n\n"
                f"👤 @{user['username'] or user['user_id']}\n"
                f"📅 Suscripción expirada el {user['end_date'].strftime('%d/%m/%Y')}\n"
                f"📋 Plan: {user['plan']}",
                parse_mode="Markdown"
            )
            logger.info(f"Usuario {user['user_id']} expulsado del grupo VIP")
        except Exception as e:
            logger.error(f"Error expulsando usuario {user['user_id']}: {e}")

async def send_monthly_report():
    """Envía reporte automático al inicio de cada mes"""
    global bot_app
    
    now = datetime.now()
    
    # Ejecutar el primer día del mes a las 8 AM
    if now.day == 1 and now.hour == 8:
        if not bot_app:
            logger.warning("Bot app no disponible para enviar reporte")
            return
        
        last_month = now.replace(day=1) - timedelta(days=1)
        report = await db.get_monthly_report(last_month.year, last_month.month)
        
        if report['transactions']:
            # Crear CSV
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
            
            await bot_app.bot.send_document(
                ADMIN_ID,
                document=output.getvalue().encode('utf-8'),
                filename=f"reporte_{last_month.year}_{last_month.month:02d}.csv",
                caption=f"📊 *REPORTE MENSUAL*\n"
                       f"📅 {last_month.strftime('%B %Y')}\n"
                       f"💰 Total: ${report['total']}\n"
                       f"👥 Nuevos usuarios: {report['new_users']}",
                parse_mode="Markdown"
            )
            output.close()
        else:
            await bot_app.bot.send_message(
                ADMIN_ID,
                f"📊 *REPORTE MENSUAL*\n"
                f"📅 {last_month.strftime('%B %Y')}\n"
                f"📭 No hubo transacciones en el mes",
                parse_mode="Markdown"
            )

# ---------- MAIN ----------
def main():
    """Función principal - SIN async"""
    import asyncio
    
    async def setup():
        global bot_app
        
        await db.init_tables()
        logger.info("📦 Base de datos lista")
        
        defaults = Defaults(parse_mode="HTML")
        bot_app = ApplicationBuilder().token(TOKEN).defaults(defaults).build()
        
        bot_app.add_handler(CommandHandler("start", start))
        bot_app.add_handler(CommandHandler("add", add_user_command))
        bot_app.add_handler(CommandHandler("renew", renew_user))
        bot_app.add_handler(CommandHandler("remove", remove_user))
        bot_app.add_handler(CommandHandler("export", export_report))
        bot_app.add_handler(CallbackQueryHandler(handle_callback))
        
        scheduler.add_job(check_expiring_subscriptions, 'interval', hours=1)
        scheduler.add_job(check_expired_subscriptions, 'interval', hours=6)
        scheduler.add_job(send_monthly_report, 'interval', hours=1)
        scheduler.start()
        
        logger.info("🤖 Bot iniciado")
        
        # Iniciar polling
        await bot_app.initialize()
        await bot_app.start()
        await bot_app.updater.start_polling()
        
        # Mantener vivo
        while True:
            await asyncio.sleep(1)
    
    # Ejecutar
    asyncio.run(setup())

if __name__ == "__main__":
    main()
