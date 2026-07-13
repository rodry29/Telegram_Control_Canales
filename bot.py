import os
import csv
import asyncio
import logging
import re
import json
import threading
import random
import time as _time
from io import StringIO
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Callable
from zoneinfo import ZoneInfo

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ChatPermissions
from telegram.error import TelegramError, BadRequest
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    ContextTypes, MessageHandler, filters, ChatMemberHandler,
    MessageReactionHandler
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
SUPER_ADMIN_ID = int(os.getenv("SUPER_ADMIN_ID", "5054216496"))
EXTRA_ADMINS = [int(x.strip()) for x in os.getenv("EXTRA_ADMINS", "").split(",") if x.strip()]
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
    "trial":   {"minutes": 1440, "price": 0,   "name": "🎁 Trial"},
    "semanal": {"days": 7,       "price": 10,  "name": "📅 Semanal (7 días)"},
    "mensual": {"days": 30,      "price": 20,  "name": "📆 Mensual (30 días)"},
    "anual":   {"days": 365,     "price": 150, "name": "🏆 Anual (365 días)"}
}
MUTE_PERMISSIONS = ChatPermissions(
    can_send_messages=False, can_send_audios=False, can_send_documents=False,
    can_send_photos=False, can_send_videos=False, can_send_video_notes=False,
    can_send_voice_notes=False, can_send_polls=False, can_send_other_messages=False,
    can_add_web_page_previews=False
)
UNMUTE_PERMISSIONS = ChatPermissions(
    can_send_messages=True, can_send_audios=True, can_send_documents=True,
    can_send_photos=True, can_send_videos=True, can_send_video_notes=True,
    can_send_voice_notes=True, can_send_polls=True, can_send_other_messages=True,
    can_add_web_page_previews=True
)
GROUPS_CONFIG = os.getenv("GROUPS_CONFIG", "")
GROUPS: Dict[int, Dict[str, Any]] = {}
GROUPS_LOCK = threading.RLock()
GROUP_MESSAGES_CACHE: Dict[int, dict] = {}
GROUP_MESSAGES_LOCK = threading.RLock()

for _group_config in GROUPS_CONFIG.split(","):
    if _group_config.strip():
        _parts = _group_config.strip().split(":")
        if len(_parts) == 4:
            _gid = int(_parts[0])
            GROUPS[_gid] = {
                "group_id":   _gid,
                "type":       _parts[1].upper(),
                "group_name": _parts[2],
                "admin_id":   int(_parts[3]),
                "settings":   {}
            }
# ==================== CACHE DE MIEMBROS ACTIVOS ====================
_known_members_cache: Dict[str, float] = {}
_KNOWN_TTL = 3600  # 1 hora en segundos

def _is_known_member(key: str) -> bool:
    ts = _known_members_cache.get(key)
    return ts is not None and (_time.monotonic() - ts) < _KNOWN_TTL

def _mark_known_member(key: str):
    _known_members_cache[key] = _time.monotonic()
    # Limpiar entradas viejas ocasionalmente para no llenar la memoria
    if len(_known_members_cache) > 5000:
        cutoff = _time.monotonic() - _KNOWN_TTL
        to_del = [k for k, v in _known_members_cache.items() if v < cutoff]
        for k in to_del:
            del _known_members_cache[k]
# ==================== UTILIDADES DE TIEMPO ====================
def now_ec() -> datetime:
    """Retorna datetime naive en UTC (para DB) pero con contexto de Ecuador para display."""
    return datetime.now(ZoneInfo("UTC"))

def fmt_dt(dt: datetime, include_time: bool = True) -> str:
    if dt is None:
        return "N/A"
    try:
        if dt.tzinfo is None:
            dt_utc = dt.replace(tzinfo=ZoneInfo("UTC"))
        else:
            dt_utc = dt.astimezone(ZoneInfo("UTC"))
        dt_ec = dt_utc.astimezone(EC_TZ)
    except Exception:
        dt_ec = dt
    if include_time:
        return dt_ec.strftime('%d/%m/%Y %H:%M')
    return dt_ec.strftime('%d/%m/%Y')

# ==================== UTILIDADES DE FECHAS ROBUSTAS ====================
def normalize_dt(dt: datetime) -> Optional[datetime]:
    """Normaliza un datetime a aware UTC para comparaciones seguras."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=ZoneInfo("UTC"))
    return dt.astimezone(ZoneInfo("UTC"))

def now_utc() -> datetime:
    """Retorna datetime aware en UTC."""
    return datetime.now(ZoneInfo("UTC"))

# ==================== CACHE DE PRECIOS ====================
_plan_config_cache: Dict[tuple, dict] = {}

def _invalidate_price_cache(group_id: int):
    # Limpiar cache de plan config para este grupo
    keys_to_remove = [k for k in _plan_config_cache if k[0] == group_id]
    for k in keys_to_remove:
        _plan_config_cache.pop(k, None)

async def update_group_settings(group_id: int, changes: dict) -> bool:
    """Único punto autorizado para mutar settings. Maneja RAM, DB y caché automáticamente."""
    PRICE_FIELDS = {
        "trial_minutes",
        "price_semanal", "price_mensual", "price_anual",
        "duration_semanal", "duration_mensual", "duration_anual",
        "combo_price_semanal", "combo_price_mensual", "combo_price_anual",
    }
    with GROUPS_LOCK:
        group = GROUPS.get(group_id)
        if not group:
            return False
        settings = dict(group.get("settings", {}))
        for k, v in changes.items():
            if v is None:
                settings.pop(k, None)
            else:
                settings[k] = v
        group["settings"] = settings
        
    await db.update_group_fields(group_id, {'settings': settings})
    
    # Invalidar caché SOLO si se modificó un campo que afecta precios/duraciones
    if any(k in PRICE_FIELDS for k in changes):
        _invalidate_price_cache(group_id)
    return True

# ==================== UTILIDADES ====================
def get_group_by_id(group_id: int) -> Optional[dict]:
    with GROUPS_LOCK:
        return GROUPS.get(group_id)

def find_group_by_admin_and_type(admin_id: int, group_type: str) -> Optional[dict]:
    with GROUPS_LOCK:
        for g in GROUPS.values():
            if g.get("type", "VIP") == group_type and g["admin_id"] == admin_id:
                return g
    return None

def find_linked_or_admin_group(start_group: dict, target_type: str) -> Optional[dict]:
    link_key = {
        "VIP":       "linked_vip_group_id",
        "FREE":      "linked_free_group_id",
        "COMUNIDAD": "linked_comunidad_group_id",
    }.get(target_type)
    if link_key:
        linked_id = start_group.get("settings", {}).get(link_key)
        if linked_id:
            g = get_group_by_id(linked_id) 
            if g and g.get("type", "VIP") == target_type:
                return g
    return find_group_by_admin_and_type(start_group["admin_id"], target_type)

def get_all_groups_snapshot() -> List[Dict[str, Any]]:
    with GROUPS_LOCK:
        return list(GROUPS.values())

def get_groups_by_admin(admin_id: int, group_type: str = None) -> list:
    with GROUPS_LOCK:
        groups = list(GROUPS.values()) if admin_id == SUPER_ADMIN_ID or admin_id in EXTRA_ADMINS else [g for g in GROUPS.values() if g["admin_id"] == admin_id]
        if group_type:
            groups = [g for g in groups if g.get("type", "VIP") == group_type]
        return groups

def can_manage_group(user_id: int, group_id: int) -> bool:
    if user_id == SUPER_ADMIN_ID or user_id in EXTRA_ADMINS:
        return True
    group = get_group_by_id(group_id)
    return group is not None and group["admin_id"] == user_id

async def require_group(update: Update, context: ContextTypes.DEFAULT_TYPE, require_admin: bool = True) -> Optional[int]:
    """Helper para obtener el group_id actual, ya sea desde context o desde el chat."""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    current_group = context.user_data.get('current_group')
    if not current_group:
        group = get_group_by_id(chat_id)
        if group and (not require_admin or can_manage_group(user_id, chat_id)):
            current_group = chat_id
        else:
            return None
    if require_admin and not can_manage_group(user_id, current_group):
        return None
    return current_group

def get_group_plan_config(group_id: int, plan: str) -> dict:
    cache_key = (group_id, plan)
    if cache_key in _plan_config_cache:
        return dict(_plan_config_cache[cache_key])
    
    base = dict(PLANS.get(plan, {}))
    group = get_group_by_id(group_id)
    if not group or not base:
        return base
    settings = group.get("settings", {})
    if plan == "trial":
        mins = settings.get("trial_minutes", base.get("minutes", 1440))
        base["minutes"] = mins
        h, m = divmod(mins, 60)
        if h >= 24:
            d = h // 24
            base["name"] = f"🎁 Trial ({d} día{'s' if d != 1 else ''})"
        elif h > 0:
            base["name"] = f"🎁 Trial ({h}h {m}m)" if m else f"🎁 Trial ({h}h)"
        else:
            base["name"] = f"🎁 Trial ({m} min)"
    elif plan in ("semanal", "mensual", "anual"):
        base["days"]  = settings.get(f"duration_{plan}", base.get("days", 7))
        base["price"] = float(settings.get(f"price_{plan}", base.get("price", 10)))
        base["name"]  = f"{'📅' if plan=='semanal' else '📆' if plan=='mensual' else '🏆'} {plan.capitalize()} ({base['days']} días) - {fmt_price(base['price'])}"
    
    _plan_config_cache[cache_key] = dict(base)
    return base

def fmt_price(amount) -> str:
    f = float(amount)
    return f"${f:.2f}" if f != int(f) else f"${int(f)}"

def escape_md_v2(text: str) -> str:
    """Escapa caracteres especiales de MarkdownV2 para Telegram."""
    if not text:
        return ""
    chars = r'_*[]()~`>#+-=|{}.!'
    for ch in chars:
        text = text.replace(ch, '\\' + ch)
    return text

def safe_name(text: str) -> str:
    """Sanitiza texto para Markdown v1 (parse_mode='Markdown')."""
    if not text:
        return ""
    return text.replace('_', '\\_').replace('*', '\\*').replace('[', '\\[').replace(']', '\\]').replace('`', '\\`')

def render_template(template: str, variables: dict) -> str:
    """
    Reemplaza variables en una plantilla.
    Escapa automáticamente las variables de texto libre para Markdown v1.
    """
    safe_keys = {"user_name", "group_name", "username", "first_name"}
    result = template
    for key, value in variables.items():
        str_val = str(value) if value is not None else ""
        if key in safe_keys:
            str_val = safe_name(str_val)
        result = result.replace(f"{{{key}}}", str_val)
    return result

def fmt_minutes(mins: int) -> str:
    h, m = divmod(mins, 60)
    if h >= 24:
        d = h // 24
        return f"{d} día{'s' if d != 1 else ''}"
    elif h > 0:
        return f"{h}h {m}m" if m else f"{h}h"
    return f"{m} min"

def _plan_emoji(plan: str) -> str:
    return {'semanal': '📅', 'mensual': '📆', 'anual': '🏆'}.get(plan, '💎')

def _build_price_list(group_id: int, plans: list = None, discounted_pct: float = 0) -> str:
    """Helper centralizado para construir listas de precios con descuento opcional."""
    plans = plans or ["semanal", "mensual", "anual"]
    lines = []
    for p in plans:
        cfg = get_group_plan_config(group_id, p)
        price = cfg['price'] * (1 - discounted_pct/100)
        original = fmt_price(cfg['price'])
        final = fmt_price(price)
        days = cfg.get('days', 365)
        
        # Lógica para destacar el costo por día en el plan anual
        daily_cost_str = ""
        if p == "anual" and days > 0:
            daily_cost = price / days
            daily_cost_str = f" *({fmt_price(daily_cost)} por día)*"
            
        if discounted_pct > 0:
            lines.append(f"• {_plan_emoji(p)} {p.capitalize()} ({days} días): ❌ {original} ➡️ *{final}*{daily_cost_str}")
        else:
            lines.append(f"• {_plan_emoji(p)} {p.capitalize()} ({days} días): *{fmt_price(cfg['price'])}*{daily_cost_str}")
    return "\n".join(lines)
# ==================== MENSAJES CONFIGURABLES ====================
DEFAULT_WELCOME_MESSAGE = (
    "🎉 *¡Bienvenido al VIP!*\n\n"
    "✨ Tu prueba de *{trial_duration}* ha comenzado.\n"
    "📅 Expira a las: *{expiry_time}*\n\n"
    "👥 ¿Sabías que también tenemos un Grupo Comunidad? Únete, mira, aporta y descarga contenido por tu cuenta.\n\n"
    "🎰 *Gira la ruleta para descuento:*"
)

DEFAULT_REJECTION_MESSAGE = (
    "🚫 *Acceso denegado — {group_name}*\n\n"
    "Este grupo es exclusivo para miembros VIP.\n\n"
    "📋 *Para acceder tienes 2 opciones:*\n\n"
    "🎁 *Opción 1 (Recomendada):*\n"
    "1️⃣ Únete a nuestro canal FREE\n"
    "2️⃣ Obtén tu prueba GRATIS desde el bot\n\n"
    "💳 *Opción 2 (Inmediata):*\n"
    "Paga directamente y accede ahora:\n"
    "{price_list}\n\n"
    "🎰 *¿Quieres descuento?*\n"
    "🎰 Gira la ruleta y gana hasta 50% OFF\n\n"
    "👇 *Elige tu camino:*"
)

DEFAULT_CHANNEL_SELECT_MESSAGE = (
    "👋 *¡Bienvenido!*\n\n"
    "Soy *Aethel*, tu asistente de acceso VIP.\n\n"
    "🎭 *Selecciona tu canal para comenzar:*"
)

DEFAULT_CHANNEL_DESCRIPTION = (
    "📌 *{channel_name}*\n\n"
    "🔥 Contenido exclusivo y actualizado.\n\n"
    "👇 *Elige una opción:*"
)

DEFAULT_VIP_MENU_MESSAGE = (
    "🔥 *Área VIP — {channel_name}*\n\n"
    "✨ Accede al contenido exclusivo.\n\n"
    "👇 *Selecciona una opción:*"
)

DEFAULT_FUEGO_NOTICE = (
    "🔥 *Descarga de contenido*\n\n"
    "Para recibir los archivos multimedia de este grupo, "
    "debes iniciar conversación con @{fuego_username}.\n\n"
    "📤 Él se encargará de enviarte todo el material."
)

DEFAULT_EXPIRED_MESSAGE = (
    "⏰ *Tu acceso ha expirado*\n\n"
    "Tu plan en *{group_name}* ha vencido.\n\n"
    "💳 ¿Quieres renovar? Presiona el botón:"
)

DEFAULT_CART_MESSAGES = [
    "🛒 *¡No te vayas sin tu acceso!*\n\nNoté que solicitaste los datos de pago pero aún no confirmé tu suscripción.\n\n¿Tienes alguna duda? [Escríbeme aquí](tg://user?id={admin_id}) y te ayudo.\n\nSi ya pagaste, envía el comprobante.",
    "⏳ *Tu acceso VIP te está esperando*\n\nEl contenido exclusivo sigue publicándose, ¡no te lo pierdas!\n\nAún puedes activar tu plan. Presiona el botón de abajo para ver los datos de pago.",
    "👋 *Última oportunidad*\n\nEsta será mi última notificación. No quiero que te pierdas el grupo VIP.\n\nSi cambias de opinión, aquí estaré esperándote con los planes disponibles."
]

DEFAULT_COMUNIDAD_WELCOME_MESSAGE = (
    "🎉 *¡Bienvenido a la Comunidad!*\n\n"
    "✨ Tu prueba de *{trial_duration}* ha comenzado.\n"
    "📅 Expira a las: *{expiry_time}*\n\n"
    "Disfruta el contenido. Si te gusta, paga antes de que termine tu prueba para no perder el acceso.\n\n"
)

DEFAULT_COMUNIDAD_MUTED_MESSAGE = (
    "🔇 *Has sido silenciado en {group_name}*\n\n"
    "Tu prueba gratuita ya fue utilizada y no tienes una suscripción activa.\n\n"
    "Puedes seguir viendo el contenido, pero no podrás participar (comentar o reaccionar) "
    "hasta que actives tu plan.\n\n"
    "💳 *¿Quieres desbloquear tu participación?*"
)
    
def format_message(template: str, variables: dict) -> str:
    if not template:
        return ""
    result = template
    for key, value in variables.items():
        result = result.replace(f"{{{key}}}", str(value))
    return result

# ==================== HELPERS DE PAGO ====================
def get_payment_keyboard(group_id: int, user_id: int, spin_used: bool = False, vip_invite_link: str = None) -> InlineKeyboardMarkup:
    group = get_group_by_id(group_id)
    if not group:
        return InlineKeyboardMarkup([])
        
    settings = group.get("settings", {})
    deuna_link = settings.get("deuna_link")
    admin_id = group.get("admin_id")
    payment_contact = settings.get("payment_contact", "").strip()
    
    # Lógica para determinar el enlace de contacto directo
    contact_url = None
    if payment_contact.isdigit():
        # Si el admin configuró un ID numérico en payment_contact, lo usamos
        contact_url = f"tg://user?id={payment_contact}"
    elif admin_id:
        # Si no, usamos el admin_id del grupo por defecto
        contact_url = f"tg://user?id={admin_id}"
    elif payment_contact.startswith("@"):
        # Si por error configuró un @username, usamos el enlace normal
        contact_url = f"https://t.me/{payment_contact[1:]}"

    keyboard = []
    
    # Botón 1: Ruleta
    if not spin_used:
        keyboard.append([InlineKeyboardButton("🎰 GIRAR RULETA VIP", callback_data=f"spin_{group_id}_{user_id}")])
    else:
        keyboard.append([InlineKeyboardButton("✅ Ruleta ya usada", callback_data=f"spin_used_{group_id}_{user_id}")])
    
    # Botón 2: Contacto Directo con el Admin (NUEVO)
    if contact_url:
        keyboard.append([InlineKeyboardButton("📤 ENVIAR COMPROBANTE AQUÍ", url=contact_url)])
    
    # Botón 3: Link al VIP (sin la palabra Trial)
    if vip_invite_link and vip_invite_link.startswith(("https://", "http://")):
        keyboard.append([InlineKeyboardButton("🔥 Entrada al VIP", url=vip_invite_link)])
    
    # Botón 4: Deuna
    if deuna_link and deuna_link.startswith(("https://", "http://")):
        keyboard.append([InlineKeyboardButton("⚡ PAGAR CON DEUNA", url=deuna_link)])
    
    # Botón 5: Volver
    keyboard.append([InlineKeyboardButton("🔙 Volver", callback_data="back_to_start")])
    
    return InlineKeyboardMarkup(keyboard)
    
def get_payment_info_text(group_id: int) -> str:
    group = get_group_by_id(group_id)
    if not group:
        return "Grupo no encontrado"
    settings = group.get("settings", {})
    bank_data = settings.get("bank_data", "").strip()
    payment_contact = settings.get("payment_contact", "admin").strip()
    paypal_data = settings.get("paypal_data", "").strip()
    vip_invite_link = settings.get("vip_invite_link", "").strip() 
    group_name = group.get("group_name", "VIP")
    lines = [f"💰 *INFORMACIÓN DE PAGO — {group_name}*", "", "📋 *Planes disponibles:*"]
    lines.append(_build_price_list(group_id))
    lines.append("")
    if bank_data:
        lines += ["🏦 *Transferencia Bancaria:*", bank_data, ""]
    if paypal_data:
        lines += ["🅿️ *PayPal:*", paypal_data, ""]
    if not bank_data and not paypal_data:
        lines += ["⚠️ *Métodos de pago no configurados aún.*", "Contacta al administrador para más información.", ""]
    if vip_invite_link:
        lines += ["🔥 *¿Quieres probar antes de pagar?*", "Presiona el botón 'Únete al VIP' para tu trial gratuito.", ""]
    lines += [
        f"📤 *Después de pagar:*",
        f"Envía el comprobante a @{payment_contact} Presiona el botón *'Enviar Comprobante'* para hacerlo de inmediato.", "",
        "⏱ *Tiempo de activación:*",
        "Tu acceso se activará cuando un administrador valide la transferencia.", "",
        "🔄 ¿No recibiste los datos? Presiona el botón de nuevo."
    ]
    return "\n".join(lines)

# Rate limiting simple en memoria para /pagar y leads
_PAYMENT_COOLDOWN: Dict[str, datetime] = {}

async def _cleanup_payment_cooldown():
    """Limpia entradas viejas del cooldown de pagos cada hora."""
    now = datetime.now(ZoneInfo("UTC"))
    old_keys = [k for k, v in _PAYMENT_COOLDOWN.items() if (now - v).total_seconds() > 3600]
    for k in old_keys:
        _PAYMENT_COOLDOWN.pop(k, None)
    if old_keys:
        logger.info(f"🧹 Limpiadas {len(old_keys)} entradas de _PAYMENT_COOLDOWN")

async def send_payment_info(bot, user_id: int, group_id: int, triggered_by: str = "comando"):
    group = get_group_by_id(group_id)
    if not group: return False
    vip_invite_link = (group.get("settings", {}).get("vip_invite_link") or group.get("settings", {}).get("invite_link") or "").strip()
    now = datetime.now(ZoneInfo("UTC"))
    cooldown_key = f"pay_{user_id}_{group_id}"
    
    last_sent = _PAYMENT_COOLDOWN.get(cooldown_key)
    if last_sent and (now - last_sent).total_seconds() < 300:
        return True
    _fire_and_forget(db.log_payment_lead(user_id, group_id), "payment_lead")

    try:
        spin_data = await db.get_spin_data(user_id, group_id)
        spin_used = spin_data.get("used", False) if spin_data.get("spin_count", 0) > 0 else False
        
        await bot.send_message(
            user_id, 
            get_payment_info_text(group_id), 
            parse_mode="Markdown", 
            reply_markup=get_payment_keyboard(group_id, user_id, spin_used=spin_used, vip_invite_link=vip_invite_link)
        )

        _PAYMENT_COOLDOWN[cooldown_key] = now
        
        chat_link = f"tg://user?id={user_id}"
        group_name = group.get("group_name", "VIP")
        await _safe_send(group["admin_id"], f"💳 *Nuevo lead de pago*\n\n👤 Usuario: [Usuario {user_id}]({chat_link})\n🆔 ID: `{user_id}`\n📌 Grupo: {group_name}\n🔔 Acción: *{triggered_by}*\n\n_Esperando comprobante._", parse_mode="Markdown")
        return True
    except Exception as e:
        logger.warning(f"No se pudo enviar info de pago a {user_id}: {e}")
        return False

def extract_user_from_reply(text: str) -> tuple:
    """Extrae user_id, group_id, username y first_name de un mensaje del bot de forma uniforme."""
    if not text:
        return None, None, None, None
    
    id_match = re.search(r'🆔\s*\*?ID:?\*?\s*`?(\d+)`?', text)
    if not id_match:
        id_match = re.search(r'tg://user\?id=(\d+)', text)
    user_id = int(id_match.group(1)) if id_match else None
    
    group_match = re.search(r'📌\s*\*?Grupo:?\*?\s*`?(-?\d+)`?', text)
    group_id = int(group_match.group(1)) if group_match else None
    
    name_match = re.search(r'👤\s*\*?Nombre:?\*?\s*(.+?)(?:\n|$)', text)
    first_name = name_match.group(1).strip().replace('*', '') if name_match else None
    
    username_match = re.search(r'@(\w+)', text)
    username = username_match.group(1) if username_match else None
    
    return user_id, group_id, username, first_name

def _is_bot_message(msg, bot_id: int) -> bool:
    if msg is None:
        return False
    return bool(msg.from_user and msg.from_user.id == bot_id)

async def _require_bot_reply(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    reply = update.message.reply_to_message
    if not reply or not _is_bot_message(reply, context.bot.id):
        await update.message.reply_text(
            "⚠️ *Verificación de seguridad fallida*\n\n"
            "Solo puedo confiar en mensajes enviados por mí mismo.\n"
            "Usa el modo directo: `/add ID mensual vip`",
            parse_mode="Markdown"
        )
        return False
    return True

async def _execute_subscription_flow(bot, reply_func, group_id: int, target_user_id: int, plan: str, custom_price: float = None, custom_days: int = None, is_renewal: bool = False):
    """Función centralizada para activar o renovar suscripciones. Recibe SIEMPRE un user_id resuelto."""
    first_name = ""
    username = f"user_{target_user_id}"
    try:
        member = await bot.get_chat_member(group_id, target_user_id)
        if member and member.user:
            first_name = member.user.first_name or ""
            username = member.user.username or f"user_{target_user_id}"
    except Exception:
        db_user = await db.get_user_by_id_full(target_user_id, group_id)
        if db_user:
            first_name = db_user.get('first_name', '') or ''
            username = db_user.get('username', '') or f"user_{target_user_id}"

    existing = await db.get_user_by_id(target_user_id, group_id)
    if not existing:
        await db.register_free_user(group_id, target_user_id, username, first_name)

    if is_renewal:
        success, msg = await db.renew_user_subscription(group_id, target_user_id, plan, custom_price, custom_days, first_name)
        action_text = "Renovación"
    else:
        success, msg = await db.add_or_update_user_by_id(group_id, target_user_id, plan, first_name, custom_price, custom_days)
        action_text = "Activación"

    group = get_group_by_id(group_id)
    group_name = safe_name(group.get('group_name', 'VIP')) if group else 'VIP'
    group_type = group.get('type', 'VIP') if group else 'VIP'
    
    await reply_func(
        f"✅ *{action_text} de suscripción*\n\n📌 *Grupo:* {group_name} ({group_type})\n🆔 *ID:* `{group_id}`\n\n{msg}",
        parse_mode="Markdown"
    )

    if success:
        await _mark_discount_if_any(target_user_id, group_id, plan)
        await _send_subscription_notification(bot, target_user_id, group_id, plan, is_renewal=is_renewal)

async def _execute_combo_subscription_flow(bot, reply_func, vip_group_id: int, target_user_id: int, plan: str,
                                            custom_price: float = None, custom_days: int = None, is_renewal: bool = False):
    if plan == "trial":
        await reply_func("❌ El combo no aplica al plan trial. Usa `/add` normalmente.", parse_mode="Markdown")
        return
    vip_group = get_group_by_id(vip_group_id)
    if not vip_group or vip_group.get("type", "VIP") != "VIP":
        await reply_func("❌ El combo debe activarse desde un grupo VIP.", parse_mode="Markdown")
        return
    comunidad_group_id = vip_group.get("settings", {}).get("linked_comunidad_group_id")
    if not comunidad_group_id or not get_group_by_id(comunidad_group_id):
        await reply_func("❌ Este VIP no tiene un grupo Comunidad vinculado. Usa `/linkcomunidad` primero.", parse_mode="Markdown")
        return

    settings = vip_group.get("settings", {})
    default_combo_price = settings.get(f"combo_price_{plan}")
    if default_combo_price is None:
        vip_cfg = get_group_plan_config(vip_group_id, plan)
        com_cfg = get_group_plan_config(comunidad_group_id, plan)
        default_combo_price = vip_cfg.get('price', 0) + com_cfg.get('price', 0)
    final_price = custom_price if custom_price is not None else float(default_combo_price)

    first_name = ""
    username = f"user_{target_user_id}"
    try:
        member = await bot.get_chat_member(vip_group_id, target_user_id)
        if member and member.user:
            first_name = member.user.first_name or ""
            username = member.user.username or f"user_{target_user_id}"
    except Exception:
        db_user = await db.get_user_by_id_full(target_user_id, vip_group_id)
        if db_user:
            first_name = db_user.get('first_name', '') or ''
            username = db_user.get('username', '') or f"user_{target_user_id}"

    existing_vip = await db.get_user_by_id(target_user_id, vip_group_id)
    if not existing_vip:
        await db.register_free_user(vip_group_id, target_user_id, username, first_name)
    if is_renewal:
        success_vip, msg_vip = await db.renew_user_subscription(vip_group_id, target_user_id, plan, final_price, custom_days, first_name)
    else:
        success_vip, msg_vip = await db.add_or_update_user_by_id(vip_group_id, target_user_id, plan, first_name, final_price, custom_days)

    existing_com = await db.get_user_by_id(target_user_id, comunidad_group_id)
    if not existing_com:
        await db.register_free_user(comunidad_group_id, target_user_id, username, first_name)
    if is_renewal:
        success_com, msg_com = await db.renew_user_subscription(comunidad_group_id, target_user_id, plan, 0, custom_days, first_name)
    else:
        success_com, msg_com = await db.add_or_update_user_by_id(comunidad_group_id, target_user_id, plan, first_name, 0, custom_days)

    action_text = "Renovación" if is_renewal else "Activación"
    await reply_func(
        f"✅ *{action_text} COMBO (VIP + Comunidad)*\n\n👑 *VIP:*\n{msg_vip}\n\n👥 *Comunidad:*\n{msg_com}\n\n"
        f"💰 *Precio combo cobrado:* {fmt_price(final_price)}",
        parse_mode="Markdown"
    )

    if success_vip or success_com:
        for gid, success in [(vip_group_id, success_vip), (comunidad_group_id, success_com)]:
            if success:
                try:
                    await bot.unban_chat_member(gid, target_user_id, only_if_banned=True)
                except Exception as e:
                    logger.warning(f"No se pudo desbanear a {target_user_id} en {gid}: {e}")
                try:
                    await bot.restrict_chat_member(gid, target_user_id, permissions=UNMUTE_PERMISSIONS)
                    await db.clear_muted(target_user_id, gid)
                except Exception:
                    pass
        
        vip_link = vip_group.get("settings", {}).get("vip_invite_link", "").strip()
        link_msg = f"\n\n🔗 *Enlace VIP:* {vip_link}" if vip_link else ""
        
        try:
            await bot.send_message(
                target_user_id,
                f"🎉 *¡Tu combo VIP + Comunidad ha sido {'renovado' if is_renewal else 'activado'}!*\n\n"
                f"👑 Acceso VIP: *{plan}*\n👥 Acceso Comunidad: *{plan}*{link_msg}\n\n¡Gracias por tu confianza! 🙌",
                parse_mode="Markdown",
                disable_web_page_preview=True
            )
        except Exception as e:
            logger.warning(f"No se pudo notificar combo a {target_user_id}: {e}")
# ==================== BASE DE DATOS ====================
RETRYABLE_PGCODES = {
    "08000", "08003", "08006", "08001", "57P03", "53300", "53200", "40001", "40P01"
}

class CircuitBreaker:
    def __init__(self, failure_threshold=10, recovery_timeout=15.0):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._failures = 0
        self._opened_at: float | None = None
        self._lock = asyncio.Lock()

    async def allow(self) -> bool:
        async with self._lock:
            if self._opened_at is None:
                return True
            if _time.monotonic() - self._opened_at > self.recovery_timeout:
                self._opened_at = None
                self._failures = 0
                return True
            return False

    async def record_success(self):
        async with self._lock:
            self._failures = 0
            self._opened_at = None

    async def record_failure(self):
        async with self._lock:
            self._failures += 1
            if self._failures >= self.failure_threshold:
                self._opened_at = _time.monotonic()
                logger.error(
                    f"🔴 Circuit breaker OPEN tras {self._failures} fallos — "
                    f"fallando rápido por {self.recovery_timeout}s"
                )

class Database:
    def __init__(self, db_url: str):
        self.db_url = db_url
        self._pool: AsyncConnectionPool = None
        self._circuit = CircuitBreaker()
        self._retry_count = 0
        self._op_count = 0
        self._last_error: str | None = None

    async def _get_pool(self) -> AsyncConnectionPool:
        if self._pool is None or self._pool.closed:
            self._pool = AsyncConnectionPool(
                conninfo=self.db_url,
                min_size=2,
                max_size=20,
                kwargs={
                    "options": "-c timezone=UTC -c statement_timeout=15000",
                    "connect_timeout": 5,        
                    "application_name": "ayudante_vip_bot",
                },
                timeout=10.0,                    
                max_idle=300.0,                  
                reconnect_timeout=30.0,
                open=False
            )
            await self._pool.open()
            logger.info("✅ psycopg3 AsyncConnectionPool creado (min=2, max=20) | Timezone UTC")
        return self._pool

    async def _run(self, func, retries=2, base_delay=0.1, max_delay=2.0, op_name="unknown"):
        self._op_count += 1
        
        if not await self._circuit.allow():
            self._last_error = "Circuit breaker abierto"
            raise psycopg.OperationalError("Circuit breaker abierto — BD posiblemente caída")

        pool = await self._get_pool()
        last_error = None
        
        for attempt in range(retries + 1):
            try:
                async with pool.connection() as conn:
                    result = await func(conn)
                    await self._circuit.record_success()
                    return result
                    
            except psycopg.OperationalError as e:
                last_error = e
                self._last_error = f"{op_name}: {e}"
                pgcode = getattr(e, 'pgcode', None)
                
                # 2. Diferenciar errores (no reintentar si son fatales/no recuperables)
                if pgcode and pgcode not in RETRYABLE_PGCODES:
                    break
                    
                # Si fue el último intento permitido, salir del bucle
                if attempt >= retries:
                    break
                    
                self._retry_count += 1
                
                # 3. Backoff exponencial con jitter decorrelacionado
                delay = min(max_delay, base_delay * (2 ** attempt))
                delay = random.uniform(0, delay)
                
                logger.warning(
                    f"DB OperationalError (intento {attempt+1}/{retries+1}) en '{op_name}': {pgcode} — "
                    f"retry en {delay:.2f}s"
                )
                await asyncio.sleep(delay)
                
        await self._circuit.record_failure()
        raise last_error

    async def close(self):
        if self._pool and not self._pool.closed:
            await self._pool.close()
            logger.info("🔌 psycopg3 Connection pool cerrado")

    async def init_tables(self):
        async def _init(conn):
            async with conn.cursor() as cur:
                await cur.execute("""
                    CREATE TABLE IF NOT EXISTS groups (
                        group_id BIGINT PRIMARY KEY, group_name TEXT, group_type TEXT DEFAULT 'VIP',
                        admin_id BIGINT, super_admin_id BIGINT, created_at TIMESTAMP DEFAULT NOW(),
                        settings JSONB DEFAULT '{}'::jsonb
                    )
                """)
                await cur.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        id SERIAL PRIMARY KEY, user_id BIGINT NOT NULL, group_id BIGINT NOT NULL,
                        username TEXT, first_name TEXT, plan TEXT NOT NULL,
                        start_date TIMESTAMP NOT NULL, end_date TIMESTAMP NOT NULL,
                        status TEXT DEFAULT 'active', trial_used BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMP DEFAULT NOW(), updated_at TIMESTAMP DEFAULT NOW(),
                        kicked_at TIMESTAMP DEFAULT NULL,
                        fuego_notice_sent BOOLEAN DEFAULT FALSE,
                        UNIQUE(user_id, group_id),
                        source TEXT DEFAULT 'Directo'
                    )
                """)
                await cur.execute("""
                    CREATE TABLE IF NOT EXISTS payments (
                        id SERIAL PRIMARY KEY, user_id BIGINT NOT NULL, group_id BIGINT NOT NULL,
                        username TEXT, first_name TEXT, plan TEXT NOT NULL,
                        amount NUMERIC(10,2) NOT NULL, duration_minutes INTEGER,
                        payment_date TIMESTAMP DEFAULT NOW()
                    )
                """)
                await cur.execute("""
                    CREATE TABLE IF NOT EXISTS warning_logs (
                        id SERIAL PRIMARY KEY, user_id BIGINT NOT NULL, group_id BIGINT NOT NULL,
                        warning_type TEXT NOT NULL, sent_at TIMESTAMP DEFAULT NOW(),
                        UNIQUE(user_id, group_id, warning_type)
                    )
                """)
                await cur.execute("""
                    CREATE TABLE IF NOT EXISTS payment_leads (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        group_id BIGINT NOT NULL,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                await cur.execute("""
                    DELETE FROM payment_leads a USING payment_leads b 
                    WHERE a.id < b.id AND a.user_id = b.user_id AND a.group_id = b.group_id
                """)
                await cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_payment_leads_user_group ON payment_leads(user_id, group_id)")
                await cur.execute("""
                    CREATE TABLE IF NOT EXISTS broadcast_logs (
                        id SERIAL PRIMARY KEY, group_id BIGINT NOT NULL, filter_type TEXT NOT NULL,
                        message_preview TEXT, total_count INTEGER DEFAULT 0, success_count INTEGER DEFAULT 0,
                        fail_count INTEGER DEFAULT 0, sent_by BIGINT, sent_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                await cur.execute("""
                    CREATE TABLE IF NOT EXISTS discount_spins (
                        user_id BIGINT NOT NULL, group_id BIGINT NOT NULL, spin_count INTEGER DEFAULT 1,
                        last_spin TIMESTAMP DEFAULT NOW(), best_discount INTEGER DEFAULT 0,
                        used BOOLEAN DEFAULT FALSE, applied_to_plan TEXT,
                        UNIQUE(user_id, group_id)
                    )
                """)
                await cur.execute("""
                    CREATE TABLE IF NOT EXISTS group_messages (
                        group_id BIGINT PRIMARY KEY,
                        welcome_message TEXT DEFAULT NULL,
                        welcome_buttons JSONB DEFAULT '[]'::jsonb,
                        rejection_message TEXT DEFAULT NULL,
                        rejection_buttons JSONB DEFAULT '[]'::jsonb,
                        channel_select_message TEXT DEFAULT NULL,
                        channel_description TEXT DEFAULT NULL,
                        vip_menu_message TEXT DEFAULT NULL,
                        fuego_notice_message TEXT DEFAULT NULL,
                        expired_message TEXT DEFAULT NULL,
                        updated_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                for idx_sql in [
                    "CREATE INDEX IF NOT EXISTS idx_discount_spins_user ON discount_spins(user_id, group_id)",
                    "CREATE INDEX IF NOT EXISTS idx_warning_logs_lookup ON warning_logs(user_id, group_id, warning_type)",
                    "CREATE INDEX IF NOT EXISTS idx_warning_logs_group ON warning_logs(group_id, sent_at)",
                    "CREATE INDEX IF NOT EXISTS idx_users_group ON users(group_id, status)",
                    "CREATE INDEX IF NOT EXISTS idx_users_end_date ON users(group_id, end_date)",
                    "CREATE INDEX IF NOT EXISTS idx_users_uid_gid ON users(user_id, group_id)",
                    "CREATE INDEX IF NOT EXISTS idx_payments_group ON payments(group_id, payment_date)",
                    "CREATE INDEX IF NOT EXISTS idx_broadcast_logs_group ON broadcast_logs(group_id, sent_at)",
                    "CREATE INDEX IF NOT EXISTS idx_users_expired ON users(group_id, status, end_date) WHERE status = 'active'",
                    "CREATE INDEX IF NOT EXISTS idx_payment_leads_group ON payment_leads(group_id, created_at)"
                ]:
                    await cur.execute(idx_sql)
                await cur.execute("""
                    DO $$                     BEGIN
                        IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='payments' AND column_name='amount' AND data_type='integer')
                            THEN ALTER TABLE payments ALTER COLUMN amount TYPE NUMERIC(10,2); END IF;
                        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='payments' AND column_name='duration_minutes')
                            THEN ALTER TABLE payments ADD COLUMN duration_minutes INTEGER; END IF;
                        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='kicked_at')
                            THEN ALTER TABLE users ADD COLUMN kicked_at TIMESTAMP DEFAULT NULL; END IF;
                        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='fuego_notice_sent')
                            THEN ALTER TABLE users ADD COLUMN fuego_notice_sent BOOLEAN DEFAULT FALSE; END IF;
                        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='group_messages' AND column_name='channel_select_message')
                            THEN ALTER TABLE group_messages ADD COLUMN channel_select_message TEXT DEFAULT NULL; END IF;
                        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='group_messages' AND column_name='channel_description')
                            THEN ALTER TABLE group_messages ADD COLUMN channel_description TEXT DEFAULT NULL; END IF;
                        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='group_messages' AND column_name='vip_menu_message')
                            THEN ALTER TABLE group_messages ADD COLUMN vip_menu_message TEXT DEFAULT NULL; END IF;
                        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='group_messages' AND column_name='fuego_notice_message')
                            THEN ALTER TABLE group_messages ADD COLUMN fuego_notice_message TEXT DEFAULT NULL; END IF;
                        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='group_messages' AND column_name='expired_message')
                            THEN ALTER TABLE group_messages ADD COLUMN expired_message TEXT DEFAULT NULL; END IF;
                        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='source')
                            THEN ALTER TABLE users ADD COLUMN source TEXT DEFAULT 'Directo'; END IF;
                        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='payment_leads' AND column_name='cart_msg_sent_at')
                            THEN ALTER TABLE payment_leads ADD COLUMN cart_msg_sent_at TIMESTAMP DEFAULT NULL; END IF;
                        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='payment_leads' AND column_name='cart_step')
                            THEN ALTER TABLE payment_leads ADD COLUMN cart_step INTEGER DEFAULT 0; END IF;
                        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='restricted_at')
                            THEN ALTER TABLE users ADD COLUMN restricted_at TIMESTAMP DEFAULT NULL; END IF;
                    END $$;
                """)
                await cur.execute("""
                    UPDATE users SET trial_used = TRUE, updated_at = NOW()
                    WHERE (trial_used = FALSE OR trial_used IS NULL)
                      AND EXISTS (SELECT 1 FROM payments p WHERE p.user_id = users.user_id AND p.group_id = users.group_id AND p.plan = 'trial')
                """)
        await self._run(_init)
        logger.info("✅ Base de datos inicializada")

    async def load_groups_from_db(self):
        async def _load(conn):
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("SELECT group_id, group_name, admin_id, COALESCE(group_type, 'VIP') as group_type, COALESCE(settings, '{}') as settings FROM groups")
                return await cur.fetchall()
        rows = await self._run(_load)
        if rows:
            with GROUPS_LOCK:
                GROUPS.clear()
                for g in rows:
                    settings = g["settings"] if isinstance(g["settings"], dict) else {}
                    GROUPS[g["group_id"]] = {"group_id": g["group_id"], "group_name": g["group_name"],
                                   "admin_id": g["admin_id"], "type": g["group_type"], "settings": settings}
            logger.info(f"📦 {len(GROUPS)} grupos cargados desde BD")
            return True
        return False
        
    async def load_group_messages(self):
        async def _load(conn):
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("SELECT * FROM group_messages")
                return await cur.fetchall()
        rows = await self._run(_load)
        with GROUP_MESSAGES_LOCK:
            GROUP_MESSAGES_CACHE.clear()
            for r in rows:
                GROUP_MESSAGES_CACHE[r['group_id']] = dict(r)
        logger.info(f"📨 {len(GROUP_MESSAGES_CACHE)} mensajes de grupo cargados en caché")

    async def save_group(self, group_id: int, group_name: str, admin_id: int, group_type: str = "VIP"):
        async def _save(conn):
            async with conn.cursor() as cur:
                await cur.execute("""
                    INSERT INTO groups (group_id, group_name, admin_id, super_admin_id, group_type)
                    VALUES (%s,%s,%s,%s,%s)
                    ON CONFLICT (group_id) DO UPDATE SET
                        group_name = EXCLUDED.group_name, admin_id = EXCLUDED.admin_id, group_type = EXCLUDED.group_type
                """, (group_id, group_name, admin_id, SUPER_ADMIN_ID, group_type))
        await self._run(_save)

    async def get_user(self, user_id: int, group_id: int, columns: str = "*") -> Optional[dict]:
        safe_columns = {"user_id", "status", "end_date", "plan", "trial_used", "fuego_notice_sent", "username", "first_name", "start_date", "source", "*"}
        if columns != "*":
            requested = [c.strip() for c in columns.split(",")]
            if not all(col in safe_columns for col in requested):
                columns = "*" 
                
        async def _get(conn):
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(f"SELECT {columns} FROM users WHERE user_id=%s AND group_id=%s LIMIT 1", (user_id, group_id))
                return await cur.fetchone()
        return await self._run(_get)

    async def get_user_by_username(self, username: str, group_id: int = None):
        async def _get(conn):
            async with conn.cursor(row_factory=dict_row) as cur:
                if group_id:
                    await cur.execute("SELECT * FROM users WHERE LOWER(username)=LOWER(%s) AND group_id=%s", (username, group_id))
                else:
                    await cur.execute("SELECT * FROM users WHERE LOWER(username)=LOWER(%s)", (username,))
                return await cur.fetchone()
        return await self._run(_get)

    async def get_user_by_id(self, user_id: int, group_id: int):
        return await self.get_user(user_id, group_id, "user_id, status, end_date, plan, trial_used, fuego_notice_sent")

    async def get_user_by_id_full(self, user_id: int, group_id: int):
        return await self.get_user(user_id, group_id, "*")

    async def get_user_groups(self, user_id: int) -> List[int]:
        async def _get(conn):
            async with conn.cursor() as cur:
                await cur.execute("SELECT group_id FROM users WHERE user_id = %s", (user_id,))
                return [row[0] for row in await cur.fetchall()]
        return await self._run(_get)

    async def register_user_auto(self, group_id: int, user_id: int, username: str, first_name: str, source: str = "Directo"):
        now = now_utc()
        trial_cfg = get_group_plan_config(group_id, "trial")
        trial_minutes = trial_cfg.get("minutes", 60)
        async def _register(conn):
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("SELECT user_id, trial_used, status, end_date, plan, source FROM users WHERE user_id=%s AND group_id=%s FOR UPDATE", (user_id, group_id))
                existing = await cur.fetchone()
                if not existing:
                    end_date = now + timedelta(minutes=trial_minutes)
                    await cur.execute("""INSERT INTO users (user_id, group_id, username, first_name, plan, start_date, end_date, trial_used, status, source) VALUES (%s,%s,%s,%s,'trial',%s,%s,TRUE,'active',%s)""", (user_id, group_id, username, first_name, now, end_date, source))
                    await cur.execute("""INSERT INTO payments (user_id, group_id, username, first_name, plan, amount, duration_minutes, payment_date) VALUES (%s,%s,%s,%s,'trial',0,%s,%s)""", (user_id, group_id, username, first_name, trial_minutes, now))
                    return True, "trial_nuevo", end_date
                await cur.execute("UPDATE users SET username=%s, first_name=%s, source=%s, updated_at=NOW() WHERE user_id=%s AND group_id=%s", (username, first_name, source, user_id, group_id))
                existing_end = normalize_dt(existing["end_date"])
                
                if existing_end > now:
                    if existing["status"] != "active":
                        await cur.execute("UPDATE users SET status='active', kicked_at=NULL, updated_at=NOW() WHERE user_id=%s AND group_id=%s", (user_id, group_id))
                    else:
                        await cur.execute("UPDATE users SET kicked_at=NULL, updated_at=NOW() WHERE user_id=%s AND group_id=%s AND kicked_at IS NOT NULL", (user_id, group_id))
                    return True, "activo", existing["end_date"]

                if existing.get("trial_used"): 
                    return False, "sin_trial", None
                
                end_date = now + timedelta(minutes=trial_minutes)
                await cur.execute("""UPDATE users SET plan='trial', start_date=%s, end_date=%s, trial_used=TRUE, status='active', kicked_at=NULL, updated_at=NOW() WHERE user_id=%s AND group_id=%s""", (now, end_date, user_id, group_id))
                await cur.execute("DELETE FROM warning_logs WHERE user_id=%s AND group_id=%s", (user_id, group_id))
                await cur.execute("""INSERT INTO payments (user_id, group_id, username, first_name, plan, amount, duration_minutes, payment_date) VALUES (%s,%s,%s,%s,'trial',0,%s,%s)""", (user_id, group_id, username, first_name, trial_minutes, now))
                return True, "trial_nuevo", end_date
        return await self._run(_register)

    async def register_free_user(self, chat_id: int, user_id: int, username: str, first_name: str, source: str = "Directo") -> bool:
        async def _reg(conn):
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1 FROM users WHERE user_id=%s AND group_id=%s", (user_id, chat_id))
                if await cur.fetchone():
                    return False
                await cur.execute("""
                    INSERT INTO users (user_id, group_id, username, first_name, plan, start_date, end_date, status, trial_used, source)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (user_id, group_id) DO NOTHING
                """, (user_id, chat_id, username, first_name, "FREE",
                      datetime.now(ZoneInfo("UTC")),
                      datetime.now(ZoneInfo("UTC")) + timedelta(days=365),
                      "potencial", False, source))
                return True
        return await self._run(_reg)

    async def _upsert_user(self, group_id: int, plan: str, identifier: str, is_id: bool,
                           first_name: str = "", custom_price: float = None, custom_days: int = None,
                           mode: str = "add") -> tuple:
        if plan not in PLANS:
            return False, "❌ Plan inválido"
        config = get_group_plan_config(group_id, plan)
        effective_price = custom_price if custom_price is not None else config.get('price', 0)
        effective_days = custom_days if custom_days is not None else config.get('days', 7)
        if plan == "trial":
            effective_mins = config.get('minutes', 1440)
        now = datetime.now(ZoneInfo("UTC"))

        async def _upsert(conn):
            async with conn.cursor(row_factory=dict_row) as cur:
                if is_id:
                    await cur.execute("SELECT * FROM users WHERE user_id=%s AND group_id=%s FOR UPDATE", (int(identifier), group_id))
                else:
                    await cur.execute("SELECT * FROM users WHERE LOWER(username)=LOWER(%s) AND group_id=%s FOR UPDATE", (identifier, group_id))
                existing = await cur.fetchone()
                if not existing:
                    return False, f"❌ No tengo registro de {'ID ' + identifier if is_id else '@' + identifier}. Pídele que envíe un mensaje al bot."
                if plan == "trial" and existing.get('trial_used'):
                    return False, "❌ Este usuario ya usó su prueba gratuita"
                existing_end = normalize_dt(existing['end_date'])
                if mode == "renew" and existing['status'] == 'active' and existing_end > now:
                    base_date = existing_end
                    action = "RENOVADO"
                else:
                    base_date = now
                    action = "REACTIVADO" if existing['status'] != 'active' else "ACTIVADO"
                if plan == "trial":
                    end_date = base_date + timedelta(minutes=effective_mins)
                    dur_minutes = effective_mins
                    expiry_str = fmt_dt(end_date)
                else:
                    end_date = base_date + timedelta(days=effective_days)
                    dur_minutes = effective_days * 1440
                    expiry_str = fmt_dt(end_date, include_time=False)
                fn = first_name or existing.get('first_name', '') or ''
                uname = existing.get('username', f"user_{existing['user_id']}")
                await cur.execute("""
                    UPDATE users SET plan=%s, start_date=%s, end_date=%s, status='active', updated_at=NOW(),
                        first_name=%s, username=%s, trial_used = trial_used OR %s, kicked_at = NULL
                    WHERE user_id=%s AND group_id=%s
                """, (plan, now if mode == "add" else existing['start_date'], end_date, fn, uname, plan == "trial", existing['user_id'], group_id))
                await cur.execute("DELETE FROM warning_logs WHERE user_id=%s AND group_id=%s", (existing['user_id'], group_id))
                await cur.execute("""
                    INSERT INTO payments (user_id, group_id, username, first_name, plan, amount, duration_minutes, payment_date)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """, (existing['user_id'], group_id, uname, fn, plan, effective_price, dur_minutes, now))
                
                payload = json.dumps({
                    "user_id": existing['user_id'], 
                    "group_id": group_id, 
                    "plan": plan
                })
                await cur.execute("NOTIFY vip_cache_invalidation, %s", (payload,))

                price_str = fmt_price(effective_price) if effective_price > 0 else "gratis"
                display = fn if fn else uname
                return True, f"✅ *{display}* {action}\n📋 Plan: {plan} | 💰 {price_str}\n📅 Expira: {expiry_str}"
        return await self._run(_upsert)

    async def add_or_update_user(self, group_id: int, username: str, plan: str, first_name: str = "",
                                  custom_price: float = None, custom_days: int = None):
        return await self._upsert_user(group_id, plan, username, False, first_name, custom_price, custom_days, "add")

    async def add_or_update_user_by_id(self, group_id: int, user_id: int, plan: str, first_name: str = "",
                                        custom_price: float = None, custom_days: int = None):
        return await self._upsert_user(group_id, plan, str(user_id), True, first_name, custom_price, custom_days, "add")

    async def renew_user_subscription(self, group_id: int, user_id: int, plan: str,
                                       custom_price: float = None, custom_days: int = None, first_name: str = ""):
        return await self._upsert_user(group_id, plan, str(user_id), True, first_name, custom_price, custom_days, "renew")

    async def get_all_active_users(self, group_id: int, limit: int = 10, offset: int = 0):
        async def _get(conn):
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("SELECT COUNT(*) FROM users WHERE group_id=%s AND status='active' AND end_date > NOW()", (group_id,))
                total = await cur.fetchval()
                
                await cur.execute("""
                    SELECT user_id, username, first_name, plan, end_date,
                           EXTRACT(DAY FROM (end_date - NOW())) AS days_left
                    FROM users WHERE group_id=%s AND status='active' AND end_date > NOW() 
                    ORDER BY end_date ASC
                    LIMIT %s OFFSET %s
                """, (group_id, limit, offset))
                users = await cur.fetchall()
                return users, total
        return await self._run(_get)

    async def get_monthly_earnings(self, group_id: int):
        start_date = datetime.now(ZoneInfo("UTC")).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        async def _get(conn):
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("SELECT plan, COUNT(*) AS count, COALESCE(SUM(amount),0) AS total FROM payments WHERE group_id=%s AND payment_date>=%s GROUP BY plan", (group_id, start_date))
                summary = await cur.fetchall()
                total = sum(row['total'] for row in summary)
                await cur.execute("SELECT COUNT(*) AS new_users FROM users WHERE group_id=%s AND created_at>=%s", (group_id, start_date))
                new_users = await cur.fetchval()
                await cur.execute("""
                    SELECT user_id, username, first_name, plan, amount, duration_minutes, payment_date
                    FROM payments WHERE group_id=%s AND payment_date >= date_trunc('month', NOW()) ORDER BY payment_date DESC LIMIT 10
                """, (group_id,))
                recent = await cur.fetchall()
                return {"summary": summary, "total": total, "new_users": new_users, "recent": recent}
        return await self._run(_get)

    async def get_total_monthly_earnings(self):
        start_date = datetime.now(ZoneInfo("UTC")).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        async def _get(conn):
            async with conn.cursor() as cur:
                await cur.execute("SELECT COALESCE(SUM(amount),0) FROM payments WHERE payment_date>=%s", (start_date,))
                return await cur.fetchval()
        return await self._run(_get)

    async def get_expired_users(self, group_id: int):
        async def _get(conn):
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("""
                    SELECT user_id, username, first_name, plan, end_date
                    FROM users WHERE group_id=%s AND status='active' AND end_date < NOW() AND kicked_at IS NULL
                """, (group_id,))
                return await cur.fetchall()
        return await self._run(_get)

    async def expire_user(self, user_id: int, group_id: int):
        async def _expire(conn):
            async with conn.cursor() as cur:
                await cur.execute("""
                    UPDATE users SET status='expired', kicked_at=NOW(), updated_at=NOW()
                    WHERE user_id=%s AND group_id=%s AND status='active' AND kicked_at IS NULL RETURNING user_id
                """, (user_id, group_id))
                return await cur.fetchone() is not None
        return await self._run(_expire)

    async def expire_users_batch(self, user_ids: List[int], group_id: int):
        if not user_ids:
            return []
        async def _expire(conn):
            async with conn.cursor() as cur:
                await cur.execute("""
                    UPDATE users SET status='expired', kicked_at=NOW(), updated_at=NOW()
                    WHERE user_id = ANY(%s) AND group_id=%s AND status='active' AND kicked_at IS NULL
                    RETURNING user_id
                """, (user_ids, group_id))
                return [row[0] for row in await cur.fetchall()]
        return await self._run(_expire)

    async def get_user_spins_anywhere(self, user_id: int) -> List[dict]:
        async def _get(conn):
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("SELECT * FROM discount_spins WHERE user_id = %s", (user_id,))
                return await cur.fetchall()
        return await self._run(_get)

    async def get_potential_clients_stats(self, group_id: int):
        now = datetime.now(ZoneInfo("UTC"))
        start_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if now.month == 1:
            start_last_month = now.replace(year=now.year-1, month=12, day=1, hour=0, minute=0, second=0, microsecond=0)
        else:
            start_last_month = now.replace(month=now.month-1, day=1, hour=0, minute=0, second=0, microsecond=0)
        end_last_month = start_of_month
        async def _get(conn):
            async with conn.cursor() as cur:
                await cur.execute("SELECT COUNT(*) FROM users WHERE group_id=%s AND status='potencial' AND created_at>=%s", (group_id, start_of_month))
                count_month = await cur.fetchval()
                await cur.execute("SELECT COUNT(*) FROM users WHERE group_id=%s AND status='potencial' AND created_at>=%s AND created_at<%s", (group_id, start_last_month, end_last_month))
                count_last_month = await cur.fetchval()
                await cur.execute("SELECT COUNT(*) FROM users WHERE group_id=%s AND status='potencial'", (group_id,))
                total_all = await cur.fetchval()
                return count_month, count_last_month, total_all
        return await self._run(_get)

    async def get_potential_clients_list(self, group_id: int):
        async def _get(conn):
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("SELECT user_id, username, first_name, created_at FROM users WHERE group_id=%s AND status='potencial' ORDER BY created_at DESC", (group_id,))
                return await cur.fetchall()
        return await self._run(_get)

    async def get_export_data(self, group_id: int):
        start_date = datetime.now(ZoneInfo("UTC")).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        async def _get(conn):
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("""
                    SELECT user_id, username, first_name, plan, amount, duration_minutes, payment_date
                    FROM payments WHERE group_id=%s AND payment_date>=%s ORDER BY payment_date DESC
                """, (group_id, start_date))
                return await cur.fetchall()
        return await self._run(_get)

    async def update_group_fields(self, group_id: int, changes: dict):
        if not changes:
            return
        async def _upd(conn):
            async with conn.cursor() as cur:
                if 'settings' in changes:
                    await cur.execute("UPDATE groups SET settings=%s WHERE group_id=%s", (json.dumps(changes['settings']), group_id))
                if 'admin' in changes:
                    await cur.execute("UPDATE groups SET admin_id=%s WHERE group_id=%s", (changes['admin'], group_id))
                if 'type' in changes:
                    await cur.execute("UPDATE groups SET group_type=%s WHERE group_id=%s", (changes['type'], group_id))
        await self._run(_upd)

    async def delete_group_from_db(self, group_id: int):
        async def _del(conn):
            async with conn.cursor() as cur:
                await cur.execute("DELETE FROM groups WHERE group_id=%s", (group_id,))
        await self._run(_del)

    async def get_total_users_count(self):
        async def _get(conn):
            async with conn.cursor() as cur:
                await cur.execute("SELECT COUNT(*) FROM users")
                return await cur.fetchval()
        return await self._run(_get)

    async def get_trial_stats(self, group_id: int) -> dict:
        async def _get(conn):
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("SELECT COUNT(*) as total_trial_users FROM users WHERE group_id=%s AND (plan='trial' OR trial_used=TRUE)", (group_id,))
                total_trial_users = await cur.fetchval()
                await cur.execute("""
                    SELECT user_id, username, first_name, end_date,
                           EXTRACT(EPOCH FROM (end_date - NOW())) / 86400 as days_left,
                           EXTRACT(EPOCH FROM (end_date - NOW())) / 3600 as hours_left
                    FROM users WHERE group_id=%s AND plan='trial' AND status='active' AND end_date > NOW() ORDER BY end_date ASC
                """, (group_id,))
                active_trials = await cur.fetchall()
                await cur.execute("SELECT COUNT(*) as expired_from_trial FROM users WHERE group_id=%s AND plan='trial' AND status='expired' AND end_date < NOW()", (group_id,))
                expired_from_trial = await cur.fetchval()
                await cur.execute("""
                    SELECT COUNT(DISTINCT u.user_id) as converted_users
                    FROM users u JOIN payments p ON u.user_id = p.user_id AND u.group_id = p.group_id
                    WHERE u.group_id=%s AND u.trial_used=TRUE AND p.amount > 0
                """, (group_id,))
                converted_users = await cur.fetchval()
                return {"total_trial_users": total_trial_users, "active_trials": list(active_trials),
                        "active_count": len(active_trials), "expired_from_trial": expired_from_trial, "converted_users": converted_users}
        return await self._run(_get)

    async def get_broadcast_targets(self, group_id: int, filter_type: str) -> list:
        async def _get(conn):
            async with conn.cursor(row_factory=dict_row) as cur:
                if filter_type == 'trial_only':
                    await cur.execute("""
                        SELECT DISTINCT u.user_id, u.username, u.first_name, u.group_id FROM users u
                        WHERE u.group_id = %s AND (u.plan = 'trial' OR u.trial_used = TRUE OR u.trial_used IS NULL)
                          AND NOT EXISTS (SELECT 1 FROM payments p WHERE p.user_id = u.user_id AND p.group_id = u.group_id AND p.amount > 0)
                        ORDER BY u.user_id
                    """, (group_id,))
                elif filter_type == 'subscribed_only':
                    await cur.execute("""
                        SELECT DISTINCT u.user_id, u.username, u.first_name, u.group_id FROM users u
                        WHERE u.group_id = %s AND u.plan != 'trial' AND u.status = 'active' AND u.end_date > NOW()
                        ORDER BY u.user_id
                    """, (group_id,))
                elif filter_type == 'expired_only':
                    await cur.execute("""
                        SELECT DISTINCT u.user_id, u.username, u.first_name, u.group_id FROM users u
                        WHERE u.group_id = %s AND u.status = 'expired'
                          AND EXISTS (SELECT 1 FROM payments p WHERE p.user_id = u.user_id AND p.group_id = u.group_id AND p.amount > 0)
                        ORDER BY u.user_id
                    """, (group_id,))
                elif filter_type == 'all_trial':
                    await cur.execute("""
                        SELECT DISTINCT u.user_id, u.username, u.first_name, u.group_id FROM users u
                        WHERE u.group_id = %s AND (u.plan = 'trial' OR u.trial_used = TRUE OR u.trial_used IS NULL)
                        ORDER BY u.user_id
                    """, (group_id,))
                else:
                    return []
                return await cur.fetchall()
        return await self._run(_get)

    async def log_broadcast_sent(self, group_id: int, filter_type: str, message_text: str, total: int, success: int, fail: int, sent_by: int):
        preview = message_text[:100] if message_text else ""
        async def _log(conn):
            async with conn.cursor() as cur:
                await cur.execute("""
                    INSERT INTO broadcast_logs (group_id, filter_type, message_preview, total_count, success_count, fail_count, sent_by)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (group_id, filter_type, preview, total, success, fail, sent_by))
        await self._run(_log)

    async def log_payment_lead(self, user_id: int, group_id: int):
        async def _log(conn):
            async with conn.cursor() as cur:
                await cur.execute("""
                    INSERT INTO payment_leads (user_id, group_id, cart_step) 
                    VALUES (%s, %s, 0)
                    ON CONFLICT (user_id, group_id) DO UPDATE SET 
                        created_at = NOW(), 
                        cart_msg_sent_at = NULL,
                        cart_step = 0
                """, (user_id, group_id))
        await self._run(_log)

    async def get_pending_abandoned_carts_by_step(self, group_id: int, step: int, delay_minutes: int) -> list:
        threshold = datetime.now(ZoneInfo("UTC")) - timedelta(minutes=delay_minutes)
        async def _get(conn):
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("""
                    SELECT user_id, group_id FROM payment_leads 
                    WHERE group_id = %s AND cart_step = %s 
                      AND COALESCE(cart_msg_sent_at, created_at) <= %s
                """, (group_id, step, threshold))
                return await cur.fetchall()
        return await self._run(_get)

    async def update_cart_step(self, user_id: int, group_id: int, step: int):
        async def _upd(conn):
            async with conn.cursor() as cur:
                await cur.execute("""
                    UPDATE payment_leads SET cart_step = %s, cart_msg_sent_at = NOW() 
                    WHERE user_id = %s AND group_id = %s
                """, (step, user_id, group_id))
        await self._run(_upd)

    async def get_broadcast_history(self, group_id: int, limit: int = 10) -> list:
        async def _get(conn):
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("""
                    SELECT filter_type, message_preview, total_count, success_count, fail_count, sent_at
                    FROM broadcast_logs WHERE group_id = %s ORDER BY sent_at DESC LIMIT %s
                """, (group_id, limit))
                return await cur.fetchall()
        return await self._run(_get)

    async def get_warning_config(self, group_id: int) -> dict:
        group = get_group_by_id(group_id)
        if group:
            wc = group.get("settings", {}).get("warning_config")
            if wc:
                return wc
                
        return {
            "enabled": False,
            "warnings": [
                {"minutes_before": 15, "message": "⏳ *Tu trial expira en {minutes_left} minutos*\n\n📅 Expira: {expiry_time}\n\n💳 ¿Quieres seguir disfrutando del contenido?\nPresiona el botón para ver los datos de pago:"}
            ]
        }

    async def save_warning_config(self, group_id: int, config: dict):
        async def _save(conn):
            async with conn.cursor() as cur:
                await cur.execute("SELECT settings FROM groups WHERE group_id = %s", (group_id,))
                row = await cur.fetchone()
                settings = row[0] if row and row[0] else {}
                if isinstance(settings, str):
                    settings = json.loads(settings)
                settings['warning_config'] = config
                await cur.execute("UPDATE groups SET settings = %s WHERE group_id = %s", (json.dumps(settings), group_id))
        await self._run(_save)
        group = get_group_by_id(group_id)
        if group:
            group.setdefault('settings', {})['warning_config'] = config

    async def get_users_for_warning_multi(self, group_ids: List[int], minutes_before: int) -> list:
        warning_threshold = datetime.now(ZoneInfo("UTC")) + timedelta(minutes=minutes_before)
        warning_type = f"warning_{minutes_before}min"
        async def _get(conn):
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("""
                    SELECT u.user_id, u.username, u.first_name, u.end_date, u.plan, u.group_id
                    FROM users u LEFT JOIN warning_logs w
                        ON u.user_id = w.user_id AND u.group_id = w.group_id AND w.warning_type = %s
                    WHERE u.group_id = ANY(%s) AND u.status = 'active'
                      AND u.end_date <= %s AND u.end_date > NOW() AND w.id IS NULL
                    ORDER BY u.end_date ASC
                """, (warning_type, group_ids, warning_threshold))
                return await cur.fetchall()
        return await self._run(_get)

    async def log_warning_sent(self, user_id: int, group_id: int, warning_type: str):
        async def _log(conn):
            async with conn.cursor() as cur:
                await cur.execute("""
                    INSERT INTO warning_logs (user_id, group_id, warning_type, sent_at)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (user_id, group_id, warning_type) DO UPDATE SET sent_at = NOW()
                """, (user_id, group_id, warning_type))
        await self._run(_log)

    async def get_warning_stats(self, group_id: int) -> list:
        async def _get(conn):
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("SELECT warning_type, COUNT(*) as count FROM warning_logs WHERE group_id = %s GROUP BY warning_type", (group_id,))
                return await cur.fetchall()
        return await self._run(_get)

    async def clear_warning_logs(self, group_id: int = None):
        async def _clear(conn):
            async with conn.cursor() as cur:
                if group_id:
                    await cur.execute("DELETE FROM warning_logs WHERE group_id = %s", (group_id,))
                else:
                    await cur.execute("DELETE FROM warning_logs")
        await self._run(_clear)

    async def get_spin_data(self, user_id: int, group_id: int) -> dict:
        async def _get(conn):
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("SELECT * FROM discount_spins WHERE user_id = %s AND group_id = %s", (user_id, group_id))
                row = await cur.fetchone()
                if not row:
                    return {"spin_count": 0, "used": False, "best_discount": 0}
                return dict(row)
        return await self._run(_get)

    async def get_spin_data_batch(self, group_id: int, user_ids: List[int]) -> Dict[int, dict]:
        async def _get(conn):
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("SELECT * FROM discount_spins WHERE group_id = %s AND user_id = ANY(%s)", (group_id, user_ids))
                rows = await cur.fetchall()
                return {r['user_id']: dict(r) for r in rows}
        return await self._run(_get)

    async def record_spin(self, user_id: int, group_id: int, discount: int):
        async def _save(conn):
            async with conn.cursor() as cur:
                await cur.execute("""
                    INSERT INTO discount_spins (user_id, group_id, spin_count, last_spin, best_discount)
                    VALUES (%s, %s, 1, NOW(), %s)
                    ON CONFLICT (user_id, group_id) DO UPDATE SET
                        spin_count = discount_spins.spin_count + 1,
                        last_spin = NOW(),
                        best_discount = GREATEST(discount_spins.best_discount, EXCLUDED.best_discount)
                """, (user_id, group_id, discount))
        await self._run(_save)

    async def mark_discount_used(self, user_id: int, group_id: int, plan: str = None):
        async def _upd(conn):
            async with conn.cursor() as cur:
                await cur.execute("""
                    UPDATE discount_spins SET used = TRUE, applied_to_plan = %s
                    WHERE user_id = %s AND group_id = %s
                """, (plan, user_id, group_id))
        await self._run(_upd)

    async def get_group_messages(self, group_id: int) -> Optional[dict]:
        with GROUP_MESSAGES_LOCK:
            if group_id in GROUP_MESSAGES_CACHE:
                return GROUP_MESSAGES_CACHE[group_id]
                
        async def _get(conn):
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("SELECT * FROM group_messages WHERE group_id = %s", (group_id,))
                row = await cur.fetchone()
                return dict(row) if row else None
        msg_data = await self._run(_get)
        
        if msg_data:
            with GROUP_MESSAGES_LOCK:
                GROUP_MESSAGES_CACHE[group_id] = msg_data
        return msg_data

    async def save_group_messages(self, group_id: int, welcome_msg: str = None, welcome_buttons: list = None,
                                   rejection_msg: str = None, rejection_buttons: list = None,
                                   channel_select_msg: str = None, channel_desc: str = None,
                                   vip_menu_msg: str = None, fuego_notice_msg: str = None,
                                   expired_msg: str = None):
        async def _save(conn):
            async with conn.cursor() as cur:
                await cur.execute("""
                    INSERT INTO group_messages (group_id, welcome_message, welcome_buttons, rejection_message, rejection_buttons,
                                                channel_select_message, channel_description, vip_menu_message,
                                                fuego_notice_message, expired_message, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (group_id) DO UPDATE SET
                        welcome_message = COALESCE(EXCLUDED.welcome_message, group_messages.welcome_message),
                        welcome_buttons = COALESCE(EXCLUDED.welcome_buttons, group_messages.welcome_buttons),
                        rejection_message = COALESCE(EXCLUDED.rejection_message, group_messages.rejection_message),
                        rejection_buttons = COALESCE(EXCLUDED.rejection_buttons, group_messages.rejection_buttons),
                        channel_select_message = COALESCE(EXCLUDED.channel_select_message, group_messages.channel_select_message),
                        channel_description = COALESCE(EXCLUDED.channel_description, group_messages.channel_description),
                        vip_menu_message = COALESCE(EXCLUDED.vip_menu_message, group_messages.vip_menu_message),
                        fuego_notice_message = COALESCE(EXCLUDED.fuego_notice_message, group_messages.fuego_notice_message),
                        expired_message = COALESCE(EXCLUDED.expired_message, group_messages.expired_message),
                        updated_at = NOW()
                """, (group_id, welcome_msg, json.dumps(welcome_buttons) if welcome_buttons else None,
                      rejection_msg, json.dumps(rejection_buttons) if rejection_buttons else None,
                      channel_select_msg, channel_desc, vip_menu_msg, fuego_notice_msg, expired_msg))
        await self._run(_save)
        
        with GROUP_MESSAGES_LOCK:
            current = GROUP_MESSAGES_CACHE.get(group_id, {})
            if welcome_msg is not None: current['welcome_message'] = welcome_msg
            if rejection_msg is not None: current['rejection_message'] = rejection_msg
            if channel_select_msg is not None: current['channel_select_message'] = channel_select_msg
            if channel_desc is not None: current['channel_description'] = channel_desc
            if vip_menu_msg is not None: current['vip_menu_message'] = vip_menu_msg
            if fuego_notice_msg is not None: current['fuego_notice_message'] = fuego_notice_msg
            if expired_msg is not None: current['expired_message'] = expired_msg
            GROUP_MESSAGES_CACHE[group_id] = current

    _VALID_MSG_COLUMNS = {
        "welcome_message", "rejection_message", "channel_select_message",
        "channel_description", "vip_menu_message", "fuego_notice_message",
        "expired_message"
    }
    
    async def mark_fuego_notice_sent(self, user_id: int, group_id: int):
        async def _upd(conn):
            async with conn.cursor() as cur:
                await cur.execute("UPDATE users SET fuego_notice_sent=TRUE WHERE user_id=%s AND group_id=%s", (user_id, group_id))
        await self._run(_upd)

    async def mark_muted(self, user_id: int, group_id: int):
        async def _upd(conn):
            async with conn.cursor() as cur:
                await cur.execute("UPDATE users SET restricted_at=NOW() WHERE user_id=%s AND group_id=%s", (user_id, group_id))
        await self._run(_upd)

    async def clear_muted(self, user_id: int, group_id: int):
        async def _upd(conn):
            async with conn.cursor() as cur:
                await cur.execute("UPDATE users SET restricted_at=NULL WHERE user_id=%s AND group_id=%s", (user_id, group_id))
        await self._run(_upd)

    async def get_global_message(self, msg_type: str) -> Optional[str]:
        if msg_type not in self._VALID_MSG_COLUMNS:
            logger.error(f"❌ Intento de inyección SQL o columna inválida bloqueado en get_global_message: {msg_type}")
            return None
            
        async def _get(conn):
            async with conn.cursor() as cur:
                await cur.execute(f"SELECT {msg_type} FROM group_messages WHERE group_id = 0")
                return await cur.fetchval()
        return await self._run(_get)

    async def save_global_message(self, msg_type: str, text: str):
        if msg_type not in self._VALID_MSG_COLUMNS:
            logger.error(f"❌ Intento de inyección SQL o columna inválida bloqueado en save_global_message: {msg_type}")
            return
        async def _save(conn):
            async with conn.cursor() as cur:
                await cur.execute(f"""
                    INSERT INTO group_messages (group_id, {msg_type}) VALUES (0, %s)
                    ON CONFLICT (group_id) DO UPDATE SET {msg_type} = EXCLUDED.{msg_type}
                """, (text,))
        await self._run(_save)
        
        with GROUP_MESSAGES_LOCK:
            current = GROUP_MESSAGES_CACHE.get(0, {})
            current[msg_type] = text
            GROUP_MESSAGES_CACHE[0] = current

    async def get_last_backup_date(self) -> Optional[datetime]:
        async def _get(conn):
            async with conn.cursor() as cur:
                await cur.execute("SELECT MAX(sent_at) FROM broadcast_logs WHERE filter_type = 'auto_backup'")
                return await cur.fetchval()
        return await self._run(_get)

    async def log_backup_sent(self):
        async def _log(conn):
            async with conn.cursor() as cur:
                await cur.execute("""
                    INSERT INTO broadcast_logs (group_id, filter_type, message_preview, total_count, success_count, fail_count, sent_by)
                    VALUES (0, 'auto_backup', 'Backup automatico', 0, 0, 0, %s)
                """, (SUPER_ADMIN_ID,))
        await self._run(_log)
# ==================== INSTANCIAS GLOBALES ====================
db = Database(DATABASE_URL)
scheduler = AsyncIOScheduler()
bot_app = None

# ==================== HELPERS INTERNOS ====================
async def _fire_and_forget(coro, name="op"):
    async def _wrapper():
        try:
            await coro
        except Exception as e:
            logger.warning(f"Fire-and-forget '{name}' falló: {e}")
    asyncio.create_task(_wrapper())

async def db_health_command(update, context):
    retry_rate = db._retry_count / max(1, db._op_count)
    await update.message.reply_text(
        f"📊 *DB Health*\n"
        f"• Operaciones: {db._op_count}\n"
        f"• Reintentos: {db._retry_count} ({retry_rate:.1%})\n"
        f"• Último error: `{db._last_error or 'ninguno'}`\n"
        f"• Circuit: {'🔴 OPEN' if db._circuit._opened_at else '🟢 CLOSED'}"
    )
async def _safe_send(chat_id: int, text: str, disable_notification: bool = False, **kwargs):
    try:
        return await bot_app.bot.send_message(chat_id, text, disable_notification=disable_notification, **kwargs)
    except Exception as e:
        logger.warning(f"No se pudo enviar mensaje a {chat_id}: {e}")
        return None

async def config_deuna_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2: 
        await update.message.reply_text("❌ Uso: `/configdeuna group_id link`", parse_mode="Markdown"); 
        return
    try:
        group_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ ID inválido")
        return
    if not can_manage_group(update.effective_user.id, group_id):
        await update.message.reply_text("❌ No autorizado")
        return
    
    link = context.args[1].strip()
    if not link.startswith("https://"): await update.message.reply_text("❌ El link debe empezar con https://"); return
    if not await update_group_settings(group_id, {'deuna_link': link}):
        await update.message.reply_text("❌ Grupo no encontrado")
        return
    await update.message.reply_text("✅ Link Deuna guardado.")

async def config_cart_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2:
        await update.message.reply_text("❌ Uso: `/configcart group_id minutos`", parse_mode="Markdown")
        return
    
    try:
        group_id = int(context.args[0])
        minutes = int(context.args[1])
    except ValueError:
        await update.message.reply_text("❌ IDs o minutos inválidos")
        return
    
    if not can_manage_group(update.effective_user.id, group_id):
        await update.message.reply_text("❌ No autorizado")
        return
    
    if minutes <= 0:
        await update.message.reply_text("❌ Los minutos deben ser positivos")
        return
    
    if not await update_group_settings(group_id, {'cart_delay_minutes': minutes}):
        await update.message.reply_text("❌ Grupo no encontrado")
        return
    await update.message.reply_text(f"✅ Carrito abandonado configurado a {minutes} minutos.")

async def _kick_user_with_retry(group_id: int, user_id: int, retries: int = 3) -> bool:
    for attempt in range(retries):
        try:
            try:
                member = await bot_app.bot.get_chat_member(group_id, user_id)
                if member.status in ("left", "kicked", "banned"):
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
            if any(phrase in error_str for phrase in ["not enough rights", "bot is not a member", "chat not found",
                                                        "user not found", "participant_id_invalid", "member_id_invalid"]):
                logger.error(f"❌ Error no recuperable expulsando {user_id}: {e}")
                return False
            wait = 2 ** attempt
            logger.warning(f"⚠️ Intento {attempt+1}/{retries} fallido para expulsar {user_id}: {e}. Reintentando en {wait}s...")
            await asyncio.sleep(wait)
    logger.error(f"❌ No se pudo expulsar al usuario {user_id} tras {retries} intentos")
    return False

async def _mute_user_with_retry(group_id: int, user_id: int, retries: int = 3) -> bool:
    for attempt in range(retries):
        try:
            await bot_app.bot.restrict_chat_member(group_id, user_id, permissions=MUTE_PERMISSIONS)
            await db.mark_muted(user_id, group_id)
            logger.info(f"🔇 Usuario {user_id} silenciado en {group_id}")
            return True
        except TelegramError as e:
            error_str = str(e).lower()
            if any(p in error_str for p in ["not enough rights", "bot is not a member", "chat not found", "user not found", "user_id_invalid"]):
                logger.error(f"❌ Error no recuperable silenciando {user_id}: {e}")
                return False
            wait = 2 ** attempt
            logger.warning(f"⚠️ Intento {attempt+1}/{retries} fallido silenciando {user_id}: {e}. Reintentando en {wait}s")
            await asyncio.sleep(wait)
    logger.error(f"❌ No se pudo silenciar a {user_id} tras {retries} intentos")
    return False

async def _unmute_user_and_unban(group_id: int, user_id: int):
    try:
        await bot_app.bot.restrict_chat_member(group_id, user_id, permissions=UNMUTE_PERMISSIONS)
    except Exception as e:
        logger.warning(f"No se pudo desmutear a {user_id} en {group_id}: {e}")
    try:
        await bot_app.bot.unban_chat_member(group_id, user_id, only_if_banned=True)
    except Exception:
        pass
    _fire_and_forget(db.clear_muted(user_id, group_id), "clear_muted")

async def verify_on_startup():
    """Verificación post-deploy: detecta usuarios que entraron durante la caída del bot."""
    logger.info("🚀 Iniciando verificación post-deploy...")
    
    # 1. Ejecutar verificación de expirados inmediatamente
    try:
        await check_expired_subscriptions()
        logger.info("✅ Verificación de expirados completada")
    except Exception as e:
        logger.error(f"❌ Error en verificación de expirados: {e}")
    
    # 2. Notificar al Super Admin
    try:
        groups_count = len(get_all_groups_snapshot())
        await _safe_send(
            SUPER_ADMIN_ID,
            f"🚀 *Bot reiniciado — Verificación completada*\n\n"
            f"📊 Grupos activos: {groups_count}\n"
            f"✅ Expulsados expirados: verificado\n"
            f"✅ Eventos pendientes: procesando...\n\n"
            f"💡 Los usuarios que entraron durante la caída serán detectados al escribir o mediante eventos pendientes.",
            parse_mode="Markdown"
        )
    except Exception:
        pass
    
    logger.info("✅ Verificación post-deploy finalizada")

def _back_button(callback: str) -> list:
    return [InlineKeyboardButton("🔙 Volver", callback_data=callback)]

def _confirm_buttons(yes_data: str, no_data: str) -> list:
    return [
        [InlineKeyboardButton("✅ Sí, confirmar", callback_data=yes_data),
         InlineKeyboardButton("❌ Cancelar", callback_data=no_data)]
    ]

def _clear_input_states(context: ContextTypes.DEFAULT_TYPE):
    """Limpia todos los estados de input activos para evitar conflictos entre flujos."""
    for k in (
        'config_msg_step', 'config_msg_group_id', 'config_msg_type',
        'broadcast_step', 'broadcast_group_id', 'broadcast_filter', 'broadcast_message',
        'warning_step', 'warning_group_id', 'warning_index', 'warning_minutes',
        'config_payment_step', 'config_payment_group_id',
        'cfg_field', 'cfg_group_id',
        'editing_field', 'editing_group_id', 'pending_changes', 'editing_mode',
    ):
        context.user_data.pop(k, None)
# ==================== FLUJO DE CANALES ====================
def get_channels() -> List[Dict[str, Any]]:
    channels = []
    for vip in get_all_groups_snapshot():
        if vip.get("type", "VIP") != "VIP":
            continue
            
        # Buscar grupo Free
        free_group = None
        free_id = vip.get("settings", {}).get("linked_free_group_id")
        if free_id:
            free_group = get_group_by_id(free_id)
            
        if not free_group:
            for g in get_all_groups_snapshot():
                if g.get("type") == "FREE" and g["admin_id"] == vip["admin_id"]:
                    free_group = g
                    break
            
        # Buscar grupo Comunidad vinculado
        comunidad_group = None
        com_id = vip.get("settings", {}).get("linked_comunidad_group_id")
        if com_id:
            comunidad_group = get_group_by_id(com_id)
            
        channel_name = vip.get("settings", {}).get("channel_name", vip["group_name"])
        
        channels.append({
            "channel_name": channel_name,
            "vip": vip,
            "free": free_group,
            "comunidad": comunidad_group  # <- AQUI se agrega la key faltante
        })
    return channels

async def _show_vip_info(send_func, user_id: int, group: dict):
    """Muestra información del VIP cuando un usuario llega desde un enlace o canal."""
    group_id = group['group_id']
    spin_data = await db.get_spin_data(user_id, group_id)
    spin_used = spin_data.get("used", False) if spin_data.get("spin_count", 0) > 0 else False
    price_lines = _build_price_list(group_id)
    custom = await db.get_group_messages(group_id)
    vip_invite_link = group.get("settings", {}).get("vip_invite_link", "").strip()
    
    default_msg = (
        "🔥 *¡Bienvenido al VIP!*\n\n"
        "Este es el canal exclusivo de *{group_name}*.\n\n"
        "📋 *Planes disponibles:*\n{price_list}\n\n"
    )
    
    # Usar el mensaje personalizado si existe, si no, el por defecto
    msg = default_msg
    if custom and custom.get('welcome_message'):
        msg = custom['welcome_message']
        
    # Formatear variables de forma segura
    cfg_t = get_group_plan_config(group_id, "trial")
    trial_str = fmt_minutes(cfg_t.get('minutes', 1440))
    
    msg = format_message(msg, {
        'group_name': safe_name(group.get('group_name', 'VIP')),
        'price_list': price_lines,
        'trial_duration': trial_str,
        'expiry_time': 'Al unirte al grupo',
        'user_name': 'Usuario'
    })
    
    keyboard = []
    if vip_invite_link and vip_invite_link.startswith(("https://", "http://")):
        keyboard.append([InlineKeyboardButton("🔥 Únete al VIP", url=vip_invite_link)])
    if not spin_used:
        keyboard.append([InlineKeyboardButton("🎰 GIRAR RULETA VIP", callback_data=f"spin_{group_id}_{user_id}")])
    keyboard.append([InlineKeyboardButton("💳 Información de Pago", callback_data=f"pay_{group_id}_{user_id}")])
    
    await send_func(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    
async def show_channel_selector(send_func, user_id: int):
    channels = get_channels()
    if not channels:
        await send_func("⚠️ No hay canales disponibles en este momento.")
        return
    global_msg = await db.get_global_message('channel_select_message') or DEFAULT_CHANNEL_SELECT_MESSAGE
    if len(channels) == 1:
        await show_channel_menu(send_func, user_id, channels[0])
        return
    keyboard = []
    for ch in channels:
        keyboard.append([InlineKeyboardButton(ch['channel_name'], callback_data=f"channel_SELECT_{ch['vip']['group_id']}")])
    await send_func(global_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def show_channel_menu(send_func, user_id: int, channel: dict):
    vip = channel['vip']
    free = channel.get('free')
    comunidad = channel.get('comunidad')
    group_id = vip['group_id']
    custom = await db.get_group_messages(group_id)
    
    desc = DEFAULT_CHANNEL_DESCRIPTION
    if custom and custom.get('channel_description'):
        desc = custom['channel_description']
        
    desc = format_message(desc, {
        'channel_name': channel['channel_name'],
        'vip_group_name': vip['group_name'],
        'free_group_name': free['group_name'] if free else "N/A",
        'comunidad_group_name': comunidad['group_name'] if comunidad else "N/A"
    })

    free_link = free.get("settings", {}).get("invite_link", "") if free else ""
    com_link = comunidad.get("settings", {}).get("invite_link", "") if comunidad else ""
    
    keyboard = []
    
    if free_link:
        keyboard.append([InlineKeyboardButton("📢 Grupo Free", url=free_link)])
    elif free:
        keyboard.append([InlineKeyboardButton("📢 Grupo Free (sin link)", callback_data="no_free_link")])
    
    if com_link:
        keyboard.append([InlineKeyboardButton("👥 Grupo Comunidad", url=com_link)])
    elif comunidad:
        keyboard.append([InlineKeyboardButton("👥 Grupo Comunidad (sin link)", callback_data="no_free_link")])
    
    keyboard.append([InlineKeyboardButton("🔥 Grupo VIP", callback_data=f"channel_vip_{group_id}")])
    await send_func(desc, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

# ==================== FUNCIONES DEL PANEL DE ADMINISTRACIÓN ====================
async def config_messages_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query; await query.answer()
    if not can_manage_group(query.from_user.id, group_id): return
    group = get_group_by_id(group_id)
    if not group: return
    cart_mins = group.get("settings", {}).get("cart_delay_minutes", 30)
    keyboard = [
        [InlineKeyboardButton("🎉 Bienvenida", callback_data=f"cfg_msg_edit_{group_id}_welcome")],
        [InlineKeyboardButton("🚫 Rechazo", callback_data=f"cfg_msg_edit_{group_id}_rejection")],
        [InlineKeyboardButton("📌 Descripción Canal", callback_data=f"cfg_msg_edit_{group_id}_channel_description")],
        [InlineKeyboardButton("🔥 Menú VIP", callback_data=f"cfg_msg_edit_{group_id}_vip_menu")],
        [InlineKeyboardButton("🤖 Aviso Fuego", callback_data=f"cfg_msg_edit_{group_id}_fuego_notice")],
        [InlineKeyboardButton("⏰ Expiración", callback_data=f"cfg_msg_edit_{group_id}_expired")],
        [InlineKeyboardButton("🛒 Carrito Abandonado (Paso 1)", callback_data=f"cfg_msg_edit_{group_id}_abandoned_1")],
        [InlineKeyboardButton("🛒 Carrito Abandonado (Paso 2)", callback_data=f"cfg_msg_edit_{group_id}_abandoned_2")],
        [InlineKeyboardButton("🛒 Carrito Abandonado (Paso 3)", callback_data=f"cfg_msg_edit_{group_id}_abandoned_3")],
        [InlineKeyboardButton(f"⏱ Tiempo Carrito ({cart_mins} min)", callback_data=f"cfg_cart_{group_id}")],
        _back_button(f"select_group_{group_id}"),
    ]
    await query.edit_message_text(f"📝 *Configurar Mensajes — {group['group_name']}*", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def show_vip_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, vip_group_id: int):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    vip = get_group_by_id(vip_group_id)
    if not vip:
        await query.edit_message_text("❌ Canal no encontrado")
        return
    custom = await db.get_group_messages(vip_group_id)
    msg = DEFAULT_VIP_MENU_MESSAGE
    if custom and custom.get('vip_menu_message'):
        msg = custom['vip_menu_message']
    msg = format_message(msg, {
        'channel_name': vip.get("settings", {}).get("channel_name", vip['group_name']),
        'group_name': vip['group_name'],
        'user_name': query.from_user.first_name or 'Usuario'
    })
    spin_data = await db.get_spin_data(user_id, vip_group_id)
    spin_used = spin_data.get("used", False) if spin_data.get("spin_count", 0) > 0 else False
    keyboard = [
        [InlineKeyboardButton("🔥 Entrar al VIP", callback_data=f"vip_enter_{vip_group_id}")],
        [InlineKeyboardButton("🎰 Ruleta de Descuento", callback_data=f"vip_spin_{vip_group_id}")],
        [InlineKeyboardButton("💳 Información de Pago", callback_data=f"vip_pay_{vip_group_id}")]
    ]
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def vip_enter_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, vip_group_id: int):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    vip = get_group_by_id(vip_group_id)
    if not vip:
        await query.answer("❌ Grupo no encontrado", show_alert=True)
        return
    vip_link = vip.get("settings", {}).get("vip_invite_link", "").strip()
    if not vip_link:
        await query.answer("❌ El admin no ha configurado el enlace de invitación VIP", show_alert=True)
        await _safe_send(vip["admin_id"], f"⚠️ Un usuario intentó entrar al VIP pero no hay link configurado. Configúralo con /configpago {vip_group_id}")
        return
    await query.edit_message_text(
        f"🔥 *Enlace de invitación al VIP*\n\n"
        f"Únete ahora: [Click aquí]({vip_link})\n\n"
        f"⚠️ *Recuerda:* Al entrar al grupo VIP, el bot te otorgará tu prueba de 1 hora automáticamente.",
        parse_mode="Markdown",
        disable_web_page_preview=True
    )

async def vip_pay_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, vip_group_id: int):
    query = update.callback_query
    await query.answer("💳 Enviando datos de pago...")
    user_id = query.from_user.id
    await send_payment_info(context.bot, user_id, vip_group_id, triggered_by="botón Información de Pago")

async def vip_spin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, vip_group_id: int):
    # Wrapper para spin_discount con callback diferente
    query = update.callback_query
    user_id = query.from_user.id
    group = get_group_by_id(vip_group_id)
    if not group or group.get("type", "VIP") != "VIP":
        await query.answer("❌ Grupo no válido", show_alert=True)
        return
    # Llamar a spin_discount con datos sintéticos
    query.data = f"spin_{vip_group_id}_{user_id}"
    await spin_discount(update, context)

# ==================== HANDLERS ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if update.callback_query:
        async def send(text: str, **kwargs):
            try:
                return await update.callback_query.message.reply_text(text, **kwargs)
            except BadRequest:
                return await context.bot.send_message(update.effective_user.id, text, **kwargs)
    elif update.message:
        send = update.message.reply_text
    else:
        send = None
    if not send:
        return

    # Procesar parámetros de inicio (FREE o VIP)
    if update.message and context.args:
        start_param = context.args[0]
        if start_param.startswith("FREE_"):
                vip_group = find_linked_or_admin_group(free_group, "VIP")
                if vip_group:
                    await _show_vip_info(send, user_id, vip_group)
                    return
                else:
                    await send("👋 *¡Bienvenido!*\n\nHas llegado desde *{free_group['group_name']}*.\n\n⚠️ No hay un grupo VIP configurado. Contacta al administrador.".format(free_group=free_group), parse_mode="Markdown")
                    return
        elif start_param.isdigit() or start_param.startswith("-100"):
            group_id = int(start_param)
            group = get_group_by_id(group_id)
            if group and group.get("type", "VIP") == "VIP":
                await _show_vip_info(send, user_id, group)
                return

    if user_id == SUPER_ADMIN_ID or user_id in EXTRA_ADMINS:
        groups_snapshot = get_all_groups_snapshot()
        vip_count = sum(1 for g in groups_snapshot if g.get("type", "VIP") == "VIP")
        free_count = sum(1 for g in groups_snapshot if g.get("type") == "FREE")
        total_earnings = await db.get_total_monthly_earnings()
        total_users = await db.get_total_users_count()
        keyboard = [
            [InlineKeyboardButton("📋 Grupos", callback_data="menu_groups")],
            [InlineKeyboardButton("💰 Ganancias", callback_data="total_earnings")],
            [InlineKeyboardButton("📟 Comandos", callback_data="menu_commands")],
        ]
        await send(
            f"👑 *Panel Super Administrador*\n\n"
            f"📊 *Resumen rápido:*\n• 👑 Grupos VIP: {vip_count}\n• 📋 Grupos FREE: {free_count}\n"
            f"• 👥 Total usuarios: {total_users}\n• 💰 Ganancias del mes: ${total_earnings}\n\nSelecciona una opción:",
            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
        )
        return

    user_groups = get_groups_by_admin(user_id)
    if not user_groups:
        # FLUJO DE CANALES PARA USUARIOS REGULARES
        await show_channel_selector(send, user_id)
        return

    if len(user_groups) == 1:
        group = user_groups[0]
        context.user_data['current_group'] = group['group_id']
        
        if group.get("type") == "COMUNIDAD":
            cfg = get_group_plan_config(group['group_id'], "trial")
            cfg_s = get_group_plan_config(group['group_id'], "semanal")
            cfg_m = get_group_plan_config(group['group_id'], "mensual")
            keyboard = [
                [InlineKeyboardButton("➕ Agregar usuario", callback_data="add_user")],
                [InlineKeyboardButton("📊 Usuarios activos", callback_data="list_active")],
                [InlineKeyboardButton("💰 Ganancias", callback_data="earnings")],
                [InlineKeyboardButton("📥 Exportar mes", callback_data="export_month")],
                [InlineKeyboardButton("📊 Estadísticas", callback_data="trial_stats")],
                [InlineKeyboardButton("⚙️ Precios y Trial", callback_data=f"cfg_group_{group['group_id']}")],
                [InlineKeyboardButton("📝 Configurar Mensajes", callback_data=f"cfg_messages_{group['group_id']}")],
                [InlineKeyboardButton("📢 Broadcast Trial", callback_data="broadcast_menu")],
            ]
            await send(
                f"👥 *Panel Comunidad - {group['group_name']}*\n\n🆔 ID: `{group['group_id']}`\n👑 Admin: `{group['admin_id']}`\n\n"
                f"💡 *Tarifas actuales:*\n• ⏱ Trial: {fmt_minutes(cfg.get('minutes', 1440))}\n"
                f"• 📅 Semanal: ${cfg_s['price']}\n• 📆 Mensual: ${cfg_m['price']}\n\n"
                f"🤝 Para activar combos con el VIP vinculado usa `/addcombo` desde ese grupo VIP.",
                reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
            )
        elif group.get("type") == "VIP":
            cfg = get_group_plan_config(group['group_id'], "trial")
            cfg_s = get_group_plan_config(group['group_id'], "semanal")
            cfg_m = get_group_plan_config(group['group_id'], "mensual")
            keyboard = [
                [InlineKeyboardButton("➕ Agregar usuario", callback_data="add_user")],
                [InlineKeyboardButton("📊 Usuarios activos", callback_data="list_active")],
                [InlineKeyboardButton("💰 Ganancias", callback_data="earnings")],
                [InlineKeyboardButton("📥 Exportar mes", callback_data="export_month")],
                [InlineKeyboardButton("📊 Estadísticas", callback_data="trial_stats")],
                [InlineKeyboardButton("⚙️ Precios y Trial", callback_data=f"cfg_group_{group['group_id']}")],
                [InlineKeyboardButton("📝 Configurar Mensajes", callback_data=f"cfg_messages_{group['group_id']}")],
                [InlineKeyboardButton("📢 Broadcast", callback_data="broadcast_menu")],
            ]
            await send(
                f"👑 *Panel VIP - {group['group_name']}*\n\n🆔 ID: `{group['group_id']}`\n👑 Admin: `{group['admin_id']}`\n\n"
                f"💡 *Tarifas actuales:*\n• ⏱ Trial: {fmt_minutes(cfg.get('minutes', 1440))}\n"
                f"• 📅 Semanal: ${cfg_s['price']}\n• 📆 Mensual: ${cfg_m['price']}\n\n"
                f"🤝 Usa `/addcombo` para activar VIP + Comunidad al vez.",
                reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
            )
        else: # FREE
            keyboard = [
                [InlineKeyboardButton("📋 Clientes potenciales", callback_data="list_potential")],
                [InlineKeyboardButton("📥 Exportar clientes", callback_data="export_clients")]
            ]
            await send(
                f"📋 *Panel FREE - {group['group_name']}*\n\n🆔 ID: `{group['group_id']}`\n👑 Admin: `{group['admin_id']}`",
                reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
            )
    else:
        keyboard = []
        for group in user_groups:
            emoji = "👑" if group.get("type", "VIP") == "VIP" else "📋"
            keyboard.append([InlineKeyboardButton(f"{emoji} {group['group_name']}", callback_data=f"select_group_{group['group_id']}")])
        await send("📋 *Selecciona el grupo que quieres gestionar*", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("✅ El bot funciona!")

async def menu_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    keyboard = [
        [InlineKeyboardButton("➕ Agregar grupo", callback_data="add_group")],
        [InlineKeyboardButton("👁️ Ver grupos", callback_data="menu_view_groups")],
        [InlineKeyboardButton("✏️ Editar grupo", callback_data="menu_edit_group_select")],
        [InlineKeyboardButton("❌ Eliminar grupo", callback_data="menu_delete_group_select")],
        _back_button("back_to_admin"),
    ]
    await query.edit_message_text("📋 *Gestión de Grupos*\n\nSelecciona una opción:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def menu_view_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    keyboard = [
        [InlineKeyboardButton("👑 Grupos VIP", callback_data="view_vip_groups")],
        [InlineKeyboardButton("👥 Grupos Comunidad", callback_data="view_comunidad_groups")],
        [InlineKeyboardButton("📋 Grupos FREE", callback_data="view_free_groups")],
        _back_button("menu_groups"),
    ]
    await query.edit_message_text("👁️ *Ver Grupos*\n\nSelecciona el tipo de grupo:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def select_group(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query
    await query.answer()
    group = get_group_by_id(group_id)
    if not group:
        await query.edit_message_text("❌ Grupo no encontrado")
        return
    context.user_data['current_group'] = group_id
    if group.get("type") == "COMUNIDAD":
        cfg = get_group_plan_config(group_id, "trial")
        cfg_s = get_group_plan_config(group_id, "semanal")
        cfg_m = get_group_plan_config(group_id, "mensual")
        keyboard = [
            [InlineKeyboardButton("➕ Agregar usuario", callback_data="add_user")],
            [InlineKeyboardButton("📊 Usuarios activos", callback_data="list_active")],
            [InlineKeyboardButton("💰 Ganancias", callback_data="earnings")],
            [InlineKeyboardButton("📥 Exportar mes", callback_data="export_month")],
            [InlineKeyboardButton("📊 Estadísticas Trial", callback_data="trial_stats")],
            [InlineKeyboardButton("⚙️ Precios y Trial", callback_data=f"cfg_group_{group_id}")],
            [InlineKeyboardButton("📝 Configurar Mensajes", callback_data=f"cfg_messages_{group_id}")],
            [InlineKeyboardButton("📢 Broadcast Trial", callback_data="broadcast_menu")],
        ]
        await query.edit_message_text(
            f"👥 *Panel Comunidad - {group['group_name']}*\n\n🆔 ID: `{group['group_id']}`\n👑 Admin: `{group['admin_id']}`\n\n"
            f"💡 *Tarifas actuales:*\n• ⏱ Trial: {fmt_minutes(cfg.get('minutes', 1440))}\n"
            f"• 📅 Semanal: ${cfg_s['price']}\n• 📆 Mensual: ${cfg_m['price']}\n\n"
            f"🤝 Para activar combos con el VIP vinculado usa `/addcombo` desde ese grupo VIP.",
            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
        )

    elif group.get("type") == "VIP":
        cfg = get_group_plan_config(group_id, "trial")
        cfg_s = get_group_plan_config(group_id, "semanal")
        cfg_m = get_group_plan_config(group_id, "mensual")
        keyboard = [
            [InlineKeyboardButton("➕ Agregar usuario", callback_data="add_user")],
            [InlineKeyboardButton("📊 Usuarios activos", callback_data="list_active")],
            [InlineKeyboardButton("💰 Ganancias", callback_data="earnings")],
            [InlineKeyboardButton("📥 Exportar mes", callback_data="export_month")],
            [InlineKeyboardButton("📊 Estadísticas Trial", callback_data="trial_stats")],
            [InlineKeyboardButton("⚙️ Precios y Trial", callback_data=f"cfg_group_{group_id}")],
            [InlineKeyboardButton("📝 Configurar Mensajes", callback_data=f"cfg_messages_{group_id}")],
            [InlineKeyboardButton("📢 Broadcast", callback_data="broadcast_menu")],
        ]
        await query.edit_message_text(
            f"👑 *Panel VIP - {group['group_name']}*\n\n🆔 ID: `{group['group_id']}`\n👑 Admin: `{group['admin_id']}`\n\n"
            f"💡 *Tarifas actuales:*\n• ⏱ Trial: {fmt_minutes(cfg.get('minutes', 1440))}\n"
            f"• 📅 Semanal: ${cfg_s['price']}\n• 📆 Mensual: ${cfg_m['price']}\n\n"
            f"🤝 Usa `/addcombo` para activar VIP + Comunidad al vez.",
            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
        )
        
    else:
        keyboard = [
            [InlineKeyboardButton("📋 Clientes potenciales", callback_data="list_potential")],
            [InlineKeyboardButton("📥 Exportar clientes", callback_data="export_clients")]
        ]
        await query.edit_message_text(
            f"📋 *Panel FREE - {group['group_name']}*\n\n🆔 ID: `{group['group_id']}`\n👑 Admin: `{group['admin_id']}`",
            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
        )

async def total_earnings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    message = query.message if query else update.message
    if query:
        await query.answer()
    total = await db.get_total_monthly_earnings()
    now = datetime.now(EC_TZ)
    await message.reply_text(
        f"💰 *GANANCIAS TOTALES DEL MES*\n\n📅 {now.strftime('%B %Y')}\n💵 Total recaudado: *${total}*\n\n📊 Incluye todos los grupos configurados.",
        parse_mode="Markdown"
    )

# ==================== AVISO DE FUEGO (REACCIÓN) ====================
async def handle_reaction_fuego(update: Update, context: ContextTypes.DEFAULT_TYPE):
    reaction = update.message_reaction
    if not reaction or not reaction.chat or not reaction.user:
        return

    group_id = reaction.chat.id
    user_id = reaction.user.id

    emojis_recibidos = [getattr(r, 'emoji', None) for r in (reaction.new_reaction or [])]
    logger.info(f"FUEGO REACCIÓN: user={user_id}, group={group_id}, emojis={emojis_recibidos}")

    group = get_group_by_id(group_id)
    if not group or group.get("type", "VIP") != "VIP":
        return

    target_emojis = ["🔥"]
    new_reactions = reaction.new_reaction or []
    has_target = any(getattr(r, 'emoji', None) in target_emojis for r in new_reactions)
    if not has_target:
        return

    # Verificar acceso básico
    db_user = await db.get_user_by_id(user_id, group_id)
    if not db_user or db_user.get('status') != 'active':
        return

    # Verificar si ya se envió aviso
    if db_user.get('fuego_notice_sent'):
        return

    # Enviar aviso configurable
    custom = await db.get_group_messages(group_id)
    fuego_msg = DEFAULT_FUEGO_NOTICE
    if custom and custom.get('fuego_notice_message'):
        fuego_msg = custom['fuego_notice_message']
    
    fuego_contact = group.get("settings", {}).get("fuego_contact", "FuegoBot")
    
    formatted = format_message(fuego_msg, {
        'user_name': getattr(reaction.user, 'first_name', None) or 'Usuario',
        'group_name': group['group_name'],
        'fuego_username': fuego_contact
    })
    
    await _safe_send(user_id, formatted, parse_mode="Markdown")
    _fire_and_forget(db.mark_fuego_notice_sent(user_id, group_id), "fuego_notice_sent")
    logger.info(f"✅ Aviso de Fuego enviado a {user_id}")

def _resolve_group_from_type(update: Update, context: ContextTypes.DEFAULT_TYPE, target_type: str) -> Optional[int]:
    """Resuelve el group_id real basado en si el admin pidió 'vip' o 'comunidad'."""
    target_type = target_type.lower()
    if target_type not in ("vip", "comunidad"):
        return None
        
    chat_id = update.effective_chat.id
    
    # Determinar el grupo base (si estamos en un grupo o si el admin lo seleccionó en /start)
    base_group_id = context.user_data.get('current_group')
    if not base_group_id and update.effective_chat.type in ("group", "supergroup"):
        base_group_id = chat_id
            
    if not base_group_id:
        return None
        
    base_group = get_group_by_id(base_group_id)
    if not base_group:
        return None
        
    current_type = base_group.get("type", "VIP")
    
    if target_type == "vip":
        if current_type == "VIP":
            return base_group_id
        elif current_type == "COMUNIDAD":
            return base_group.get("settings", {}).get("linked_vip_group_id")
        elif current_type == "FREE":
            return base_group.get("settings", {}).get("linked_vip_group_id")
                
    elif target_type == "comunidad":
        if current_type == "COMUNIDAD":
            return base_group_id
        elif current_type == "VIP":
            return base_group.get("settings", {}).get("linked_comunidad_group_id")
        elif current_type == "FREE":
            vip_id = base_group.get("settings", {}).get("linked_vip_group_id")
            if vip_id:
                vip_group = get_group_by_id(vip_id)
                if vip_group:
                    return vip_group.get("settings", {}).get("linked_comunidad_group_id")
    return None
    
async def handle_reply_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.reply_to_message: return
    if not update.message.text or not update.message.text.startswith('/add'): return
    if not await _require_bot_reply(update, context):
        return
    
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    
    text = update.message.reply_to_message.text or update.message.reply_to_message.caption or ""
    target_user_id, _, _, _ = extract_user_from_reply(text)
    
    if not target_user_id:
        await update.message.reply_text(
            "❌ No pude encontrar el ID del usuario en el mensaje.\n"
            "Asegúrate de responder al mensaje de registro del trial.\n\n"
            "Uso: `/add plan grupo` (ej: `/add mensual vip`)",
            parse_mode="Markdown"
        )
        return
        
    args = update.message.text.split()
    if len(args) < 3:
        await update.message.reply_text("❌ *Uso (respondiendo):* `/add plan grupo`\n\n*Ejemplos:*\n• `/add mensual vip`\n• `/add semanal comunidad`", parse_mode="Markdown")
        return
        
    plan = args[1].lower()
    target_type = args[2].lower()
    
    if plan not in PLANS:
        await update.message.reply_text("❌ Plan inválido. Usa: trial, semanal, mensual, anual")
        return
        
    if target_type not in ("vip", "comunidad"):
        await update.message.reply_text("❌ Grupo inválido. Debes especificar `vip` o `comunidad`.", parse_mode="Markdown")
        return
        
    current_group = _resolve_group_from_type(update, context, target_type)
    if not current_group or not can_manage_group(user_id, current_group):
        await update.message.reply_text("❌ No autorizado o no se pudo resolver el grupo. Asegúrate de tener el grupo base seleccionado con /start.")
        return
        
    custom_price = None
    custom_days = None
    if len(args) >= 4:
        try:
            custom_price = float(args[3].replace(",", "."))
            if custom_price < 0: raise ValueError
        except ValueError:
            await update.message.reply_text("❌ Precio inválido", parse_mode="Markdown"); return
            
    if len(args) >= 5:
        try:
            custom_days = int(args[4])
            if custom_days <= 0: raise ValueError
        except ValueError:
            await update.message.reply_text("❌ Días inválidos", parse_mode="Markdown"); return

    await _execute_subscription_flow(
        context.bot, update.message.reply_text, current_group, target_user_id, plan, custom_price, custom_days, is_renewal=False
    )

async def add_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message and update.message.reply_to_message:
        await handle_reply_add(update, context)
        return
        
    user_id = update.effective_user.id
        
    if len(context.args) < 3:
        await update.message.reply_text(
            "❌ *Uso:* `/add @username|ID plan grupo [precio] [días]`\n\n"
            "*Ejemplos:*\n• `/add @juan mensual vip`\n• `/add 123456 semanal comunidad`\n"
            "• `/add @juan mensual vip 5.99`\n\n"
            "💡 *Grupos permitidos:* `vip` o `comunidad`",
            parse_mode="Markdown"
        )
        return
        
    identifier = context.args[0].replace("@", "")
    plan = context.args[1].lower()
    target_type = context.args[2].lower()
    
    if plan not in PLANS:
        await update.message.reply_text("❌ Plan inválido. Usa: trial, semanal, mensual, anual")
        return
        
    if target_type not in ("vip", "comunidad"):
        await update.message.reply_text("❌ Grupo inválido. Debes especificar `vip` o `comunidad`.", parse_mode="Markdown")
        return
        
    current_group = _resolve_group_from_type(update, context, target_type)
    if not current_group or not can_manage_group(user_id, current_group):
        await update.message.reply_text("❌ No autorizado o no se pudo resolver el grupo. Asegúrate de tener el grupo base seleccionado con /start.")
        return
        
    custom_price = None
    custom_days = None
    if len(context.args) >= 4:
        try:
            custom_price = float(context.args[3].replace(",", "."))
            if custom_price < 0: raise ValueError
        except ValueError:
            await update.message.reply_text("❌ Precio inválido", parse_mode="Markdown"); return
            
    if len(context.args) >= 5:
        try:
            custom_days = int(context.args[4])
            if custom_days <= 0: raise ValueError
        except ValueError:
            await update.message.reply_text("❌ Días inválidos", parse_mode="Markdown"); return

    target_user_id = None
    
    if identifier.isdigit():
        target_user_id = int(identifier)
    else:
        try:
            member = await context.bot.get_chat_member(current_group, identifier)
            if member and member.user: target_user_id = member.user.id
        except Exception: pass
        
        if not target_user_id:
            user_record = await db.get_user_by_username(identifier, current_group)
            if user_record: target_user_id = user_record['user_id']
            
        if not target_user_id:
            await update.message.reply_text(f"❌ No se encontró a @{identifier}. Asegúrate de que esté en el grupo o registrado.")
            return

    await _execute_subscription_flow(
        context.bot, update.message.reply_text, current_group, target_user_id, plan, custom_price, custom_days, is_renewal=False
    )

async def handle_reply_addcombo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.reply_to_message: return
    if not update.message.text or not update.message.text.startswith('/addcombo'): return
    if not await _require_bot_reply(update, context):
        return
        
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    text = update.message.reply_to_message.text or update.message.reply_to_message.caption or ""
    target_user_id, target_group_id, _, _ = extract_user_from_reply(text)
    if not target_user_id:
        await update.message.reply_text("❌ No pude encontrar el ID del usuario en el mensaje.", parse_mode="Markdown")
        return
    current_group = target_group_id or context.user_data.get('current_group') or (chat_id if update.effective_chat.type in ("group", "supergroup") else None)
    if not current_group or not can_manage_group(user_id, current_group):
        await update.message.reply_text("❌ No autorizado para gestionar este grupo")
        return
    args = update.message.text.split()
    if len(args) < 2:
        await update.message.reply_text("❌ *Uso:* `/addcombo plan [precio] [días]`", parse_mode="Markdown")
        return
    plan = args[1].lower()
    if plan not in ("semanal", "mensual", "anual"):
        await update.message.reply_text("❌ Plan inválido. Usa: semanal, mensual, anual"); return
    custom_price = None; custom_days = None
    if len(args) >= 3:
        try:
            custom_price = float(args[2].replace(",", "."))
            if custom_price < 0: raise ValueError
        except ValueError:
            await update.message.reply_text("❌ Precio inválido"); return
    if len(args) >= 4:
        try:
            custom_days = int(args[3])
            if custom_days <= 0: raise ValueError
        except ValueError:
            await update.message.reply_text("❌ Días inválidos"); return
    await _execute_combo_subscription_flow(context.bot, update.message.reply_text, current_group, target_user_id, plan, custom_price, custom_days, is_renewal=False)

async def addcombo_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message and update.message.reply_to_message:
        await handle_reply_addcombo(update, context)
        return
    current_group = await require_group(update, context)
    if not current_group:
        await update.message.reply_text("❌ Usa /start primero o no estás autorizado")
        return
    if len(context.args) < 2:
        await update.message.reply_text("❌ *Uso:* `/addcombo @username|ID plan [precio] [días]`", parse_mode="Markdown")
        return
    identifier = context.args[0].replace("@", "")
    plan = context.args[1].lower()
    if plan not in ("semanal", "mensual", "anual"):
        await update.message.reply_text("❌ Plan inválido. Usa: semanal, mensual, anual"); return
    custom_price = None; custom_days = None
    if len(context.args) >= 3:
        try:
            custom_price = float(context.args[2].replace(",", "."))
            if custom_price < 0: raise ValueError
        except ValueError:
            await update.message.reply_text("❌ Precio inválido"); return
    if len(context.args) >= 4:
        try:
            custom_days = int(context.args[3])
            if custom_days <= 0: raise ValueError
        except ValueError:
            await update.message.reply_text("❌ Días inválidos"); return
    target_user_id = None
    if identifier.isdigit():
        target_user_id = int(identifier)
    else:
        try:
            member = await context.bot.get_chat_member(current_group, identifier)
            if member and member.user: target_user_id = member.user.id
        except Exception: pass
        if not target_user_id:
            user_record = await db.get_user_by_username(identifier, current_group)
            if user_record: target_user_id = user_record['user_id']
        if not target_user_id:
            await update.message.reply_text(f"❌ No se encontró a @{identifier}."); return
    await _execute_combo_subscription_flow(context.bot, update.message.reply_text, current_group, target_user_id, plan, custom_price, custom_days, is_renewal=False)

async def handle_reply_renewcombo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.reply_to_message: return
    if not update.message.text or not update.message.text.startswith('/renewcombo'): return
    if not await _require_bot_reply(update, context):
        return
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    text = update.message.reply_to_message.text or update.message.reply_to_message.caption or ""
    target_user_id, target_group_id, _, _ = extract_user_from_reply(text)
    if not target_user_id:
        await update.message.reply_text("❌ No pude encontrar el ID del usuario en el mensaje.", parse_mode="Markdown")
        return
    current_group = target_group_id or context.user_data.get('current_group') or (chat_id if update.effective_chat.type in ("group", "supergroup") else None)
    if not current_group or not can_manage_group(user_id, current_group):
        await update.message.reply_text("❌ No autorizado para gestionar este grupo")
        return
    args = update.message.text.split()
    if len(args) < 2:
        await update.message.reply_text("❌ *Uso:* `/renewcombo plan [precio] [días]`", parse_mode="Markdown")
        return
    plan = args[1].lower()
    if plan not in ("semanal", "mensual", "anual"):
        await update.message.reply_text("❌ Plan inválido. Usa: semanal, mensual, anual"); return
    custom_price = None; custom_days = None
    if len(args) >= 3:
        try:
            custom_price = float(args[2].replace(",", "."))
            if custom_price < 0: raise ValueError
        except ValueError:
            await update.message.reply_text("❌ Precio inválido"); return
    if len(args) >= 4:
        try:
            custom_days = int(args[3])
            if custom_days <= 0: raise ValueError
        except ValueError:
            await update.message.reply_text("❌ Días inválidos"); return
    await _execute_combo_subscription_flow(context.bot, update.message.reply_text, current_group, target_user_id, plan, custom_price, custom_days, is_renewal=True)

async def renewcombo_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message and update.message.reply_to_message:
        await handle_reply_renewcombo(update, context)
        return
    current_group = await require_group(update, context)
    if not current_group:
        await update.message.reply_text("❌ Usa /start primero o no estás autorizado")
        return
    if len(context.args) < 2:
        await update.message.reply_text("❌ *Uso:* `/renewcombo @username|ID plan [precio] [días]`", parse_mode="Markdown")
        return
    identifier = context.args[0].replace("@", "")
    plan = context.args[1].lower()
    if plan not in ("semanal", "mensual", "anual"):
        await update.message.reply_text("❌ Plan inválido. Usa: semanal, mensual, anual"); return
    custom_price = None; custom_days = None
    if len(context.args) >= 3:
        try:
            custom_price = float(context.args[2].replace(",", "."))
            if custom_price < 0: raise ValueError
        except ValueError:
            await update.message.reply_text("❌ Precio inválido"); return
    if len(context.args) >= 4:
        try:
            custom_days = int(context.args[3])
            if custom_days <= 0: raise ValueError
        except ValueError:
            await update.message.reply_text("❌ Días inválidos"); return
    target_user_id = None
    if identifier.isdigit():
        target_user_id = int(identifier)
    else:
        try:
            member = await context.bot.get_chat_member(current_group, identifier)
            if member and member.user: target_user_id = member.user.id
        except Exception: pass
        if not target_user_id:
            user_record = await db.get_user_by_username(identifier, current_group)
            if user_record: target_user_id = user_record['user_id']
        if not target_user_id:
            await update.message.reply_text(f"❌ No se encontró a @{identifier}."); return
    await _execute_combo_subscription_flow(context.bot, update.message.reply_text, current_group, target_user_id, plan, custom_price, custom_days, is_renewal=True)

# ==================== HANDLER PARA REPLY CON /add ====================
async def _mark_discount_if_any(target_user_id: int, current_group: int, plan: str):
    """Verifica si el usuario tenía un descuento de ruleta sin usar y lo marca como usado."""
    spin_data = await db.get_spin_data(target_user_id, current_group)
    if spin_data and not spin_data.get("used") and spin_data.get("spin_count", 0) > 0:
        await db.mark_discount_used(target_user_id, current_group, plan)

async def _send_subscription_notification(bot, target_user_id: int, current_group: int, plan: str, is_renewal: bool):
    """Envía un mensaje de confirmación al usuario dependiendo de si es activación o renovación."""
    group = get_group_by_id(current_group)
    group_name = safe_name(group['group_name']) if group else 'VIP'
    group_type = group.get('type', 'VIP') if group else 'VIP'

    try:
        await bot.unban_chat_member(current_group, target_user_id, only_if_banned=True)
        logger.info(f"✅ Usuario {target_user_id} desbaneado de {current_group} (si estaba baneado)")
    except Exception as e:
        logger.warning(f"No se pudo desbanear a {target_user_id} en {current_group}: {e}")

    try:
        await bot.restrict_chat_member(current_group, target_user_id, permissions=UNMUTE_PERMISSIONS)
        await db.clear_muted(target_user_id, current_group)
    except Exception as e:
        logger.warning(f"No se pudo desmutear a {target_user_id} en {current_group}: {e}")

    if is_renewal:
        msg = f"🔄 *¡Tu suscripción ha sido renovada!*\n\nTu plan *{plan}* en *{group_name}* ha sido extendido. ¡Gracias! 🎉"
    else:
        msg = f"🎉 *¡Suscripción activada!*\n\nTu plan *{plan}* en *{group_name}* ({group_type}) ha sido activado. ¡Gracias!"
    
    if group and group.get("type", "VIP") == "VIP":
        vip_link = group.get("settings", {}).get("vip_invite_link", "").strip()
        if vip_link and vip_link.startswith(("https://", "http://")):
            msg += f"\n\n🔗 *Enlace para entrar al VIP:*\n{vip_link}\n\n⚠️ Si no puedes entrar, espera 30 segundos e intenta de nuevo."
    try:
        await bot.send_message(target_user_id, msg, parse_mode="Markdown", disable_web_page_preview=True)
    except Exception as e:
        logger.warning(f"No se pudo notificar al usuario {target_user_id}: {e}")

# ==================== COMANDO /add ====================
async def info_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    is_admin = user_id == SUPER_ADMIN_ID or user_id in EXTRA_ADMINS or get_groups_by_admin(user_id)
    if not is_admin: return
    
    target_id = None
    
    # 1. Si se responde a un mensaje DEL BOT, extraer ID de ahí
    if update.message.reply_to_message and _is_bot_message(update.message.reply_to_message, context.bot.id):
        text = update.message.reply_to_message.text or ""
        target_id, _, _, _ = extract_user_from_reply(text)
    
    # 2. Si no se encontró por respuesta (o no era del bot), buscar en los argumentos
    if not target_id and context.args:
        if context.args[0].isdigit(): 
            target_id = int(context.args[0])
        
    # 2. Si no se encontró por respuesta, buscar en los argumentos
    if not target_id and context.args:
        if context.args[0].isdigit(): 
            target_id = int(context.args[0])
            
    if not target_id:
        await update.message.reply_text("❌ Responde a un mensaje de alerta del bot o usa `/info ID`.", parse_mode="Markdown"); return
        
    # 3. Determinar en qué grupo buscar primero (el actual seleccionado o el del chat)
    group_id_to_query = context.user_data.get('current_group')
    if not group_id_to_query and update.effective_chat.type in ("group", "supergroup"):
        group_id_to_query = update.effective_chat.id
        
    # 4. Intentar buscar en el grupo específico
    if group_id_to_query:
        user = await db.get_user_by_id_full(target_id, group_id_to_query)
        group = get_group_by_id(group_id_to_query)
        group_name = safe_name(group.get('group_name', 'VIP')) if group else 'VIP'
        g_type = 'vip' if (group and group.get('type') == 'VIP') else 'comunidad'
        
        # Si NO está en la tabla users, pero queremos saber si tiene ruleta
        if not user:
            spin_data = await db.get_spin_data(target_id, group_id_to_query)
            if spin_data.get("spin_count", 0) > 0:
                best_discount = spin_data.get("best_discount", 0)
                spin_used = spin_data.get("used", False)
                
                msg = f"👤 *EXPEDIENTE DE CLIENTE POTENCIAL*\n\n"
                msg += f"📌 *Grupo:* {group_name}\n"
                msg += f"🆔 *ID:* `{target_id}`\n"
                msg += f"📋 *Estado:* NO REGISTRADO (Aún no ingresa al grupo)\n"
                msg += f"🎰 *Ruleta:* {'✅ Usado' if spin_used else '⏳ Pendiente'}\n"
                
                if best_discount > 0:
                    msg += f"🏆 *Descuento:* {best_discount}% OFF\n\n*💰 Precios con descuento:*\n"
                    msg += _build_price_list(group_id_to_query, discounted_pct=best_discount) + "\n\n💡 *Para activar pago:*\n"
                    for p in ["semanal", "mensual", "anual"]:
                        cfg = get_group_plan_config(group_id_to_query, p); price = cfg['price'] * (1 - best_discount/100)
                        msg += f"`/add {target_id} {p} {price:.2f} {g_type}`\n"
                else:
                    msg += "\n*💰 Precios normales:*\n" + _build_price_list(group_id_to_query) + "\n\n💡 *Para activar pago:*\n"
                    for p in ["semanal", "mensual", "anual"]: 
                        msg += f"`/add {target_id} {p} {g_type}`\n"
                        
                await update.message.reply_text(msg, parse_mode="Markdown")
                return
                
        # Si SÍ está en la tabla users
        if user:
            spin_data = await db.get_spin_data(target_id, group_id_to_query)
            best_discount = spin_data.get("best_discount", 0)
            spin_used = spin_data.get("used", False)
            
            msg = f"📊 *EXPEDIENTE DE CLIENTE*\n\n"
            msg += f"📌 *Grupo:* {group_name}\n"
            msg += f"👤 *Nombre:* {user.get('first_name', 'N/A')}\n"
            msg += f"🆔 *ID:* `{user['user_id']}`\n"
            username = user.get('username')
            if username and not username.startswith('user_'): msg += f"🔗 *Username:* @{username}\n"
            msg += f"🌍 *Origen:* {user.get('source', 'Directo')}\n"
            msg += f"📋 *Estado:* {user.get('status', 'N/A').upper()}\n"
            msg += f"📦 *Plan:* {user.get('plan', 'N/A').upper()}\n"
            msg += f"📅 *Expira:* {fmt_dt(user.get('end_date'))}\n"
            msg += f"🎰 *Ruleta:* {'✅ Usado' if spin_used else '⏳ Pendiente'}\n"
            
            if best_discount > 0:
                msg += f"🏆 *Descuento:* {best_discount}% OFF\n\n*💰 Precios con descuento:*\n"
                msg += _build_price_list(group_id_to_query, discounted_pct=best_discount) + "\n\n💡 *Comandos:*\n"
                for p in ["semanal", "mensual", "anual"]:
                    cfg = get_group_plan_config(group_id_to_query, p); price = cfg['price'] * (1 - best_discount/100)
                    msg += f"`/add {target_id} {p} {price:.2f} {g_type}`\n"
            else:
                msg += "\n*💰 Precios normales:*\n" + _build_price_list(group_id_to_query) + "\n\n💡 *Comandos:*\n"
                for p in ["semanal", "mensual", "anual"]: 
                    msg += f"`/add {target_id} {p} {g_type}`\n"
            await update.message.reply_text(msg, parse_mode="Markdown")
            return
            
    # 5. Si no está en el grupo actual ni tiene ruleta ahí, buscar en TODOS los grupos
    user_groups = await db.get_user_groups(target_id)
    spins_anywhere = await db.get_user_spins_anywhere(target_id)
    
    if not user_groups and not spins_anywhere:
        await update.message.reply_text(f"❌ El usuario `{target_id}` no está registrado en ningún grupo ni tiene giros de ruleta.", parse_mode="Markdown")
        return
        
    msg = f"👤 *EXPEDIENTE MULTI-GRUPO*\n\n🆔 *ID:* `{target_id}`\n\n"
    
    if user_groups:
        msg += "📝 *Registros en grupos:*\n"
        for gid in user_groups:
            u = await db.get_user_by_id_full(target_id, gid)
            g_obj = get_group_by_id(gid)
            g_name = safe_name(g_obj.get('group_name', 'VIP')) if g_obj else "Desconocido"
            g_type_str = "VIP" if g_obj.get("type") == "VIP" else "Comunidad" if g_obj.get("type") == "COMUNIDAD" else "FREE"
            
            if u:
                spin_data = await db.get_spin_data(target_id, gid)
                best_discount = spin_data.get("best_discount", 0)
                spin_used = spin_data.get("used", False)
                
                msg += f"📌 *{g_name}* ({g_type_str})\n"
                msg += f"   • Estado: {u.get('status', 'N/A').upper()}\n"
                msg += f"   • Plan: {u.get('plan', 'N/A').upper()}\n"
                msg += f"   • Expira: {fmt_dt(u.get('end_date'))}\n"
                msg += f"   • Ruleta: {'✅ Usado' if spin_used else '⏳ Pendiente'} ({best_discount}% OFF)\n\n"
                
    if not user_groups and spins_anywhere:
        msg += "📝 *Giros de ruleta (No registrado en users):*\n"
        for spin in spins_anywhere:
            gid = spin['group_id']
            g_obj = get_group_by_id(gid)
            g_name = safe_name(g_obj.get('group_name', 'VIP')) if g_obj else "Desconocido"
            g_type_str = "VIP" if g_obj.get("type") == "VIP" else "Comunidad"
            
            best = spin.get("best_discount", 0)
            used = spin.get("used", False)
            
            msg += f"📌 *{g_name}* ({g_type_str})\n"
            msg += f"   • Ruleta: {'✅ Usado' if used else '⏳ Pendiente'}\n"
            msg += f"   • Descuento: {best}% OFF\n\n"
            
    msg += "💡 *Para activar en un grupo específico, usa:*\n"
    msg += f"`/add {target_id} mensual vip` o `comunidad`"
            
    await update.message.reply_text(msg, parse_mode="Markdown")

async def ruleta_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    current_group = get_group_by_id(chat_id)
    target_vip_group_id = None

    # 1. Si se usa dentro del grupo VIP
    if current_group and current_group.get("type") == "VIP":
        target_vip_group_id = chat_id
        
    # 2. Si se usa dentro del grupo FREE
    elif current_group and current_group.get("type") == "FREE":
        target_vip_group_id = (find_linked_or_admin_group(current_group, "VIP") or {}).get("group_id")
        if not target_vip_group_id:
            # Si no hay vinculación explícita, buscamos el VIP del mismo admin
            for g_id, g_obj in GROUPS.items():
                if g_obj.get("type") == "VIP" and g_obj["admin_id"] == current_group["admin_id"]:
                    target_vip_group_id = g_id
                    break
                    
    # 3. Si se usa en el chat privado del bot
    else:
        user_group_ids = await db.get_user_groups(user_id)
        if user_group_ids:
            # 1. ¿Está en algún VIP?
            for gid in user_group_ids:
                g_obj = get_group_by_id(gid)
                if g_obj and g_obj.get("type") == "VIP":
                    target_vip_group_id = gid
                    break
            # 2. ¿Está en algún FREE? Derivar al VIP del admin
            if not target_vip_group_id:
                for gid in user_group_ids:
                    g_obj = get_group_by_id(gid)
                    if g_obj and g_obj.get("type") == "FREE":
                        target_vip_group_id = (find_linked_or_admin_group(g_obj, "VIP") or {}).get("group_id")
                        if target_vip_group_id:
                            break

    if not target_vip_group_id:
        await update.message.reply_text("❌ No se encontró un grupo VIP asociado. Usa /start para ver los canales disponibles.")
        return

    group_id = target_vip_group_id
    spin_data = await db.get_spin_data(user_id, group_id)
    
    if spin_data.get("spin_count", 0) >= 1:
        await update.message.reply_text("✅ Ya giraste la ruleta. Presiona el botón para reclamar tu descuento y entrar al VIP.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("💳 Info de Pago", callback_data=f"pay_{group_id}_{user_id}")]]), parse_mode="Markdown")
        return
    
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🎰 GIRAR RULETA VIP", callback_data=f"spin_{group_id}_{user_id}")]])
    await update.message.reply_text("🎯 *¡Ruleta VIP!*\n\nPresiona el botón para girar y ganar hasta 50% de descuento en tu membresía.", reply_markup=keyboard, parse_mode="Markdown")
# ==================== HANDLER PARA REPLY CON /renew ====================
async def handle_reply_renew(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.reply_to_message: return
    if not update.message.text or not update.message.text.startswith('/renew'): return
        
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    
    text = update.message.reply_to_message.text or update.message.reply_to_message.caption or ""
    target_user_id, target_group_id, _, _ = extract_user_from_reply(text)
    
    if not target_user_id:
        await update.message.reply_text(
            "❌ No pude encontrar el ID del usuario en el mensaje.\n"
            "Asegúrate de responder al mensaje de registro o alerta del bot.\n\n"
            "También puedes usar:\n• `/renew 123456789 mensual`\n• `/renew @usuario mensual`",
            parse_mode="Markdown"
        )
        return
        
    current_group = context.user_data.get('current_group') or (chat_id if update.effective_chat.type in ("group", "supergroup") else None)
    if not current_group or not can_manage_group(user_id, current_group):
        await update.message.reply_text("❌ No autorizado para gestionar este grupo")
        return
        
    args = update.message.text.split()
    if len(args) < 2:
        await update.message.reply_text("❌ *Uso:* `/renew plan [precio] [días]`", parse_mode="Markdown"); return
        
    plan = args[1].lower()
    if plan not in PLANS:
        await update.message.reply_text("❌ Plan inválido. Usa: trial, semanal, mensual, anual"); return
        
    custom_price = None
    custom_days = None
    if len(args) >= 3:
        try:
            custom_price = float(args[2].replace(",", "."))
            if custom_price < 0: raise ValueError
        except ValueError:
            await update.message.reply_text("❌ Precio inválido", parse_mode="Markdown"); return
            
    if len(args) >= 4:
        try:
            custom_days = int(args[3])
            if custom_days <= 0: raise ValueError
        except ValueError:
            await update.message.reply_text("❌ Días inválidos", parse_mode="Markdown"); return

    # Delegamos al flujo central
    await _execute_subscription_flow(
        context.bot, update.message.reply_text, current_group, target_user_id, plan, custom_price, custom_days, is_renewal=True
    )

async def renew_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message and update.message.reply_to_message:
        await handle_reply_renew(update, context)
        return
        
    current_group = await require_group(update, context)
    if not current_group:
        await update.message.reply_text("❌ Usa /start primero o no estás autorizado")
        return
        
    if len(context.args) < 2:
        await update.message.reply_text("❌ *Uso:* `/renew @username|ID plan [precio] [días]`", parse_mode="Markdown")
        return
        
    identifier = context.args[0].replace("@", "")
    plan = context.args[1].lower()
    if plan not in PLANS:
        await update.message.reply_text("❌ Plan inválido. Usa: trial, semanal, mensual, anual"); return
        
    custom_price = None
    custom_days = None
    if len(context.args) >= 3:
        try:
            custom_price = float(context.args[2].replace(",", "."))
            if custom_price < 0: raise ValueError
        except ValueError:
            await update.message.reply_text("❌ Precio inválido", parse_mode="Markdown"); return
            
    if len(context.args) >= 4:
        try:
            custom_days = int(context.args[3])
            if custom_days <= 0: raise ValueError
        except ValueError:
            await update.message.reply_text("❌ Días inválidos", parse_mode="Markdown"); return

    target_user_id = None
    
    if identifier.isdigit():
        target_user_id = int(identifier)
    else:
        try:
            member = await context.bot.get_chat_member(current_group, identifier)
            if member and member.user: target_user_id = member.user.id
        except Exception: pass
        
        if not target_user_id:
            user_record = await db.get_user_by_username(identifier, current_group)
            if user_record: target_user_id = user_record['user_id']
            
        if not target_user_id:
            await update.message.reply_text(f"❌ No se encontró a @{identifier}.")
            return

    # Delegamos al flujo central
    await _execute_subscription_flow(
        context.bot, update.message.reply_text, current_group, target_user_id, plan, custom_price, custom_days, is_renewal=True
    )

async def list_active_users(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str = "list_active"):
    query = update.callback_query
    message = query.message if query else update.message
    if query: await query.answer()
    
    group_id = context.user_data.get('current_group')
    if not group_id:
        await message.reply_text("❌ Selecciona un grupo con /start")
        return

    # Determinar en qué página estamos
    page = 1
    if data.startswith("list_active_page_"):
        try:
            page = int(data.split("_")[-1])
            if page < 1: page = 1
        except ValueError:
            page = 1
            
    per_page = 10
    offset = (page - 1) * per_page
    
    # Pedimos los datos a la BD
    users, total_users = await db.get_all_active_users(group_id, limit=per_page, offset=offset)
    
    if not users:
        if total_users == 0:
            await query.edit_message_text("📭 No hay usuarios activos")
        else:
            await query.edit_message_text("📭 No hay más usuarios en esta página.")
        return
        
    msg = f"📊 *USUARIOS ACTIVOS* ({total_users})\n\n"
    now = now_utc()
        
    for user in users:
        end_date = user['end_date']
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=ZoneInfo("UTC"))
            
        remaining = end_date - now
        total_mins = max(0, int(remaining.total_seconds() / 60))
        days_left = total_mins // 1440
        emoji = "🟢" if days_left > 7 else "🟡" if days_left > 1 else "🔴"
        expiry_text = f"{total_mins} min" if total_mins < 60 else f"{days_left} días"
        
        raw_name = user.get('first_name', '')
        raw_username = user.get('username', '')
        
        if raw_name:
            display = safe_name(raw_name)
        elif raw_username and not raw_username.startswith('user_'):
            display = f"@{safe_name(raw_username)}"
        else:
            display = f"Usuario {user['user_id']}"
            
        chat_link = f"tg://user?id={user['user_id']}"
        msg += f"{emoji} {display}\n   📅 Expira: {fmt_dt(end_date)} ({expiry_text})\n   🔗 [Abrir chat]({chat_link})\n\n"
        
    # Lógica de paginación
    total_pages = (total_users + per_page - 1) // per_page
    msg += f"\n📄 *Página {page} de {total_pages}*"
    
    keyboard = []
    nav_buttons = []
    if page > 1:
        nav_buttons.append(InlineKeyboardButton("⬅️ Anterior", callback_data=f"list_active_page_{page-1}"))
    if page < total_pages:
        nav_buttons.append(InlineKeyboardButton("Siguiente ➡️", callback_data=f"list_active_page_{page+1}"))
        
    if nav_buttons:
        keyboard.append(nav_buttons)
        
    keyboard.append([InlineKeyboardButton("🔙 Volver", callback_data=f"select_group_{group_id}")])
    
    if query:
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    else:
        await message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    
async def show_earnings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    message = query.message if query else update.message
    if query: await query.answer()
    group_id = context.user_data.get('current_group')
    if not group_id:
        await message.reply_text("❌ Selecciona un grupo con /start")
        return

    now = datetime.now(ZoneInfo("UTC"))
    start_today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start_week = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    start_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    def _get_stats(conn):
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Usamos "amount > 0" para ignorar los trials gratuitos
            cur.execute("SELECT COUNT(*) as count, COALESCE(SUM(amount),0) as total FROM payments WHERE group_id=%s AND payment_date>=%s AND amount > 0", (group_id, start_today))
            today = cur.fetchone()
            cur.execute("SELECT COUNT(*) as count, COALESCE(SUM(amount),0) as total FROM payments WHERE group_id=%s AND payment_date>=%s AND amount > 0", (group_id, start_week))
            week = cur.fetchone()
            cur.execute("SELECT COUNT(*) as count, COALESCE(SUM(amount),0) as total FROM payments WHERE group_id=%s AND payment_date>=%s AND amount > 0", (group_id, start_month))
            month = cur.fetchone()
            return today, week, month

    today, week, month = await db._run(_get_stats)

    msg = f"💰 *RESUMEN DE GANANCIAS*\n\n"
    msg += f"📅 *Hoy:*\n• Ventas: {today['count']}\n• Total: *{fmt_price(today['total'])}*\n\n"
    msg += f"📆 *Esta Semana:*\n• Ventas: {week['count']}\n• Total: *{fmt_price(week['total'])}*\n\n"
    msg += f"🗓 *Este Mes:*\n• Ventas: {month['count']}\n• Total: *{fmt_price(month['total'])}*\n"

    await message.reply_text(msg, parse_mode="Markdown")

async def export_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    message = query.message if query else update.message
    if query:
        await query.answer()
    group_id = context.user_data.get('current_group')
    if not group_id:
        await message.reply_text("❌ Selecciona un grupo con /start")
        return
    now = datetime.now(EC_TZ)
    transactions = await db.get_export_data(group_id)
    if not transactions:
        await message.reply_text(f"📭 No hay transacciones en {now.strftime('%B %Y')}")
        return
    output = StringIO()
    try:
        writer = csv.writer(output)
        writer.writerow(['Fecha', 'User ID', 'Username', 'Nombre', 'Plan', 'Duración', 'Monto USD', 'Link de contacto'])
        for t in transactions:
            dur = fmt_minutes(t['duration_minutes']) if t.get('duration_minutes') else ""
            writer.writerow([fmt_dt(t['payment_date']), t['user_id'], t['username'] or 'Sin username', t['first_name'] or 'Sin nombre',
                             t['plan'].upper(), dur, f"{float(t['amount']):.2f}", f"tg://user?id={t['user_id']}"])
        output.seek(0)
        await message.reply_document(
            document=output.getvalue().encode('utf-8-sig'),
            filename=f"reporte_{now.year}_{now.month:02d}.csv",
            caption=f"📊 Reporte de {now.strftime('%B %Y')}"
        )
    finally:
        output.close()

async def auto_backup():
    global bot_app
    if not bot_app:
        return
    last_backup_date = await db.get_last_backup_date()
    now = now_utc()
    should_backup = False
    if not last_backup_date:
        should_backup = True
    else:
        last_backup_date = normalize_dt(last_backup_date)
        should_backup = (now - last_backup_date).days >= 15
    if should_backup:
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(['group_id', 'group_name', 'group_type', 'admin_id', 'backup_date'])
        groups_snapshot = get_all_groups_snapshot()
        for group in groups_snapshot:
            writer.writerow([group['group_id'], group['group_name'], group.get('type', 'VIP'), group['admin_id'],
                             now.strftime('%Y-%m-%d %H:%M:%S')])
        output.seek(0)
        try:
            await bot_app.bot.send_document(
                SUPER_ADMIN_ID,
                document=output.getvalue().encode('utf-8-sig'),
                filename=f"backup_automatico_{now.strftime('%Y%m%d')}.csv",
                caption=(f"📦 *Backup Automático*\n\n📅 Fecha: {now.strftime('%d/%m/%Y %H:%M')} (EC)\n"
                         f"📊 Grupos incluidos: {len(groups_snapshot)}\n\n⚠️ Guarda este archivo en un lugar seguro."),
                parse_mode="Markdown"
            )
            output.close()
            _fire_and_forget(db.log_backup_sent(), "log_backup")
            logger.info("✅ Backup automático enviado")
        except Exception as e:
            logger.error(f"❌ Error enviando backup automático: {e}")

async def manual_backup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != SUPER_ADMIN_ID and update.effective_user.id not in EXTRA_ADMINS:
        await update.message.reply_text("❌ Solo Super Admin")
        return
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(['group_id', 'group_name', 'group_type', 'admin_id', 'backup_date'])
    now = datetime.now(EC_TZ)
    groups_snapshot = get_all_groups_snapshot()
    for group in groups_snapshot:
            writer.writerow([group['group_id'], group['group_name'], group.get('type', 'VIP'), group['admin_id'],
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
    if update.effective_user.id != SUPER_ADMIN_ID and update.effective_user.id not in EXTRA_ADMINS:
        await update.message.reply_text("❌ Solo Super Admin")
        return
    if not update.message.document:
        await update.message.reply_text("❌ Envía el archivo CSV de backup junto con el comando.")
        return
    await update.message.reply_text("🔄 Restaurando configuración...")
    try:
        import io
        file = await update.message.document.get_file()
        file_content = await file.download_as_bytearray()
        reader = csv.reader(io.StringIO(file_content.decode('utf-8')))
        header = next(reader, None)
        if not header or len(header) < 4:
            await update.message.reply_text("❌ CSV inválido: encabezado incorrecto")
            return
        expected = ['group_id', 'group_name', 'group_type', 'admin_id']
        if header[:4] != expected:
            await update.message.reply_text(f"❌ CSV inválido: encabezado esperado {expected}, recibido {header[:4]}")
            return
        restored_count = 0
        to_save_db = []
        with GROUPS_LOCK:
            for row in reader:
                if len(row) >= 4:
                    try:
                        group_id = int(row[0])
                        group_name = row[1]
                        group_type = row[2]
                        admin_id = int(row[3])
                    except (ValueError, IndexError):
                        continue
                    if group_type not in ("VIP", "FREE"):
                        continue
                    existing = get_group_by_id(group_id)
                    if existing:
                        if existing['admin_id'] != admin_id and update.effective_user.id != SUPER_ADMIN_ID:
                            logger.warning(
                                f"Restore: saltando grupo {group_id} — "
                                f"cambio de admin_id de {existing['admin_id']} a {admin_id} "
                                f"requiere Super Admin"
                            )
                            continue
                        for g in GROUPS.values():
                            if g["group_id"] == group_id:
                                g.update({"group_name": group_name, "type": group_type, "admin_id": admin_id})
                                break
                        to_save_db.append((group_id, {'admin': admin_id, 'type': group_type}))
                    else:
                        GROUPS[group_id] = {"group_id": group_id, "group_name": group_name, "type": group_type,
                                       "admin_id": admin_id, "settings": {}}
                        to_save_db.append((group_id, group_name, admin_id, group_type))
                    restored_count += 1
        for item in to_save_db:
            if len(item) == 2:
                group_id, changes = item
                await db.update_group_fields(group_id, changes)
            else:
                group_id, group_name, admin_id, group_type = item
                await db.save_group(group_id, group_name, admin_id, group_type)
        await update.message.reply_text(f"✅ *Restauración completa*\n\n📊 Grupos restaurados: {restored_count}", parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"❌ Error al restaurar: {e}")
        return

async def list_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    message = query.message if query else update.message
    if query:
        await query.answer()
    if update.effective_user.id != SUPER_ADMIN_ID and update.effective_user.id not in EXTRA_ADMINS:
        await message.reply_text("❌ Solo Super Admin")
        return
    groups_snapshot = get_all_groups_snapshot()
    if not groups_snapshot:
        await message.reply_text("📭 No hay grupos")
        return
    msg = "📊 *GRUPOS CONFIGURADOS*\n\n"
    for group in groups_snapshot:
        msg += f"📌 *{group['group_name']}*\n   🆔 ID: `{group['group_id']}`\n   👑 Admin: `{group['admin_id']}`\n   📋 Tipo: {group.get('type', 'VIP')}\n\n"
    await message.reply_text(msg, parse_mode="Markdown")

async def add_group_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != SUPER_ADMIN_ID and update.effective_user.id not in EXTRA_ADMINS:
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
        if group_type not in ["VIP", "FREE", "COMUNIDAD"]:
            await update.message.reply_text("❌ TIPO debe ser VIP, FREE o COMUNIDAD")
            return
        with GROUPS_LOCK:
            if group_id in GROUPS:
                await update.message.reply_text(f"⚠️ El grupo {group_id} ya existe. Usa editar para cambiarlo.")
                return
            GROUPS[group_id] = {"group_id": group_id, "type": group_type, "group_name": group_name, "admin_id": admin_id, "settings": {}}
        await db.save_group(group_id, group_name, admin_id, group_type)
        await update.message.reply_text(f"✅ Grupo {group_name} agregado")
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")

async def view_vip_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_groups_by_type(update, context, "VIP", True)

async def view_free_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_groups_by_type(update, context, "FREE", True)

async def view_comunidad_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_groups_by_type(update, context, "COMUNIDAD", True)
    
async def show_groups_by_type(update: Update, context: ContextTypes.DEFAULT_TYPE, group_type: str, select_mode: bool = False):
    query = update.callback_query
    await query.answer()
    with GROUPS_LOCK:
        groups = [g for g in GROUPS.values() if g.get("type", "VIP") == group_type]
    if not groups:
        await query.edit_message_text(f"📭 No hay grupos {group_type}")
        return
    keyboard = [[InlineKeyboardButton(f"📌 {g['group_name']}", callback_data=f"select_group_{g['group_id']}")] for g in groups]
    if select_mode:
        keyboard.append(_back_button("menu_view_groups"))
    await query.edit_message_text(f"📋 *Grupos {group_type}*", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def menu_edit_group_select(update, context):
    query = update.callback_query
    await query.answer()
    groups_snapshot = get_all_groups_snapshot()
    if not groups_snapshot:
        await query.edit_message_text("📭 No hay grupos configurados")
        return
    keyboard = [
        [InlineKeyboardButton(
            f"{'👑' if g.get('type', 'VIP') == 'VIP' else '📋'} {g['group_name']}",
            callback_data=f"edit_multiple_{g['group_id']}"
        )] for g in groups_snapshot  # ✅ snapshot
    ]
    keyboard.append(_back_button("menu_groups"))
    await query.edit_message_text(
        "✏️ *Selecciona el grupo que deseas editar*",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def menu_delete_group_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    groups_snapshot = get_all_groups_snapshot()
    if not groups_snapshot:
        await query.edit_message_text("📭 No hay grupos configurados")
        return
    keyboard = [[InlineKeyboardButton(f"{'👑' if g.get('type', 'VIP') == 'VIP' else '📋'} {g['group_name']}",
                                      callback_data=f"delete_confirm_{g['group_id']}")] for g in groups_snapshot]
    keyboard.append(_back_button("menu_groups"))
    await query.edit_message_text("❌ *Eliminar Grupo*\n\n⚠️ Esta acción es irreversible. Selecciona el grupo:",
                                  reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def delete_group_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query
    await query.answer()
    group = get_group_by_id(group_id)
    if not group:
        await query.edit_message_text("❌ Grupo no encontrado")
        return
    keyboard = _confirm_buttons(f"delete_yes_{group_id}", "menu_groups")
    await query.edit_message_text(
        f"⚠️ *Confirmar eliminación*\n\n📌 *{group['group_name']}*\n🆔 ID: `{group['group_id']}`\n\n*Esta acción no se puede deshacer.*",
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
    with GROUPS_LOCK:
        GROUPS.pop(group_id, None)
    await db.delete_group_from_db(group_id)
    await query.edit_message_text(f"✅ *Grupo eliminado*\n\n📌 {group_name}\n🆔 ID: `{group_id}`", parse_mode="Markdown")
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
        "• `/configfuego group_id @username` - Configurar contacto de Fuego\n"
        "• `/configpago group_id` - Configurar datos de pago\n"
        "• `/configlink group_id link` - Configurar link de FREE o Comunidad\n"
        "• `/configfuego group_id @username` - Configurar contacto de Fuego\n"
        "• `/configmsg group_id tipo` - Configurar mensajes del flujo\n"
        "• `/broadcast group_id` - Enviar mensaje masivo\n\n"
        "*Usuarios (VIP):*\n• `/pagar` - Ver datos de pago\n\n"
        "*Planes:*\n• `trial` / `semanal` / `mensual` / `anual`\n\n"
        "*Ejemplos:*\n"
        "• `/add @juan semanal`\n• `/add 123456789 mensual`\n"
        "• `/add @juan semanal 5.99`\n• Responder al registro: `/add mensual`"
    )
    await query.edit_message_text(commands_text, reply_markup=InlineKeyboardMarkup([_back_button("back_to_admin")]), parse_mode="Markdown")

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
    now = datetime.now(EC_TZ)
    if count_last_month > 0:
        growth = ((count_month - count_last_month) / count_last_month) * 100
        growth_text = f"{'📈' if growth > 0 else '📉' if growth < 0 else '➖'} {growth:+.1f}% vs mes anterior"
    else:
        growth_text = "📊 Primer mes con registros" if count_month > 0 else "📊 Sin registros este mes"
    msg = (f"📋 *CLIENTES POTENCIALES - {group['group_name']}*\n\n"
           f"📅 *{now.strftime('%B %Y')}:* {count_month} nuevos\n"
           f"📆 *Mes anterior:* {count_last_month}\n"
           f"📊 *Total histórico:* {total_all}\n\n{growth_text}")
    await query.edit_message_text(msg, parse_mode="Markdown")

async def export_clients(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
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
        writer.writerow([c['user_id'], c['username'] or '', c['first_name'] or '', fmt_dt(c['created_at'])])
    output.seek(0)
    await query.message.reply_document(
        document=output.getvalue().encode('utf-8-sig'),
        filename=f"clientes_{datetime.now(EC_TZ).strftime('%Y%m%d')}.csv",
        caption="📋 Clientes potenciales"
    )
    output.close()

async def trial_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    message = query.message if query else update.message
    if query: await query.answer()
    group_id = context.user_data.get('current_group')
    if not group_id:
        await message.reply_text("❌ Selecciona un grupo con /start")
        return
    group = get_group_by_id(group_id)
    if not group or group.get("type", "VIP") not in ("VIP", "COMUNIDAD"):
        await message.reply_text("❌ Solo disponible en grupos VIP o Comunidad")
        return

    # Calcular fechas en hora de Ecuador
    now_ec = datetime.now(EC_TZ)
    start_today_ec = now_ec.replace(hour=0, minute=0, second=0, microsecond=0)
    start_week_ec = (start_today_ec - timedelta(days=start_today_ec.weekday()))
    start_month_ec = start_today_ec.replace(day=1)

    # Convertir a UTC para la base de datos
    start_today = start_today_ec.astimezone(ZoneInfo("UTC"))
    start_week = start_week_ec.astimezone(ZoneInfo("UTC"))
    start_month = start_month_ec.astimezone(ZoneInfo("UTC"))

    def _get_stats(conn):
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Trials
            cur.execute("SELECT COUNT(*) as c FROM users WHERE group_id=%s AND (plan='trial' OR trial_used=TRUE) AND created_at>=%s", (group_id, start_today))
            t_today = cur.fetchone()['c']
            cur.execute("SELECT COUNT(*) as c FROM users WHERE group_id=%s AND (plan='trial' OR trial_used=TRUE) AND created_at>=%s", (group_id, start_week))
            t_week = cur.fetchone()['c']
            cur.execute("SELECT COUNT(*) as c FROM users WHERE group_id=%s AND (plan='trial' OR trial_used=TRUE) AND created_at>=%s", (group_id, start_month))
            t_month = cur.fetchone()['c']
            
            # Leads (Info de pago)
            cur.execute("SELECT COUNT(DISTINCT user_id) as c FROM payment_leads WHERE group_id=%s AND created_at>=%s", (group_id, start_today))
            l_today = cur.fetchone()['c']
            cur.execute("SELECT COUNT(DISTINCT user_id) as c FROM payment_leads WHERE group_id=%s AND created_at>=%s", (group_id, start_week))
            l_week = cur.fetchone()['c']
            cur.execute("SELECT COUNT(DISTINCT user_id) as c FROM payment_leads WHERE group_id=%s AND created_at>=%s", (group_id, start_month))
            l_month = cur.fetchone()['c']

            # Ruletas
            cur.execute("SELECT COUNT(*) as c FROM discount_spins WHERE group_id=%s AND last_spin>=%s", (group_id, start_today))
            r_today = cur.fetchone()['c']
            cur.execute("SELECT COUNT(*) as c FROM discount_spins WHERE group_id=%s AND last_spin>=%s", (group_id, start_week))
            r_week = cur.fetchone()['c']
            cur.execute("SELECT COUNT(*) as c FROM discount_spins WHERE group_id=%s AND last_spin>=%s", (group_id, start_month))
            r_month = cur.fetchone()['c']

            # Convertidos y activos
            cur.execute("SELECT COUNT(DISTINCT u.user_id) as c FROM users u JOIN payments p ON u.user_id = p.user_id AND u.group_id = p.group_id WHERE u.group_id=%s AND u.trial_used=TRUE AND p.amount > 0", (group_id,))
            converted = cur.fetchone()['c']
            cur.execute("SELECT COUNT(*) as c FROM users WHERE group_id=%s AND plan='trial' AND status='active' AND end_date > NOW()", (group_id,))
            active = cur.fetchone()['c']
            return t_today, t_week, t_month, l_today, l_week, l_month, r_today, r_week, r_month, converted, active

    t_today, t_week, t_month, l_today, l_week, l_month, r_today, r_week, r_month, converted, active = await db._run(_get_stats)

    msg = f"📊 *ESTADÍSTICAS DE VENTAS — {group['group_name']}*\n\n"
    
    msg += f"🆓 *NUEVOS TRIALS (Ingresos)*\n"
    msg += f"• 📅 Hoy: *{t_today}*\n"
    msg += f"• 📆 Semana: *{t_week}*\n"
    msg += f"• 🗓 Mes: *{t_month}*\n\n"
    
    msg += f"💳 *LEADS DE PAGO (Interés)*\n"
    msg += f"• 📅 Hoy: *{l_today}*\n"
    msg += f"• 📆 Semana: *{l_week}*\n"
    msg += f"• 🗓 Mes: *{l_month}*\n\n"
    
    msg += f"🎰 *GIROS DE RULETA*\n"
    msg += f"• 📅 Hoy: *{r_today}*\n"
    msg += f"• 📆 Semana: *{r_week}*\n"
    msg += f"• 🗓 Mes: *{r_month}*\n\n"
    
    msg += f"🔥 *Trial Activos Ahora:* {active}\n"
    msg += f"💰 *Convertidos a pago (Histórico):* {converted}\n"
    
    if t_month > 0:
        conversion = (converted / t_month) * 100
        msg += f"📈 *Conversión Histórica:* {conversion:.1f}%"

    keyboard = [_back_button(f"select_group_{group_id}")]
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
    context.user_data['editing_mode'] = 'multiple'
    context.user_data.setdefault('pending_changes', {})
    keyboard = [
        [InlineKeyboardButton("📝 Cambiar nombre", callback_data=f"multi_name_{group_id}")],
        [InlineKeyboardButton("👤 Cambiar admin", callback_data=f"multi_admin_{group_id}")],
        [InlineKeyboardButton("🔄 Cambiar tipo", callback_data=f"multi_type_{group_id}")],
        [InlineKeyboardButton("✅ Aplicar cambios", callback_data=f"multi_apply_{group_id}")],
        _back_button("menu_edit_group_select"),
    ]
    pending = context.user_data.get('pending_changes', {})
    pending_text = ""
    if pending:
        pending_text = "\n\n📝 *Cambios pendientes:*\n"
        if 'name' in pending: pending_text += f"• Nuevo nombre: `{pending['name']}`\n"
        if 'admin' in pending: pending_text += f"• Nuevo admin: `{pending['admin']}`\n"
        if 'type' in pending: pending_text += f"• Nuevo tipo: `{pending['type']}`\n"
    await query.edit_message_text(
        f"✏️ *Edición múltiple - {group['group_name']}*\n\n"
        f"📋 Tipo actual: {group.get('type', 'VIP')}\n👑 Admin actual: `{group['admin_id']}`{pending_text}\n\n"
        f"*Escribe 'cancelar' para cancelar la edición.*",
        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
    )

async def multi_name_request(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query
    await query.answer()
    _clear_input_states(context) 
    context.user_data['editing_field'] = 'multi_name'
    context.user_data['editing_group_id'] = group_id
    await query.edit_message_text("✏️ *Cambiar nombre*\n\nEnvía el nuevo nombre.\n*Escribe 'cancelar' para cancelar.*", parse_mode="Markdown")

async def multi_admin_request(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query
    await query.answer()
    _clear_input_states(context)   
    context.user_data['editing_field'] = 'multi_admin'
    context.user_data['editing_group_id'] = group_id
    await query.edit_message_text("👤 *Cambiar administrador*\n\nEnvía el ID del nuevo administrador.\n*Escribe 'cancelar' para cancelar.*", parse_mode="Markdown")

async def multi_type_request(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query
    await query.answer()
    keyboard = [
        [InlineKeyboardButton("👑 VIP", callback_data=f"multi_set_type_{group_id}_VIP")],
        [InlineKeyboardButton("📋 FREE", callback_data=f"multi_set_type_{group_id}_FREE")],
        _back_button(f"edit_multiple_{group_id}"),
    ]
    await query.edit_message_text("🔄 *Selecciona el nuevo tipo*", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def multi_set_type(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int, new_type: str):
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
    with GROUPS_LOCK:
        for g in GROUPS.values():
            if g["group_id"] == group_id:
                if 'name' in pending: g["group_name"] = pending['name']; changes_made.append(f"📝 Nombre → {pending['name']}")
                if 'admin' in pending: g["admin_id"] = pending['admin']; changes_made.append(f"👤 Admin → {pending['admin']}")
                if 'type' in pending: g["type"] = pending['type']; changes_made.append(f"🔄 Tipo → {pending['type']}")
                break
    await db.update_group_fields(group_id, pending)
    for key in ('pending_changes', 'editing_mode', 'editing_group_id'):
        context.user_data.pop(key, None)
    await query.edit_message_text(f"✅ *Cambios aplicados*\n\n" + "\n".join(changes_made), parse_mode="Markdown")
    await asyncio.sleep(2)
    await menu_edit_group_select(update, context)

# ==================== DETECCIÓN DE MIEMBROS ====================
async def _process_new_vip_member(chat_id: int, user_id: int, username: str, first_name: str, group: dict, source: str = "Directo"):
    display = safe_name(first_name or (f"@{username}" if username and not username.startswith('user_') else f"Usuario {user_id}"))
    chat_link = f"tg://user?id={user_id}"

    free_group = find_linked_or_admin_group(group, "FREE")
    if not free_group:
        vip_settings = group.get("settings", {})
        free_id = vip_settings.get("linked_free_group_id")
        if free_id: free_group = get_group_by_id(free_id)
    free_link = free_group.get("settings", {}).get("invite_link") if free_group else None

    # Grupo Comunidad vinculado (opcional) — solo invitación, NO otorga trial automático
    linked_comunidad_id = group.get("settings", {}).get("linked_comunidad_group_id")
    comm_group = get_group_by_id(linked_comunidad_id) if linked_comunidad_id else None
    comm_invite_link = comm_group.get("settings", {}).get("invite_link") if comm_group else None

    allow_access, result_code, end_date = await db.register_user_auto(chat_id, user_id, username, first_name, source)

    if allow_access:
        if result_code == "trial_nuevo":
            cfg_t = get_group_plan_config(chat_id, "trial")
            trial_str = fmt_minutes(cfg_t.get('minutes', 1440))
            expiry_str = fmt_dt(end_date) if end_date else "N/A"
            custom_messages = await db.get_group_messages(chat_id)

            if custom_messages and custom_messages.get('welcome_message'):
                welcome_msg = custom_messages['welcome_message']
            else:
                welcome_msg = DEFAULT_WELCOME_MESSAGE

            welcome_msg = format_message(welcome_msg, {
                'trial_duration': trial_str,
                'expiry_time': expiry_str,
                'group_name': safe_name(group['group_name']),
                'user_name': safe_name(first_name or username)
            })

            keyboard = []
            if free_link:
                keyboard.append([InlineKeyboardButton("📢 Únete al canal FREE", url=free_link)])
            if comm_invite_link:
                keyboard.append([InlineKeyboardButton("👥 Conoce nuestra Comunidad", url=comm_invite_link)])
            keyboard.append([InlineKeyboardButton("🎰 GIRAR RULETA VIP", callback_data=f"spin_{chat_id}_{user_id}")])
            keyboard.append([InlineKeyboardButton("💳 Ver Datos de Pago", callback_data=f"pay_{chat_id}_{user_id}")])

            await _safe_send(user_id, welcome_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
            await _safe_send(
                group["admin_id"],
                f"🆕 *Nuevo usuario VIP*\n\n"
                f"👤 *Nombre:* [{display}]({chat_link})\n"
                f"🆔 *ID:* `{user_id}`\n"
                f"📌 *Grupo:* {safe_name(group['group_name'])}\n"
                f"🌍 *Origen:* {source}\n"
                f"⏱ *Trial VIP:* {trial_str}\n\n"
                f"💡 *Para activar pago:* `/add {user_id} mensual`\n"
                f"🤝 *Combo con Comunidad:* `/addcombo {user_id} mensual`",
                parse_mode="Markdown", disable_notification=True
            )
        elif result_code == "activo":
            pass
        return True

    motivo_admin = "Plan vencido" if result_code == "expirado" else "Trial VIP ya utilizado"

    kick_success = await _kick_user_with_retry(chat_id, user_id)
    if kick_success:
        custom_messages = await db.get_group_messages(chat_id)
        expired_msg = DEFAULT_EXPIRED_MESSAGE
        if custom_messages and custom_messages.get('expired_message'):
            expired_msg = custom_messages['expired_message']
        expired_msg = format_message(expired_msg, {'group_name': group['group_name']})
        await _safe_send(user_id, expired_msg, reply_markup=get_payment_keyboard(chat_id, user_id), parse_mode="Markdown")
        await _safe_send(
            group["admin_id"],
            f"🚫 *Reingreso denegado (VIP)*\n\n"
            f"👤 [{display}]({chat_link})\n"
            f"🆔 *ID:* `{user_id}`\n"
            f"📌 *Grupo:* {safe_name(group['group_name'])}\n"
            f"🌍 *Origen:* {safe_name(source)}\n"
            f"⚠️ *Motivo:* {safe_name(motivo_admin)}",
            parse_mode="Markdown", disable_notification=True
        )
    return False
# --- NUEVO HANDLER PARA COMUNIDAD ---
async def _process_new_comunidad_member(chat_id: int, user_id: int, username: str, first_name: str, group: dict, source: str = "Directo"):
    display = safe_name(first_name or (f"@{username}" if username and not username.startswith('user_') else f"Usuario {user_id}"))
    chat_link = f"tg://user?id={user_id}"

    allow_access, result_code, end_date = await db.register_user_auto(chat_id, user_id, username, first_name, source)

    if allow_access:
        if result_code == "trial_nuevo":
            cfg_t = get_group_plan_config(chat_id, "trial")
            trial_str = fmt_minutes(cfg_t.get('minutes', 1440))
            expiry_str = fmt_dt(end_date) if end_date else "N/A"
            
            # Cargar mensaje personalizado de la BD
            custom_messages = await db.get_group_messages(chat_id)
            welcome_msg = DEFAULT_COMUNIDAD_WELCOME_MESSAGE
            if custom_messages and custom_messages.get('welcome_message'):
                welcome_msg = custom_messages['welcome_message']
                
            welcome_msg = format_message(welcome_msg, {
                'trial_duration': trial_str,
                'expiry_time': expiry_str,
                'group_name': safe_name(group['group_name']),
                'user_name': safe_name(first_name or username)
            })
            keyboard = [[InlineKeyboardButton("💳 Ver Datos de Pago", callback_data=f"pay_{chat_id}_{user_id}")]]
            await _safe_send(user_id, welcome_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
            await _safe_send(
                group["admin_id"],
                f"🆕 *Nuevo usuario Comunidad*\n\n"
                f"👤 [{display}]({chat_link})\n"
                f"🆔 *ID:* `{user_id}`\n"
                f"📌 *Grupo:* {safe_name(group['group_name'])}\n"
                f"🌍 *Origen:* {safe_name(source)}\n"
                f"⏱ *Trial:* {trial_str}\n\n"
                f"💡 `/add {user_id} mensual comunidad`",
                parse_mode="Markdown", disable_notification=True
            )
        # result_code == "activo" -> reingreso permitido, sin acción
        return True

    # Trial ya usado o plan vencido, sin pago activo -> SILENCIAR, no expulsar
    muted = await _mute_user_with_retry(chat_id, user_id)
    if muted:
        # Cargar mensaje de silenciado personalizado si existe
        custom_messages = await db.get_group_messages(chat_id)
        muted_msg = DEFAULT_COMUNIDAD_MUTED_MESSAGE
        if custom_messages and custom_messages.get('expired_message'):
            muted_msg = custom_messages['expired_message']
            
        expired_msg = format_message(muted_msg, {'group_name': safe_name(group['group_name'])})
        await _safe_send(user_id, expired_msg, reply_markup=get_payment_keyboard(chat_id, user_id), parse_mode="Markdown")
        motivo = "Plan vencido" if result_code == "expirado" else "Trial ya utilizado"
        await _safe_send(
            group["admin_id"],
            f"🔇 *Silenciado*\n\n"
            f"👤 [{display}]({chat_link})\n"
            f"🆔 *ID:* `{user_id}`\n"
            f"📌 *Grupo:* {safe_name(group['group_name'])}\n"
            f"🌍 *Origen:* {safe_name(source)}\n"
            f"⚠️ *Motivo:* {safe_name(motivo)}",
            parse_mode="Markdown", disable_notification=True
        )
    return False
    
async def _process_new_free_member(chat_id: int, user_id: int, username: str, first_name: str, group: dict, source: str = "Directo"):
    display = safe_name(first_name or (f"@{username}" if username and not username.startswith('user_') else f"Usuario {user_id}"))
    chat_link = f"tg://user?id={user_id}"
    is_new = await db.register_free_user(chat_id, user_id, username, first_name, source)
    if is_new:
        await _safe_send(
            group["admin_id"],
            f"📋 *Nuevo cliente potencial*\n\n"
            f"👤 *Nombre:* {display}\n"
            f"🆔 *ID:* `{user_id}`\n"
            f"📌 *Grupo:* {safe_name(group['group_name'])}\n"
            f"🌍 *Origen:* {source}\n"
            f"🔗 [Abrir chat]({chat_link})",
            parse_mode="Markdown"
        )
    return is_new

async def track_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    result = update.chat_member
    if not result: return
    new_member = result.new_chat_member.user
    if new_member.is_bot: return
    
    chat_id = result.chat.id
    old_status = result.old_chat_member.status
    new_status = result.new_chat_member.status
    joined = (old_status in ("left", "kicked", "restricted", "banned") and new_status in ("member", "administrator", "creator"))
    if not joined: return
    
    group = get_group_by_id(chat_id)
    if not group: return
    
    source = "Directo"
    if result.invite_link:
        link_name = result.invite_link.name
        link_url = result.invite_link.invite_link or ""
        if link_name:
            source = f"Link: {link_name}"
        elif link_url:
            # Extraer identificador corto del link
            if "+" in link_url:
                short = link_url.split("+")[-1][:15]
            else:
                short = link_url.split("/")[-1][:15]
            source = f"Link sin nombre (…{short})"
        else:
            source = "Link (sin datos)"
    
    if result.via_chat_folder_invite_link:
        source = "Link de carpeta"
    if result.from_user and result.from_user.id != new_member.id and not result.invite_link:
        source = f"Añadido por: {result.from_user.first_name or result.from_user.id}"
        
    user_id = new_member.id
    username = new_member.username or f"user_{user_id}"
    first_name = new_member.first_name or ""
    
    group_type = group.get("type", "VIP")
    
    if group_type == "VIP": 
        await _process_new_vip_member(chat_id, user_id, username, first_name, group, source)
    elif group_type == "COMUNIDAD": 
        await _process_new_comunidad_member(chat_id, user_id, username, first_name, group, source)
    else: 
        await _process_new_free_member(chat_id, user_id, username, first_name, group, source)

async def detect_active_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or update.effective_chat.type not in ("group", "supergroup"):
        return
    chat_id = update.effective_chat.id
    group = get_group_by_id(chat_id)
    if not group:
        return
    user = update.effective_user
    user_id = user.id
    username = user.username or f"user_{user_id}"
    first_name = user.first_name or ""
    
    cache_key = f"known_{chat_id}_{user_id}"
    
    if _is_known_member(cache_key):
        return
        
    existing = await db.get_user_by_id(user_id, chat_id)
    display = safe_name(first_name or (f"@{username}" if username and not username.startswith('user_') else f"Usuario {user_id}"))
    chat_link = f"tg://user?id={user_id}"
    
    if group["type"] == "FREE":
        is_new = await db.register_free_user(chat_id, user_id, username, first_name)
        if is_new:
            await _safe_send(
                group["admin_id"],
                f"📋 *Nuevo cliente potencial detectado*\n\n"
                f"👤 *Nombre:* {display}\n"
                f"🆔 *ID:* `{user_id}`\n"
                f"📌 *Grupo:* {safe_name(group['group_name'])}\n"
                f"🔗 [Abrir chat]({chat_link})\n\n_Detectado al escribir en el grupo._",
                parse_mode="Markdown", disable_notification=True
            )
        _mark_known_member(cache_key)
    elif group["type"] == "VIP":
        if not existing:
            logger.info(f"🔍 Usuario {user_id} detectado en VIP {chat_id} sin registro previo — procesando como nuevo")
            await _process_new_vip_member(chat_id, user_id, username, first_name, group, source="Detectado al escribir (posible caída del bot)")
        _mark_known_member(cache_key)
    elif group["type"] == "COMUNIDAD":
        if not existing:
            logger.info(f"🔍 Usuario {user_id} detectado en Comunidad {chat_id} sin registro previo — procesando como nuevo")
            await _process_new_comunidad_member(chat_id, user_id, username, first_name, group, source="Detectado al escribir (posible caída del bot)")
        _mark_known_member(cache_key)
# ==================== CONFIGURACIÓN DE PRECIOS Y TRIAL ====================
async def menu_group_settings(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query
    await query.answer()
    group = get_group_by_id(group_id)
    if not group:
        await query.edit_message_text("❌ Grupo no encontrado")
        return
        
    cfg_trial = get_group_plan_config(group_id, "trial")
    cfg_semanal = get_group_plan_config(group_id, "semanal")
    cfg_mensual = get_group_plan_config(group_id, "mensual")
    cfg_anual = get_group_plan_config(group_id, "anual")
    
    trial_str = fmt_minutes(cfg_trial.get('minutes', 60))
    deuna_status = "✅" if group.get("settings", {}).get("deuna_link") else "❌"
    
    keyboard = [
        [InlineKeyboardButton(f"⏱ Trial: {trial_str}", callback_data=f"cfg_trial_{group_id}")],
        [
            InlineKeyboardButton(f"💲 Semanal: {fmt_price(cfg_semanal['price'])}", callback_data=f"cfg_price_semanal_{group_id}"),
            InlineKeyboardButton(f"📅 {cfg_semanal.get('days', 7)} días", callback_data=f"cfg_dur_semanal_{group_id}")
        ],
        [
            InlineKeyboardButton(f"💲 Mensual: {fmt_price(cfg_mensual['price'])}", callback_data=f"cfg_price_mensual_{group_id}"),
            InlineKeyboardButton(f"📅 {cfg_mensual.get('days', 30)} días", callback_data=f"cfg_dur_mensual_{group_id}")
        ],
        [
            InlineKeyboardButton(f"💲 Anual: {fmt_price(cfg_anual['price'])}", callback_data=f"cfg_price_anual_{group_id}"),
            InlineKeyboardButton(f"📅 {cfg_anual.get('days', 365)} días", callback_data=f"cfg_dur_anual_{group_id}")
        ],
        [InlineKeyboardButton("💳 Datos Bancarios y PayPal", callback_data=f"cfg_payment_{group_id}")],
        [InlineKeyboardButton(f"⚡ Link Deuna ({deuna_status})", callback_data=f"cfg_deuna_{group_id}")],
        [InlineKeyboardButton("🔔 Configurar Avisos", callback_data=f"cfg_warnings_{group_id}")],
    ]
    if group.get("type", "VIP") == "VIP":
        linked_com = group.get("settings", {}).get("linked_comunidad_group_id")
        if linked_com and get_group_by_id(linked_com):
            keyboard.append([InlineKeyboardButton("🤝 Configurar Combo VIP+Comunidad", callback_data=f"cfg_combo_{group_id}")])
        else:
            keyboard.append([InlineKeyboardButton("🤝 Combo (vincula Comunidad primero)", callback_data=f"cfg_combo_nolink_{group_id}")])
    keyboard.append(_back_button(f"select_group_{group_id}"))
    
    await query.edit_message_text(
        f"⚙️ *Configuración - {group['group_name']}*\n\n"
        f"• ⏱ *Trial:* {trial_str}\n"
        f"• 📅 *Semanal:* {fmt_price(cfg_semanal['price'])} ({cfg_semanal.get('days', 7)} días)\n"
        f"• 📆 *Mensual:* {fmt_price(cfg_mensual['price'])} ({cfg_mensual.get('days', 30)} días)\n"
        f"• 🏆 *Anual:* {fmt_price(cfg_anual['price'])} ({cfg_anual.get('days', 365)} días)\n\n"
        f"_Selecciona una opción para editar:_",
        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
    )

async def cfg_trial_request(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query
    await query.answer()
    _clear_input_states(context)
    context.user_data['cfg_field'] = 'trial_minutes'
    context.user_data['cfg_group_id'] = group_id
    cfg = get_group_plan_config(group_id, "trial")
    await query.edit_message_text(
        f"⏱ *Cambiar duración del Trial*\n\nValor actual: *{fmt_minutes(cfg.get('minutes', 1440))}*\n\n"
        f"Envía el número de *minutos*:\n• 20 min = 20\n• 1 hora = 60\n• 1 día = 1440\n• 7 días = 10080\n\n"
        f"*Escribe 'cancelar' para cancelar.*",
        parse_mode="Markdown"
    )

async def cfg_price_request(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int, plan: str):
    query = update.callback_query
    await query.answer()
    _clear_input_states(context)
    context.user_data['cfg_field'] = f'price_{plan}'
    context.user_data['cfg_group_id'] = group_id
    cfg = get_group_plan_config(group_id, plan)
    plan_name = {"semanal": "Semanal", "mensual": "Mensual", "anual": "Anual"}.get(plan, plan.capitalize())
    await query.edit_message_text(
        f"💲 *Precio base - {plan_name}*\n\nValor actual: *{fmt_price(cfg['price'])}*\n\n"
        f"Envía el precio (ej: `10`, `5.99`)\n\n*Escribe 'cancelar' para cancelar.*",
        parse_mode="Markdown"
    )

async def cfg_duration_request(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int, plan: str):
    query = update.callback_query
    await query.answer()
    _clear_input_states(context) 
    context.user_data['cfg_field'] = f'duration_{plan}'
    context.user_data['cfg_group_id'] = group_id
    cfg = get_group_plan_config(group_id, plan)
    plan_name = {"semanal": "Semanal", "mensual": "Mensual", "anual": "Anual"}.get(plan, plan.capitalize())
    await query.edit_message_text(
        f"📅 *Días de duración - {plan_name}*\n\nValor actual: *{cfg.get('days', 7)} días*\n\n"
        f"Envía el número de días (ej: `7`, `14`, `30`)\n\n*Escribe 'cancelar' para cancelar.*",
        parse_mode="Markdown"
    )

async def cfg_deuna_request(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query
    await query.answer()
    _clear_input_states(context)
    context.user_data['cfg_field'] = 'deuna_link'
    context.user_data['cfg_group_id'] = group_id
    group = get_group_by_id(group_id)
    current_link = group.get("settings", {}).get("deuna_link", "")
    await query.edit_message_text(
        f"⚡ *Configurar Link Deuna*\n\nActual: `{current_link or 'No configurado'}`\n\nEnvía el nuevo link (debe empezar con https://).\n\n*Escribe 'cancelar' para salir.*",
        parse_mode="Markdown"
    )

async def menu_combo_settings(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query
    await query.answer()
    if not can_manage_group(query.from_user.id, group_id):
        await query.edit_message_text("❌ No autorizado")
        return
    group = get_group_by_id(group_id)
    if not group or group.get("type", "VIP") != "VIP":
        await query.edit_message_text("❌ Solo disponible en grupos VIP")
        return
    comunidad_id = group.get("settings", {}).get("linked_comunidad_group_id")
    if not comunidad_id or not get_group_by_id(comunidad_id):
        await query.edit_message_text(
            "❌ Vincula un grupo Comunidad primero con:\n`/linkcomunidad vip_id comunidad_id`",
            parse_mode="Markdown"
        )
        return
    settings = group.get("settings", {})
    lines = ["🤝 *Configuración de Combo VIP + Comunidad*\n"]
    keyboard = []
    for plan in ["semanal", "mensual", "anual"]:
        vip_cfg = get_group_plan_config(group_id, plan)
        com_cfg = get_group_plan_config(comunidad_id, plan)
        current = settings.get(f"combo_price_{plan}")
        suma = vip_cfg['price'] + com_cfg['price']
        display_price = fmt_price(current) if current is not None else f"{fmt_price(suma)} (suma, sin configurar)"
        lines.append(f"• {_plan_emoji(plan)} {plan.capitalize()}: *{display_price}*")
        keyboard.append([InlineKeyboardButton(f"💲 Editar {plan.capitalize()}", callback_data=f"cfg_combo_price_{plan}_{group_id}")])
    keyboard.append(_back_button(f"cfg_group_{group_id}"))
    lines.append("\n_El combo aplica la duración configurada de cada plan en cada grupo respectivamente._")
    await query.edit_message_text("\n".join(lines), reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def cfg_combo_price_request(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int, plan: str):
    query = update.callback_query
    await query.answer()
    _clear_input_states(context)
    context.user_data['cfg_field'] = f'combo_price_{plan}'
    context.user_data['cfg_group_id'] = group_id
    await query.edit_message_text(
        f"💲 *Precio Combo - {plan.capitalize()}*\n\nEnvía el nuevo precio del combo (ej: `25`, `25.99`).\n\n"
        f"*Escribe 'cancelar' para cancelar.*",
        parse_mode="Markdown"
    )

async def cfg_combo_nolink_notice(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query
    await query.answer("❌ Vincula un grupo Comunidad primero con /linkcomunidad", show_alert=True)

async def cfg_cart_request(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query
    await query.answer()
    _clear_input_states(context)
    context.user_data['cfg_field'] = 'cart_delay_minutes'
    context.user_data['cfg_group_id'] = group_id
    group = get_group_by_id(group_id)
    current_mins = group.get("settings", {}).get("cart_delay_minutes", 30)
    await query.edit_message_text(
        f"🛒 *Configurar Carrito Abandonado*\n\nActual: *{current_mins} minutos*\n\nEnvía el número de minutos (ej. `30`).\n\n*Escribe 'cancelar' para salir.*",
        parse_mode="Markdown"
    )

async def handle_cfg_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        return
    field = context.user_data.get('cfg_field')
    group_id = context.user_data.get('cfg_group_id')
    if not field or not group_id:
        return
    if not can_manage_group(update.effective_user.id, group_id):
        return

    text = update.message.text.strip().replace(",", ".")

    if text.lower() == 'cancelar':
        _clear_input_states(context)
        await update.message.reply_text("❌ Configuración cancelada")
        return

    # ── Deuna ──────────────────────────────────────────────────────────────
    if field == 'deuna_link':
        if not text.startswith("https://"):
            await update.message.reply_text("❌ El link debe empezar con `https://`")
            return
        if not await update_group_settings(group_id, {'deuna_link': text}):
            await update.message.reply_text("❌ Grupo no encontrado")
            _clear_input_states(context)
            return
        _clear_input_states(context)
        await update.message.reply_text("✅ *Link Deuna guardado correctamente.*", parse_mode="Markdown")
        return

    # ── Carrito abandonado ─────────────────────────────────────────────────
    if field == 'cart_delay_minutes':
        try:
            minutes = int(text)
            if minutes <= 0:
                raise ValueError
        except ValueError:
            await update.message.reply_text("❌ Debe ser un número entero positivo (ej. `30`).")
            return
        if not await update_group_settings(group_id, {'cart_delay_minutes': minutes}):
            await update.message.reply_text("❌ Grupo no encontrado")
            _clear_input_states(context)
            return
        _clear_input_states(context)
        await update.message.reply_text(f"✅ *Carrito abandonado configurado a {minutes} minutos.*", parse_mode="Markdown")
        return

    # ── Precios y duraciones ───────────────────────────────────────────────
    is_price = field.startswith("price_") or field.startswith("combo_price_")
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
        await update.message.reply_text("❌ Valor inválido.")
        return

    if not await update_group_settings(group_id, {field: value}):
        await update.message.reply_text("❌ Grupo no encontrado")
        _clear_input_states(context)
        return

    _clear_input_states(context)

    labels = {
        "trial_minutes":    f"Trial: *{fmt_minutes(value)}*",
        "price_semanal":    f"Precio semanal base: *{fmt_price(value)}*",
        "price_mensual":    f"Precio mensual base: *{fmt_price(value)}*",
        "price_anual":      f"Precio anual base: *{fmt_price(value)}*",
        "duration_semanal": f"Duración semanal: *{value} días*",
        "duration_mensual": f"Duración mensual: *{value} días*",
        "duration_anual":   f"Duración anual: *{value} días*",
    }
    if field.startswith("combo_price_"):
        plan_name = field.replace("combo_price_", "")
        label = f"Precio combo {plan_name.capitalize()}: *{fmt_price(value)}*"
    else:
        label = labels.get(field, f"{field}: *{value}*")
    await update.message.reply_text(f"✅ *Configuración actualizada*\n\n• {label}", parse_mode="Markdown")

async def search_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != SUPER_ADMIN_ID and update.effective_user.id not in EXTRA_ADMINS:
        await update.message.reply_text("❌ Solo Super Admin")
        return
    if len(context.args) < 1:
        await update.message.reply_text("❌ Usa: `/searchgrupo nombre`", parse_mode="Markdown")
        return
    search_term = " ".join(context.args).lower()
    with GROUPS_LOCK:
        results = [g for g in GROUPS.values() if search_term in g['group_name'].lower()]
    if not results:
        await update.message.reply_text(f"📭 No se encontraron grupos con '{search_term}'")
        return
    msg = f"🔍 *Resultados para '{search_term}'*\n\n"
    for group in results:
        emoji = "👑" if group.get("type", "VIP") == "VIP" else "📋"
        msg += f"{emoji} *{group['group_name']}*\n   🆔 ID: `{group['group_id']}`\n   👑 Admin: `{group['admin_id']}`\n\n"
    await update.message.reply_text(msg, parse_mode="Markdown")

async def handle_message_config_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    step = context.user_data.get('config_msg_step')
    group_id = context.user_data.get('config_msg_group_id')
    msg_type = context.user_data.get('config_msg_type')

    if not step or not group_id or not msg_type:
        return

    text = update.message.text.strip()

    if text.lower() == 'cancelar':
        for k in ('config_msg_step', 'config_msg_group_id', 'config_msg_type'):
            context.user_data.pop(k, None)
        await update.message.reply_text("❌ Configuración cancelada")
        return

    # Delegamos la lógica de guardado a la nueva función
    await save_configured_message(update, context, text, msg_type, group_id)

async def save_configured_message(update, context, text, msg_type, group_id):
    # Normalizar
    text = None if text and text.lower().strip() == 'default' else text

    # Función helper asíncrona específica para el carrito abandonado
    async def save_abandoned():
        group = get_group_by_id(group_id)
        if group:
            step_index = int(msg_type.split("_")[1]) - 1
            msgs = list(group.get("settings", {}).get('abandoned_cart_messages', DEFAULT_CART_MESSAGES))
            while len(msgs) <= step_index:
                msgs.append("")
            msgs[step_index] = text
            await update_group_settings(group_id, {'abandoned_cart_messages': msgs})
    # Mapeo unificado
    handlers = {
        "welcome": lambda: db.save_group_messages(group_id, welcome_msg=text),
        "rejection": lambda: db.save_group_messages(group_id, rejection_msg=text),
        "channel_select": lambda: db.save_global_message('channel_select_message', text),
        "channel_description": lambda: db.save_group_messages(group_id, channel_desc=text),
        "vip_menu": lambda: db.save_group_messages(group_id, vip_menu_msg=text),
        "fuego_notice": lambda: db.save_group_messages(group_id, fuego_notice_msg=text),
        "expired": lambda: db.save_group_messages(group_id, expired_msg=text),
        "abandoned_1": save_abandoned,
        "abandoned_2": save_abandoned,
        "abandoned_3": save_abandoned,
    }

    handler = handlers.get(msg_type)
    if not handler:
        await update.message.reply_text("❌ Tipo de mensaje no reconocido.")
        return

    try:
        await handler()
    except Exception as e:
        logger.error(f"Error guardando mensaje {msg_type} para grupo {group_id}: {e}")
        await update.message.reply_text(
            "❌ No se pudo guardar el mensaje. Intenta de nuevo más tarde."
        )
        return

    # Limpiar estado solo si todo salió bien
    for k in ('config_msg_step', 'config_msg_group_id', 'config_msg_type'):
        context.user_data.pop(k, None)

    # Escapar para Markdown
    safe_type = msg_type.replace('_', r'\_')
    await update.message.reply_text(
        f"✅ *Mensaje de {safe_type} actualizado*\n\n"
        f"📋 Grupo: `{group_id}`\n\n"
        f"El mensaje se usará a partir de ahora.",
        parse_mode="Markdown"
    )
async def handle_edit_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        return
    if context.user_data.get('config_msg_step') and context.user_data.get('config_msg_group_id'):
        await handle_message_config_input(update, context)
        return
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
    if update.effective_user.id != SUPER_ADMIN_ID and update.effective_user.id not in EXTRA_ADMINS:
        return
    text = update.message.text.strip()
    if text.lower() == 'cancelar':
        _clear_input_states(context)
        await update.message.reply_text("❌ Edición cancelada")
        return
    field = context.user_data.get('editing_field')
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
    if update.effective_user.id not in [SUPER_ADMIN_ID] + EXTRA_ADMINS:
        await update.message.reply_text("❌ No autorizado")
        return
    if len(context.args) < 1:
        await update.message.reply_text("❌ Usa: `/getlink @username` o `/getlink ID`", parse_mode="Markdown")
        return
    identifier = context.args[0].replace("@", "")
    try:
        if identifier.isdigit():
            user_id = int(identifier)
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
                name = user.get('first_name') or user.get('username', identifier)
                await update.message.reply_text(f"🔗 Chat con {name}:\n`{chat_link}`", parse_mode="Markdown")
            else:
                await update.message.reply_text(f"❌ No se encontró @{identifier}")
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")

async def sync_all_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != SUPER_ADMIN_ID and update.effective_user.id not in EXTRA_ADMINS:
        await update.message.reply_text("❌ Solo Super Admin")
        return
    msg = "📊 *Estado actual de grupos*\n\n"
    
    groups_snapshot = get_all_groups_snapshot()
    for group in groups_snapshot:
        _, total_users = await db.get_all_active_users(group["group_id"])
        emoji = "👑" if group.get("type", "VIP") == "VIP" else "📋"
        msg += f"{emoji} *{group['group_name']}*: {total_users} usuarios activos\n"
        
    await update.message.reply_text(msg, parse_mode="Markdown")

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
    lines = [f"🔍 *Diagnóstico — {group['group_name']}*\n", f"🆔 ID: `{target_chat}`", f"📋 Tipo: {group.get('type', 'VIP')}", f"👑 Admin registrado: `{group['admin_id']}`\n"]
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
    lines += ["", "📡 *Handlers activos:*", "• ChatMemberHandler (v20+) ✅", "• NEW_CHAT_MEMBERS (fallback) ✅", "• Verificación de expirados: cada 5 min ✅"]
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def fix_trial_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != SUPER_ADMIN_ID and update.effective_user.id not in EXTRA_ADMINS:
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
                ('total', "SELECT COUNT(*) FROM users WHERE group_id=%s"),
                ('trial_true', "SELECT COUNT(*) FROM users WHERE group_id=%s AND trial_used=TRUE"),
                ('trial_false', "SELECT COUNT(*) FROM users WHERE group_id=%s AND trial_used=FALSE"),
                ('trial_null', "SELECT COUNT(*) FROM users WHERE group_id=%s AND trial_used IS NULL"),
                ('plan_trial', "SELECT COUNT(*) FROM users WHERE group_id=%s AND plan='trial'"),
            ]:
                cur.execute(q, (group_id,))
                counts[col] = cur.fetchone()[0]
            cur.execute("SELECT COUNT(DISTINCT user_id) FROM payments WHERE group_id=%s AND plan='trial'", (group_id,))
            counts['payments_trial'] = cur.fetchone()[0]
            cur.execute("""
                SELECT COUNT(*) FROM users u
                WHERE u.group_id=%s AND (u.trial_used=FALSE OR u.trial_used IS NULL)
                  AND EXISTS (SELECT 1 FROM payments p WHERE p.user_id=u.user_id AND p.group_id=u.group_id AND p.plan='trial')
            """, (group_id,))
            counts['need_fix'] = cur.fetchone()[0]
            return counts
    diag = await db._run(_diag)
    msg = (f"🔍 *Diagnóstico trial_used — Grupo {group_id}*\n\n"
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
        msg += f"✅ *Corregidos: {fixed} usuarios*"
    else:
        msg += "✅ No hay usuarios que corregir"
    await update.message.reply_text(msg, parse_mode="Markdown")

async def link_vip_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if not context.args:
        await update.message.reply_text(
            "❌ *Uso: `/linkvip free_group_id vip_group_id`*\n\n"
            "Ejemplo: `/linkvip -1001234567890 -1009876543210`\n\n"
            "💡 Esto vincula el FREE con el VIP bidireccionalmente.",
            parse_mode="Markdown"
        )
        return

    try:
        free_id = int(context.args[0])
        vip_id = int(context.args[1])
    except (ValueError, IndexError):
        await update.message.reply_text("❌ IDs inválidos. Usa: `/linkvip free_id vip_id`")
        return

    if not can_manage_group(user_id, free_id):
        await update.message.reply_text("❌ No autorizado para gestionar este grupo FREE")
        return

    free_group = get_group_by_id(free_id)
    if not free_group or free_group.get("type") != "FREE":
        await update.message.reply_text("❌ El primer grupo debe ser FREE")
        return

    vip_group = get_group_by_id(vip_id)
    if not vip_group or vip_group.get("type", "VIP") != "VIP":
        await update.message.reply_text("❌ El segundo grupo debe ser VIP")
        return

    await update_group_settings(free_id, {'linked_vip_group_id': vip_id})
    await update_group_settings(vip_id, {'linked_free_group_id': free_id})

    await update.message.reply_text(
        f"✅ *Vinculación completada*\n\n"
        f"📋 FREE: {free_group['group_name']}\n"
        f"👑 VIP: {vip_group['group_name']}\n\n"
        f"🔥 *Ahora el VIP da acceso directo al Trial.*\n"
        f"✅ Usuarios entran al VIP → Trial automático\n"
        f"📢 Se les invita al FREE en el mensaje de bienvenida",
        parse_mode="Markdown"
    )

# ==================== COMANDO PARA VINCULAR COMUNIDAD ====================
async def link_comunidad_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not context.args or len(context.args) < 2:
        await update.message.reply_text("❌ *Uso: `/linkcomunidad vip_group_id comunidad_group_id`*", parse_mode="Markdown")
        return
    try:
        vip_id = int(context.args[0])
        com_id = int(context.args[1])
    except ValueError:
        await update.message.reply_text("❌ IDs inválidos.")
        return

    if not can_manage_group(user_id, vip_id) or not can_manage_group(user_id, com_id):
        await update.message.reply_text("❌ No autorizado para gestionar estos grupos.")
        return

    vip_group = get_group_by_id(vip_id)
    com_group = get_group_by_id(com_id)
    if not vip_group or vip_group.get("type") != "VIP":
        await update.message.reply_text("❌ El primer ID debe ser un grupo VIP.")
        return
    if not com_group or com_group.get("type") != "COMUNIDAD":
        await update.message.reply_text("❌ El segundo ID debe ser un grupo COMUNIDAD (usa /addgroup para crearlo con tipo COMUNIDAD).")
        return

    await update_group_settings(vip_id, {'linked_comunidad_group_id': com_id})
    await update_group_settings(com_id, {'linked_vip_group_id': vip_id})

    await update.message.reply_text("✅ *Grupo VIP y Comunidad vinculados exitosamente.*", parse_mode="Markdown")

async def check_spin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    current_group = await require_group(update, context)
    if not current_group:
        await update.message.reply_text("❌ No autorizado. Selecciona un grupo primero con /start")
        return
    if not context.args:
        await update.message.reply_text(
            "❌ *Uso: `/checkspin @usuario` o `/checkspin ID`*\n\nVerifica si un usuario tiene descuento de ruleta.",
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
            f"👤 *{user.get('first_name', 'Usuario')}*\n\n🎰 *No ha usado la ruleta.*\nPrecio normal aplicable.",
            parse_mode="Markdown"
        )
        return
    best = spin_data.get("best_discount", 0)
    used = spin_data.get("used", False)
    msg = f"👤 *{user.get('first_name', 'Usuario')}* (`{target_id}`)\n\n🎰 *Ruleta:* {'✅ Usado' if used else '⏳ Pendiente'}\n🏆 Descuento: *{best}% OFF*\n\n"
    if not used:
        msg += _build_price_list(current_group, discounted_pct=best) + "\n\n"
        msg += f"✅ *Puedes aplicar este descuento al activar con:*\n"
        for p in ["semanal", "mensual", "anual"]:
            cfg = get_group_plan_config(current_group, p)
            price = cfg['price'] * (1 - best/100)
            msg += f"`/add {target_id} {p} {price:.2f}`\n"
    else:
        msg += f"🚫 *Descuento ya fue aplicado a:* {spin_data.get('applied_to_plan', 'N/A')}\n💰 Precio normal para renovación."
    await update.message.reply_text(msg, parse_mode="Markdown")


async def _prompt_message_config(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int, msg_type: str):
    """Función única para pedirle al admin que escriba un mensaje configurado."""
    _clear_input_states(context)
    context.user_data['config_msg_group_id'] = group_id
    context.user_data['config_msg_type'] = msg_type
    context.user_data['config_msg_step'] = 'text'

    group = get_group_by_id(group_id) if group_id != 0 else None
    current = await db.get_group_messages(group_id) if group_id != 0 else None
    current_msg = ""

    defaults = {
        "welcome": DEFAULT_WELCOME_MESSAGE,
        "rejection": DEFAULT_REJECTION_MESSAGE,
        "channel_select": DEFAULT_CHANNEL_SELECT_MESSAGE,
        "channel_description": DEFAULT_CHANNEL_DESCRIPTION,
        "vip_menu": DEFAULT_VIP_MENU_MESSAGE,
        "fuego_notice": DEFAULT_FUEGO_NOTICE,
        "expired": DEFAULT_EXPIRED_MESSAGE,
        "abandoned_1": DEFAULT_CART_MESSAGES[0] if len(DEFAULT_CART_MESSAGES) > 0 else "",
        "abandoned_2": DEFAULT_CART_MESSAGES[1] if len(DEFAULT_CART_MESSAGES) > 1 else "",
        "abandoned_3": DEFAULT_CART_MESSAGES[2] if len(DEFAULT_CART_MESSAGES) > 2 else "",
    }
    default_msg = defaults.get(msg_type, "")
    
    key_map = {
        "welcome": "welcome_message", "rejection": "rejection_message",
        "channel_description": "channel_description", "vip_menu": "vip_menu_message",
        "fuego_notice": "fuego_notice_message", "expired": "expired_message"
    }

    if msg_type.startswith("abandoned_") and group:
        step_index = int(msg_type.split("_")[1]) - 1
        msgs = group.get("settings", {}).get("abandoned_cart_messages", DEFAULT_CART_MESSAGES)
        current_msg = msgs[step_index] if step_index < len(msgs) else ""
    elif msg_type == "channel_select":
        current_msg = await db.get_global_message('channel_select_message') or ""
    elif current:
        current_msg = current.get(key_map.get(msg_type, ''), '') or ''

    group_name = group['group_name'] if group else "Global"
    text = (
        f"✏️ *Configurar mensaje de {msg_type} — {group_name}*\n\n"
        f"📋 *Actual:*\n`{current_msg or 'No configurado (usando default)'}`\n\n"
        f"📝 *Default:*\n`{default_msg[:200]}...`\n\n"
        f"Envía el nuevo mensaje. Usa las variables entre `{{}}`.\n\n"
        f"*Escribe 'default' para restaurar el mensaje por defecto.*\n"
        f"*Escribe 'cancelar' para cancelar.*"
    )

    if update.callback_query:
        query = update.callback_query
        await query.answer()
        await query.edit_message_text(text, parse_mode="Markdown")
    else:
        await update.message.reply_text(text, parse_mode="Markdown")
        
async def config_messages_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not context.args:
        await update.message.reply_text(
            "❌ *Uso: `/configmsg group_id tipo`*\n\n"
            "Tipos: `welcome`, `rejection`, `channel_description`, `vip_menu`, `fuego_notice`, `expired`, `abandoned_1`, `abandoned_2`, `abandoned_3`",
            parse_mode="Markdown"
        )
        return
    try:
        group_id = int(context.args[0])
        msg_type = context.args[1].lower() if len(context.args) > 1 else "welcome"
    except (ValueError, IndexError):
        await update.message.reply_text("❌ Formato inválido")
        return

    if msg_type == "channel_select":
        if user_id not in [SUPER_ADMIN_ID] + EXTRA_ADMINS:
            return
        await _prompt_message_config(update, context, 0, msg_type)
        return

    if not can_manage_group(user_id, group_id):
        await update.message.reply_text("❌ No autorizado")
        return
        
    group = get_group_by_id(group_id)
    if not group:
        await update.message.reply_text("❌ Grupo no encontrado")
        return

    valid_types = ["welcome", "rejection", "channel_description", "vip_menu", "fuego_notice", "expired", "abandoned_1", "abandoned_2", "abandoned_3"]
    if msg_type not in valid_types:
        await update.message.reply_text(f"❌ Tipo inválido. Usa: {', '.join(valid_types)}")
        return
        
    await _prompt_message_config(update, context, group_id, msg_type)

# ==================== PAGO ====================
async def pay_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
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
                "❌ *Uso del comando /pagar*\n\n• En un grupo VIP: escribe `/pagar`\n• En privado: `/pagar ID_DEL_GRUPO`",
                parse_mode="Markdown"
            )
            return
    group = get_group_by_id(target_group)
    if not group or group.get("type") != "VIP":
        await update.message.reply_text("❌ Grupo no encontrado o no es VIP")
        return
    
    vip_invite_link = group.get("settings", {}).get("vip_invite_link", "").strip()
    
    success = await send_payment_info(context.bot, user_id, target_group, triggered_by="comando /pagar")
    if success and update.effective_chat.type in ("group", "supergroup"):
        await update.message.reply_text("💳 *Te he enviado los datos de pago por privado.*\nRevisa tu chat conmigo.", parse_mode="Markdown")
    elif not success:
        await update.message.reply_text("❌ No se pudieron enviar los datos de pago. Asegúrate de haber iniciado chat privado conmigo primero (/start).", parse_mode="Markdown")

async def config_payment_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not context.args:
        await update.message.reply_text("❌ *Uso: `/configpago group_id`*\n\nEjemplo: `/configpago -1001234567890`", parse_mode="Markdown")
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
    if not group or group.get("type", "VIP") not in ("VIP", "COMUNIDAD"):
        await update.message.reply_text("❌ Grupo no encontrado o no es VIP/Comunidad")
        return
    _clear_input_states(context)
    context.user_data['config_payment_group_id'] = group_id
    context.user_data['config_payment_step'] = 'bank_data'
    settings = group.get("settings", {})
    current_bank = settings.get("bank_data", "")
    await update.message.reply_text(
        f"⚙️ *Configuración de Pago — {group['group_name']}*\n\n"
        f"Paso 1/4: *Datos bancarios*\nEnvía los datos de transferencia bancaria.\n\n"
        f"📋 *Actual:*\n`{current_bank or 'No configurado'}`\n\n"
        f"*Escribe 'saltar' para dejar el valor actual, o 'eliminar' para borrarlo.*",
        parse_mode="Markdown"
    )

async def config_link_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not context.args:
        await update.message.reply_text(
            "❌ *Uso: `/configlink group_id link_invitación`*\n\n"
            "Ejemplo:\n`/configlink -1001234567890 https://t.me/+AbCdEfGhIjKlMnOp`\n\n"
            "O para canal público:\n`/configlink -1001234567890 https://t.me/micanal`",
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
    if not group or group.get("type") not in ("FREE", "COMUNIDAD"):
        await update.message.reply_text("❌ Este comando solo funciona en grupos FREE o Comunidad")
        return
        
    invite_link = " ".join(context.args[1:]).strip()
    if not invite_link.startswith(("https://t.me/", "http://t.me/")):
        await update.message.reply_text("❌ Link inválido. Debe empezar con `https://t.me/`", parse_mode="Markdown")
        return
        
    if not await update_group_settings(group_id, {'invite_link': invite_link}):
        await update.message.reply_text("❌ Grupo no encontrado")
        return
        
    await update.message.reply_text(
        f"✅ *Link configurado*\n\n📋 Grupo: {group['group_name']}\n🔗 Link: `{invite_link}`\n\n"
        f"Los usuarios podrán unirse al canal desde el botón.",
        parse_mode="Markdown"
    )

async def config_fuego_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if len(context.args) < 2:
        await update.message.reply_text(
            "❌ *Uso: `/configfuego group_id @username`*\n\n"
            "Ejemplo: `/configfuego -1001234567890 @FuegoBot`\n\n"
            "Configura el contacto de Fuego para que los usuarios sepan a quién escribir para descargas.",
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
    if not group or group.get("type") != "VIP":
        await update.message.reply_text("❌ Grupo no encontrado o no es VIP")
        return
    fuego_username = context.args[1].lstrip('@').strip()
    if not await update_group_settings(group_id, {'fuego_contact': fuego_username}):
        await update.message.reply_text("❌ Grupo no encontrado")
        return
        
    await update.message.reply_text(
        f"✅ *Contacto de Fuego configurado*\n\n📋 Grupo: {group['group_name']}\n🤖 Fuego: @{fuego_username}\n\n"
        f"Los usuarios recibirán este contacto cuando soliciten descargas.",
        parse_mode="Markdown"
    )

async def handle_payment_config_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        return
    step = context.user_data.get('config_payment_step')
    group_id = context.user_data.get('config_payment_group_id')
    if not step or not group_id:
        return
    if not can_manage_group(update.effective_user.id, group_id):
        return
    text = update.message.text.strip()
    
    if step == 'bank_data':
        changes = {}
        if text.lower() not in ('saltar',):
            if text.lower() == 'eliminar':
                changes['bank_data'] = None
            else:
                changes['bank_data'] = text
                
        if changes and not await update_group_settings(group_id, changes):
            await update.message.reply_text("❌ Grupo no encontrado")
            context.user_data.pop('config_payment_step', None)
            context.user_data.pop('config_payment_group_id', None)
            return
            
        context.user_data['config_payment_step'] = 'payment_contact'
        group = get_group_by_id(group_id)
        current_contact = group.get("settings", {}).get("payment_contact", "") if group else ""
        await update.message.reply_text(
            f"✅ *Datos bancarios guardados.*\n\nPaso 2/3: *Contacto para comprobantes*\n"
            f"Envía el username de Telegram.\n\n📋 *Actual:* `{current_contact or 'No configurado'}`\n\n"
            f"*Escribe 'saltar' para dejar el valor actual.*",
            parse_mode="Markdown"
        )
        
    elif step == 'payment_contact':
        changes = {}
        if text.lower() != 'saltar':
            changes['payment_contact'] = text.lstrip('@').strip()
            
        if changes and not await update_group_settings(group_id, changes):
            await update.message.reply_text("❌ Grupo no encontrado")
            context.user_data.pop('config_payment_step', None)
            context.user_data.pop('config_payment_group_id', None)
            return
            
        context.user_data['config_payment_step'] = 'paypal_data'
        group = get_group_by_id(group_id)
        current_paypal = group.get("settings", {}).get("paypal_data", "") if group else ""
        await update.message.reply_text(
            f"✅ *Contacto guardado.*\n\nPaso 3/3: *PayPal (opcional)*\n"
            f"Envía los datos de PayPal o escribe 'saltar' / 'eliminar'.\n\n"
            f"📋 *Actual:* `{current_paypal or 'No configurado'}`",
            parse_mode="Markdown"
        )
        
    elif step == 'paypal_data':
        changes = {}
        if text.lower() not in ('saltar',):
            if text.lower() == 'eliminar':
                changes['paypal_data'] = None
            else:
                changes['paypal_data'] = text
                
        if changes and not await update_group_settings(group_id, changes):
            await update.message.reply_text("❌ Grupo no encontrado")
            context.user_data.pop('config_payment_step', None)
            context.user_data.pop('config_payment_group_id', None)
            return
            
        context.user_data['config_payment_step'] = 'vip_invite_link'
        group = get_group_by_id(group_id)
        current_vip_link = group.get("settings", {}).get("vip_invite_link", "") if group else ""
        await update.message.reply_text(
            f"✅ *PayPal guardado.*\n\nPaso 4/4: *Link de invitación al VIP*\n"
            f"Envía el link de invitación al grupo VIP (para el botón 'Entrar al VIP').\n\n"
            f"📋 *Actual:* `{current_vip_link or 'No configurado'}`\n\n"
            f"*Escribe 'saltar' para dejar el valor actual, 'eliminar' para borrarlo, o pega el link de invitación.*\n\n"
            f"💡 *Para obtener el link:* Ve al grupo VIP → Administradores → Invitar por link → Copiar",
            parse_mode="Markdown"
        )
        
    elif step == 'vip_invite_link':
        changes = {}
        if text.lower() not in ('saltar', 'eliminar'):
            if not text.startswith(("https://", "http://")):
                await update.message.reply_text(
                    "❌ *Link inválido.*\nEl link debe empezar con `https://` o `http://`\n\n"
                    "Envía el link de invitación o 'saltar' / 'eliminar'.",
                    parse_mode="Markdown"
                )
                return
            changes['vip_invite_link'] = text
        elif text.lower() == 'eliminar':
            changes['vip_invite_link'] = None
            
        if changes and not await update_group_settings(group_id, changes):
            await update.message.reply_text("❌ Grupo no encontrado")
            context.user_data.pop('config_payment_step', None)
            context.user_data.pop('config_payment_group_id', None)
            return
            
        context.user_data.pop('config_payment_step', None)
        context.user_data.pop('config_payment_group_id', None)
        
        group = get_group_by_id(group_id)
        settings = group.get("settings", {}) if group else {}
        vip_link_status = settings.get('vip_invite_link', 'No configurado')
        await update.message.reply_text(
            f"✅ *Configuración de pago completada*\n\n📌 *Grupo:* {group['group_name'] if group else 'Desconocido'}\n\n"
            f"🏦 *Bancaria:*\n`{settings.get('bank_data', 'No configurado')}`\n\n"
            f"📤 *Contacto:* @{settings.get('payment_contact', 'No configurado')}\n\n"
            f"🅿️ *PayPal:* `{settings.get('paypal_data', 'No configurado')}`\n\n"
            f"🔥 *Link VIP:* `{vip_link_status}`\n\n"
            f"Los usuarios verán el botón 'Entrar al VIP' en la información de pago.",
            parse_mode="Markdown"
        )

async def config_payment_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query
    await query.answer()
    _clear_input_states(context)
    if not can_manage_group(query.from_user.id, group_id):
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
        f"⚙️ *Configuración de Pago — {group['group_name']}*\n\n"
        f"Paso 1/4: *Datos bancarios*\nEnvía los datos de transferencia bancaria.\n\n"
        f"📋 *Actual:*\n`{current_bank or 'No configurado'}`\n\n"
        f"*Escribe 'saltar' para dejar el valor actual, o 'eliminar' para borrarlo.*",
        parse_mode="Markdown"
    )

# ==================== RULETA VIP ====================
_SPINNING: set = set()
_SPINNING_LOCK = asyncio.Lock()

async def spin_discount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
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

    async with _SPINNING_LOCK:
        if (user_id, group_id) in _SPINNING:
            await query.answer("⏳ Procesando...", show_alert=True)
            return
        _SPINNING.add((user_id, group_id))

    try:
        spin_data = await db.get_spin_data(user_id, group_id)

        if spin_data.get("spin_count", 0) >= 1:
            best = spin_data.get("best_discount", 0)
            chat_link = f"tg://user?id={user_id}"
            await _safe_send(
                group["admin_id"],
                f"⚠️ *Intento de ruleta repetida*\n\n"
                f"👤 Usuario: [{query.from_user.first_name or 'Usuario'}]({chat_link})\n"
                f"🆔 ID: `{user_id}`\n📌 Grupo: {group['group_name']}\n\n"
                f"🎰 Este usuario ya giró la ruleta ({best}% OFF).\n"
                f"{'✅ Descuento ya fue aplicado.' if spin_data.get('used') else '⏳ Descuento aún pendiente.'}\n\n"
                f"Verifica con: `/checkspin {user_id}`",
                parse_mode="Markdown", disable_notification=True
            )
            if spin_data.get("used"):
                await query.edit_message_text(
                    f"🎰 *Ruleta VIP — {group['group_name']}*\n\n"
                    f"Ya usaste tu spin y tu descuento del *{best}%* fue aplicado.\n\n"
                    f"🚫 *La ruleta es 1 vez por usuario.*\n"
                    f"Para renovar, paga al precio normal:\n\n"
                    + _build_price_list(group_id)
                    + "\n\n💳 Presiona el botón para ver datos de pago:",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("💳 Pagar Acceso", callback_data=f"pay_{group_id}_{user_id}")]]),
                    parse_mode="Markdown"
                )
            else:
                await query.edit_message_text(
                    f"🎰 *Ruleta VIP — {group['group_name']}*\n\n"
                    f"Ya giraste la ruleta. Tu descuento es: *{best}% OFF*\n\n"
                    f"⏳ ¿Aún no lo usaste? Contacta a @{group.get('settings', {}).get('payment_contact', 'admin')}\n\n"
                    f"💳 Presiona para pagar con tu descuento:",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("💳 Info de Pago con Descuento", callback_data=f"pay_{group_id}_{user_id}")]]),
                    parse_mode="Markdown"
                )
            return

        discounts = [10]*50 + [15]*30 + [20]*15 + [25]*3 + [30]*1 + [50]*1
        discount = random.choice(discounts)
        _fire_and_forget(db.record_spin(user_id, group_id, discount), "record_spin")

        for msg in ["🎰 Girando...", "🎰 Girando... 🎲", "🎰 Girando... 🎲 🎲"]:
            try:
                await query.edit_message_text(msg)
                await asyncio.sleep(0.8)
            except Exception:
                pass

        price_lines = _build_price_list(group_id, discounted_pct=discount)
        await query.edit_message_text(
            f"🎰 *¡RULETA VIP — {group['group_name']}*\n\n"
            f"@{query.from_user.username or query.from_user.first_name} giró la ruleta...\n\n"
            f"🎉 *¡FELICIDADES!*\n\n🏆 *Descuento: {discount}% OFF*\n\n"
            f"💰 *Precios con tu descuento:*\n" + price_lines + "\n\n"
            f"⏳ *Descuento permanente* — puedes usarlo cuando quieras\n"
            f"📤 Envía el comprobante + captura de este mensaje a:\n"
            f"@{group.get('settings', {}).get('payment_contact', 'admin')}\n\n"
            f"_Solo puedes usar la ruleta 1 vez. ¡No lo pierdas!_",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("💳 Información de Pago", callback_data=f"pay_{group_id}_{user_id}")]]),
            parse_mode="Markdown"
        )
        # Alerta al Admin
        await _safe_send(
            group["admin_id"],
            f"🎰 *ALERTA DE RULETA VIP*\n\n"
            f"👤 Usuario: {query.from_user.first_name}\n"
            f"🆔 ID: `{user_id}`\n"
            f"🏆 Descuento: *{discount}% OFF*\n\n"
            f"💡 *Responde a este mensaje con /info para ver detalles.*",
            parse_mode="Markdown"
        )

    finally:
        async with _SPINNING_LOCK:
            _SPINNING.discard((user_id, group_id))

async def spin_used_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("🚫 Ya usaste tu ruleta. Si tienes dudas, contacta al admin.", show_alert=True)
    parts = query.data.split("_")
    if len(parts) >= 4:
        group_id = int(parts[2])
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

# ==================== SISTEMA DE AVISOS ====================
async def send_trial_warnings():
    if not bot_app:
        return
    logger.info("⏰ Enviando avisos de expiración de trial...")
    
    semaphore = asyncio.Semaphore(25)
    groups_snapshot = [g for g in get_all_groups_snapshot() if g.get("type", "VIP") in ("VIP", "COMUNIDAD")]
    
    if not groups_snapshot:
        return

    # 1. Agrupar todas las reglas activas por 'minutes_before' para minimizar queries
    rules_by_minutes = {}
    for group in groups_snapshot:
        group_id = group["group_id"]
        warning_config = await db.get_warning_config(group_id)
        
        if not warning_config or not warning_config.get("enabled", False):
            continue
            
        for warning in warning_config.get("warnings", []):
            minutes_before = warning.get("minutes_before", 15)
            message_template = warning.get("message", "⏳ Tu trial expira pronto")
            
            if minutes_before not in rules_by_minutes:
                rules_by_minutes[minutes_before] = []
                
            rules_by_minutes[minutes_before].append({
                "group_id": group_id,
                "group_name": group.get("group_name", "VIP"),
                "message_template": message_template,
                "vip_invite_link": group.get("settings", {}).get("vip_invite_link", "")
            })

    # 2. Por cada conjunto de minutos, hacer 1 sola query multi-grupo
    for minutes_before, rules in rules_by_minutes.items():
        group_ids = [r["group_id"] for r in rules]
        warning_type = f"warning_{minutes_before}min"
        
        try:
            users = await db.get_users_for_warning_multi(group_ids, minutes_before)
        except Exception as e:
            logger.error(f"❌ Error obteniendo usuarios para aviso de {minutes_before}min: {e}")
            continue
            
        if not users:
            continue
            
        # Agrupar usuarios por grupo para optimizar la carga de la ruleta en batch
        users_by_group = {}
        for u in users:
            users_by_group.setdefault(u['group_id'], []).append(u)
            
        spin_data_map = {}
        for gid, g_users in users_by_group.items():
            user_ids = [u['user_id'] for u in g_users]
            spin_data_map[gid] = await db.get_spin_data_batch(gid, user_ids)
            
        # Mapeo rápido de reglas por group_id
        rules_map = {r["group_id"]: r for r in rules}

        async def _send_warning(user):
            async with semaphore:
                user_id = user['user_id']
                gid = user['group_id']
                rule = rules_map.get(gid)
                if not rule:
                    return
                    
                end_date = normalize_dt(user['end_date'])
                remaining = end_date - now_utc()
                mins_left = max(0, int(remaining.total_seconds() / 60))
                if mins_left <= 0:
                    return
                    
                expiry_str = fmt_dt(user['end_date'])
                expiry_time = expiry_str.split(' ')[1] if ' ' in expiry_str else expiry_str
                
                # ✅ Usar el nuevo helper que escapa automáticamente
                message = render_template(rule["message_template"], {
                    "minutes_left": mins_left,
                    "expiry_time": expiry_time,
                    "expiry_datetime": expiry_str,
                    "group_name": rule["group_name"],
                    "user_name": user.get("first_name", "Usuario") or "Usuario"
                })
                
                try:
                    spin_data = spin_data_map.get(gid, {}).get(user_id, {})
                    spin_used = spin_data.get("used", False) if spin_data.get("spin_count", 0) > 0 else False
                    await bot_app.bot.send_message(
                        user_id, message,
                        reply_markup=get_payment_keyboard(gid, user_id, spin_used=spin_used, vip_invite_link=rule["vip_invite_link"]),
                        parse_mode="Markdown"
                    )
                    _fire_and_forget(db.log_warning_sent(user_id, gid, warning_type), "log_warning")
                    logger.info(f"⚠️ Aviso enviado a {user_id}: {mins_left} min restantes en grupo {gid}")
                except BadRequest as e:
                    # ✅ Guard anti-loop: si la plantilla del admin rompe Markdown, 
                    # marcamos como enviado para no spamear el log ni reintentar cada minuto.
                    logger.error(f"❌ Markdown inválido en aviso para {user_id} (grupo {gid}): {e}")
                    _fire_and_forget(db.log_warning_sent(user_id, gid, warning_type), "log_warning_guard")
                except Exception as e:
                    logger.warning(f"No se pudo enviar aviso a {user_id}: {e}")

        # Procesar en chunks para evitar saturación de memoria
        chunk_size = 100
        for i in range(0, len(users), chunk_size):
            chunk = users[i:i+chunk_size]
            await asyncio.gather(*[_send_warning(u) for u in chunk])

async def menu_warning_settings(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
    query = update.callback_query
    await query.answer()
    group = get_group_by_id(group_id)
    if not group or not can_manage_group(query.from_user.id, group_id):
        await query.edit_message_text("❌ No autorizado o grupo no encontrado")
        return
    warning_config = await db.get_warning_config(group_id)
    enabled = warning_config.get("enabled", False)
    warnings = warning_config.get("warnings", [])
    status_emoji = "🟢" if enabled else "🔴"
    status_text = "ACTIVADOS" if enabled else "DESACTIVADOS"
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
        [InlineKeyboardButton(f"{status_emoji} {'Desactivar' if enabled else 'Activar'} avisos", callback_data=f"warn_toggle_{group_id}")],
        [InlineKeyboardButton("➕ Añadir aviso", callback_data=f"warn_add_{group_id}")],
    ]
    for i, w in enumerate(warnings):
        keyboard.append([
            InlineKeyboardButton(f"✏️ Editar {w.get('minutes_before', 15)}min", callback_data=f"warn_edit_{group_id}_{i}"),
            InlineKeyboardButton("🗑️ Eliminar", callback_data=f"warn_delete_{group_id}_{i}")
        ])
    keyboard.append([InlineKeyboardButton("🧪 Probar aviso", callback_data=f"warn_test_{group_id}")])
    keyboard.append(_back_button(f"cfg_group_{group_id}"))
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
    _clear_input_states(context)
    if not can_manage_group(query.from_user.id, group_id):
        await query.answer("❌ No autorizado", show_alert=True)
        return
    context.user_data.update({'warning_group_id': group_id, 'warning_step': 'minutes', 'warning_index': None})
    await query.edit_message_text(
        "➕ *Nuevo aviso de expiración*\n\nEnvía cuántos minutos antes debe enviarse el aviso.\n\n"
        "*Ejemplos:* `15`, `60`, `5`\n\n*Escribe 'cancelar' para cancelar.*",
        parse_mode="Markdown"
    )

async def edit_warning_step1(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int, warning_index: int):
    query = update.callback_query
    await query.answer()
    _clear_input_states(context)
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
    test_w = warnings[0]
    msg_tpl = test_w.get("message", "⏳ Tu trial expira pronto")
    group = get_group_by_id(group_id)
    
    message = render_template(msg_tpl, {
        "minutes_left": "15",
        "expiry_time": "22:45",
        "expiry_datetime": "15/06/2026 22:45",
        "group_name": group.get("group_name", "VIP") if group else "VIP",
        "user_name": query.from_user.first_name or "Usuario"
    })
    
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
    step = context.user_data.get('warning_step')
    group_id = context.user_data.get('warning_group_id')
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
    warnings = warning_config.get("warnings", [])

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
        changed = False
        if text.lower() == 'saltar' and step == 'edit_message':
            pass
        else:
            msg_text = update.message.text.strip()
            if step == 'edit_message' and warning_index is not None and warning_index < len(warnings):
                if warnings[warning_index].get("message") != msg_text:
                    warnings[warning_index]["message"] = msg_text
                    changed = True
            else:
                minutes = context.user_data.get('warning_minutes', 15)
                warnings.append({"minutes_before": minutes, "message": msg_text})
                warnings.sort(key=lambda w: w.get("minutes_before", 0), reverse=True)
                changed = True
        if changed:
            warning_config["warnings"] = warnings
            await db.save_warning_config(group_id, warning_config)
        for k in ('warning_step', 'warning_group_id', 'warning_index', 'warning_minutes'):
            context.user_data.pop(k, None)
        keyboard = [_back_button(f"cfg_warnings_{group_id}")]
        await update.message.reply_text("✅ *Aviso guardado correctamente*", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

# ==================== BROADCAST ====================
async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not context.args:
        await update.message.reply_text("❌ *Uso: `/broadcast ID_DEL_GRUPO`*\n\nEjemplo: `/broadcast -1001234567890`", parse_mode="Markdown")
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
    _clear_input_states(context)
    context.user_data.update({'broadcast_group_id': group_id, 'broadcast_step': 'filter'})
    keyboard = [
        [InlineKeyboardButton("🆓 Solo trial SIN compra", callback_data=f"bc|filter|{group_id}|trial_only")],
        [InlineKeyboardButton("💳 Solo con suscripción activa", callback_data=f"bc|filter|{group_id}|subscribed_only")],
        [InlineKeyboardButton("⏰ Solo expirados no renovados", callback_data=f"bc|filter|{group_id}|expired_only")],
        [InlineKeyboardButton("👥 TODOS los que usaron trial", callback_data=f"bc|filter|{group_id}|all_trial")],
        [InlineKeyboardButton("❌ Cancelar", callback_data=f"bc|cancel|{group_id}")]
    ]
    await update.message.reply_text(
        f"📢 *Broadcast Masivo — {group['group_name']}*\n\nSelecciona a quién enviar el mensaje:",
        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
    )

async def broadcast_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    await query.answer()
    if not data.startswith("bc|"):
        return
    parts = data.split("|")
    if len(parts) < 3:
        await query.answer("❌ Error en datos", show_alert=True)
        return
    action = parts[1]
    try:
        group_id = int(parts[2])
    except ValueError:
        await query.answer("❌ ID de grupo inválido", show_alert=True)
        return

    if action == "filter":
        if len(parts) < 4:
            await query.answer("❌ Filtro no especificado", show_alert=True)
            return
        filter_type = parts[3]
        valid_filters = ['trial_only', 'subscribed_only', 'expired_only', 'all_trial']
        if filter_type not in valid_filters:
            await query.answer(f"❌ Filtro inválido: {filter_type}", show_alert=True)
            return
        context.user_data.update({'broadcast_group_id': group_id, 'broadcast_filter': filter_type, 'broadcast_step': 'message'})
        targets = await db.get_broadcast_targets(group_id, filter_type)
        filter_names = {
            'trial_only': '🆓 Solo trial sin compra', 'subscribed_only': '💳 Solo con suscripción activa',
            'expired_only': '⏰ Solo expirados no renovados', 'all_trial': '👥 Todos los que usaron trial'
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
    elif action == "cancel":
        for k in ('broadcast_group_id', 'broadcast_step', 'broadcast_filter', 'broadcast_message'):
            context.user_data.pop(k, None)
        await query.edit_message_text("❌ Broadcast cancelado")
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
            "🚀 *Enviando broadcast...*\n\nEsto puede tomar varios minutos. Te notificaré cuando termine.",
            parse_mode="Markdown"
        )
        await execute_broadcast(context.bot, group_id, filter_type, message_text, query.from_user.id)
    elif action == "history":
        await broadcast_history_callback(update, context, group_id)
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
    trial_only = await db.get_broadcast_targets(group_id, 'trial_only')
    subscribed = await db.get_broadcast_targets(group_id, 'subscribed_only')
    expired = await db.get_broadcast_targets(group_id, 'expired_only')
    all_trial = await db.get_broadcast_targets(group_id, 'all_trial')
    keyboard = [
        [InlineKeyboardButton(f"🆓 Solo sin compra ({len(trial_only)})", callback_data=f"bc|filter|{group_id}|trial_only")],
        [InlineKeyboardButton(f"💳 Solo suscriptos ({len(subscribed)})", callback_data=f"bc|filter|{group_id}|subscribed_only")],
        [InlineKeyboardButton(f"⏰ Solo expirados ({len(expired)})", callback_data=f"bc|filter|{group_id}|expired_only")],
        [InlineKeyboardButton(f"👥 Todos ({len(all_trial)})", callback_data=f"bc|filter|{group_id}|all_trial")],
        [InlineKeyboardButton("📜 Historial", callback_data=f"bc|history|{group_id}")],
        _back_button(f"select_group_{group_id}"),
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
    if group_id is None:
        data = query.data
        if data.startswith("bc|history|"):
            try:
                group_id = int(data.split("|")[2])
            except (ValueError, IndexError):
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
            'trial_only': '🆓 Sin compra', 'subscribed_only': '💳 Suscriptos',
            'expired_only': '⏰ Expirados', 'all_trial': '👥 Todos'
        }
        msg = "📜 *Historial de Broadcasts*\n\n"
        for i, h in enumerate(history, 1):
            f_name = filter_names.get(h['filter_type'], h['filter_type'])
            date_str = fmt_dt(h['sent_at'])
            msg += (f"{i}. *{f_name}* | {date_str}\n"
                    f"   ✅ {h['success_count']} enviados | ❌ {h['fail_count']} fallidos\n"
                    f"   📝 {h['message_preview'] or 'Sin preview'}...\n\n")
    keyboard = [_back_button(f"bc|menu|{group_id}")]
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def handle_broadcast_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        return
    step = context.user_data.get('broadcast_step')
    group_id = context.user_data.get('broadcast_group_id')
    filter_type = context.user_data.get('broadcast_filter')
    if step != 'message' or not group_id or not filter_type:
        return

    text = update.message.text.strip()

    if text.lower() == 'cancelar':
        _clear_input_states(context)
        await update.message.reply_text("❌ Broadcast cancelado")
        return

    try:
        await context.bot.send_message(
            update.effective_user.id,
            f"🧪 *Vista previa (Markdown):*\n\n{text}",
            parse_mode="Markdown"
        )
    except Exception:
        await update.message.reply_text(
            "⚠️ El mensaje tiene Markdown inválido. "
            "Revisa caracteres como `_`, `*`, `[` sin cerrar.\n\n"
            "Corrige y reenvía el mensaje, o escribe 'cancelar'."
        )
        return

    context.user_data['broadcast_message'] = text
    context.user_data['broadcast_step'] = 'confirm'

    targets = await db.get_broadcast_targets(group_id, filter_type)

    preview_msg = (
        text
        .replace("{user_name}", "Juan")
        .replace("{group_name}", "VIP")
        .replace("{username}", "@juan")
    )
    
    if len(preview_msg) > 300:
        preview_msg = preview_msg[:300] + "..."

    filter_names = {
        'trial_only':       '🆓 Solo trial sin compra',
        'subscribed_only':  '💳 Solo con suscripción activa',
        'expired_only':     '⏰ Solo expirados no renovados',
        'all_trial':        '👥 Todos los que usaron trial',
    }

    keyboard = [
        [InlineKeyboardButton("✅ Sí, enviar ahora", callback_data=f"bc|confirm|{group_id}|{filter_type}")],
        [InlineKeyboardButton("✏️ Editar mensaje", callback_data=f"bc|filter|{group_id}|{filter_type}")],
        [InlineKeyboardButton("❌ Cancelar", callback_data=f"bc|cancel|{group_id}")]
    ]

    header = (
        f"📢 *Confirmar Broadcast*\n\n"
        f"Filtro: {filter_names.get(filter_type, filter_type)}\n"
        f"Destinatarios: *{len(targets)}* usuarios\n\n"
        f"⚠️ ¿Estás seguro?"
    )

    try:
        await update.message.reply_text(header, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
        await update.message.reply_text(f"Vista previa (variables reemplazadas):\n\n{preview_msg}")
    except Exception as e:
        logger.warning(f"Error enviando confirmación de broadcast: {e}")
        await update.message.reply_text(
            "⚠️ No se pudo mostrar la vista previa (posible Markdown inválido en el mensaje).\n\n"
            "¿Deseas enviarlo de todas formas?",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

async def execute_broadcast(bot, group_id: int, filter_type: str, message_text: str, sent_by: int):
    targets = await db.get_broadcast_targets(group_id, filter_type)
    if not targets:
        await bot.send_message(
            sent_by,
            "📭 *Broadcast completado*\n\nNo hay usuarios con el filtro seleccionado.",
            parse_mode="Markdown",
        )
        return

    group = get_group_by_id(group_id)
    group_name = safe_name(group.get('group_name', 'VIP')) if group else 'VIP'
    total = len(targets)

    success_count = 0
    fail_count = 0
    processed = 0

    semaphore = asyncio.Semaphore(25)

    try:
        progress_msg = await bot.send_message(
            sent_by,
            f"🚀 *Broadcast en progreso...*\n\n• Total: {total} usuarios\n• Enviados: 0",
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.error(f"❌ No se pudo enviar mensaje de progreso: {e}")
        progress_msg = None

    async def _send_one(user):
        nonlocal success_count, fail_count, processed
        async with semaphore:
            user_id = user['user_id']
            display_raw = user.get('first_name', 'Usuario') or 'Usuario'
            group_name_raw = group.get('group_name', 'VIP') if group else 'VIP'
            
            personalized = render_template(message_text, {
                "user_name": display_raw,
                "group_name": group_name_raw,
                "username": user.get('username', '')
            })

            try:
                await bot.send_message(
                    user_id, personalized,
                    parse_mode="Markdown",
                    disable_web_page_preview=True,
                )
                success_count += 1
            except Exception:
                # Fallback: si falla el Markdown (a pesar del safe_name), 
                # intenta enviarlo como texto plano sin parse_mode
                try:
                    await bot.send_message(
                        user_id, personalized,
                        disable_web_page_preview=True,
                    )
                    success_count += 1
                except Exception:
                    fail_count += 1
            processed += 1
    
    chunk_size = 100
    for i in range(0, total, chunk_size):
        chunk = targets[i : i + chunk_size]
        await asyncio.gather(*[_send_one(u) for u in chunk])

        if progress_msg:
            try:
                pct = int(processed / total * 100)
                await bot.edit_message_text(
                    f"🚀 *Broadcast en progreso...*\n\n"
                    f"• Total: {total}\n"
                    f"• ✅ Enviados: {success_count}\n"
                    f"• ❌ Fallidos: {fail_count}\n"
                    f"• 📊 Progreso: {pct}%",
                    chat_id=sent_by,
                    message_id=progress_msg.message_id,
                    parse_mode="Markdown",
                )
            except Exception:
                pass

        await asyncio.sleep(0.5)

    _fire_and_forget(
        db.log_broadcast_sent(
            group_id, filter_type, message_text,
            total, success_count, fail_count, sent_by,
        ),
        "log_broadcast",
    )
    await bot.send_message(
        sent_by,
        f"✅ *Broadcast completado*\n\n"
        f"• Total: {total}\n"
        f"• ✅ Enviados: {success_count}\n"
        f"• ❌ Fallidos: {fail_count}",
        parse_mode="Markdown",
    )

async def process_abandoned_carts():
    if not bot_app:
        return
    logger.info("🛒 Verificando secuencias de carrito abandonado...")
    
    groups_snapshot = get_all_groups_snapshot()
    
    for group in groups_snapshot:
        group_id = group["group_id"]
        custom_msgs = group.get("settings", {}).get("abandoned_cart_messages", DEFAULT_CART_MESSAGES)
        
        # Paso 1: Usa el delay configurado por el admin (por defecto 30 si no hay)
        step1_delay = group.get("settings", {}).get("cart_delay_minutes", 30)
        # Paso 2: 24 horas después del paso 1. Paso 3: 72 horas después del paso 2.
        step_delays = {1: step1_delay, 2: 1440, 3: 4320}
        
        for step, delay in step_delays.items():
            pending_carts = await db.get_pending_abandoned_carts_by_step(group_id, step - 1, delay)
            if not pending_carts:
                continue
                
            for cart in pending_carts:
                user_id = cart['user_id']
                
                # 1. Verificar si ya pagó (si está activo y no es trial)
                user = await db.get_user_by_id(user_id, group_id)
                if user and user.get('status') == 'active' and user.get('plan') not in ('trial', 'FREE'):
                    await db.update_cart_step(user_id, group_id, 99)
                    continue
                    
                # 2. Enviar el mensaje correspondiente al paso
                msg_index = step - 1
                if msg_index < len(custom_msgs):
                    msg_text = custom_msgs[msg_index]
                    msg_text = format_message(msg_text, {
                        'group_name': group.get("group_name", "VIP"),
                        'admin_id': group.get("admin_id")
                    })
                    
                    keyboard = [[InlineKeyboardButton("💳 Ver Datos de Pago", callback_data=f"pay_{group_id}_{user_id}")]]
                    try:
                        await bot_app.bot.send_message(
                            user_id, 
                            msg_text, 
                            reply_markup=InlineKeyboardMarkup(keyboard), 
                            parse_mode="Markdown"
                        )
                        await db.update_cart_step(user_id, group_id, step)
                    except Exception as e:
                        logger.warning(f"No se pudo enviar carrito paso {step} a {user_id}: {e}")
                        await db.update_cart_step(user_id, group_id, step)
# ==================== TAREAS PROGRAMADAS ====================
async def check_expired_subscriptions():
    if not bot_app: return
    logger.info("🔍 Verificando suscripciones y trials expirados...")
    groups_to_check = [g for g in get_all_groups_snapshot() if g.get("type", "VIP") in ("VIP", "COMUNIDAD")]

    sem = asyncio.Semaphore(5)

    async def _process_group(group: dict):
        async with sem:
            group_id = group["group_id"]
            group_type = group.get("type", "VIP")

            expired_users = await db.get_expired_users(group_id)
            if not expired_users:
                return
            user_ids = [u['user_id'] for u in expired_users]
            updated_ids = await db.expire_users_batch(user_ids, group_id)
            if not updated_ids:
                return
            updated_ids_set = set(updated_ids)

            custom = await db.get_group_messages(group_id)
            expired_msg_template = DEFAULT_EXPIRED_MESSAGE
            if custom and custom.get('expired_message'):
                expired_msg_template = custom['expired_message']

            for user in expired_users:
                if user['user_id'] not in updated_ids_set:
                    continue
                user_id = user['user_id']
                display = safe_name(user.get('first_name') or f"Usuario {user_id}")
                chat_link = f"tg://user?id={user_id}"

                if group_type == "VIP":
                    ok = await _kick_user_with_retry(group_id, user_id)
                    action_text = "expulsado del VIP" if ok else "⚠️ no se pudo expulsar del VIP"
                    emoji = "🚫"
                else:  # COMUNIDAD
                    ok = await _mute_user_with_retry(group_id, user_id)
                    action_text = "silenciado en Comunidad" if ok else "⚠️ no se pudo silenciar en Comunidad"
                    emoji = "🔇"

                await _safe_send(
                    group["admin_id"],
                    f"{emoji} *Suscripción/Trial Expirado*\n\n"
                    f"👤 [{display}]({chat_link})\n"
                    f"🆔 *ID:* `{user_id}`\n"
                    f"📌 *Grupo:* {safe_name(group['group_name'])}\n"
                    f"✅ Usuario {safe_name(action_text)}.",
                    parse_mode="Markdown"
                )
                expired_msg = format_message(expired_msg_template, {'group_name': safe_name(group['group_name'])})
                await _safe_send(user_id, expired_msg, reply_markup=get_payment_keyboard(group_id, user_id), parse_mode="Markdown")

    await asyncio.gather(*[_process_group(g) for g in groups_to_check], return_exceptions=True)
    
# ==================== CALLBACK HANDLER ====================
async def _handle_back_to_admin(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str):
    query = update.callback_query
    await query.answer()
    try:
        await query.message.delete()
    except Exception:
        pass
    await start(update, context)

async def _handle_pay_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str):
    query = update.callback_query
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

async def _handle_add_user_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str):
    query = update.callback_query
    await query.answer()
    await update.callback_query.message.reply_text("📝 Usa: `/add @username plan` o `/add ID plan`", parse_mode="Markdown")

async def _handle_add_group_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str):
    query = update.callback_query
    await query.answer()
    await update.callback_query.message.reply_text('📝 Usa: `/addgroup group_id TIPO "nombre" admin_id`', parse_mode="Markdown")

async def _handle_no_free_link(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str):
    await update.callback_query.answer("❌ El admin no ha configurado el link del canal", show_alert=True)

CALLBACK_EXACT = {
    "add_user": _handle_add_user_callback,
    "earnings": lambda u, c, d: show_earnings(u, c),
    "export_month": lambda u, c, d: export_report(u, c),
    "list_potential": lambda u, c, d: list_potential_clients(u, c),
    "export_clients": lambda u, c, d: export_clients(u, c),
    "vip_groups": lambda u, c, d: view_vip_groups(u, c),
    "view_vip_groups": lambda u, c, d: view_vip_groups(u, c),
    "free_groups": lambda u, c, d: view_free_groups(u, c),
    "view_free_groups": lambda u, c, d: view_free_groups(u, c),
    "comunidad_groups": lambda u, c, d: view_comunidad_groups(u, c),
    "view_comunidad_groups": lambda u, c, d: view_comunidad_groups(u, c),
    "total_earnings": lambda u, c, d: total_earnings(u, c),
    "all_groups": lambda u, c, d: list_groups(u, c),
    "groups": lambda u, c, d: list_groups(u, c),
    "add_group": _handle_add_group_callback,
    "back_to_admin": _handle_back_to_admin,
    "menu_groups": lambda u, c, d: menu_groups(u, c),
    "menu_view_groups": lambda u, c, d: menu_view_groups(u, c),
    "menu_edit_group_select": lambda u, c, d: menu_edit_group_select(u, c),
    "menu_delete_group_select": lambda u, c, d: menu_delete_group_select(u, c),
    "menu_commands": lambda u, c, d: menu_commands(u, c),
    "broadcast_menu": lambda u, c, d: broadcast_menu_callback(u, c),
    "trial_stats": lambda u, c, d: trial_stats(u, c),
    "no_free_link": _handle_no_free_link,
    "back_to_start": lambda u, c, d: start(u, c),
}

CALLBACK_PREFIXES = [
    ("select_group_", lambda u, c, d: select_group(u, c, int(d.replace("select_group_", "")))),
    ("delete_confirm_", lambda u, c, d: delete_group_confirm(u, c, int(d.replace("delete_confirm_", "")))),
    ("delete_yes_", lambda u, c, d: delete_group_execute(u, c, int(d.replace("delete_yes_", "")))),
    ("multi_apply_", lambda u, c, d: multi_apply_changes(u, c, int(d.replace("multi_apply_", "")))),
    ("multi_name_", lambda u, c, d: multi_name_request(u, c, int(d.replace("multi_name_", "")))),
    ("multi_admin_", lambda u, c, d: multi_admin_request(u, c, int(d.replace("multi_admin_", "")))),
    ("multi_type_", lambda u, c, d: multi_type_request(u, c, int(d.replace("multi_type_", "")))),
    ("multi_set_type_", lambda u, c, d: (
        lambda suffix: multi_set_type(u, c, int(suffix.rsplit("_", 1)[0]), suffix.rsplit("_", 1)[1])
    )(d.replace("multi_set_type_", ""))),
    ("edit_multiple_", lambda u, c, d: edit_group_multiple(u, c, int(d.replace("edit_multiple_", "")))),
    ("cfg_combo_price_", lambda u, c, d: cfg_combo_price_request(
        u, c,
        int(d.rsplit("_", 1)[1]),
        d.replace("cfg_combo_price_", "").rsplit("_", 1)[0]
    )),
    ("cfg_combo_nolink_", lambda u, c, d: cfg_combo_nolink_notice(u, c, int(d.replace("cfg_combo_nolink_", "")))),
    ("cfg_combo_", lambda u, c, d: menu_combo_settings(u, c, int(d.replace("cfg_combo_", "")))),
    ("cfg_group_", lambda u, c, d: menu_group_settings(u, c, int(d.replace("cfg_group_", "")))),
    ("cfg_trial_", lambda u, c, d: cfg_trial_request(u, c, int(d.replace("cfg_trial_", "")))),
    ("cfg_price_semanal_", lambda u, c, d: cfg_price_request(u, c, int(d.replace("cfg_price_semanal_", "")), "semanal")),
    ("cfg_price_mensual_", lambda u, c, d: cfg_price_request(u, c, int(d.replace("cfg_price_mensual_", "")), "mensual")),
    ("cfg_dur_semanal_", lambda u, c, d: cfg_duration_request(u, c, int(d.replace("cfg_dur_semanal_", "")), "semanal")),
    ("cfg_dur_mensual_", lambda u, c, d: cfg_duration_request(u, c, int(d.replace("cfg_dur_mensual_", "")), "mensual")),
    ("cfg_price_anual_", lambda u, c, d: cfg_price_request(u, c, int(d.replace("cfg_price_anual_", "")), "anual")),
    ("cfg_dur_anual_", lambda u, c, d: cfg_duration_request(u, c, int(d.replace("cfg_dur_anual_", "")), "anual")),
    ("cfg_payment_", lambda u, c, d: config_payment_callback(u, c, int(d.replace("cfg_payment_", "")))),
    ("cfg_warnings_", lambda u, c, d: menu_warning_settings(u, c, int(d.replace("cfg_warnings_", "")))),
    ("cfg_messages_", lambda u, c, d: config_messages_callback(u, c, int(d.replace("cfg_messages_", "")))),
    ("cfg_msg_edit_", lambda u, c, d: _start_message_config(u, c, int(d.split("_")[3]), "_".join(d.split("_")[4:]))),
    ("warn_toggle_", lambda u, c, d: toggle_warnings(u, c, int(d.replace("warn_toggle_", "")))),
    ("warn_add_", lambda u, c, d: add_warning_step1(u, c, int(d.replace("warn_add_", "")))),
    ("warn_edit_", lambda u, c, d: edit_warning_step1(u, c, int(d.split("_")[2]), int(d.split("_")[3]))),
    ("warn_delete_", lambda u, c, d: delete_warning(u, c, int(d.split("_")[2]), int(d.split("_")[3]))),
    ("warn_test_", lambda u, c, d: test_warning(u, c, int(d.replace("warn_test_", "")))),
    ("pay_", _handle_pay_callback),
    ("spin_used_", lambda u, c, d: spin_used_callback(u, c)),
    ("spin_", lambda u, c, d: spin_discount(u, c)),
    ("bc|", lambda u, c, d: broadcast_callback_handler(u, c)),
    ("channel_SELECT_", lambda u, c, d: show_channel_menu_callback(u, c, int(d.replace("channel_SELECT_", "")))),
    ("channel_vip_", lambda u, c, d: show_vip_menu(u, c, int(d.replace("channel_vip_", "")))),
    ("vip_enter_", lambda u, c, d: vip_enter_callback(u, c, int(d.replace("vip_enter_", "")))),
    ("vip_pay_", lambda u, c, d: vip_pay_callback(u, c, int(d.replace("vip_pay_", "")))),
    ("vip_spin_", lambda u, c, d: vip_spin_callback(u, c, int(d.replace("vip_spin_", "")))),
    ("cfg_deuna_", lambda u, c, d: cfg_deuna_request(u, c, int(d.replace("cfg_deuna_", "")))),
    ("cfg_cart_", lambda u, c, d: cfg_cart_request(u, c, int(d.replace("cfg_cart_", "")))),
    ("list_active", lambda u, c, d: list_active_users(u, c, d)),
]
async def show_channel_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, vip_group_id: int):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    channels = get_channels()
    channel = None
    for ch in channels:
        if ch['vip']['group_id'] == vip_group_id:
            channel = ch
            break
    if not channel:
        await query.edit_message_text("❌ Canal no encontrado")
        return
    await show_channel_menu(lambda text, **kwargs: query.edit_message_text(text, **kwargs), user_id, channel)

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    logger.info(f"🔔 CALLBACK: {data}")
    try:
        await query.answer()
    except Exception:
        pass  # puede ya estar respondida
    
    try:
        handler = CALLBACK_EXACT.get(data)
        if handler:
            await handler(update, context, data)
            return

        for prefix, handler in CALLBACK_PREFIXES:
            if data.startswith(prefix):
                await handler(update, context, data)
                return
        logger.warning(f"Callback desconocido: {data}")
    except Exception as e:
        logger.error(f"❌ Error en callback '{data}': {e}", exc_info=True)
        try:
            await query.answer("❌ Ocurrió un error, intenta de nuevo", show_alert=True)
        except Exception:
            pass

async def _start_message_config(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int, msg_type: str):
    # Validación de permisos
    if group_id == 0:
        if update.callback_query.from_user.id not in [SUPER_ADMIN_ID] + EXTRA_ADMINS:
            await update.callback_query.answer("❌ Solo Super Admin", show_alert=True)
            return
    elif msg_type != "channel_select":
        if not can_manage_group(update.callback_query.from_user.id, group_id):
            await update.callback_query.answer("❌ No autorizado", show_alert=True)
            return
            
    await _prompt_message_config(update, context, group_id, msg_type)
# ==================== MAIN ====================
async def main():
    global bot_app
    await db.init_tables()
    await db.load_groups_from_db()
    await db.load_group_messages() 
    with GROUPS_LOCK:
        logger.info(f"📦 {len(GROUPS)} grupos disponibles")

    bot_app = ApplicationBuilder().token(TOKEN).build()

    # Comandos
    bot_app.add_handler(CommandHandler("start", start))
    bot_app.add_handler(CommandHandler("add", add_user_command))
    bot_app.add_handler(CommandHandler("renew", renew_user_command))
    bot_app.add_handler(CommandHandler("groups", list_groups))
    bot_app.add_handler(CommandHandler("addgroup", add_group_command))
    bot_app.add_handler(CommandHandler("backup", manual_backup))
    bot_app.add_handler(CommandHandler("restore", restore_backup))
    bot_app.add_handler(CommandHandler("getlink", get_link))
    bot_app.add_handler(CommandHandler("syncall", sync_all_groups))
    bot_app.add_handler(CommandHandler("searchgrupo", search_group))
    bot_app.add_handler(CommandHandler("pagar", pay_command))
    bot_app.add_handler(CommandHandler("configpago", config_payment_command))
    bot_app.add_handler(CommandHandler("test", test))
    bot_app.add_handler(CommandHandler("diaggrupo", diagnose_group))
    bot_app.add_handler(CommandHandler("broadcast", broadcast_command))
    bot_app.add_handler(CommandHandler("fixtrial", fix_trial_command))
    bot_app.add_handler(CommandHandler("linkvip", link_vip_command))
    bot_app.add_handler(CommandHandler("linkcomunidad", link_comunidad_command)) 
    bot_app.add_handler(CommandHandler("checkspin", check_spin_command))
    bot_app.add_handler(CommandHandler("configlink", config_link_command))
    bot_app.add_handler(CommandHandler("configmsg", config_messages_command))
    bot_app.add_handler(CommandHandler("configfuego", config_fuego_command))
    bot_app.add_handler(CommandHandler("info", info_command))
    bot_app.add_handler(CommandHandler("configdeuna", config_deuna_command))
    bot_app.add_handler(CommandHandler("configcart", config_cart_command))
    bot_app.add_handler(CommandHandler("addcombo", addcombo_command))
    bot_app.add_handler(CommandHandler("renewcombo", renewcombo_command))
    bot_app.add_handler(CommandHandler("dbhealth", db_health_command))

    # Callbacks
    bot_app.add_handler(CallbackQueryHandler(handle_callback))

    # Detección de miembros (ChatMemberHandler es suficiente, detect_new_member eliminado para evitar duplicados)
    bot_app.add_handler(ChatMemberHandler(track_chat_member, ChatMemberHandler.CHAT_MEMBER))
    
    # Reacciones para aviso de Fuego
    bot_app.add_handler(MessageReactionHandler(handle_reaction_fuego))

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
    scheduler.add_job(check_expired_subscriptions, 'interval', minutes=5, id='check_expired', replace_existing=True)
    scheduler.add_job(auto_backup, 'interval', hours=24, id='auto_backup', replace_existing=True)
    scheduler.add_job(send_trial_warnings, 'interval', minutes=1, id='trial_warnings', replace_existing=True)
    scheduler.add_job(_cleanup_payment_cooldown, 'interval', hours=1, id='cleanup_payment', replace_existing=True)
    scheduler.add_job(process_abandoned_carts, 'interval', minutes=5, id='check_carts', replace_existing=True)
    scheduler.start()

    logger.info("🤖 Bot iniciado")
    await bot_app.initialize()
    await bot_app.start()
    await verify_on_startup()
    await bot_app.updater.start_polling(
        drop_pending_updates=False,
        allowed_updates=["message", "callback_query", "chat_member", "my_chat_member", "message_reaction"]
    )
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
