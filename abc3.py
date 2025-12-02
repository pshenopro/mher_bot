import os
import json
import uuid
import asyncio
import random
import re
import time
from threading import Lock
from datetime import datetime, timezone, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters
)
from telegram.error import TelegramError


# === НАСТРОЙКИ ===
BOT_TOKEN = "8236243437:AAHKFByxAFQEiVyuFAzfz3jz0Fs1EmnELcQ"  # ← ОБЯЗАТЕЛЬНО ЗАМЕНИТЕ СВОЙ ТОКЕН!
ANIMATIONS_DIR = "animations/Ghetto"
BALANCE_FILE = "balances.json"
STATS_FILE = "chat_stats.json"
ANIMATION_CACHE_FILE = "animation_cache.json"

# === ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ДЛЯ АВТОРИЗАЦИИ ===
ACTIVE_GAME_MESSAGES = {}  # {chat_id: {message_id: owner_user_id}}

# === БЛОКИРОВКИ ДЛЯ ФАЙЛОВ ===
file_locks = {
    BALANCE_FILE: Lock(),
    STATS_FILE: Lock(), 
    ANIMATION_CACHE_FILE: Lock()
}

def safe_load_json(filename):
    """Безопасная загрузка JSON с блокировкой"""
    lock = file_locks.get(filename)
    if lock:
        lock.acquire()
    
    try:
        if os.path.exists(filename):
            with open(filename, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data
        return {}
    except (json.JSONDecodeError, FileNotFoundError, Exception) as e:
        print(f"❌ Ошибка загрузки {filename}: {e}")
        return {}
    finally:
        if lock:
            lock.release()

def safe_save_json(filename, data):
    """Безопасное сохранение JSON с блокировкой"""
    lock = file_locks.get(filename)
    if lock:
        lock.acquire()
    
    try:
        # Создаем временный файл для атомарной записи
        temp_file = filename + ".tmp"
        with open(temp_file, "w", encoding="utf-8") as f:                
            json.dump(data, f, ensure_ascii=False, indent=2)
            
        # Атомарная замена файла (работает на всех ОС)
        os.replace(temp_file, filename)
        return True
        
    except Exception as e:
        print(f"❌ Ошибка сохранения {filename}: {e}")
        # Пытаемся удалить временный файл в случае ошибки
        try:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        except:
            pass
        return False
    finally:
        if lock:
            lock.release()

if not os.path.exists(ANIMATIONS_DIR):
    print(f"❌ Папка не найдена: {ANIMATIONS_DIR}")
    print("Создайте папку 'animations/Ghetto' и поместите туда 36 файлов: roll_1_1.mp4 ... roll_6_6.mp4")


# === ТИТУЛЫ / РАНГИ ===
LEVEL_RANKS = [
    (1, "Бродяга", "🥾"),
    (3, "Любитель", "🎲"),
    (7, "Игрок", "🃏"),
    (15, "Профессионал", "🔥"),
    (25, "Магнат", "💰"),
    (40, "Король Костей", "👑"),
    (75, "Легенда Казино", "🌟")
]

def get_user_rank_title(level):
    title = LEVEL_RANKS[0][1]
    emoji = LEVEL_RANKS[0][2]
    for min_level, rank_title, rank_emoji in reversed(LEVEL_RANKS):
        if level >= min_level:
            title = rank_title
            emoji = rank_emoji
            break
    return title, emoji

# === НАСТРОЙКИ РУЛЕТКИ ===
ROULETTE_REWARDS = [
    ("💎 Джекпот!", 1000, 2),
    ("💰 Большой куш", 300, 10),
    ("✨ Средний выигрыш", 100, 25),
    ("✔️ Небольшой приз", 50, 40),
    ("❌ Пусто", 10, 23),
]

REWARD_STICKER_IDS = [
    "CAACAgIAAxkBAAEPw7JpFZqr-0np7yssGSOR0tOHpWzGqwACOlQAApEDsEs4pDSqruMX1DYE",
    "CAACAgIAAxkBAAEP1TJpIPcfbML_dPN-XNWiuzpnvQ8B7QACMmkAAidoeUmsicT83uW_eDYE",
    "CAACAgIAAxkBAAEP1PZpIPE27L6Mg401VnGLQNNFQpEeRgAC7YAAApws2UnJYN2NZK1zojYE",
    "CAACAgIAAxkBAAEP1TRpIPdvYaKK8FKJ4cZhUanKIURITwACTYUAAuU02Uk4xXhrph-f6zYE",
    "CAACAgIAAxkBAAEPw7lpFZrCa8B-2Prcu72Y17Wq7pMsTwACT2UAAoq5sUsmvLXXJAkS1TYE",
]

# === МЕНЮ ===
MAIN_MENU = ReplyKeyboardMarkup(
    [
        ["🎲 Дуэль", "🎯 Угадай сумму"],
        ["🎰 Быстрые игры", "🎁 Бонус"],
        ["💰 Баланс", "🏆 Топ чата", "📊 Статистика"]
    ],
    resize_keyboard=True,
    one_time_keyboard=False
)

# === INLINE-КЛАВИАТУРЫ ДЛЯ ИГРОВОГО ПРОЦЕССА ===
GUESS_INLINE_MENU = InlineKeyboardMarkup([
    [InlineKeyboardButton(str(i), callback_data=f"mode_guess_{i}") for i in range(2, 8)], 
    [InlineKeyboardButton(str(i), callback_data=f"mode_guess_{i}") for i in range(8, 13)], 
    [InlineKeyboardButton("⬅️ Главное меню", callback_data="back_to_main")]
])

PLAY_INLINE_MENU = InlineKeyboardMarkup([
    [InlineKeyboardButton("чётное", callback_data="mode_play_чётное"), 
     InlineKeyboardButton("нечётное", callback_data="mode_play_нечётное")],
    [InlineKeyboardButton("больше 7", callback_data="mode_play_больше 7"), 
     InlineKeyboardButton("меньше 7", callback_data="mode_play_меньше 7")],
    [InlineKeyboardButton("⬅️ Главное меню", callback_data="back_to_main")]
])

MODE_SWITCH_MAP = {
    "чётное": "нечётное",
    "нечётное": "чётное",
    "больше 7": "меньше 7",
    "меньше 7": "больше 7",
}

# === ФУНКЦИИ РАБОТЫ С ДАННЫМИ ===
def load_balances():
    return safe_load_json(BALANCE_FILE)

def save_balances(balances):
    return safe_save_json(BALANCE_FILE, balances)

def load_chat_stats():
    return safe_load_json(STATS_FILE)

def save_chat_stats(stats):
    return safe_save_json(STATS_FILE, stats)

def load_animation_cache():
    cache = safe_load_json(ANIMATION_CACHE_FILE)
    # Миграция старых ключей (если нужно) - оставляем твою существующую логику
    migrated_cache = {}
    needs_save = False
    for key, file_id in cache.items():
        if key.startswith("roll_"):
            new_key = key.replace("roll_", "")
            if new_key not in migrated_cache:
                migrated_cache[new_key] = file_id
                needs_save = True
        elif re.fullmatch(r'[1-6]_[1-6]', key):
            migrated_cache[key] = file_id
        else:
            migrated_cache[key] = file_id  # Сохраняем как есть
    
    if needs_save:
        safe_save_json(ANIMATION_CACHE_FILE, migrated_cache)
        return migrated_cache
    return cache

def save_animation_cache(cache):
    return safe_save_json(ANIMATION_CACHE_FILE, cache)

def get_user_profile(balances, user_id):
    if user_id not in balances:
        balances[user_id] = {
            "balance": 500,
            "last_daily": None,
            "xp": 0,
            "level": 1,
            "streak": 0,
            "current_game_streak": 0,
            "last_active": None,
            "last_spin": None,
            "games_played": 0,
            "wins": 0,
            "losses": 0,
            "total_won": 0,
            "total_lost": 0,
            "guess_wins": 0,
            "guess_losses": 0,
            "play_wins": 0,
            "play_losses": 0,
        }
    p = balances[user_id]
    defaults = {
        "balance": 500, "xp": 0, "level": 1, "streak": 0,
        "last_active": None, "last_daily": None,
        "current_game_streak": 0, 
        "last_spin": None,
        "games_played": 0, "wins": 0, "losses": 0,
        "total_won": 0, "total_lost": 0,
        "guess_wins": 0, "guess_losses": 0,
        "play_wins": 0, "play_losses": 0,
        "username": None,
    }
    for k, v in defaults.items():
        p.setdefault(k, v)
    return p

def update_streak_and_get_bonus_xp(balances, user_id):
    XP_PER_DAY = 20
    BONUS_MULTIPLIER = 1.15
    today_dt = datetime.now(timezone.utc).date() 
    today = today_dt.isoformat()
    profile = get_user_profile(balances, user_id)
    last_active = profile.get("last_active") 

    if last_active == today:
        return 0

    yesterday_dt = today_dt - timedelta(days=1)
    yesterday = yesterday_dt.isoformat() 
    
    if last_active == yesterday:
        profile["streak"] += 1
    else:
        profile["streak"] = 1
        
    profile["last_active"] = today
    current_streak = profile["streak"]
    xp_bonus = XP_PER_DAY + int(XP_PER_DAY * BONUS_MULTIPLIER * (current_streak - 1))
    
    if "balance" in profile:
        bonus_coins = current_streak * 10 
        profile["balance"] += bonus_coins
        
    add_xp(profile, xp_bonus)
    return xp_bonus

def add_xp(profile: dict, xp_amount: int) -> int:
    if xp_amount <= 0:
        return 0
    profile["xp"] += xp_amount
    current_level = profile["level"]
    required_xp = current_level * 100 
    levels_gained = 0 
    while profile["xp"] >= required_xp:
        profile["xp"] -= required_xp
        profile["level"] += 1
        levels_gained += 1
        profile["balance"] += 100 
        current_level = profile["level"]
        required_xp = current_level * 100
    return levels_gained

def add_win(chat_id: str, user_id: str):
    stats = load_chat_stats()
    chat_id_str = str(chat_id)
    user_id_str = str(user_id)
    if chat_id_str not in stats:
        stats[chat_id_str] = {}
    stats[chat_id_str][user_id_str] = stats[chat_id_str].get(user_id_str, 0) + 1
    save_chat_stats(stats)

# === ФУНКЦИИ ДЛЯ АВТОРИЗАЦИИ ===
def cleanup_active_game_message(chat_id: int, message_id: int):
    """Удаляет сообщение из ACTIVE_GAME_MESSAGES после завершения игры/закрытия меню."""
    chat_id_str = str(chat_id)
    if chat_id_str in ACTIVE_GAME_MESSAGES and message_id in ACTIVE_GAME_MESSAGES[chat_id_str]:
        del ACTIVE_GAME_MESSAGES[chat_id_str][message_id]
        if not ACTIVE_GAME_MESSAGES[chat_id_str]:
            del ACTIVE_GAME_MESSAGES[chat_id_str]

def check_message_owner(chat_id: int, message_id: int, user_id: str) -> bool:
    """Проверяет, является ли пользователь владельцем сообщения."""
    chat_id_str = str(chat_id)
    owner_id = ACTIVE_GAME_MESSAGES.get(chat_id_str, {}).get(message_id)
    return owner_id == user_id

# === INLINE-ОБРАБОТЧИКИ МЕНЮ ===
async def handle_menu_guess(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    try:
        await update.message.delete()
    except:
        pass
        
    msg = await context.bot.send_message(
        chat_id=chat_id, 
        text="🎯 Выбери сумму кубиков:", 
        reply_markup=GUESS_INLINE_MENU
    )
    
    # Сохраняем владельца меню для авторизации
    chat_id_str = str(chat_id)
    user_id_str = str(update.effective_user.id)
    if chat_id_str not in ACTIVE_GAME_MESSAGES:
        ACTIVE_GAME_MESSAGES[chat_id_str] = {}
    ACTIVE_GAME_MESSAGES[chat_id_str][msg.message_id] = user_id_str

async def handle_menu_play(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    try:
        await update.message.delete()
    except:
        pass
        
    msg = await context.bot.send_message(
        chat_id=chat_id, 
        text="🎰 Выбери тип игры:", 
        reply_markup=PLAY_INLINE_MENU
    )
    
    # Сохраняем владельца меню для авторизации
    chat_id_str = str(chat_id)
    user_id_str = str(update.effective_user.id)
    if chat_id_str not in ACTIVE_GAME_MESSAGES:
        ACTIVE_GAME_MESSAGES[chat_id_str] = {}
    ACTIVE_GAME_MESSAGES[chat_id_str][msg.message_id] = user_id_str

async def handle_mode_selection_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = str(query.from_user.id)
    chat_id = query.message.chat.id
    message_id = query.message.message_id
    
    # ПРОВЕРКА АВТОРИЗАЦИИ
    if not check_message_owner(chat_id, message_id, user_id):
        await query.answer("❌ Эта кнопка не для тебя! Начни свою игру.")
        return
    
    await query.answer()
    
    data = query.data.split('_', 2)
    game_type = data[1]  # 'guess' or 'play'
    mode_value = data[2]  # '5' или 'чётное'

    balances = load_balances()
    profile = get_user_profile(balances, user_id)
    current_balance = profile["balance"]
    
    # Создаем Inline-меню для ставки
    bet_menu = get_bet_inline_menu(game_type, mode_value, current_balance)
    
    try:
        await query.edit_message_text(
            text=f"Вы выбрал(а): **{mode_value}**\n\n👇 Выбери сумму ставки:",
            parse_mode="Markdown",
            reply_markup=bet_menu
        )
    except TelegramError:
        pass
    
    # Сохраняем состояние игры для обработки ручного ввода ставки
    context.user_data[f"active_game_state_{user_id}"] = {"type": game_type, "mode": mode_value}

def get_bet_inline_menu(game_type: str, mode_value: str, current_balance: int):
    buttons = [
        [InlineKeyboardButton("50", callback_data=f"bet_{game_type}_{mode_value}_50"), 
         InlineKeyboardButton("100", callback_data=f"bet_{game_type}_{mode_value}_100"), 
         InlineKeyboardButton("200", callback_data=f"bet_{game_type}_{mode_value}_200")],
        [InlineKeyboardButton("500", callback_data=f"bet_{game_type}_{mode_value}_500"), 
         InlineKeyboardButton("1000", callback_data=f"bet_{game_type}_{mode_value}_1000"), 
         InlineKeyboardButton("2000", callback_data=f"bet_{game_type}_{mode_value}_2000")]
    ]
    
    if current_balance > 0:
        buttons.append([InlineKeyboardButton(f"💰 Ва-банк ({current_balance})", callback_data=f"bet_{game_type}_{mode_value}_vabank")])
    
    buttons.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"back_to_mode_{game_type}")])
    
    return InlineKeyboardMarkup(buttons)

async def handle_bet_selection_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = str(query.from_user.id)
    chat_id = query.message.chat.id
    message_id = query.message.message_id
    
    # ПРОВЕРКА АВТОРИЗАЦИИ
    if not check_message_owner(chat_id, message_id, user_id):
        await query.answer("❌ Эта кнопка не для тебя! Начни свою игру.")
        return
    
    await query.answer()
    
    data = query.data.split('_', 3)
    game_type = data[1]
    mode_value = data[2]
    bet_value_str = data[3]
    
    balances = load_balances()
    profile = get_user_profile(balances, user_id)
    
    if bet_value_str == "vabank":
        bet = profile["balance"]
    else:
        try:
            bet = int(bet_value_str)
        except ValueError:
            await query.answer("❌ Ошибка ставки. Попробуйте снова.", show_alert=True)
            return

    if bet < 10:
        await query.answer("❌ Минимальная ставка: 10 монет!", show_alert=True)
        return
        
    if profile["balance"] < bet:
        await query.answer(f"❌ Недостаточно монет! На балансе {profile['balance']}.", show_alert=True)
        bet_menu = get_bet_inline_menu(game_type, mode_value, profile["balance"])
        try:
            await query.edit_message_text(
                text=f"Вы выбрал(а): **{mode_value}**\n\n❌ Недостаточно средств. Выбери другую ставку.",
                parse_mode="Markdown",
                reply_markup=bet_menu
            )
        except TelegramError:
            pass
        return

    # Удаляем интерактивное меню перед началом игры
    try:
        await query.delete_message()
    except:
        pass
        
    # Очищаем информацию о меню
    cleanup_active_game_message(chat_id, message_id)
        
    # Сбрасываем игровое состояние
    context.user_data.pop(f"active_game_state_{user_id}", None)

    # Запускаем игру
    if game_type == "guess":
        context.args = [str(bet), mode_value]
        class MockUpdate:
            def __init__(self, query, context):
                self.effective_user = query.from_user
                self.effective_chat = query.message.chat
                self.bot = context.bot
                self.message = self
            async def delete(self): pass
        
        await guess(MockUpdate(query, context), context)
        
    elif game_type == "play":
        context.args = [mode_value, str(bet)]
        class MockUpdate:
            def __init__(self, query, context):
                self.effective_user = query.from_user
                self.effective_chat = query.message.chat
                self.bot = context.bot
                self.message = self
            async def delete(self): pass
            
        await play(MockUpdate(query, context), context)

async def handle_inline_back_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    chat_id = query.message.chat.id
    user_id = str(query.from_user.id)
    message_id = query.message.message_id
    
    # ПРОВЕРКА АВТОРИЗАЦИИ
    if not check_message_owner(chat_id, message_id, user_id):
        await query.answer("❌ Эта кнопка не для тебя! Начни свою игру.")
        return
    
    await query.answer()
    
    if data == "back_to_main":
        try:
            await query.delete_message()
        except:
            pass
        cleanup_active_game_message(chat_id, message_id)
        await context.bot.send_message(chat_id=chat_id, text="🏠 Возврат в Главное меню", reply_markup=MAIN_MENU)
        
    elif data.startswith("back_to_mode_"):
        game_type = data.split('_')[-1]  # 'guess' or 'play'
        
        if game_type == 'guess':
            reply_markup = GUESS_INLINE_MENU
            text = "🎯 Выбери сумму кубиков:"
        elif game_type == 'play':
            reply_markup = PLAY_INLINE_MENU
            text = "🎰 Выбери тип игры:"
        else:
            return 
            
        try:
            await query.edit_message_text(text=text, reply_markup=reply_markup)
        except TelegramError:
            pass
        
        context.user_data.pop(f"active_game_state_{user_id}", None)

# === ОБРАБОТКА РУЧНОГО ВВОДА СТАВКИ ===
async def handle_custom_bet_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    chat_id = update.effective_chat.id
    text = update.message.text.strip()
    
    game_state_key = f"active_game_state_{user_id}"
    if game_state_key not in context.user_data:
        return False
        
    active_game_state = context.user_data.get(game_state_key, {})
    game_type = active_game_state.get("type")
    mode_value = active_game_state.get("mode")
    
    if not game_type or not mode_value:
        return False
        
    try:
        bet = int(text)
    except ValueError:
        await update.message.reply_text("❌ Введите числовое значение ставки.", reply_markup=ReplyKeyboardRemove())
        return True

    balances = load_balances()
    profile = get_user_profile(balances, user_id)
    
    if bet < 10:
        await update.message.reply_text("❌ Минимальная ставка: 10 монет!", reply_markup=ReplyKeyboardRemove())
        return True
    
    if profile["balance"] < bet:
        await update.message.reply_text("❌ Недостаточно монет!", reply_markup=ReplyKeyboardRemove())
        return True

    # Удаляем сообщение пользователя со ставкой
    try:
        await update.message.delete()
    except:
        pass
        
    # Сбрасываем игровое состояние
    context.user_data.pop(game_state_key, None)
        
    # Запуск игры
    if game_type == "guess":
        context.args = [str(bet), mode_value]
        await guess(update, context)
    elif game_type == "play":
        context.args = [mode_value, str(bet)]
        await play(update, context)
        
    return True

# === ОБРАБОТЧИКИ ГЛАВНОГО МЕНЮ ===
async def handle_menu_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await update.message.delete()
    except:
        pass
    await balance(update, context)

async def handle_menu_top(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await update.message.delete()
    except:
        pass
    await topchat(update, context)

async def handle_menu_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await update.message.delete()
    except:
        pass
    await statsme(update, context)

# === КОМАНДЫ ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    balances = load_balances()
    user_id = str(user.id)
    profile = get_user_profile(balances, user_id)
    profile['username'] = user.first_name 
    update_streak_and_get_bonus_xp(balances, user_id)
    save_balances(balances)
    await update.message.reply_text(
        f"🎲 Привет, {user.first_name}!\n"
        f"Жми **'🎁 Бонус'** или /daily, чтобы крутануть рулетку!",
        reply_markup=MAIN_MENU
    )

async def balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    balances = load_balances()
    bal = get_user_profile(balances, str(user.id))["balance"]
    await update.message.reply_text(f"💰 Баланс: {bal} монет")

async def statsme(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_id = str(update.effective_user.id)
    username = update.effective_user.first_name
    
    balances = load_balances()
    profile = get_user_profile(balances, user_id)

    level = profile['level']
    xp_needed_for_next_level = level * 100 
    
    title, emoji = get_user_rank_title(level)

    win_rate = (profile['wins'] / profile['games_played'] * 100) if profile['games_played'] > 0 else 0
    total_net = profile['total_won'] - profile['total_lost']

    stats_msg = (
        f"👤 **Твой профиль, {username}**\n\n"
        f"🏅 **Ранг:** {emoji} {title}\n"
        f"📈 **Уровень:** {level} (Опыт: {profile['xp']}/{xp_needed_for_next_level})\n"
        f"💰 **Баланс:** {profile['balance']}\n"
        f"🔥 **Стрик побед:** {profile['current_game_streak']}\n"
        f"--- Игровая статистика ---\n"
        f"🕹 **Игр сыграно:** {profile['games_played']}\n"
        f"✅ **Побед:** {profile['wins']} | ❌ **Поражений:** {profile['losses']} | 🤝 **Ничьих:** {profile.get('draws', 0)}\n"
        f"📊 **Винрейт:** {win_rate:.1f}%\n"
        f"💸 **Чистый профит:** {total_net:+d}"
    )
    await context.bot.send_message(chat_id=chat_id, text=stats_msg, parse_mode="Markdown", reply_markup=MAIN_MENU)

async def topchat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    balances = load_balances()
    stats = load_chat_stats()

    local_user_ids = stats.get(chat_id, {}).keys()

    if not local_user_ids:
        await context.bot.send_message(chat_id=chat_id, text="В этом чате пока нет игроков, которые совершали ставки.")
        return

    local_active_users = {}
    for user_id in local_user_ids:
        data = balances.get(user_id)
        if data and data.get('balance', 0) > 0:
            local_active_users[user_id] = data

    if not local_active_users:
        await context.bot.send_message(chat_id=chat_id, text="Все активные игроки этого чата имеют нулевой или отрицательный баланс.")
        return

    sorted_users = sorted(local_active_users.items(), key=lambda item: item[1].get('balance', 0), reverse=True)
    top_list = []
    
    for i, (user_id, data) in enumerate(sorted_users[:10]):
        index = i + 1
        index_emoji = ""
        if index == 1: index_emoji = "🥇"
        elif index == 2: index_emoji = "🥈"
        elif index == 3: index_emoji = "🥉"
        else: index_emoji = f"▪️{index}."
        
        level = data.get("level", 1)
        title, emoji = get_user_rank_title(level)
        name = data.get('username') or f"ID{user_id}" 
        entry = f"{index_emoji} **{name}** — {data['balance']} 💰 | Ур. {level} ({emoji} {title})"
        top_list.append(entry)
        
    msg = "🏆 **ТОП-10 ИГРОКОВ ЭТОГО ЧАТА**\n\n" + "\n".join(top_list)
    await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode="Markdown")

async def global_top(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    balances = load_balances()
    
    active_users = {uid: data for uid, data in balances.items() if data.get('balance', 0) > 0}
    sorted_users = sorted(active_users.items(), key=lambda item: item[1].get('balance', 0), reverse=True)
    top_list = []
    
    for i, (user_id, data) in enumerate(sorted_users[:10]):
        index = i + 1
        index_emoji = ""
        if index == 1: index_emoji = "🥇"
        elif index == 2: index_emoji = "🥈"
        elif index == 3: index_emoji = "🥉"
        else: index_emoji = f"▪️{index}."
        
        level = data.get("level", 1)
        title, emoji = get_user_rank_title(level)
        name = data.get('username') or f"ID{user_id}" 
        entry = f"{index_emoji} **{name}** — {data['balance']} 💰 | Ур. {level} ({emoji} {title})"
        top_list.append(entry)
    
    if not top_list:
        msg = "Нет активных пользователей в глобальном топе."
    else:
        msg = "🌐 **ГЛОБАЛЬНЫЙ ТОП-10 ИГРОКОВ**\n\n" + "\n".join(top_list)

    await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode="Markdown")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "🎮 **Как играть:**\n\n"
        "🎲 **Дуэль** (с другим игроком):\n"
        " → Ответь на сообщение и напиши `/duel 100`\n\n"
        "🎯 **Угадай сумму**:\n"
        " → Выбери сумму в меню → укажи ставку\n"
        " → Сумма 2 или 12 → ×30\n"
        " → Сумма 7 → ×5\n\n"
        "🎰 **Быстрые игры**:\n"
        " → Выбери режим → укажи ставку\n"
        " → Выигрыш: ×1.9 (бонусы за стрик!)\n\n"
        "💡 **Доступные команды:**\n"
        "/daily — Ежедневная рулетка 🎁\n"
        "/statsme — Твоя личная статистика 📊\n"
        "/topchat — Топ игроков этого чата 🏆\n"
        "/globaltop — Глобальный топ игроков 🌐\n"
        "/balance — Твой баланс 💰\n"
    )
    await update.message.reply_text(text, parse_mode="Markdown")

# === РУЛЕТКА ===
async def daily_spin_roulette(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    username = update.effective_user.first_name
    balances = load_balances()
    profile = get_user_profile(balances, user_id)
    now = datetime.now(timezone.utc)
    chat_id = update.effective_chat.id

    if profile["last_spin"]:
        try:
            last_spin_dt = datetime.fromisoformat(profile["last_spin"])
        except ValueError:
            last_spin_dt = datetime(1970, 1, 1, tzinfo=timezone.utc)
            
        profile["last_spin"] = last_spin_dt.isoformat() 
        time_since_last_spin = now - last_spin_dt
        
        if time_since_last_spin < timedelta(hours=24):
            time_left = timedelta(hours=24) - time_since_last_spin
            hours, remainder = divmod(int(time_left.total_seconds()), 3600)
            minutes, seconds = divmod(remainder, 60)
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"❌ **{username}**, ежедневный бонус доступен раз в 24 часа.\n"
                     f"Осталось: **{hours} ч {minutes} мин**."
            )
            return

    total_weight = sum(item[2] for item in ROULETTE_REWARDS)
    weights = [item[2] / total_weight for item in ROULETTE_REWARDS]
    
    result_index = random.choices(range(len(ROULETTE_REWARDS)), weights=weights, k=1)[0]
    reward_name, reward_amount, _ = ROULETTE_REWARDS[result_index]
    sticker_id = REWARD_STICKER_IDS[result_index]

    profile["balance"] += reward_amount
    profile["last_spin"] = now.isoformat()
    add_xp(profile, 5) 
    save_balances(balances)

    await context.bot.send_sticker(chat_id=chat_id, sticker=sticker_id)
    await context.bot.send_message(
        chat_id=chat_id,
        text=f"🎰 **{username}**, ты выиграл: {reward_name}\n"
             f"💰 Начислено: **{reward_amount}** монет!\n"
             f"Текущий баланс: {profile['balance']}"
    )

# === ИГРОВАЯ ЛОГИКА ===
def roll_dice():
    return random.randint(1, 6), random.randint(1, 6)

animation_cache = load_animation_cache()

async def send_cached_video(context: ContextTypes.DEFAULT_TYPE, chat_id, d1, d2):
    file_key = f"{d1}_{d2}"
    
    if context.bot_data and "animation_cache" in context.bot_data:
        animation_cache_data = context.bot_data["animation_cache"]
    else:
        animation_cache_data = load_animation_cache()
    
    if file_key in animation_cache_data:
        file_id = animation_cache_data[file_key]
        try:
            msg = await context.bot.send_animation(chat_id=chat_id, animation=file_id)
            return msg
        except TelegramError as e:
            print(f"DEBUG: Ошибка при отправке кэшированного видео {file_key}: {e}")
            del animation_cache_data[file_key]
            save_animation_cache(animation_cache_data)

    anim_path = os.path.join(ANIMATIONS_DIR, f"roll_{d1}_{d2}.mp4")
    if not os.path.exists(anim_path):
        return await context.bot.send_message(chat_id=chat_id, text=f"🎲 Бросок: {d1} и {d2} (Видео не найдено)")

    try:
        with open(anim_path, 'rb') as video_file:
            msg = await context.bot.send_animation(chat_id=chat_id, animation=video_file)
            
            if msg.animation and msg.animation.file_id:
                animation_cache_data[file_key] = msg.animation.file_id
                context.bot_data["animation_cache"] = animation_cache_data
                save_animation_cache(animation_cache_data)
            return msg
    except Exception as e:
        print(f"DEBUG: ФАТАЛЬНАЯ ОШИБКА при загрузке анимации {file_key}: {e}")
        return await context.bot.send_message(chat_id=chat_id, text=f"🎲 Бросок: {d1} и {d2} (Ошибка загрузки видео)")

async def play(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    if not context.args or len(context.args) != 2:
        await context.bot.send_message(chat_id=chat_id, text="❌ Ошибка данных игры. Попробуйте снова через меню.", reply_markup=MAIN_MENU)
        return

    mode_text = context.args[0] 
    bet_str = context.args[1]

    try:
        bet = int(bet_str)
    except ValueError:
        await context.bot.send_message(chat_id=chat_id, text="❌ Ошибка: ставка не является числом.")
        return

    # === ЛОГИКА ОЧИСТКИ ===
    last_msg_id = context.user_data.get("last_result_msg_id")
    if last_msg_id:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=last_msg_id)
        except:
            pass
    
    try:
        await update.message.delete()
    except:
        pass

    context.user_data.pop("bet_mode_type", None)
    context.user_data.pop("bet_mode_value", None)

    # === ЛОГИКА ИГРЫ ===
    user_id = str(update.effective_user.id)
    username = update.effective_user.first_name
    
    balances = load_balances()
    profile = get_user_profile(balances, user_id)
    profile['username'] = username 
    save_balances(balances) 
    
    if profile["balance"] < bet:
        await context.bot.send_message(chat_id=chat_id, text="❌ Недостаточно монет!", reply_markup=MAIN_MENU)
        return

    profile["balance"] -= bet
    profile["total_lost"] += bet
    add_win(chat_id, user_id) 

    d1, d2 = roll_dice()
    total = d1 + d2
    
    roll_video_msg = await send_cached_video(context, chat_id, d1, d2)
    
    await asyncio.sleep(4.5)

    win = False
    is_draw = False

    mode_map = {
        "чётное": "чёт",
        "нечётное": "нечет",
        "больше 7": "больше",
        "меньше 7": "меньше"
    }
    short_mode = mode_map.get(mode_text, mode_text)

    if total == 7 and (short_mode == "больше" or short_mode == "меньше"):
        is_draw = True
    elif short_mode == "чёт" and total % 2 == 0: win = True
    elif short_mode == "нечет" and total % 2 == 1: win = True
    elif short_mode == "больше" and total > 7: win = True
    elif short_mode == "меньше" and total < 7: win = True

    profile["games_played"] += 1
    xp_gained = 0
    streak_msg = ""
    
    if is_draw:
        profile["balance"] += bet
        profile["total_lost"] -= bet
        profile.setdefault("draws", 0)
        profile["draws"] += 1
        
        phrase = "Сумма 7!"
        result_text = "🤝 Ставка возвращена. (Стрик сохранен)"
        xp_gained = 5
        
    elif win:
        profile["current_game_streak"] += 1
        current_streak = profile["current_game_streak"]
        
        bonus_multiplier = 0
        
        if current_streak >= 5:
            bonus_multiplier = 0.3
            streak_msg = "\n⚡️ **НЕУДЕРЖИМЫЙ!** (Бонус +30% к выигрышу!)"
        elif current_streak >= 3:
            bonus_multiplier = 0.15
            streak_msg = "\n🔥 **СТРИК x3!** (Бонус +15% к выигрышу!)"
        
        final_multiplier = 1.9 + bonus_multiplier
        win_amount = int(bet * final_multiplier)
        
        profile["balance"] += win_amount
        profile["play_wins"] += 1
        profile["wins"] += 1
        profile["total_won"] += win_amount - bet
        
        phrase = "Ого! Это победа! 🏆"
        result_text = f"+{win_amount - bet} монет (x{round(final_multiplier, 2)})"
        xp_gained = 10 + (current_streak * 2)
        
    else:
        profile["current_game_streak"] = 0
        profile["play_losses"] += 1
        profile["losses"] += 1
        
        phrase = "Эх, в другой раз... 💸"
        result_text = f"-{bet} монет"
        xp_gained = 0
        
    levels_gained = add_xp(profile, xp_gained)
    save_balances(balances)

    level_up_msg = ""
    if levels_gained > 0:
        level_up_msg = f"\n\n⬆️ **LEVEL UP!** Ты достиг уровня {profile['level']}!"

    final_msg = (
        f"👤 **{username}** ставил(а) на **{mode_text}**"
        f" и выпало **{total}**."
        f"{streak_msg}\n\n"
        f"{phrase} {result_text}\n"
        f"💰 Текущий баланс: {profile['balance']}"
        f"{level_up_msg}"
    )
    
    next_bet = bet
    double_bet = bet * 2
    user_id_int = update.effective_user.id
    
    # Кнопки для повтора и смены режима с АВТОРИЗАЦИЕЙ
    replay_buttons = [
        [
            InlineKeyboardButton(f"🔄 Ещё ({next_bet})", callback_data=f"repeat|play|{user_id_int}|{mode_text}|{next_bet}"),
            InlineKeyboardButton(f"❌2 ({double_bet})", callback_data=f"repeat|play|{user_id_int}|{mode_text}|{double_bet}")
        ]
    ]
    
    switch_mode_text = MODE_SWITCH_MAP.get(mode_text)
    if switch_mode_text:
        replay_buttons.append(
            [InlineKeyboardButton(
                f"↔️ На {switch_mode_text} ({next_bet})", 
                callback_data=f"repeat|play|{user_id_int}|{switch_mode_text}|{next_bet}" 
            )]
        )
        
    replay_buttons.append([InlineKeyboardButton("🏠 Меню", callback_data=f"back_to_menu|play|{user_id_int}")])
    custom_keyboard = InlineKeyboardMarkup(replay_buttons)
    
    final_result_msg = await context.bot.send_message(
        chat_id=chat_id, 
        text=final_msg, 
        parse_mode="Markdown", 
        reply_markup=custom_keyboard
    )

    context.user_data["last_result_msg_id"] = final_result_msg.message_id

    if roll_video_msg:
        async def delete_video_later():
            await asyncio.sleep(30)
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=roll_video_msg.message_id)
            except:
                pass
        asyncio.create_task(delete_video_later())

async def guess(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    
    if not context.args or len(context.args) != 2:
        await context.bot.send_message(chat_id=chat_id, text="❌ Ошибка данных.", reply_markup=MAIN_MENU)
        return
    try:
        bet = int(context.args[0])
        guess_total = int(context.args[1])
    except:
        await context.bot.send_message(chat_id=chat_id, text="❌ Ошибка аргументов!", reply_markup=MAIN_MENU)
        return
    
    # === ЛОГИКА ОЧИСТКИ ===
    last_msg_id = context.user_data.get("last_result_msg_id")
    if last_msg_id:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=last_msg_id)
        except:
            pass

    try:
        await update.message.delete()
    except:
        pass

    context.user_data.pop("bet_mode_type", None)
    context.user_data.pop("bet_mode_value", None)
    
    # === ЛОГИКА ИГРЫ ===
    user_id = str(update.effective_user.id)
    username = update.effective_user.first_name
    
    balances = load_balances()
    profile = get_user_profile(balances, user_id)
    profile['username'] = username 
    save_balances(balances) 

    if profile["balance"] < bet:
        await context.bot.send_message(chat_id=chat_id, text="❌ Недостаточно монет!", reply_markup=MAIN_MENU)
        return

    d1, d2 = roll_dice()
    total = d1 + d2
    
    win = total == guess_total
    multiplier = 5
    if guess_total in [2, 12]: multiplier = 30
    elif guess_total == 7: multiplier = 5 

    xp_gained = 0
    streak_msg = ""
    bonus_coins = 0

    if win:
        profile["current_game_streak"] += 1
        current_streak = profile["current_game_streak"]
        
        if current_streak >= 5:
            bonus_coins = 150
            streak_msg = f"\n⚡️ **НЕУДЕРЖИМЫЙ!** (Бонус +{bonus_coins} монет!)"
        elif current_streak >= 3:
            bonus_coins = 50
            streak_msg = f"\n🔥 **СТРИК x3!** (Бонус +{bonus_coins} монет!)"
        
        reward = bet * multiplier + bonus_coins
        profile["balance"] += reward
        profile["guess_wins"] += 1
        profile["wins"] += 1
        profile["total_won"] += reward
        phrase = "Ого! Это победа! 🏆"
        result = f"+{reward} монет (x{multiplier})"
        xp_gained = 30 + (current_streak * 5)
        
    else:
        profile["current_game_streak"] = 0
        profile["balance"] -= bet
        profile["guess_losses"] += 1
        profile["total_lost"] += bet
        phrase = "Эх, в другой раз... 💸"
        result = f"-{bet} монет"
        xp_gained = 0

    profile["games_played"] += 1
    add_win(chat_id, user_id) 
    levels_gained = add_xp(profile, xp_gained)
    save_balances(balances)

    roll_video_msg = await send_cached_video(context, chat_id, d1, d2)

    await asyncio.sleep(4.5)

    level_up_msg = ""
    if levels_gained > 0:
        level_up_msg = f"\n\n⬆️ **LEVEL UP!** Ты достиг уровня {profile['level']}!"

    final_msg = (
        f"👤 **{username}** ставил(а) на сумму **{guess_total}**, а выпало **{total}**."
        f"{streak_msg}\n\n"
        f"{phrase} {result}\n"
        f"💰 Текущий баланс: {profile['balance']}"
        f"{level_up_msg}"
    )

    next_bet = bet
    double_bet = bet * 2
    user_id_int = update.effective_user.id
    
    replay_buttons = [
        [
            InlineKeyboardButton(f"🔄 Ещё ({next_bet})", callback_data=f"repeat|guess|{user_id_int}|{guess_total}|{next_bet}"),
            InlineKeyboardButton(f"❌2 ({double_bet})", callback_data=f"repeat|guess|{user_id_int}|{guess_total}|{double_bet}")
        ],
        [InlineKeyboardButton("🏠 Меню", callback_data=f"back_to_menu|guess|{user_id_int}")]
    ]

    custom_keyboard = InlineKeyboardMarkup(replay_buttons)

    final_result_msg = await context.bot.send_message(
        chat_id=chat_id, 
        text=final_msg, 
        parse_mode="Markdown", 
        reply_markup=custom_keyboard
    )

    context.user_data["last_result_msg_id"] = final_result_msg.message_id

    if roll_video_msg:
        async def delete_video_later():
            await asyncio.sleep(30)
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=roll_video_msg.message_id)
            except:
                pass
        asyncio.create_task(delete_video_later())

# === ОБРАБОТЧИКИ С АВТОРИЗАЦИЕЙ ===
async def handle_repeat_game_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data_parts = query.data.split("|")
    
    if len(data_parts) != 5:
        await context.bot.send_message(
            chat_id=query.message.chat.id,
            text="❌ Ошибка данных повтора игры."
        )
        return

    game_type = data_parts[1]
    expected_user_id = data_parts[2]  # Владелец игры
    mode_value = data_parts[3]  # mode_text (play) or guess_total (guess)
    bet_str = data_parts[4]

    user_id = str(query.from_user.id)
    
    # ПРОВЕРКА АВТОРИЗАЦИИ
    if user_id != expected_user_id:
        await query.answer("❌ Эта кнопка не для тебя! Начни свою игру.")
        return

    try:
        await query.delete_message()
    except Exception:
        pass

    chat_id = query.message.chat.id
    balances = load_balances()
    profile = get_user_profile(balances, user_id)
    
    if bet_str == "vabank":
        bet = profile["balance"]
    else:
        try:
            bet = int(bet_str)
        except ValueError:
            await query.answer("❌ Ошибка ставки.", show_alert=True)
            return

    if bet < 10:
        await query.answer("❌ Минимальная ставка: 10 монет!", show_alert=True)
        return

    if profile["balance"] < bet:
        await query.answer(f"❌ Недостаточно монет! На балансе {profile['balance']}.", show_alert=True)
        return

    class MockMessageUpdate:
        def __init__(self, query, context):
            self.effective_user = query.from_user
            self.effective_chat = query.message.chat
            self.bot = context.bot
            self.message = self 
            self.message_id = -1 
            
        async def delete(self):
            pass
    
    mock_update = MockMessageUpdate(query, context)

    if game_type == "play":
        context.args = [mode_value, bet_str]
        await play(mock_update, context)
        
    elif game_type == "guess":
        context.args = [bet_str, mode_value]
        await guess(mock_update, context)
    
    context.args = None

async def handle_back_to_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        await query.answer()
    except:
        pass

    user_id = str(query.from_user.id)
    balances = load_balances()
    update_streak_and_get_bonus_xp(balances, user_id)
    save_balances(balances)
    
    data = query.data.split("|")
    if len(data) == 3:
        expected_user_id = data[2]
        if user_id != expected_user_id:
            await query.answer("❌ Эта кнопка не для тебя! Начни свою игру.")
            return

    try:
        await query.delete_message()
    except Exception:
        pass
        
    try:
        msg = await context.bot.send_message(
            chat_id=query.message.chat.id, 
            text="🏠 Главное меню", 
            reply_markup=MAIN_MENU
        )
        async def delayed_delete_stats():
            await asyncio.sleep(15)
            try:
                await context.bot.delete_message(chat_id=query.message.chat.id, message_id=msg.message_id)
            except:
                pass
        asyncio.create_task(delayed_delete_stats())
    except:
        pass

# === ДУЭЛИ (ИСПРАВЛЕННАЯ ЛОГИКА) ===
async def handle_menu_duel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🎲 Чтобы вызвать на дуэль:\n"
        "1. Ответь на сообщение игрока фразой:\n"
        "2. Давай на 100 или напиши `/duel 100`\n"
        "Минимальная ставка: 10 монет"
    )

async def handle_duel_phrase(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.reply_to_message:
        return

    text = update.message.text.strip().lower()
    # ИСПРАВЛЕННОЕ РЕГУЛЯРНОЕ ВЫРАЖЕНИЕ
    match = re.search(r'давай\s+на\s+(\d+)', text)
    if not match:
        return

    try:
        bet = int(match.group(1))
    except (ValueError, IndexError):
        return

    if bet < 10:
        await update.message.reply_text("❌ Минимальная ставка: 10 монет")
        return

    challenger = update.effective_user
    opponent = update.message.reply_to_message.from_user

    if challenger.id == opponent.id:
        await update.message.reply_text("❌ Нельзя играть сам с собой!")
        return

    balances = load_balances()
    ch_id = str(challenger.id)
    op_id = str(opponent.id)

    ch_profile = get_user_profile(balances, ch_id)
    if ch_profile["balance"] < bet:
        await update.message.reply_text("❌ Недостаточно монет!")
        return

    # Списание ставки
    ch_profile["balance"] -= bet
    ch_profile['username'] = challenger.first_name

    # Инициализация профиля оппонента
    op_profile = get_user_profile(balances, op_id)
    op_profile['username'] = opponent.first_name
    save_balances(balances)

    duel_id = str(uuid.uuid4())[:8]
    context.bot_data[f"duel_{duel_id}"] = {
        "id": duel_id,
        "challenger_id": ch_id,
        "opponent_id": op_id,
        "bet": bet,
        "challenger_name": challenger.first_name,
        "opponent_name": opponent.first_name,
        "next_player": ch_id,
        "rolls": {},
        "message_ids": []
    }

    keyboard = [
        [InlineKeyboardButton("✅ Принять", callback_data=f"accept_{duel_id}")],
        [InlineKeyboardButton("❌ Отклонить", callback_data=f"decline_{duel_id}")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    msg = await update.message.reply_text(
        f"{challenger.first_name} вызывает {opponent.first_name} на дуэль!\n"
        f"Ставка: {bet} монет 🎲 (Списано с баланса)",
        reply_markup=reply_markup
    )
    context.bot_data[f"duel_{duel_id}"]["message_ids"].append(msg.message_id)

async def duel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /duel"""
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("❌ Используйте: `/duel 100` (укажите сумму ставки)", parse_mode="Markdown")
        return
    
    try:
        bet = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ Неверная сумма ставки")
        return
        
    if bet < 10:
        await update.message.reply_text("❌ Минимальная ставка: 10 монет")
        return

    # Проверяем, есть ли ответ на сообщение
    if not update.message.reply_to_message:
        await update.message.reply_text(
            "❌ Для дуэли необходимо ответить на сообщение оппонента командой `/duel 100`",
            parse_mode="Markdown"
        )
        return

    # Используем ту же логику, что и в handle_duel_phrase
    challenger = update.effective_user
    opponent = update.message.reply_to_message.from_user

    if challenger.id == opponent.id:
        await update.message.reply_text("❌ Нельзя играть сам с собой!")
        return

    balances = load_balances()
    ch_id = str(challenger.id)
    op_id = str(opponent.id)

    ch_profile = get_user_profile(balances, ch_id)
    if ch_profile["balance"] < bet:
        await update.message.reply_text("❌ Недостаточно монет!")
        return

    # Списание ставки
    ch_profile["balance"] -= bet
    ch_profile['username'] = challenger.first_name

    # Инициализация профиля оппонента
    op_profile = get_user_profile(balances, op_id)
    op_profile['username'] = opponent.first_name
    save_balances(balances)

    duel_id = str(uuid.uuid4())[:8]
    context.bot_data[f"duel_{duel_id}"] = {
        "id": duel_id,
        "challenger_id": ch_id,
        "opponent_id": op_id,
        "bet": bet,
        "challenger_name": challenger.first_name,
        "opponent_name": opponent.first_name,
        "next_player": ch_id,
        "rolls": {},
        "message_ids": []
    }

    keyboard = [
        [InlineKeyboardButton("✅ Принять", callback_data=f"accept_{duel_id}")],
        [InlineKeyboardButton("❌ Отклонить", callback_data=f"decline_{duel_id}")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    msg = await update.message.reply_text(
        f"{challenger.first_name} вызывает {opponent.first_name} на дуэль!\n"
        f"Ставка: {bet} монет 🎲 (Списано с баланса)",
        reply_markup=reply_markup
    )
    context.bot_data[f"duel_{duel_id}"]["message_ids"].append(msg.message_id)

async def handle_accept_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data
    if not data.startswith("accept_"):
        return

    duel_id = data.split("_", 1)[1]
    duel_key = f"duel_{duel_id}"

    if duel_key not in context.bot_data:
        await query.edit_message_text("❌ Дуэль не найдена (возможно, истекло время).")
        return

    duel_data = context.bot_data[duel_key]
    opponent_id = str(update.effective_user.id)

    if duel_data["opponent_id"] != opponent_id:
        await query.answer("❌ Эта дуэль не для вас!", show_alert=True)
        return

    balances = load_balances()
    op_profile = get_user_profile(balances, opponent_id)
    bet = duel_data["bet"]

    if op_profile["balance"] < bet:
        await query.answer("❌ Недостаточно монет для принятия вызова!", show_alert=True)
        return

    # Списание у оппонента
    op_profile["balance"] -= bet
    save_balances(balances)

    add_win(query.message.chat.id, duel_data["challenger_id"])
    add_win(query.message.chat.id, opponent_id)

    ch_name = duel_data["challenger_name"]
    keyboard = [[InlineKeyboardButton("🎲 Бросить", callback_data=f"roll_{duel_id}_{duel_data['challenger_id']}")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    msg = await query.edit_message_text(
        f"🎲 Дуэль началась! Банк: {bet * 2} монет\nХод {ch_name}",
        reply_markup=reply_markup
    )
    duel_data["message_ids"].append(msg.message_id)

async def handle_decline_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data
    if not data.startswith("decline_"):
        return

    duel_id = data.split("_", 1)[1]
    duel_key = f"duel_{duel_id}"

    if duel_key not in context.bot_data:
        await query.edit_message_text("❌ Дуэль не найдена (возможно, истекло время).")
        return

    duel_data = context.bot_data[duel_key]
    opponent_id = str(update.effective_user.id)

    if duel_data["opponent_id"] != opponent_id:
        await query.answer("❌ Эта дуэль не для вас!", show_alert=True)
        return

    challenger_name = duel_data["challenger_name"]
    bet = duel_data["bet"]
    ch_id = duel_data["challenger_id"]

    # Возврат средств
    balances = load_balances()
    ch_prof = get_user_profile(balances, ch_id)
    ch_prof["balance"] += bet
    save_balances(balances)

    await query.edit_message_text(
        f"❌ Дуэль с {challenger_name} отклонена.\n"
        f"💰 {challenger_name}, ваша ставка {bet} монет возвращена."
    )
    
    # Удаляем сообщения дуэли
    for msg_id in duel_data["message_ids"]:
        try:
            await context.bot.delete_message(chat_id=query.message.chat.id, message_id=msg_id)
        except:
            pass
            
    del context.bot_data[duel_key]

async def handle_roll_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    
    try:
        await query.answer()
    except TelegramError:
        pass

    data = query.data
    parts = data.split("_")
    
    if len(parts) != 3 or parts[0] != "roll":
        await query.edit_message_text("❌ Ошибка: неверные данные.")
        return

    duel_id = parts[1]
    real_player_id = str(update.effective_user.id)
    duel_key = f"duel_{duel_id}"

    if duel_key not in context.bot_data:
        await query.edit_message_text("❌ Дуэль не найдена.")
        return

    duel_data = context.bot_data[duel_key]

    if duel_data["next_player"] != real_player_id:
        await query.answer("❌ Не ваш ход!", show_alert=True)
        return
        
    chat_id = query.message.chat_id
    bet = duel_data["bet"]
    
    try:
        await query.delete_message()
    except:
        pass

    roll1 = random.randint(1, 6)
    roll2 = random.randint(1, 6)
    total = roll1 + roll2

    roll_msg = await send_cached_video(context, chat_id, roll1, roll2)
    if roll_msg:
        duel_data["message_ids"].append(roll_msg.message_id)

    await asyncio.sleep(4.5)

    duel_data["rolls"][real_player_id] = total

    ch_id = duel_data["challenger_id"]
    op_id = duel_data["opponent_id"]
    ch_name = duel_data["challenger_name"]
    op_name = duel_data["opponent_name"]
    
    if real_player_id == ch_id:
        duel_data["next_player"] = op_id
        
        async def auto_lose():
            await asyncio.sleep(120) 
            if duel_key not in context.bot_data:
                return
            current_duel = context.bot_data.get(duel_key)
            if not current_duel or current_duel.get("next_player") != op_id or op_id in current_duel.get("rolls", {}):
                return
                
            balances = load_balances()
            ch_prof = get_user_profile(balances, ch_id)
            op_prof = get_user_profile(balances, op_id)
            
            win_amount = bet * 2 
            
            ch_prof["balance"] += win_amount
            ch_prof["games_played"] += 1
            op_prof["games_played"] += 1
            ch_prof["wins"] += 1
            op_prof["losses"] += 1
            ch_prof["total_won"] += bet
            op_prof["total_lost"] += bet
            add_xp(ch_prof, 10) 
            add_xp(op_prof, 0)
            save_balances(balances)
            
            final_text = f"⏰ {op_name} не успел(а) бросить кубики!\n🎲 **{ch_name}**, ты забрал банк: **{bet * 2}** монет!"
            
            await context.bot.send_message(
                chat_id=chat_id,
                text=final_text,
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Меню", callback_data="back_to_menu|duel")]])
            )

            for msg_id in current_duel["message_ids"]:
                try:
                    await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
                except:
                    pass

            del context.bot_data[duel_key]
        
        asyncio.create_task(auto_lose())

        keyboard = [[InlineKeyboardButton("🎲 Бросить", callback_data=f"roll_{duel_id}_{op_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        msg = await context.bot.send_message(chat_id=chat_id, text=f"Ход {op_name} (у вас 2 минуты!)", reply_markup=reply_markup)
        duel_data["message_ids"].append(msg.message_id)

    elif real_player_id == op_id:
        duel_data["next_player"] = None
        await finish_duel(context, duel_id, chat_id)

async def finish_duel(context: ContextTypes.DEFAULT_TYPE, duel_id: str, chat_id: int):
    duel_key = f"duel_{duel_id}"
    if duel_key not in context.bot_data:
        return
        
    duel_data = context.bot_data[duel_key]
    bet = duel_data["bet"]
    
    ch_id = duel_data["challenger_id"]
    op_id = duel_data["opponent_id"]
    ch_name = duel_data["challenger_name"]
    op_name = duel_data["opponent_name"]

    ch_roll = duel_data["rolls"].get(ch_id, 0)
    op_roll = duel_data["rolls"].get(op_id, 0)
    
    balances = load_balances()
    ch_prof = get_user_profile(balances, ch_id)
    op_prof = get_user_profile(balances, op_id)
    ch_prof.setdefault("draws", 0)
    op_prof.setdefault("draws", 0)

    winner_name = None
    win_amount = bet * 2
    
    if ch_roll > op_roll:
        winner_name = ch_name
        ch_prof["games_played"] += 1
        op_prof["games_played"] += 1
        ch_prof["wins"] += 1
        op_prof["losses"] += 1
        ch_prof["total_won"] += bet
        op_prof["total_lost"] += bet
        levels_gained = add_xp(ch_prof, 10) 
        add_xp(op_prof, 0)
        ch_prof["balance"] += win_amount
        
        final_message = f"🎲 {ch_name} *{ch_roll}* vs {op_name} *{op_roll}*\n→ 🏆 **{winner_name}** забрал банк!"

    elif op_roll > ch_roll:
        winner_name = op_name
        ch_prof["games_played"] += 1
        op_prof["games_played"] += 1
        op_prof["wins"] += 1
        ch_prof["losses"] += 1
        op_prof["total_won"] += bet
        ch_prof["total_lost"] += bet
        levels_gained = add_xp(op_prof, 10) 
        add_xp(ch_prof, 0)
        op_prof["balance"] += win_amount
        
        final_message = f"🎲 {ch_name} *{ch_roll}* vs {op_name} *{op_roll}*\n→ 🏆 **{winner_name}** забрал банк!"
        
    else:
        ch_prof["draws"] += 1
        op_prof["draws"] += 1
        ch_prof["games_played"] += 1
        op_prof["games_played"] += 1
        ch_prof["balance"] += bet
        op_prof["balance"] += bet
        ch_prof["total_lost"] -= bet
        op_prof["total_lost"] -= bet
        
        add_xp(ch_prof, 5)
        add_xp(op_prof, 5)
        
        final_message = f"🎲 {ch_name} *{ch_roll}* vs {op_name} *{op_roll}*\n→ 🤝 **Ничья!** Ставки возвращены."

    save_balances(balances)
    
    await context.bot.send_message(
        chat_id=chat_id,
        text=final_message,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Меню", callback_data="back_to_menu|duel")]]),
        parse_mode="Markdown"
    )

    for msg_id in duel_data["message_ids"]:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
        except:
            pass

    del context.bot_data[duel_key]

async def handle_menu_daily(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await daily_spin_roulette(update, context)

# === ОБРАБОТЧИКИ ТЕКСТА ===
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    
    # 1. Проверка на фразу дуэли
    if re.search(r'давай\s+на\s+\d+', text.lower()):
        await handle_duel_phrase(update, context)
        return

    # 2. Проверка на ввод произвольной ставки
    if await handle_custom_bet_input(update, context):
        return

# === РЕГИСТРАЦИЯ ОБРАБОТЧИКОВ ===
def main():
    print("Запускаю бота...")
    app = Application.builder().token(BOT_TOKEN).build()
    
    app.bot_data["animation_cache"] = load_animation_cache()
    print(f"Кэш анимаций загружен: {len(app.bot_data['animation_cache'])} записей")
    
    # Команды
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("balance", balance))
    app.add_handler(CommandHandler("statsme", statsme))
    app.add_handler(CommandHandler("topchat", topchat))
    app.add_handler(CommandHandler("globaltop", global_top)) 
    app.add_handler(CommandHandler("duel", duel_command))  # ИСПРАВЛЕННЫЙ ОБРАБОТЧИК
    app.add_handler(CommandHandler("daily", daily_spin_roulette))
    
    # Inline-кнопки
    app.add_handler(CallbackQueryHandler(handle_accept_callback, pattern=r"^accept_"))
    app.add_handler(CallbackQueryHandler(handle_decline_callback, pattern=r"^decline_"))
    app.add_handler(CallbackQueryHandler(handle_roll_callback, pattern=r"^roll_"))
    app.add_handler(CallbackQueryHandler(handle_back_to_menu, pattern=r"^back_to_menu"))
    app.add_handler(CallbackQueryHandler(handle_repeat_game_callback, pattern=r"^repeat\|"))
    
    # Новые Inline-обработчики для меню
    app.add_handler(CallbackQueryHandler(handle_inline_back_callback, pattern=r"^back_to_main|^back_to_mode_"))
    app.add_handler(CallbackQueryHandler(handle_mode_selection_callback, pattern=r"^mode_"))
    app.add_handler(CallbackQueryHandler(handle_bet_selection_callback, pattern=r"^bet_"))
    
    # Reply-кнопки главного меню
    app.add_handler(MessageHandler(filters.Regex(re.compile(r"^🎲 Дуэль$")), handle_menu_duel))
    app.add_handler(MessageHandler(filters.Regex(re.compile(r"^🎯 Угадай сумму$")), handle_menu_guess))
    app.add_handler(MessageHandler(filters.Regex(re.compile(r"^🎰 Быстрые игры$")), handle_menu_play))
    app.add_handler(MessageHandler(filters.Regex(re.compile(r"^🎁 Бонус$")), handle_menu_daily))
    app.add_handler(MessageHandler(filters.Regex(re.compile(r"^💰 Баланс$")), handle_menu_balance))
    app.add_handler(MessageHandler(filters.Regex(re.compile(r"^🏆 Топ чата$")), handle_menu_top))
    app.add_handler(MessageHandler(filters.Regex(re.compile(r"^📊 Статистика$")), handle_menu_stats))
    
    # Обработчик текста
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_text))

    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()