import asyncio
import logging
import os
import re
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
from contextlib import contextmanager
import json
import sqlite3
import urllib.parse
from urllib.parse import urljoin, quote_plus

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, InputMediaPhoto
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from aiogram.client.default import DefaultBotProperties
import aiohttp
from bs4 import BeautifulSoup

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфігурація - **УВАГА: ЗАВЖДИ ВИКОРИСТОВУЙТЕ ЗМІННІ СЕРЕДОВИЩА ДЛЯ ТОКЕНУ**
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    logger.warning("BOT_TOKEN не встановлено у змінних середовища. Використовується заглушка.")
    BOT_TOKEN = "8264057355:AAGgZMq5_2lNJRs5JV8KudlPyiQv6iKj_Sk"

DATABASE_PATH = "bot_database.db"

# Міста та їх URL
CITIES: Dict[str, Dict[str, Any]] = {
    "chernivtsi": {
        "name": "Чернівці (Обленерго)",
        "schedule_url": "https://oblenergo.cv.ua/shutdowns/",
        "parser_type": "chernivtsi_custom",
        "groups": 12
    },
    "kyiv": {
        "name": "Київ (ДТЕК)",
        "schedule_url": "https://www.dtek-kem.com.ua/ua/shutdowns",
        "search_url_api": "https://api-kem-dtek.com.ua/api/v1/user_schedules_info",
        "parser_type": "kyiv_dtek_address",
        "note": "Потрібна адреса (2 етапи)."
    },
    "khmelnytskyi": {
        "name": "Хмельницький (Обленерго)",
        "schedule_url": "https://hoe.com.ua/page/pogodinni-vidkljuchennja",
        "parser_type": "image_based",
        "note": "Графік у форматі зображення."
    },
    "kamyanets": {
        "name": "Кам'янець-Подільський (Обленерго)",
        "schedule_url": "https://hoe.com.ua/page/pogodinni-vidkljuchennja",
        "parser_type": "image_based",
        "note": "Графік у форматі зображення."
    }
}


# FSM стани
class UserStates(StatesGroup):
    waiting_for_group = State()
    waiting_for_kyiv_street = State()  # ОНОВЛЕНО
    waiting_for_kyiv_house = State()  # ДОДАНО


# База даних
@contextmanager
def get_db():
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def init_db():
    """Ініціалізація бази даних"""
    with get_db() as conn:
        # ОНОВЛЕНО: Додано поля dtek_street_id та dtek_house_id
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                city TEXT DEFAULT 'chernivtsi',
                group_number INTEGER,
                address TEXT,
                dtek_street_id TEXT,
                dtek_house_id TEXT,
                notifications_enabled INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # ОНОВЛЕНО: Додано поля dtek_street_id та dtek_house_id
        conn.execute("""
            CREATE TABLE IF NOT EXISTS user_cities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                city TEXT,
                group_number INTEGER,
                address TEXT,
                dtek_street_id TEXT,
                dtek_house_id TEXT,
                UNIQUE(user_id, city),
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS schedules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                city TEXT,
                group_number INTEGER,
                date TEXT,
                schedule_data TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(city, group_number, date)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS image_schedules (
                city TEXT PRIMARY KEY,
                image_url TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS notifications_sent (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                city TEXT,
                group_number INTEGER,
                date TEXT,
                time TEXT,
                sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()


# Користувачі
class UserManager:
    @staticmethod
    def save_user(user_id: int, username: str, city: str = "chernivtsi", group_number: Optional[int] = None):
        with get_db() as conn:
            conn.execute("""
                INSERT INTO users (user_id, username, city, group_number)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    username = excluded.username,
                    city = excluded.city 
            """, (user_id, username, city, group_number))
            conn.commit()

    @staticmethod
    def update_city(user_id: int, city: str):
        with get_db() as conn:
            conn.execute(
                "UPDATE users SET city = ?, group_number = NULL, address = NULL, dtek_street_id = NULL, dtek_house_id = NULL WHERE user_id = ?",
                (city, user_id)
            )
            conn.commit()

    @staticmethod
    def update_group(user_id: int, group_number: int):
        with get_db() as conn:
            conn.execute(
                "UPDATE users SET group_number = ? WHERE user_id = ?",
                (group_number, user_id)
            )
            user = UserManager.get_user(user_id)
            if user:
                conn.execute("""
                    INSERT INTO user_cities (user_id, city, group_number)
                    VALUES (?, ?, ?)
                    ON CONFLICT(user_id, city) DO UPDATE SET
                        group_number = excluded.group_number,
                        address = NULL, dtek_street_id = NULL, dtek_house_id = NULL
                """, (user_id, user.get('city', 'chernivtsi'), group_number))
            conn.commit()

    @staticmethod
    def update_address(user_id: int, address: str, city: str, street_id: str, house_id: str):
        """ОНОВЛЕНО: Зберігає ID для ДТЕК"""
        with get_db() as conn:
            conn.execute(
                "UPDATE users SET address = ?, city = ?, group_number = NULL, dtek_street_id = ?, dtek_house_id = ? WHERE user_id = ?",
                (address, city, street_id, house_id, user_id)
            )
            conn.execute("""
                INSERT INTO user_cities (user_id, city, address, dtek_street_id, dtek_house_id, group_number)
                VALUES (?, ?, ?, ?, ?, NULL)
                ON CONFLICT(user_id, city) DO UPDATE SET
                    address = excluded.address,
                    dtek_street_id = excluded.dtek_street_id,
                    dtek_house_id = excluded.dtek_house_id
            """, (user_id, city, address, street_id, house_id))
            conn.commit()

    @staticmethod
    def get_user(user_id: int) -> Optional[Dict]:
        with get_db() as conn:
            row = conn.execute(
                "SELECT * FROM users WHERE user_id = ?", (user_id,)
            ).fetchone()
            return dict(row) if row else None

    @staticmethod
    def get_user_cities(user_id: int) -> List[Dict]:
        with get_db() as conn:
            rows = conn.execute(
                "SELECT * FROM user_cities WHERE user_id = ?", (user_id,)
            ).fetchall()
            return [dict(row) for row in rows]

    @staticmethod
    def get_users_by_city_and_group(city: str, group_number: int) -> List[Dict]:
        with get_db() as conn:
            rows = conn.execute(
                "SELECT * FROM users WHERE city = ? AND group_number = ? AND notifications_enabled = 1",
                (city, group_number)
            ).fetchall()
            return [dict(row) for row in rows]

    @staticmethod
    def get_users_by_city(city: str) -> List[Dict]:
        with get_db() as conn:
            rows = conn.execute(
                "SELECT * FROM users WHERE city = ? AND notifications_enabled = 1",
                (city,)
            ).fetchall()
            return [dict(row) for row in rows]

    @staticmethod
    def toggle_notifications(user_id: int) -> bool:
        with get_db() as conn:
            current = conn.execute(
                "SELECT notifications_enabled FROM users WHERE user_id = ?", (user_id,)
            ).fetchone()

            if current:
                new_state = 0 if current['notifications_enabled'] else 1
                conn.execute(
                    "UPDATE users SET notifications_enabled = ? WHERE user_id = ?",
                    (new_state, user_id)
                )
                conn.commit()
                return bool(new_state)
            return False


# Покращений парсер графіків
class ScheduleParser:
    # Заголовки, що імітують браузер (для ДТЕК)
    _DTEK_HEADERS = {
        'Content-Type': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'X-Requested-With': 'XMLHttpRequest',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Referer': CITIES['kyiv']['schedule_url'],
        'Accept-Language': 'uk-UA,uk;q=0.9',
        'Sec-Fetch-Mode': 'cors'
    }

    @staticmethod
    async def fetch_schedule(city: str = "chernivtsi", user: Optional[Dict] = None) -> Optional[
        Dict[int, List[Dict]]]:
        """ОНОВЛЕНО: Приймає user dict для отримання ID ДТЕК"""
        try:
            city_data = CITIES.get(city)
            if not city_data:
                logger.error(f"Місто {city} не знайдено в конфігурації")
                return None

            parser_type = city_data.get('parser_type', 'default')

            if parser_type == 'chernivtsi_custom':
                return await ScheduleParser._parse_chernivtsi(city_data)
            elif parser_type == 'kyiv_dtek_address' and user:
                # Отримуємо графік за збереженими ID
                street_id = user.get('dtek_street_id')
                house_id = user.get('dtek_house_id')
                if not street_id or not house_id:
                    logger.warning(f"Для {user['user_id']} не встановлено ID ДТЕК")
                    return {0: [{'time': 'Помилка', 'status': 'error',
                                 'message': 'Адресу не налаштовано. Будь ласка, оберіть "🏠 Змінити адресу".'}]}

                schedule = await ScheduleParser._get_kyiv_schedule(city_data, street_id, house_id)
                return {0: schedule} if schedule else None
            elif parser_type == 'image_based':
                return await ScheduleParser._parse_image_based(city_data, city)
            else:
                return await ScheduleParser._parse_generic(city_data, city)

        except Exception as e:
            logger.error(f"[{city}] Критична помилка парсингу: {e}", exc_info=True)
            return None

    @staticmethod
    async def _parse_chernivtsi(city_data: dict) -> Optional[Dict[int, List[Dict]]]:
        """Парсер для Чернівців (з посиленими заголовками)"""
        try:
            url = city_data['schedule_url']
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'uk-UA,uk;q=0.9',
                'Referer': url,
                'Cache-Control': 'no-cache'
            }

            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=30) as response:
                    if response.status != 200:
                        logger.error(f"HTTP {response.status} для Чернівців")
                        return None
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    # ... (логіка парсингу Чернівців залишається)
                    time_headers = []
                    time_container = soup.find('div', {'id': 'gsv'})
                    if time_container:
                        for time_block in time_container.find_all('b', string=re.compile(r'\d{1,2}:\d{2}', re.I)):
                            hour_match = re.search(r'(\d{1,2}):\d{2}', time_block.get_text(strip=True))
                            if hour_match:
                                hour = int(hour_match.group(1))
                                time_headers.append(f"{hour:02d}:00-{hour:02d}:30")
                                time_headers.append(f"{hour:02d}:30-{hour + 1:02d}:00" if hour < 23 else "23:30-00:00")
                    if not time_headers:
                        logger.warning("[Чернівці] Часові інтервали не знайдено.")
                        return None
                    schedule_data = {}
                    for group_num in range(1, city_data['groups'] + 1):
                        group_div = soup.find('div', {'id': f'inf{group_num}'})
                        if not group_div:
                            continue
                        schedule_data[group_num] = []
                        cells = [tag for tag in group_div.descendants if tag.name in ['u', 'o', 's']]
                        for idx, cell in enumerate(cells):
                            if idx >= len(time_headers):
                                break
                            tag_name = cell.name
                            if tag_name == 'u':
                                status = 'on'
                            elif tag_name == 'o':
                                status = 'off'
                            elif tag_name == 's':
                                status = 'maybe'
                            else:
                                status = 'on'
                            schedule_data[group_num].append({'time': time_headers[idx], 'status': status})
                    return schedule_data if schedule_data else None

        except Exception as e:
            logger.error(f"[Чернівці] Помилка парсингу: {e}", exc_info=True)
            return None

    # --- НОВІ ФУНКЦІЇ ДЛЯ ДТЕК ---
    @staticmethod
    async def _find_kyiv_streets(city_data: dict, street_name: str) -> Optional[List[Dict]]:
        """Етап 1: Пошук вулиці за назвою"""
        api_url = city_data['search_url_api']
        clean_address = re.sub(r'[*\-_~]', '', street_name).strip()
        try:
            async with aiohttp.ClientSession() as session:
                response = await session.post(
                    api_url,
                    json={"search": clean_address},
                    headers=ScheduleParser._DTEK_HEADERS
                )
                data = await response.json()
                if data and data.get('results'):
                    return data['results']
                return None
        except Exception as e:
            logger.error(f"[Київ] Помилка _find_kyiv_streets: {e}", exc_info=True)
            return None

    @staticmethod
    async def _find_kyiv_houses(city_data: dict, street_id: str) -> Optional[List[Dict]]:
        """Етап 2: Пошук будинків на вулиці"""
        api_url = city_data['search_url_api']
        try:
            async with aiohttp.ClientSession() as session:
                response = await session.post(
                    api_url,
                    json={"street_id": street_id},
                    headers=ScheduleParser._DTEK_HEADERS
                )
                data = await response.json()
                if data and data.get('results'):
                    return data['results']
                return None
        except Exception as e:
            logger.error(f"[Київ] Помилка _find_kyiv_houses: {e}", exc_info=True)
            return None

    @staticmethod
    async def _get_kyiv_schedule(city_data: dict, street_id: str, house_id: str) -> Optional[List[Dict]]:
        """Етап 3: Отримання графіку за ID"""
        api_url = city_data['search_url_api']
        try:
            async with aiohttp.ClientSession() as session:
                response = await session.post(
                    api_url,
                    json={
                        "street_id": street_id,
                        "house_id": house_id,
                        "language": "ua"
                    },
                    headers=ScheduleParser._DTEK_HEADERS
                )
                data = await response.json()
                schedule_list = []
                raw_schedule = data.get('current_schedule', [])
                for item in raw_schedule:
                    schedule_list.append({
                        'time': item['time'],
                        'status': item['status'].lower().replace('possible', 'maybe')
                    })
                if not schedule_list:
                    return [{'time': 'Інформація', 'status': 'info',
                             'message': 'Графік відсутній, відключень наразі немає.'}]
                return schedule_list
        except Exception as e:
            logger.error(f"[Київ] Помилка _get_kyiv_schedule: {e}", exc_info=True)
            return [{'time': 'Помилка', 'status': 'error', 'message': 'Помилка обробки даних графіку ДТЕК.'}]

    # --- КІНЕЦЬ ФУНКЦІЙ ДТЕК ---

    @staticmethod
    async def _parse_image_based(city_data: dict, city: str) -> Optional[Dict[int, List[Dict]]]:
        """ОНОВЛЕНО: Парсер для міст з графіками у вигляді зображень (Хмельницький/Кам'янець)"""
        try:
            url = city_data['schedule_url']
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Cache-Control': 'no-cache'
            }

            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=30) as response:
                    if response.status != 200:
                        return None
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    images = soup.find_all('img', src=re.compile(r'(grafik|schedule|vidkl|pogod|jpg|jpeg|png)', re.I))

                    if images:
                        img_url = images[0].get('src')
                        if not img_url.startswith('http'):
                            img_url = urljoin(url, img_url)
                        logger.info(f"[{city}] Знайдено зображення графіку: {img_url}")
                        ScheduleParser._save_image_url(city, img_url)
                        return {0: [{'time': 'Інформація', 'status': 'info',
                                     'message': f'Актуальний графік у форматі зображення', 'image_url': img_url}]}

            logger.warning(f"[{city}] Зображення графіку не знайдено.")
            return {0: [{'time': 'Інформація', 'status': 'info',
                         'message': 'Графік у форматі зображення. Зображення на сайті не знайдено.'}]}

        except Exception as e:
            logger.error(f"[{city}] Помилка парсингу зображення: {e}", exc_info=True)
            return None

    @staticmethod
    def _save_image_url(city: str, image_url: str):
        with get_db() as conn:
            conn.execute("""
                INSERT INTO image_schedules (city, image_url)
                VALUES (?, ?)
                ON CONFLICT(city) DO UPDATE SET
                    image_url = excluded.image_url,
                    updated_at = CURRENT_TIMESTAMP
            """, (city, image_url))
            conn.commit()

    @staticmethod
    def _get_image_url(city: str) -> Optional[str]:
        with get_db() as conn:
            row = conn.execute(
                "SELECT image_url FROM image_schedules WHERE city = ?", (city,)
            ).fetchone()
            return row['image_url'] if row else None

    @staticmethod
    def _parse_generic(city_data: dict, city: str) -> Optional[Dict[int, List[Dict]]]:
        return None

    @staticmethod
    def save_schedule(city: str, group_number: int, date: str, schedule_data: str):
        with get_db() as conn:
            conn.execute("""
                INSERT INTO schedules (city, group_number, date, schedule_data)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(city, group_number, date) DO UPDATE SET
                    schedule_data = excluded.schedule_data,
                    updated_at = CURRENT_TIMESTAMP
            """, (city, group_number, date, schedule_data))
            conn.commit()

    @staticmethod
    def get_schedule(city: str, group_number: int, date: str) -> Optional[str]:
        with get_db() as conn:
            row = conn.execute(
                "SELECT schedule_data FROM schedules WHERE city = ? AND group_number = ? AND date = ?",
                (city, group_number, date)
            ).fetchone()
            return row['schedule_data'] if row else None


# Форматування
def format_schedule(schedule: List[Dict], city_data: dict = None) -> str:
    if not schedule:
        return "✅ Наразі планових відключень немає!"

    if schedule and schedule[0].get('status') in ['info', 'error']:
        msg = schedule[0].get('message', '')
        if 'image_url' in schedule[0]:
            return f"📷 {msg}\n\n[Переглянути зображення]({schedule[0]['image_url']})"
        emoji = "❌" if schedule[0].get('status') == 'error' else "ℹ️"
        return f"{emoji} {msg}"

    has_outages = any(item['status'] == 'off' for item in schedule)
    if not has_outages:
        return "✅ Чудові новини! Сьогодні планових відключень немає!"

    text = "📊 Графік відключень на сьогодні:\n\n"
    for item in schedule:
        emoji = {"off": "🔴", "on": "🟢", "maybe": "⚪"}.get(item['status'], "⚪")
        status_text = {"off": "Відключення", "on": "Світло є", "maybe": "Можливо"}.get(item['status'], "Невідомо")
        text += f"{emoji} {item['time']} - {status_text}\n"
    text += "\n🔴 - гарантоване відключення\n🟢 - гарантоване включення\n⚪ - можливе включення\n"
    return text


# Клавіатури
def get_main_keyboard(user_city: str = "chernivtsi") -> InlineKeyboardMarkup:
    city_name = CITIES.get(user_city, {}).get('name', 'Чернівці')
    city_data = CITIES.get(user_city, {})
    if city_data.get('parser_type') == 'kyiv_dtek_address':
        group_or_address_button = InlineKeyboardButton(text="🏠 Змінити адресу", callback_data="change_address")
    else:
        group_or_address_button = InlineKeyboardButton(text="⚙️ Змінити групу", callback_data="change_group")
    keyboard = [
        [InlineKeyboardButton(text="📊 Мій графік", callback_data="my_schedule")],
        [InlineKeyboardButton(text="🔄 Оновити графік", callback_data="refresh_schedule")],
        [InlineKeyboardButton(text=f"🏙 Місто: {city_name}", callback_data="change_city")],
        [group_or_address_button],
        [InlineKeyboardButton(text="🔔 Налаштування", callback_data="settings")],
        [InlineKeyboardButton(text="❓ Допомога", callback_data="help")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def get_cities_keyboard() -> InlineKeyboardMarkup:
    keyboard = []
    for city_id, city_data in CITIES.items():
        text = city_data['name']
        if city_data.get('note'):
            text += " ⚠️"
        keyboard.append([InlineKeyboardButton(text=text, callback_data=f"city_{city_id}")])
    keyboard.append([InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_menu")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def get_groups_keyboard() -> InlineKeyboardMarkup:
    keyboard = []
    for i in range(0, 18, 3):
        row = []
        for j in range(3):
            group_num = i + j + 1
            if group_num <= 18:
                row.append(InlineKeyboardButton(text=f"Група {group_num}", callback_data=f"group_{group_num}"))
        if row:
            keyboard.append(row)
    keyboard.append([InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_menu")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


# Бот
router = Router()


@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    user = UserManager.get_user(message.from_user.id)
    if not user:
        UserManager.save_user(message.from_user.id, message.from_user.username or "Unknown")
        user = UserManager.get_user(message.from_user.id)
    city = user.get('city', 'chernivtsi')
    city_data = CITIES.get(city, {})
    city_name = city_data.get('name', 'Чернівці')
    welcome_text = ("👋 Вітаю! Я бот для відстеження графіків відключення світла.\n\n"
                    "🔹 Я буду надсилати вам:\n  • Актуальний графік відключень\n"
                    "  • Сповіщення за 30 хв до відключення\n  • Інформацію про зміни в графіку\n\n")
    is_address_city = city_data.get('parser_type') == 'kyiv_dtek_address'
    if is_address_city and user.get('address'):
        welcome_text = (f"👋 З поверненням!\n\n🏙 Місто: {city_name}\n🏠 Адреса: {user['address']}")
    elif not is_address_city and user.get('group_number'):
        welcome_text = (f"👋 З поверненням!\n\n🏙 Місто: {city_name}\n⚡️ Група: {user['group_number']}")
    else:
        welcome_text += "Спочатку оберіть місто та, якщо потрібно, групу/адресу 👇"
    await message.answer(welcome_text, reply_markup=get_main_keyboard(city))


@router.message(Command("debug"))
async def cmd_debug(message: Message):
    user = UserManager.get_user(message.from_user.id)
    if not user:
        await message.answer("❌ Користувача не знайдено в базі даних")
        return
    debug_text = "🔍 Ваші дані в системі:\n\n"
    debug_text += f"User ID: {user['user_id']}\n"
    debug_text += f"Username: {user.get('username', 'N/A')}\n"
    debug_text += f"Місто: {user.get('city', 'N/A')}\n"
    debug_text += f"Група: {user.get('group_number', 'N/A')}\n"
    debug_text += f"Адреса: {user.get('address', 'N/A')}\n"
    debug_text += f"DTEK Street ID: {user.get('dtek_street_id', 'N/A')}\n"
    debug_text += f"DTEK House ID: {user.get('dtek_house_id', 'N/A')}\n"
    debug_text += f"Сповіщення: {'✅' if user.get('notifications_enabled') else '❌'}\n"
    await message.answer(debug_text)


@router.message(Command("update"))
async def cmd_update(message: Message):
    await message.answer("⏳ Запускаю повне оновлення графіків для всіх міст...")
    bot = message.bot
    await update_schedules(bot)
    await check_and_notify_image_changes(bot)
    await message.answer("✅ Графіки в базі даних оновлено!")


@router.message(Command("test"))
async def cmd_test(message: Message):
    user = UserManager.get_user(message.from_user.id)
    city = user.get('city', 'chernivtsi') if user else 'chernivtsi'
    city_data = CITIES[city]
    city_name = city_data['name']
    await message.answer(f"⏳ Тестую парсинг для {city_name}...")
    schedules = await ScheduleParser.fetch_schedule(city, user=user)  # Передаємо user
    if schedules is None:
        await message.answer(f"❌ Помилка завантаження для {city_name}")
    elif not schedules:
        await message.answer(f"✅ Графіки для {city_name} порожні (відключень немає)")
    else:
        text = f"✅ Успішно! {city_name}\nЗнайдено {len(schedules)} груп/адрес\n\n"
        first_key = min(schedules.keys())
        text += f"Приклад (ключ {first_key}):\n"
        schedule_list = schedules[first_key]
        if schedule_list and schedule_list[0].get('status') in ['info', 'error']:
            text += format_schedule(schedule_list)
        else:
            for item in schedule_list[:5]:
                emoji = {"off": "🔴", "on": "🟢", "maybe": "⚪", "info": "ℹ️"}.get(item['status'], "⚪")
                text += f"{emoji} {item['time']}: {item['status']}\n"
        await message.answer(text, disable_web_page_preview=False)


@router.callback_query(F.data == "my_schedule")
async def show_schedule(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    user = UserManager.get_user(callback.from_user.id)

    if not user:
        UserManager.save_user(callback.from_user.id, callback.from_user.username or "Unknown")
        user = UserManager.get_user(callback.from_user.id)

    if not user:
        await callback.message.answer("❌ Сталася помилка. Спробуйте /start")
        return

    city = user.get('city', 'chernivtsi')
    city_data = CITIES.get(city, {})
    city_name = city_data.get('name', 'Чернівці')
    parser_type = city_data.get('parser_type')
    is_address_city = parser_type == 'kyiv_dtek_address'

    if is_address_city and (not user.get('address') or not user.get('dtek_street_id')):
        await callback.message.answer(f"❌ Для {city_name} потрібно вказати **адресу**.",
                                      reply_markup=get_main_keyboard(city))
        await change_address(callback, state)  # Передаємо state
        return
    elif parser_type == 'image_based' and not user.get('group_number'):
        # Для image-based міст ми все ще використовуємо групу як "підписку"
        await callback.message.answer(f"❌ Для {city_name} потрібно обрати **групу**.",
                                      reply_markup=get_main_keyboard(city))
        await change_group(callback, state)  # Передаємо state
        return
    elif parser_type == 'chernivtsi_custom' and not user.get('group_number'):
        await callback.message.answer(f"❌ Для {city_name} потрібно обрати **групу**.",
                                      reply_markup=get_main_keyboard(city))
        await change_group(callback, state)  # Передаємо state
        return

    group_num = user.get('group_number', 0)
    address = user.get('address')
    today = datetime.now().strftime("%Y-%m-%d")

    # Для image-based міст логіка інша - показуємо URL з image_schedules
    if parser_type == 'image_based':
        await callback.message.answer("⏳ Завантажую актуальний графік...")
        img_url = ScheduleParser._get_image_url(city)
        if img_url:
            text = f"🏙 {city_name}\n⚡️ Група {group_num}\n\n" + format_schedule(
                [{'status': 'info', 'message': 'Актуальний графік у форматі зображення', 'image_url': img_url}])
            await callback.message.answer(text, reply_markup=get_main_keyboard(city), disable_web_page_preview=False)
        else:
            await callback.message.answer(f"❌ Графік для {city_name} ще не завантажено. Спробуйте оновити.",
                                          reply_markup=get_main_keyboard(city))
        return

    # Логіка для Києва та Чернівців (з кешуванням в БД)
    schedule_data = ScheduleParser.get_schedule(city, group_num, today)

    if schedule_data:
        schedule = json.loads(schedule_data)
        text = format_schedule(schedule, city_data)
        info_line = f"⚡️ Група {group_num}" if not is_address_city else f"🏠 Адреса: {address}"
        text = f"🏙 {city_name}\n{info_line}\n\n" + text
        with get_db() as conn:
            updated = conn.execute("SELECT updated_at FROM schedules WHERE city = ? AND group_number = ? AND date = ?",
                                   (city, group_num, today)).fetchone()
            if updated:
                text += f"\n\n🕐 Оновлено: {updated['updated_at'].split('.')[0]}"
        await callback.message.answer(text, reply_markup=get_main_keyboard(city), disable_web_page_preview=False)
        return

    await callback.message.answer("⏳ Завантажую актуальний графік...")

    try:
        schedules = await ScheduleParser.fetch_schedule(city, user=user)
        target_key = group_num if not is_address_city else 0

        if schedules and target_key in schedules:
            schedule = schedules[target_key]

            # Не кешуємо помилки
            if schedule[0].get('status') in ['error']:
                text = format_schedule(schedule, city_data)
                await callback.message.answer(f"🏙 {city_name}\n\n" + text, reply_markup=get_main_keyboard(city),
                                              disable_web_page_preview=False)
                return

            schedule_json = json.dumps(schedule, ensure_ascii=False)
            ScheduleParser.save_schedule(city, group_num, today, schedule_json)
            text = format_schedule(schedule, city_data)
            info_line = f"⚡️ Група {group_num}" if not is_address_city else f"🏠 Адреса: {address}"
            text = f"🏙 {city_name}\n{info_line}\n\n" + text
            text += f"\n\n🕐 Оновлено щойно"
            await callback.message.answer(text, reply_markup=get_main_keyboard(city), disable_web_page_preview=False)
            return

    except Exception as e:
        logger.error(f"Error fetching schedule: {e}", exc_info=True)

    await callback.message.answer(
        f"❌ Не вдалося отримати графік для {city_name}.\n\nСпробуйте пізніше або перевірте на сайті.",
        reply_markup=get_main_keyboard(city))


@router.callback_query(F.data == "refresh_schedule")
async def refresh_schedule(callback: CallbackQuery, state: FSMContext):
    """ОНОВЛЕНО: Виправлено логіку оновлення"""
    await callback.answer("⏳ Оновлюю...")
    user = UserManager.get_user(callback.from_user.id)
    if not user:
        await callback.message.answer("❌ Спробуйте /start")
        return

    city = user.get('city', 'chernivtsi')
    city_data = CITIES.get(city, {})
    city_name = city_data.get('name', 'Чернівці')

    try:
        schedules = await ScheduleParser.fetch_schedule(city, user=user)

        is_address_city = city_data.get('parser_type') == 'kyiv_dtek_address'
        group_num = user.get('group_number', 0)
        target_key = 0 if is_address_city else group_num
        today = datetime.now().strftime("%Y-%m-%d")

        if schedules and target_key in schedules:
            schedule = schedules[target_key]

            # Не кешуємо помилки
            if schedule[0].get('status') in ['error']:
                text = format_schedule(schedule, city_data)
                await callback.message.answer(f"🏙 {city_name}\n\n" + text, reply_markup=get_main_keyboard(city),
                                              disable_web_page_preview=False)
                return

            schedule_json = json.dumps(schedule, ensure_ascii=False)
            ScheduleParser.save_schedule(city, group_num, today, schedule_json)  # 0 group_num для Києва
            text = format_schedule(schedule, city_data)

            if is_address_city:
                info_line = f"🏠 Адреса: {user.get('address')}"
            else:
                info_line = f"⚡️ Група {group_num}"

            text = f"🏙 {city_name}\n{info_line}\n\n" + text
            text += f"\n\n🕐 Оновлено щойно"
            await callback.message.answer(text, reply_markup=get_main_keyboard(city), disable_web_page_preview=False)
        else:
            await callback.message.answer(f"❌ Не вдалося оновити графік для {city_name}",
                                          reply_markup=get_main_keyboard(city))
    except Exception as e:
        logger.error(f"Error refreshing schedule: {e}", exc_info=True)
        await callback.message.answer("❌ Помилка оновлення.", reply_markup=get_main_keyboard(city))


@router.callback_query(F.data == "change_city")
async def change_city(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    await callback.message.answer("🏙 Оберіть місто:\n\n⚠️ - особливості отримання графіку",
                                  reply_markup=get_cities_keyboard())


@router.callback_query(F.data.startswith("city_"))
async def select_city(callback: CallbackQuery, state: FSMContext):
    """ОНОВЛЕНО: Виправлено логіку збереження міста"""
    await callback.answer()
    await state.clear()
    city_id = callback.data.split("_")[1]
    city_data = CITIES.get(city_id, {})
    city_name = city_data.get('name', 'Невідоме місто')

    UserManager.update_city(callback.from_user.id, city_id)
    user_cities = UserManager.get_user_cities(callback.from_user.id)

    message_text = f"✅ Місто {city_name} встановлено!\n\n"
    if city_data.get('note'):
        message_text += f"ℹ️ {city_data['note']}\n\n"

    if city_data.get('parser_type') == 'kyiv_dtek_address':
        # Перевіряємо, чи є збережені ID
        existing_data = next((uc for uc in user_cities if uc['city'] == city_id and uc['dtek_street_id']), None)
        if existing_data:
            UserManager.update_address(callback.from_user.id, existing_data['address'], city_id,
                                       existing_data['dtek_street_id'], existing_data['dtek_house_id'])
            message_text += f"🏠 Ваша збережена адреса: {existing_data['address']}"
            await callback.message.answer(message_text, reply_markup=get_main_keyboard(city_id))
        else:
            message_text += "Тепер введіть **назву вашої вулиці** (наприклад, *Хрещатик* або *Перемоги*):"
            await callback.message.answer(message_text)
            await state.set_state(UserStates.waiting_for_kyiv_street)
    else:
        existing_group = next((uc['group_number'] for uc in user_cities if uc['city'] == city_id), None)
        if existing_group:
            UserManager.update_group(callback.from_user.id, existing_group)
            message_text += f"⚡️ Ваша збережена група: {existing_group}"
            await callback.message.answer(message_text, reply_markup=get_main_keyboard(city_id))
        else:
            message_text += "Тепер оберіть групу відключень:"
            await callback.message.answer(message_text, reply_markup=get_groups_keyboard())


@router.callback_query(F.data == "change_group")
async def change_group(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    await callback.message.answer("Оберіть свою групу відключень:", reply_markup=get_groups_keyboard())


@router.callback_query(F.data.startswith("group_"))
async def select_group(callback: CallbackQuery, state: FSMContext):
    """ОНОВЛЕНО: Виправлено логіку збереження групи (Кам'янець-Подільський)"""
    await callback.answer()
    await state.clear()
    group_num = int(callback.data.split("_")[1])
    user = UserManager.get_user(callback.from_user.id)

    # Використовуємо місто, яке вже збережено в БД (після select_city)
    city = user.get('city', 'chernivtsi')
    city_name = CITIES.get(city, {}).get('name', 'Чернівці')

    UserManager.update_group(callback.from_user.id, group_num)

    await callback.message.answer(
        f"✅ Налаштування збережено!\n\n🏙 Місто: {city_name}\n⚡️ Група: {group_num}",
        reply_markup=get_main_keyboard(city)
    )


# --- НОВИЙ ФУНКЦІОНАЛ ДЛЯ АДРЕСИ (КИЇВ/ДТЕК) ---
@router.callback_query(F.data == "change_address")
async def change_address(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    user = UserManager.get_user(callback.from_user.id)
    city = user.get('city', 'kyiv') if user else 'kyiv'
    city_name = CITIES.get(city, {}).get('name', 'Київ')
    if CITIES.get(city, {}).get('parser_type') != 'kyiv_dtek_address':
        await callback.message.answer(f"❌ Для міста {city_name} не використовується адресний пошук.",
                                      reply_markup=get_main_keyboard(city))
        return
    await callback.message.answer("🏠 Введіть **назву вашої вулиці** (наприклад, *Хрещатик* або *Перемоги*):")
    await state.set_state(UserStates.waiting_for_kyiv_street)


@router.message(UserStates.waiting_for_kyiv_street)
async def process_kyiv_street(message: Message, state: FSMContext):
    """Етап 1: Обробка вулиці"""
    street_name = message.text.strip()
    city_data = CITIES['kyiv']

    await message.answer(f"⏳ Шукаю вулицю *{street_name}*...")

    streets = await ScheduleParser._find_kyiv_streets(city_data, street_name)

    if not streets:
        await message.answer("❌ Вулицю не знайдено. Спробуйте ввести назву ще раз (без 'вул.' чи 'просп.'):")
        return

    # Якщо забагато варіантів, беремо перші 5
    if len(streets) > 5:
        streets = streets[:5]

    # Зберігаємо знайдені вулиці в FSM
    await state.update_data(found_streets=streets)

    # Створюємо клавіатуру з варіантами
    keyboard = []
    for street in streets:
        keyboard.append([InlineKeyboardButton(text=street['name'], callback_data=f"kyivstreet_{street['street_id']}")])

    await message.answer("Оберіть вашу вулицю зі списку:", reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))


@router.callback_query(F.data.startswith("kyivstreet_"))
async def process_kyiv_street_select(callback: CallbackQuery, state: FSMContext):
    """Етап 2: Вулицю обрано, запитуємо будинок"""
    await callback.answer()
    street_id = callback.data.split("_")[1]

    data = await state.get_data()
    found_streets = data.get('found_streets', [])

    selected_street = next((s for s in found_streets if s['street_id'] == street_id), None)

    if not selected_street:
        await callback.message.answer("❌ Помилка. Спробуйте пошук вулиці ще раз.")
        await state.clear()
        return

    street_name = selected_street['name']
    await state.update_data(street_id=street_id, street_name=street_name)

    city_data = CITIES['kyiv']
    houses = await ScheduleParser._find_kyiv_houses(city_data, street_id)

    if not houses:
        await callback.message.answer("❌ На цій вулиці не знайдено будинків. Спробуйте іншу вулицю.")
        await state.clear()
        return

    # Зберігаємо будинки в FSM
    await state.update_data(found_houses=houses)

    await callback.message.answer(f"✅ Вулиця: *{street_name}*\n\n🏠 Тепер введіть **номер вашого будинку**:")
    await state.set_state(UserStates.waiting_for_kyiv_house)


@router.message(UserStates.waiting_for_kyiv_house)
async def process_kyiv_house(message: Message, state: FSMContext):
    """Етап 3: Обробка будинку та отримання графіку"""
    house_number = message.text.strip().lower()
    data = await state.get_data()

    found_houses = data.get('found_houses', [])
    street_id = data.get('street_id')
    street_name = data.get('street_name')

    if not found_houses or not street_id:
        await message.answer("❌ Помилка стану FSM. Спробуйте /start і почніть заново.")
        await state.clear()
        return

    selected_house = next((h for h in found_houses if h['name'].lower() == house_number), None)

    if not selected_house:
        available_houses = ", ".join([h['name'] for h in found_houses[:10]])
        await message.answer(
            f"❌ Будинок *{house_number}* не знайдено на цій вулиці.\n\nДоступні варіанти: {available_houses}...\n\nСпробуйте ввести номер ще раз:")
        return

    house_id = selected_house['house_id']
    full_address = f"{street_name}, {selected_house['name']}"

    await message.answer(f"⏳ Завантажую графік для: *{full_address}*...")

    city_data = CITIES['kyiv']
    city_name = city_data['name']  # <-- ДОДАНО ЦЮ СТРІЧКУ
    schedule = await ScheduleParser._get_kyiv_schedule(city_data, street_id, house_id)

    if not schedule or schedule[0].get('status') == 'error':
        await message.answer(f"❌ Не вдалося отримати графік. {schedule[0].get('message', '') if schedule else ''}")
        await state.clear()
        return

    # Зберігаємо в БД
    UserManager.update_address(message.from_user.id, full_address, 'kyiv', street_id, house_id)

    # Зберігаємо в кеш (schedules)
    today = datetime.now().strftime("%Y-%m-%d")
    schedule_json = json.dumps(schedule, ensure_ascii=False)
    ScheduleParser.save_schedule('kyiv', 0, today, schedule_json)

    text = format_schedule(schedule, city_data)
    text = f"🏙 {city_name}\n🏠 Адреса: {full_address}\n\n" + text

    await message.answer(text, reply_markup=get_main_keyboard('kyiv'))
    await state.clear()

# --- КІНЕЦЬ НОВОГО ФУНКЦІОНАЛУ ДЛЯ АДРЕСИ ---


@router.callback_query(F.data == "back_to_menu")
async def back_to_menu(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    user = UserManager.get_user(callback.from_user.id)
    city = user.get('city', 'chernivtsi') if user else 'chernivtsi'
    await callback.message.answer("Головне меню:", reply_markup=get_main_keyboard(city))


@router.callback_query(F.data == "settings")
async def settings(callback: CallbackQuery):
    await callback.answer()
    user = UserManager.get_user(callback.from_user.id)
    enabled = user.get('notifications_enabled', 1) if user else 1
    status = "✅ Увімкнено" if enabled else "❌ Вимкнено"
    keyboard = [[InlineKeyboardButton(text="🔕 Вимкнути сповіщення" if enabled else "🔔 Увімкнути сповіщення",
                                      callback_data="toggle_notifications")],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_menu")]]
    await callback.message.answer(f"⚙️ Налаштування\n\nСповіщення: {status}",
                                  reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))


@router.callback_query(F.data == "toggle_notifications")
async def toggle_notif(callback: CallbackQuery):
    enabled = UserManager.toggle_notifications(callback.from_user.id)
    status = "увімкнено" if enabled else "вимкнено"
    await callback.answer(f"Сповіщення {status}")
    await settings(callback)


@router.callback_query(F.data == "help")
async def show_help(callback: CallbackQuery):
    await callback.answer()
    help_text = ("❓ Допомога\n\n📋 Команди:\n/start - Головне меню\n/update - Оновити графіки вручну\n"
                 "/debug - Показати збережені налаштування\n/test - Тестування парсингу для вашого міста\n\n🏙 Доступні міста:\n")
    for city_data in CITIES.values():
        help_text += f"  • {city_data['name']}"
        if city_data.get('note'):
            help_text += f" - {city_data['note']}"
        help_text += "\n"
    help_text += ("\n❓ Як дізнатися свою групу/адресу?\nПерейдіть на сайт енергопостачальника\n"
                  "вашого міста та введіть свою адресу.\n\n🔧 Якщо графік не показується:\n"
                  "1. Використайте /debug щоб перевірити налаштування\n2. Спробуйте /update для оновлення графіків\n"
                  "3. Використайте /test для тестування парсингу\n4. Переоберіть місто та групу/адресу через меню")
    user = UserManager.get_user(callback.from_user.id)
    city = user.get('city', 'chernivtsi') if user else 'chernivtsi'
    await callback.message.answer(help_text, reply_markup=get_main_keyboard(city))


# Scheduled tasks
async def update_schedules(bot: Bot):
    """Оновлення графіків для всіх міст"""
    logger.info("📅 Оновлення графіків для всіх міст...")
    today = datetime.now().strftime("%Y-%m-%d")

    for city_id, city_data in CITIES.items():
        try:
            parser_type = city_data.get('parser_type', 'default')

            if parser_type == 'image_based':
                await ScheduleParser.fetch_schedule(city_id, user=None)
                logger.info(f"[{city_id}] URL зображення оновлено.")
                continue

            if parser_type in ['kyiv_dtek_address']:
                # Оновлюємо графіки для всіх користувачів Києва
                users = UserManager.get_users_by_city(city_id)
                for user in users:
                    if user.get('dtek_street_id') and user.get('dtek_house_id'):
                        schedule = await ScheduleParser._get_kyiv_schedule(city_data, user['dtek_street_id'],
                                                                           user['dtek_house_id'])
                        if schedule and schedule[0].get('status') not in ['error']:
                            schedule_json = json.dumps(schedule, ensure_ascii=False)
                            ScheduleParser.save_schedule(city_id, 0, today, schedule_json)  # 0 = адреса
                logger.info(f"[{city_id}] Оновлено {len(users)} адресних графіків")
                continue

            logger.info(f"[{city_id}] Початок оновлення...")
            schedules = await ScheduleParser.fetch_schedule(city_id, user=None)

            if schedules is None:
                logger.warning(f"[{city_id}] Не вдалося отримати графіки")
                continue

            if not schedules:
                logger.info(f"[{city_id}] Графіки порожні - відключень немає")
                max_groups = city_data.get('groups', 12)
                for group_num in range(1, max_groups + 1):
                    schedule_json = json.dumps([], ensure_ascii=False)
                    ScheduleParser.save_schedule(city_id, group_num, today, schedule_json)
            else:
                for group_num, schedule in schedules.items():
                    schedule_json = json.dumps(schedule, ensure_ascii=False)
                    ScheduleParser.save_schedule(city_id, group_num, today, schedule_json)

                logger.info(f"[{city_id}] ✅ Оновлено: {len(schedules)} груп")

        except Exception as e:
            logger.error(f"[{city_id}] ❌ Помилка: {e}", exc_info=True)

    logger.info("📅 Оновлення завершено")


async def check_and_notify_image_changes(bot: Bot):
    """НОВИЙ ПЛАНУВАЛЬНИК: Перевірка змін у графіках-зображеннях (Хмельницький/Кам'янець)"""
    logger.info("📸 Перевірка оновлень зображень графіків...")

    for city_id, city_data in CITIES.items():
        if city_data.get('parser_type') != 'image_based':
            continue

        city_name = city_data['name']
        old_url = ScheduleParser._get_image_url(city_id)

        await ScheduleParser.fetch_schedule(city_id, user=None)
        new_url = ScheduleParser._get_image_url(city_id)

        if not new_url:
            continue

        if old_url and old_url != new_url:
            logger.info(f"[{city_id}] ЗНАЙДЕНО ЗМІНУ ГРАФІКУ! {old_url} -> {new_url}")

            users_to_notify = UserManager.get_users_by_city(city_id)
            caption = f"⚠️ **ОНОВЛЕННЯ ГРАФІКУ!** ({city_name})\n\nАктуальний графік відключень змінився. Перевірте нове зображення."

            for user in users_to_notify:
                try:
                    await bot.send_photo(
                        user['user_id'],
                        photo=new_url,
                        caption=caption,
                    )
                except Exception as e:
                    logger.error(
                        f"❌ Помилка надсилання сповіщення про зміну графіку (зображення) {user['user_id']}: {e}")

        elif not old_url and new_url:
            logger.info(f"[{city_id}] Зображення вперше збережено: {new_url}")


async def send_notifications(bot: Bot):
    """Надсилання сповіщень про відключення"""
    logger.info("🔔 Перевірка сповіщень...")

    now = datetime.now()
    target_time = now + timedelta(minutes=30)
    target_hour_minute = target_time.strftime("%H:%M")
    today = now.strftime("%Y-%m-%d")

    for city_id, city_data in CITIES.items():
        parser_type = city_data.get('parser_type')
        city_name = city_data['name']

        if parser_type == 'image_based':
            continue

        # 1. Міста за ГРУПАМИ (Чернівці)
        if parser_type not in ['kyiv_dtek_address']:
            with get_db() as conn:
                schedules = conn.execute(
                    "SELECT DISTINCT group_number FROM schedules WHERE city = ? AND date = ? AND group_number IS NOT NULL",
                    (city_id, today)
                ).fetchall()
            for row in schedules:
                group_num = row['group_number']
                users = UserManager.get_users_by_city_and_group(city_id, group_num)
                if not users: continue
                schedule_json = ScheduleParser.get_schedule(city_id, group_num, today)
                if not schedule_json: continue
                try:
                    schedule = json.loads(schedule_json)
                    for item in schedule:
                        start_time_str = item['time'].split('-')[0]
                        if item['status'] == 'off' and target_hour_minute.split(':')[0] in start_time_str.split(':')[
                            0] and abs(int(target_hour_minute.split(':')[1]) - int(start_time_str.split(':')[1])) <= 15:
                            for user in users:
                                try:
                                    await bot.send_message(
                                        user['user_id'],
                                        f"⚠️ Увага! Через 30 хвилин (з {item['time']}) "
                                        f"очікується відключення світла\n\n"
                                        f"🏙 {city_name}\n"
                                        f"⚡️ Група {group_num}"
                                    )
                                except Exception as e:
                                    logger.error(f"❌ Помилка надсилання {user['user_id']}: {e}")
                except json.JSONDecodeError:
                    logger.error(f"❌ JSON error для {city_id}, група {group_num}")
                    continue

        # 2. Міста за АДРЕСОЮ (Київ)
        if parser_type == 'kyiv_dtek_address':
            users = UserManager.get_users_by_city(city_id)
            for user in users:
                if not user.get('dtek_street_id') or not user.get('dtek_house_id'):
                    continue

                address = user.get('address')
                schedule = await ScheduleParser._get_kyiv_schedule(city_data, user['dtek_street_id'],
                                                                   user['dtek_house_id'])

                if schedule and schedule[0].get('status') not in ['error', 'info']:
                    for item in schedule:
                        start_time_str = item['time'].split('-')[0]
                        if item['status'] == 'off' and target_hour_minute.split(':')[0] in start_time_str.split(':')[
                            0] and abs(int(target_hour_minute.split(':')[1]) - int(start_time_str.split(':')[1])) <= 15:
                            try:
                                await bot.send_message(
                                    user['user_id'],
                                    f"⚠️ Увага! Через 30 хвилин (з {item['time']}) "
                                    f"очікується відключення світла\n\n"
                                    f"🏙 {city_name}\n"
                                    f"🏠 Адреса: {address}"
                                )
                            except Exception as e:
                                logger.error(f"❌ Помилка надсилання адресному користувачу {user['user_id']}: {e}")


async def main():
    logger.info("🚀 Запуск бота...")

    init_db()

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode='Markdown')
    )
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    scheduler = AsyncIOScheduler()

    scheduler.add_job(
        update_schedules,
        CronTrigger(minute="*/30"),
        args=[bot]
    )

    scheduler.add_job(
        send_notifications,
        CronTrigger(minute="*/15"),
        args=[bot]
    )

    scheduler.add_job(
        check_and_notify_image_changes,
        CronTrigger(hour="*", minute="10"),
        args=[bot]
    )

    scheduler.start()
    logger.info("⏰ Scheduler запущено")

    logger.info("📥 Виконую перше оновлення графіків...")
    try:
        await update_schedules(bot)
        await check_and_notify_image_changes(bot)
        logger.info("✅ Перше оновлення завершено")
    except Exception as e:
        logger.error(f"❌ Помилка при першому оновленні: {e}", exc_info=True)

    logger.info("✅ Бот запущено!")
    logger.info(f"🏙 Підтримка міст: {', '.join([c['name'] for c in CITIES.values()])}")

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())