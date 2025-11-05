import asyncio
import logging
import os
import re
from datetime import datetime, timedelta
from typing import Optional, Dict, List
from contextlib import contextmanager
import json
import sqlite3

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import aiohttp
from bs4 import BeautifulSoup

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфігурація
BOT_TOKEN = os.getenv("BOT_TOKEN", "8264057355:AAGgZMq5_2lNJRs5JV8KudlPyiQv6iKj_Sk")
DATABASE_PATH = "bot_database.db"

# Міста та їх URL
CITIES = {
    "chernivtsi": {
        "name": "Чернівці",
        "schedule_url": "https://oblenergo.cv.ua/shutdowns/",
        "search_url": "https://oblenergo.cv.ua/shutdowns-search/",
        "parser_type": "chernivtsi_custom"
    },
    "kyiv": {
        "name": "Київ",
        "schedule_url": "https://www.dtek-kem.com.ua/ua/shutdowns",
        "search_url": "https://www.dtek-kem.com.ua/ua/shutdowns",
        "parser_type": "kyiv_address_based",
        "note": "Потрібна адреса для перевірки"
    },
    "khmelnytskyi": {
        "name": "Хмельницький",
        "schedule_url": "https://hoe.com.ua/page/pogodinni-vidkljuchennja",
        "search_url": "https://hoe.com.ua/page/pogodinni-vidkljuchennja",
        "parser_type": "image_based",
        "note": "Графік у форматі зображення"
    },
    "kamyanets": {
        "name": "Кам'янець-Подільський",
        "schedule_url": "https://hoe.com.ua/page/pogodinni-vidkljuchennja",
        "search_url": "https://hoe.com.ua/page/pogodinni-vidkljuchennja",
        "parser_type": "image_based",
        "note": "Графік у форматі зображення"
    }
}


# FSM стани
class UserStates(StatesGroup):
    waiting_for_group = State()
    waiting_for_address = State()


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
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                city TEXT DEFAULT 'chernivtsi',
                group_number INTEGER,
                address TEXT,
                notifications_enabled INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS user_cities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                city TEXT,
                group_number INTEGER,
                address TEXT,
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
                    username = excluded.username
            """, (user_id, username, city, group_number))
            conn.commit()

    @staticmethod
    def update_city(user_id: int, city: str):
        with get_db() as conn:
            conn.execute(
                "UPDATE users SET city = ? WHERE user_id = ?",
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
                        group_number = excluded.group_number
                """, (user_id, user.get('city', 'chernivtsi'), group_number))

            conn.commit()

    @staticmethod
    def update_address(user_id: int, address: str):
        with get_db() as conn:
            conn.execute(
                "UPDATE users SET address = ? WHERE user_id = ?",
                (address, user_id)
            )
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
    @staticmethod
    async def fetch_schedule(city: str = "chernivtsi") -> Optional[Dict[int, List[Dict]]]:
        """Парсинг графіків з сайту конкретного міста"""
        try:
            city_data = CITIES.get(city)
            if not city_data:
                logger.error(f"Місто {city} не знайдено в конфігурації")
                return None

            parser_type = city_data.get('parser_type', 'default')

            if parser_type == 'chernivtsi_custom':
                return await ScheduleParser._parse_chernivtsi(city_data)
            elif parser_type == 'kyiv_address_based':
                return await ScheduleParser._parse_kyiv(city_data)
            elif parser_type == 'image_based':
                return await ScheduleParser._parse_image_based(city_data, city)
            else:
                return await ScheduleParser._parse_generic(city_data, city)

        except Exception as e:
            logger.error(f"[{city}] Критична помилка парсингу: {e}", exc_info=True)
            return None

    @staticmethod
    async def _parse_chernivtsi(city_data: dict) -> Optional[Dict[int, List[Dict]]]:
        """Спеціальний парсер для Чернівців з custom HTML структурою"""
        try:
            url = city_data['schedule_url']
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'uk-UA,uk;q=0.9',
            }

            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=30) as response:
                    if response.status != 200:
                        logger.error(f"HTTP {response.status} для Чернівців")
                        return None

                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')

                    # Шукаємо контейнер з графіком
                    schedule_container = soup.find('div', {'id': 'gsv'})
                    if not schedule_container:
                        logger.warning("Контейнер #gsv не знайдено")
                        return None

                    # Отримуємо заголовки часу
                    time_headers = []
                    time_container = schedule_container.find('p')
                    if time_container:
                        for time_block in time_container.find_all('u', recursive=False):
                            time_text = time_block.find('b')
                            if time_text:
                                hour = time_text.get_text(strip=True).split(':')[0]
                                time_headers.append(f"{hour}:00-{hour}:30")
                                time_headers.append(f"{hour}:30-{int(hour) + 1:02d}:00")

                    logger.info(f"[Чернівці] Знайдено часових інтервалів: {len(time_headers)}")

                    # Парсимо графіки груп
                    schedule_data = {}

                    for group_num in range(1, 13):  # Чернівці мають 12 груп
                        group_div = soup.find('div', {'id': f'inf{group_num}'})
                        if not group_div:
                            continue

                        schedule_data[group_num] = []

                        # Парсимо теги u, o, s для статусів
                        cells = group_div.find_all(['u', 'o', 's'])

                        for idx, cell in enumerate(cells):
                            if idx >= len(time_headers):
                                break

                            tag_name = cell.name
                            classes = ' '.join(cell.get('class', []))

                            # Визначаємо статус на основі тегу
                            if tag_name == 'u':  # <u> = зелений (світло є)
                                status = 'on'
                            elif tag_name == 'o':  # <o> = червоний (відключення)
                                status = 'off'
                            elif tag_name == 's':  # <s> = можливо
                                status = 'maybe'
                            else:
                                status = 'on'

                            schedule_data[group_num].append({
                                'time': time_headers[idx] if idx < len(time_headers) else f"{idx}:00-{idx}:30",
                                'status': status
                            })

                        logger.info(f"[Чернівці] Група {group_num}: {len(schedule_data[group_num])} інтервалів")

                    return schedule_data if schedule_data else None

        except Exception as e:
            logger.error(f"[Чернівці] Помилка парсингу: {e}", exc_info=True)
            return None

    @staticmethod
    async def _parse_kyiv(city_data: dict) -> Optional[Dict[int, List[Dict]]]:
        """Парсер для Києва (потребує адреси)"""
        try:
            logger.info("[Київ] Київ потребує пошуку по адресі, повертаємо порожній графік")
            # Для Києва повертаємо інфо що потрібна адреса
            return {
                0: [{
                    'time': 'Інформація',
                    'status': 'info',
                    'message': 'Для Києва потрібно вказати адресу через пошук на сайті ДТЕК'
                }]
            }
        except Exception as e:
            logger.error(f"[Київ] Помилка: {e}", exc_info=True)
            return None

    @staticmethod
    async def _parse_image_based(city_data: dict, city: str) -> Optional[Dict[int, List[Dict]]]:
        """Парсер для міст з графіками у вигляді зображень"""
        try:
            url = city_data['schedule_url']
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            }

            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=30) as response:
                    if response.status != 200:
                        return None

                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')

                    # Шукаємо зображення з графіком
                    images = soup.find_all('img', src=re.compile(r'(grafik|schedule|vidkl)', re.I))

                    if images:
                        img_url = images[0].get('src')
                        if not img_url.startswith('http'):
                            base_url = '/'.join(url.split('/')[:3])
                            img_url = base_url + img_url

                        logger.info(f"[{city}] Знайдено зображення графіку: {img_url}")

                        return {
                            0: [{
                                'time': 'Інформація',
                                'status': 'info',
                                'message': f'Графік у форматі зображення',
                                'image_url': img_url
                            }]
                        }

            logger.warning(f"[{city}] Зображення графіку не знайдено")
            return None

        except Exception as e:
            logger.error(f"[{city}] Помилка парсингу зображення: {e}", exc_info=True)
            return None

    @staticmethod
    async def _parse_generic(city_data: dict, city: str) -> Optional[Dict[int, List[Dict]]]:
        """Загальний парсер для інших міст"""
        try:
            url = city_data['schedule_url']
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'uk-UA,uk;q=0.9',
            }

            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=30) as response:
                    if response.status != 200:
                        return None

                    html = await response.text()

                    # Метод 1: JavaScript
                    schedule_data = ScheduleParser._parse_js_data(html, city)
                    if schedule_data:
                        return schedule_data

                    # Метод 2: HTML таблиця
                    schedule_data = ScheduleParser._parse_html_table(html, city)
                    if schedule_data:
                        return schedule_data

                    return {}

        except Exception as e:
            logger.error(f"[{city}] Помилка: {e}", exc_info=True)
            return None

    @staticmethod
    def _parse_js_data(html: str, city: str) -> Optional[Dict[int, List[Dict]]]:
        """Пошук JSON у JavaScript"""
        patterns = [
            r'var\s+schedule\s*=\s*(\{.+?\});',
            r'const\s+schedule\s*=\s*(\{.+?\});',
            r'let\s+schedule\s*=\s*(\{.+?\});',
            r'var\s+groups\s*=\s*(\[.+?\]);',
        ]

        for pattern in patterns:
            match = re.search(pattern, html, re.DOTALL)
            if match:
                try:
                    json_str = match.group(1)
                    data = json.loads(json_str)

                    if isinstance(data, dict):
                        return ScheduleParser._normalize_schedule_data(data)
                    elif isinstance(data, list):
                        return ScheduleParser._normalize_schedule_list(data)
                except json.JSONDecodeError:
                    continue

        return None

    @staticmethod
    def _parse_html_table(html: str, city: str) -> Optional[Dict[int, List[Dict]]]:
        """Парсинг HTML таблиці"""
        soup = BeautifulSoup(html, 'html.parser')

        table = soup.find('table', class_=re.compile(r'schedule|shutdowns|outages', re.I))
        if not table:
            tables = soup.find_all('table')
            for t in tables:
                if len(t.find_all('tr')) > 5:
                    table = t
                    break

        if not table:
            return None

        return ScheduleParser._parse_table_element(table, city)

    @staticmethod
    def _parse_table_element(table, city: str) -> Optional[Dict[int, List[Dict]]]:
        """Парсинг елемента таблиці"""
        schedule_data = {}

        try:
            headers = []
            header_row = table.find('thead') or table.find('tr')

            if header_row:
                for th in header_row.find_all(['th', 'td']):
                    text = th.get_text(strip=True)
                    if re.search(r'\d{1,2}[:.\-]\d{1,2}', text):
                        headers.append(text)

            if not headers:
                headers = [f"{i:02d}:00-{i + 2:02d}:00" for i in range(0, 24, 2)]

            tbody = table.find('tbody')
            rows = (tbody.find_all('tr') if tbody else table.find_all('tr'))[1:]

            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) < 2:
                    continue

                group_text = cells[0].get_text(strip=True)
                group_match = re.search(r'\d+', group_text)

                if not group_match:
                    continue

                group_num = int(group_match.group())
                if group_num < 1 or group_num > 20:
                    continue

                schedule_data[group_num] = []

                for i, cell in enumerate(cells[1:]):
                    if i >= len(headers):
                        break

                    status = ScheduleParser._determine_cell_status(cell, city)

                    schedule_data[group_num].append({
                        'time': headers[i],
                        'status': status
                    })

            return schedule_data if schedule_data else None

        except Exception as e:
            logger.error(f"[{city}] Помилка парсингу таблиці: {e}", exc_info=True)
            return None

    @staticmethod
    def _determine_cell_status(cell, city: str) -> str:
        """Визначення статусу комірки"""
        style = str(cell.get('style', '')).lower()
        classes = ' '.join(cell.get('class', [])).lower()
        text = cell.get_text(strip=True).lower()
        bgcolor = str(cell.get('bgcolor', '')).lower()

        red_indicators = ['red', '#ff0000', '#f00', 'rgb(255,0,0)', 'danger', 'outage', 'off']
        if any(ind in style or ind in classes or ind in bgcolor for ind in red_indicators):
            return 'off'

        green_indicators = ['green', '#00ff00', '#0f0', 'rgb(0,255,0)', 'success', 'on']
        if any(ind in style or ind in classes or ind in bgcolor for ind in green_indicators):
            return 'on'

        maybe_indicators = ['yellow', 'gray', 'grey', 'warning', 'maybe']
        if any(ind in style or ind in classes or ind in bgcolor for ind in maybe_indicators):
            return 'maybe'

        if any(word in text for word in ['відключення', 'немає', 'off']):
            return 'off'
        if any(word in text for word in ['можливо', 'maybe']):
            return 'maybe'

        return 'on'

    @staticmethod
    def _normalize_schedule_data(data: dict) -> Dict[int, List[Dict]]:
        """Нормалізація даних"""
        result = {}
        for key, value in data.items():
            try:
                group_num = int(re.search(r'\d+', str(key)).group())
                if isinstance(value, list):
                    result[group_num] = value
            except:
                continue
        return result if result else None

    @staticmethod
    def _normalize_schedule_list(data: list) -> Dict[int, List[Dict]]:
        """Нормалізація list"""
        result = {}
        for item in data:
            if isinstance(item, dict) and 'group' in item:
                try:
                    result[int(item['group'])] = item.get('schedule', [])
                except:
                    continue
        return result if result else None

    @staticmethod
    def save_schedule(city: str, group_number: int, date: str, schedule_data: str):
        """Збереження графіку"""
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
        """Отримання графіку"""
        with get_db() as conn:
            row = conn.execute(
                "SELECT schedule_data FROM schedules WHERE city = ? AND group_number = ? AND date = ?",
                (city, group_number, date)
            ).fetchone()
            return row['schedule_data'] if row else None


# Форматування
def format_schedule(schedule: List[Dict], city_data: dict = None) -> str:
    """Форматування графіку"""
    if not schedule:
        return "✅ Наразі планових відключень немає!"

    # Перевірка на спеціальні повідомлення
    if schedule and schedule[0].get('status') == 'info':
        msg = schedule[0].get('message', '')
        if 'image_url' in schedule[0]:
            return f"📷 {msg}\n\nПодивитись графік: {schedule[0]['image_url']}"
        return f"ℹ️ {msg}"

    has_outages = any(item['status'] == 'off' for item in schedule)

    if not has_outages:
        return "✅ Чудові новини! Сьогодні планових відключень немає!"

    text = "📊 Графік відключень на сьогодні:\n\n"

    for item in schedule:
        emoji = {"off": "🔴", "on": "🟢", "maybe": "⚪"}.get(item['status'], "⚪")
        status_text = {"off": "Відключення", "on": "Світло є", "maybe": "Можливо"}.get(item['status'], "Невідомо")
        text += f"{emoji} {item['time']} - {status_text}\n"

    text += "\n🔴 - гарантоване відключення\n"
    text += "🟢 - гарантоване включення\n"
    text += "⚪ - можливе включення\n"

    return text


# Клавіатури
def get_main_keyboard(user_city: str = "chernivtsi") -> InlineKeyboardMarkup:
    city_name = CITIES.get(user_city, {}).get('name', 'Чернівці')
    keyboard = [
        [InlineKeyboardButton(text="📊 Мій графік", callback_data="my_schedule")],
        [InlineKeyboardButton(text="🔄 Оновити графік", callback_data="refresh_schedule")],
        [InlineKeyboardButton(text=f"🏙 Місто: {city_name}", callback_data="change_city")],
        [InlineKeyboardButton(text="⚙️ Змінити групу", callback_data="change_group")],
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
                row.append(InlineKeyboardButton(
                    text=f"Група {group_num}",
                    callback_data=f"group_{group_num}"
                ))
        if row:
            keyboard.append(row)

    keyboard.append([InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_menu")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


# Бот
router = Router()


@router.message(Command("start"))
async def cmd_start(message: Message):
    user = UserManager.get_user(message.from_user.id)

    if not user:
        UserManager.save_user(
            message.from_user.id,
            message.from_user.username or "Unknown"
        )
        user = UserManager.get_user(message.from_user.id)

    welcome_text = (
        "👋 Вітаю! Я бот для відстеження графіків відключення світла.\n\n"
        "🔹 Я буду надсилати вам:\n"
        "  • Актуальний графік відключень\n"
        "  • Сповіщення за 30 хв до відключення\n"
        "  • Інформацію про зміни в графіку\n\n"
    )

    if user and user.get('group_number'):
        city_name = CITIES.get(user.get('city', 'chernivtsi'), {}).get('name', 'Чернівці')
        welcome_text = (
            f"👋 З поверненням!\n\n"
            f"🏙 Місто: {city_name}\n"
            f"⚡️ Група: {user['group_number']}"
        )
    else:
        welcome_text += "Спочатку оберіть місто та групу відключень 👇"

    await message.answer(welcome_text,
                         reply_markup=get_main_keyboard(user.get('city', 'chernivtsi') if user else 'chernivtsi'))


@router.message(Command("update"))
async def cmd_update(message: Message):
    """Команда для ручного оновлення графіків"""
    await message.answer("⏳ Оновлюю графіки...")

    bot = message.bot
    await update_schedules(bot)

    await message.answer("✅ Графіки оновлено!")


@router.message(Command("debug"))
async def cmd_debug(message: Message):
    """Команда для відладки"""
    user = UserManager.get_user(message.from_user.id)

    if not user:
        await message.answer("❌ Користувача не знайдено в базі даних")
        return

    debug_text = "🔍 Ваші дані в системі:\n\n"
    debug_text += f"User ID: {user['user_id']}\n"
    debug_text += f"Username: {user.get('username', 'N/A')}\n"
    debug_text += f"Місто: {user.get('city', 'N/A')}\n"
    debug_text += f"Група: {user.get('group_number', 'N/A')}\n"
    debug_text += f"Сповіщення: {'✅' if user.get('notifications_enabled') else '❌'}\n"

    if user.get('group_number') and user.get('city'):
        today = datetime.now().strftime("%Y-%m-%d")
        schedule_data = ScheduleParser.get_schedule(user['city'], user['group_number'], today)
        debug_text += f"\nГрафік в БД: {'✅ Є' if schedule_data else '❌ Немає'}"

    await message.answer(debug_text)


@router.message(Command("test"))
async def cmd_test(message: Message):
    """Тестове оновлення для конкретного міста"""
    user = UserManager.get_user(message.from_user.id)
    city = user.get('city', 'chernivtsi') if user else 'chernivtsi'
    city_name = CITIES[city]['name']

    await message.answer(f"⏳ Тестую парсинг для {city_name}...")

    schedules = await ScheduleParser.fetch_schedule(city)

    if schedules is None:
        await message.answer(f"❌ Помилка завантаження для {city_name}")
    elif not schedules:
        await message.answer(f"✅ Графіки для {city_name} порожні (відключень немає)")
    else:
        text = f"✅ Успішно! {city_name}\nЗнайдено {len(schedules)} груп\n\n"

        # Показуємо приклад першої групи
        first_group = min(schedules.keys())
        text += f"Приклад (група {first_group}):\n"
        for item in schedules[first_group][:5]:
            emoji = {"off": "🔴", "on": "🟢", "maybe": "⚪", "info": "ℹ️"}.get(item['status'], "⚪")
            text += f"{emoji} {item['time']}: {item['status']}\n"

        await message.answer(text)


@router.callback_query(F.data == "my_schedule")
async def show_schedule(callback: CallbackQuery):
    await callback.answer()

    user = UserManager.get_user(callback.from_user.id)

    if not user:
        await callback.message.answer(
            "❌ Помилка: користувача не знайдено в базі даних.\nСпробуйте /start",
            reply_markup=get_cities_keyboard()
        )
        return

    if not user.get('group_number'):
        await callback.message.answer(
            "❌ Спочатку оберіть місто та групу відключень",
            reply_markup=get_cities_keyboard()
        )
        return

    city = user.get('city', 'chernivtsi')
    group_num = user['group_number']
    city_data = CITIES.get(city, {})
    city_name = city_data.get('name', 'Чернівці')

    # Перевірка на спеціальні типи парсерів
    parser_type = city_data.get('parser_type', 'default')

    if parser_type == 'kyiv_address_based':
        await callback.message.answer(
            f"🏙 {city_name}\n\n"
            "⚠️ Для Києва графіки відображаються по конкретних адресах.\n\n"
            "Будь ласка, перевірте свій графік на сайті:\n"
            f"{city_data['schedule_url']}\n\n"
            "Введіть свою адресу на сайті для перегляду графіку.",
            reply_markup=get_main_keyboard(city)
        )
        return

    if parser_type == 'image_based':
        await callback.message.answer(
            f"🏙 {city_name}\n\n"
            "📷 Графіки для цього міста публікуються у форматі зображення.\n\n"
            "Перегляньте актуальний графік на сайті:\n"
            f"{city_data['schedule_url']}",
            reply_markup=get_main_keyboard(city)
        )
        return

    # Отримуємо графік з БД
    today = datetime.now().strftime("%Y-%m-%d")
    schedule_data = ScheduleParser.get_schedule(city, group_num, today)

    if not schedule_data:
        # Спробуємо завантажити
        await callback.message.answer("⏳ Завантажую актуальний графік...")

        try:
            schedules = await ScheduleParser.fetch_schedule(city)
            if schedules and group_num in schedules:
                schedule_json = json.dumps(schedules[group_num], ensure_ascii=False)
                ScheduleParser.save_schedule(city, group_num, today, schedule_json)
                schedule_data = schedule_json
            elif schedules is not None and not schedules:
                schedule_json = json.dumps([], ensure_ascii=False)
                ScheduleParser.save_schedule(city, group_num, today, schedule_json)
                schedule_data = schedule_json
        except Exception as e:
            logger.error(f"Error fetching schedule: {e}", exc_info=True)

        if not schedule_data:
            await callback.message.answer(
                f"📊 Графік для {city_name}, група {group_num} поки недоступний.\n\n"
                "Можливо:\n"
                "• Графік ще не оновлено\n"
                "• Проблеми з сайтом енергопостачальника\n\n"
                f"Перевірте на сайті:\n{city_data['schedule_url']}",
                reply_markup=get_main_keyboard(city)
            )
            return

    # Парсимо і форматуємо
    try:
        schedule = json.loads(schedule_data)
        text = format_schedule(schedule, city_data)
        text = f"🏙 {city_name}\n⚡️ Група {group_num}\n\n" + text

        # Додаємо час оновлення
        with get_db() as conn:
            updated = conn.execute(
                "SELECT updated_at FROM schedules WHERE city = ? AND group_number = ? AND date = ?",
                (city, group_num, today)
            ).fetchone()
            if updated:
                text += f"\n\n🕐 Оновлено: {updated['updated_at']}"

        await callback.message.answer(text, reply_markup=get_main_keyboard(city))
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {e}")
        await callback.message.answer(
            "❌ Помилка читання графіку. Спробуйте пізніше.",
            reply_markup=get_main_keyboard(city)
        )


@router.callback_query(F.data == "refresh_schedule")
async def refresh_schedule(callback: CallbackQuery):
    """Оновлення графіку для користувача"""
    await callback.answer("⏳ Оновлюю...")

    user = UserManager.get_user(callback.from_user.id)

    if not user or not user.get('group_number'):
        await callback.message.answer(
            "❌ Спочатку оберіть місто та групу відключень",
            reply_markup=get_cities_keyboard()
        )
        return

    city = user.get('city', 'chernivtsi')
    group_num = user['group_number']
    city_data = CITIES.get(city, {})
    city_name = city_data.get('name', 'Чернівці')

    try:
        schedules = await ScheduleParser.fetch_schedule(city)
        today = datetime.now().strftime("%Y-%m-%d")

        if schedules and group_num in schedules:
            schedule_json = json.dumps(schedules[group_num], ensure_ascii=False)
            ScheduleParser.save_schedule(city, group_num, today, schedule_json)

            schedule = schedules[group_num]
            text = format_schedule(schedule, city_data)
            text = f"🏙 {city_name}\n⚡️ Група {group_num}\n\n" + text
            text += f"\n\n🕐 Оновлено щойно"

            await callback.message.answer(text, reply_markup=get_main_keyboard(city))
        elif schedules is not None and not schedules:
            schedule_json = json.dumps([], ensure_ascii=False)
            ScheduleParser.save_schedule(city, group_num, today, schedule_json)

            await callback.message.answer(
                f"🏙 {city_name}\n⚡️ Група {group_num}\n\n"
                "✅ Чудові новини! Сьогодні планових відключень для вашої групи немає!\n\n"
                "🕐 Оновлено щойно",
                reply_markup=get_main_keyboard(city)
            )
        else:
            await callback.message.answer(
                f"❌ Не вдалося завантажити графік для {city_name}\n"
                "Спробуйте пізніше або перевірте на сайті",
                reply_markup=get_main_keyboard(city)
            )
    except Exception as e:
        logger.error(f"Error refreshing schedule: {e}", exc_info=True)
        await callback.message.answer(
            "❌ Помилка оновлення графіку. Спробуйте пізніше.",
            reply_markup=get_main_keyboard(city)
        )


@router.callback_query(F.data == "change_city")
async def change_city(callback: CallbackQuery):
    await callback.answer()
    await callback.message.answer(
        "🏙 Оберіть місто:\n\n"
        "⚠️ - особливості отримання графіку",
        reply_markup=get_cities_keyboard()
    )


@router.callback_query(F.data.startswith("city_"))
async def select_city(callback: CallbackQuery):
    await callback.answer()

    city_id = callback.data.split("_")[1]
    city_data = CITIES.get(city_id, {})
    city_name = city_data.get('name', 'Невідоме місто')

    UserManager.update_city(callback.from_user.id, city_id)

    user_cities = UserManager.get_user_cities(callback.from_user.id)
    existing_group = next((uc['group_number'] for uc in user_cities if uc['city'] == city_id), None)

    message_text = f"✅ Місто {city_name} встановлено!\n\n"

    if city_data.get('note'):
        message_text += f"ℹ️ {city_data['note']}\n\n"

    if existing_group:
        UserManager.update_group(callback.from_user.id, existing_group)
        message_text += f"⚡️ Ваша збережена група: {existing_group}"
        await callback.message.answer(message_text, reply_markup=get_main_keyboard(city_id))
    else:
        message_text += "Тепер оберіть групу відключень:"
        await callback.message.answer(message_text, reply_markup=get_groups_keyboard())


@router.callback_query(F.data == "change_group")
async def change_group(callback: CallbackQuery):
    await callback.answer()
    await callback.message.answer(
        "Оберіть свою групу відключень:",
        reply_markup=get_groups_keyboard()
    )


@router.callback_query(F.data.startswith("group_"))
async def select_group(callback: CallbackQuery):
    await callback.answer()

    group_num = int(callback.data.split("_")[1])
    user = UserManager.get_user(callback.from_user.id)

    if not user:
        UserManager.save_user(
            callback.from_user.id,
            callback.from_user.username or "Unknown",
            city="chernivtsi",
            group_number=group_num
        )
        user = UserManager.get_user(callback.from_user.id)

    city = user.get('city', 'chernivtsi')
    city_name = CITIES.get(city, {}).get('name', 'Чернівці')

    UserManager.update_group(callback.from_user.id, group_num)

    await callback.message.answer(
        f"✅ Налаштування збережено!\n\n"
        f"🏙 Місто: {city_name}\n"
        f"⚡️ Група: {group_num}\n\n"
        "Тепер ви будете отримувати сповіщення про відключення.",
        reply_markup=get_main_keyboard(city)
    )


@router.callback_query(F.data == "settings")
async def settings(callback: CallbackQuery):
    await callback.answer()

    user = UserManager.get_user(callback.from_user.id)
    enabled = user.get('notifications_enabled', 1) if user else 1

    status = "✅ Увімкнено" if enabled else "❌ Вимкнено"

    keyboard = [
        [InlineKeyboardButton(
            text="🔕 Вимкнути сповіщення" if enabled else "🔔 Увімкнути сповіщення",
            callback_data="toggle_notifications"
        )],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_menu")]
    ]

    await callback.message.answer(
        f"⚙️ Налаштування\n\nСповіщення: {status}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
    )


@router.callback_query(F.data == "toggle_notifications")
async def toggle_notif(callback: CallbackQuery):
    enabled = UserManager.toggle_notifications(callback.from_user.id)
    status = "увімкнено" if enabled else "вимкнено"

    await callback.answer(f"Сповіщення {status}")
    await settings(callback)


@router.callback_query(F.data == "help")
async def show_help(callback: CallbackQuery):
    await callback.answer()

    help_text = (
        "❓ Допомога\n\n"
        "📋 Команди:\n"
        "/start - Головне меню\n"
        "/update - Оновити графіки вручну\n"
        "/debug - Показати збережені налаштування\n"
        "/test - Тестування парсингу для вашого міста\n\n"
        "🏙 Доступні міста:\n"
    )

    for city_data in CITIES.values():
        help_text += f"  • {city_data['name']}"
        if city_data.get('note'):
            help_text += f" - {city_data['note']}"
        help_text += "\n"

    help_text += (
        "\n❓ Як дізнатися свою групу?\n"
        "Перейдіть на сайт енергопостачальника\n"
        "вашого міста та введіть свою адресу.\n\n"
        "🔧 Якщо графік не показується:\n"
        "1. Використайте /debug щоб перевірити налаштування\n"
        "2. Спробуйте /update для оновлення графіків\n"
        "3. Використайте /test для тестування парсингу\n"
        "4. Переоберіть місто та групу через меню"
    )

    user = UserManager.get_user(callback.from_user.id)
    city = user.get('city', 'chernivtsi') if user else 'chernivtsi'

    await callback.message.answer(help_text, reply_markup=get_main_keyboard(city))


@router.callback_query(F.data == "back_to_menu")
async def back_to_menu(callback: CallbackQuery):
    await callback.answer()
    user = UserManager.get_user(callback.from_user.id)
    city = user.get('city', 'chernivtsi') if user else 'chernivtsi'

    await callback.message.answer(
        "Головне меню:",
        reply_markup=get_main_keyboard(city)
    )


# Scheduled tasks
async def update_schedules(bot: Bot):
    """Оновлення графіків для всіх міст"""
    logger.info("📅 Оновлення графіків для всіх міст...")

    today = datetime.now().strftime("%Y-%m-%d")

    for city_id, city_data in CITIES.items():
        try:
            parser_type = city_data.get('parser_type', 'default')

            # Пропускаємо міста з особливими типами парсерів
            if parser_type in ['kyiv_address_based', 'image_based']:
                logger.info(f"[{city_id}] Пропускаємо автооновлення ({parser_type})")
                continue

            logger.info(f"[{city_id}] Початок оновлення...")
            schedules = await ScheduleParser.fetch_schedule(city_id)

            if schedules is None:
                logger.warning(f"[{city_id}] Не вдалося отримати графіки")
                continue

            if not schedules:
                logger.info(f"[{city_id}] Графіки порожні - відключень немає")
                for group_num in range(1, 19):
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


async def send_notifications(bot: Bot):
    """Надсилання сповіщень про відключення"""
    logger.info("🔔 Перевірка сповіщень...")

    now = datetime.now()
    target_time = now + timedelta(minutes=30)
    target_hour = target_time.strftime("%H")
    today = now.strftime("%Y-%m-%d")

    for city_id, city_data in CITIES.items():
        # Пропускаємо міста з особливими парсерами
        if city_data.get('parser_type') in ['kyiv_address_based', 'image_based']:
            continue

        with get_db() as conn:
            schedules = conn.execute(
                "SELECT DISTINCT group_number FROM schedules WHERE city = ? AND date = ?",
                (city_id, today)
            ).fetchall()

        for row in schedules:
            group_num = row['group_number']
            users = UserManager.get_users_by_city_and_group(city_id, group_num)

            if not users:
                continue

            schedule_json = ScheduleParser.get_schedule(city_id, group_num, today)
            if not schedule_json:
                continue

            try:
                schedule = json.loads(schedule_json)

                if not schedule:
                    continue

                for item in schedule:
                    if item['status'] == 'off' and target_hour in item['time']:
                        for user in users:
                            try:
                                await bot.send_message(
                                    user['user_id'],
                                    f"⚠️ Увага! Через 30 хвилин (о {item['time']}) "
                                    f"очікується відключення світла\n\n"
                                    f"🏙 {city_data['name']}\n"
                                    f"⚡️ Група {group_num}"
                                )
                                logger.info(f"✉️ Сповіщення надіслано {user['user_id']}")
                            except Exception as e:
                                logger.error(f"❌ Помилка надсилання {user['user_id']}: {e}")
            except json.JSONDecodeError:
                logger.error(f"❌ JSON error для {city_id}, група {group_num}")
                continue


async def main():
    logger.info("🚀 Запуск бота...")

    init_db()

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    # Scheduler
    scheduler = AsyncIOScheduler()

    # Оновлення графіків кожні 30 хвилин
    scheduler.add_job(
        update_schedules,
        CronTrigger(minute="*/30"),
        args=[bot]
    )

    # Перевірка сповіщень кожні 15 хвилин
    scheduler.add_job(
        send_notifications,
        CronTrigger(minute="*/15"),
        args=[bot]
    )

    scheduler.start()
    logger.info("⏰ Scheduler запущено")

    # Перше оновлення
    logger.info("📥 Виконую перше оновлення графіків...")
    try:
        await update_schedules(bot)
        logger.info("✅ Перше оновлення завершено")
    except Exception as e:
        logger.error(f"❌ Помилка при першому оновленні: {e}", exc_info=True)

    logger.info("✅ Бот запущено!")
    logger.info(f"🏙 Підтримка міст: {', '.join([c['name'] for c in CITIES.values()])}")

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())