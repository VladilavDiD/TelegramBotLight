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
    # Припускаємо, що тут ви підставили свій справжній токен для роботи
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
        "note": "Потрібна адреса для перевірки на сайті ДТЕК."
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

        # НОВА ТАБЛИЦЯ для відстеження URL зображень
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
    # ... (методи UserManager залишаються без змін)
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
                        group_number = excluded.group_number,
                        address = NULL
                """, (user_id, user.get('city', 'chernivtsi'), group_number))

            conn.commit()

    @staticmethod
    def update_address(user_id: int, address: str, city: str):
        with get_db() as conn:
            conn.execute(
                "UPDATE users SET address = ?, city = ? WHERE user_id = ?",
                (address, city, user_id)
            )
            conn.execute("""
                INSERT INTO user_cities (user_id, city, address, group_number)
                VALUES (?, ?, ?, NULL)
                ON CONFLICT(user_id, city) DO UPDATE SET
                    address = excluded.address
            """, (user_id, city, address))
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
            # Для міст з групою
            rows = conn.execute(
                "SELECT * FROM users WHERE city = ? AND group_number = ? AND notifications_enabled = 1",
                (city, group_number)
            ).fetchall()
            return [dict(row) for row in rows]

    @staticmethod
    def get_users_by_city(city: str) -> List[Dict]:
        with get_db() as conn:
            # Для міст з адресою (Київ) або зображенням (Хмельницький)
            rows = conn.execute(
                "SELECT * FROM users WHERE city = ? AND notifications_enabled = 1 AND (address IS NOT NULL OR group_number IS NOT NULL)",
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
    @staticmethod
    async def fetch_schedule(city: str = "chernivtsi", address: Optional[str] = None) -> Optional[
        Dict[int, List[Dict]]]:
        """Парсинг графіків з сайту конкретного міста"""
        try:
            city_data = CITIES.get(city)
            if not city_data:
                logger.error(f"Місто {city} не знайдено в конфігурації")
                return None

            parser_type = city_data.get('parser_type', 'default')

            if parser_type == 'chernivtsi_custom':
                return await ScheduleParser._parse_chernivtsi(city_data)
            elif parser_type == 'kyiv_dtek_address' and address:
                # Київ: парсинг за адресою
                schedule = await ScheduleParser._parse_kyiv_dtek(city_data, address)
                # Повертаємо у вигляді {0: schedule_list}
                return {0: schedule} if schedule else None
            elif parser_type == 'image_based':
                # Хмельницький/Кам'янець-Подільський: шукаємо зображення
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
                'Referer': url
            }

            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=30) as response:
                    if response.status != 200:
                        logger.error(f"HTTP {response.status} для Чернівців")
                        return None

                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')

                    # 1. Отримуємо загальний список часових інтервалів
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

                    # 2. Парсимо графіки груп (1-12)
                    schedule_data = {}
                    for group_num in range(1, city_data['groups'] + 1):
                        group_div = soup.find('div', {'id': f'inf{group_num}'})
                        if not group_div:
                            continue

                        schedule_data[group_num] = []

                        # Використовуємо .descendants для надійнішого пошуку всіх вкладених тегів u, o, s
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

                            schedule_data[group_num].append({
                                'time': time_headers[idx],
                                'status': status
                            })

                    return schedule_data if schedule_data else None

        except Exception as e:
            logger.error(f"[Чернівці] Помилка парсингу: {e}", exc_info=True)
            return None

    @staticmethod
    async def _parse_kyiv_dtek(city_data: dict, address: str) -> Optional[List[Dict]]:
        """ВИПРАВЛЕНО: Парсер для Києва (ДТЕК) через API за адресою"""
        api_url = city_data['search_url_api']

        # 1. Отримання ID адреси (ВИПРАВЛЕНО: кодування адреси)
        try:
            # Коректне кодування адреси для URL та JSON
            safe_address = quote_plus(address)

            async with aiohttp.ClientSession() as session:
                response = await session.post(
                    api_url,
                    json={"search": address},
                    headers={
                        'Content-Type': 'application/json',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                    }
                )
                data = await response.json()

                if not data or not data.get('results'):
                    logger.warning(f"[Київ] Адреса не знайдена: {address} ({data.get('error', 'no result')})")
                    return [{
                        'time': 'Помилка',
                        'status': 'error',
                        'message': 'Не вдалося знайти адресу. Перевірте формат вводу та наявність на сайті ДТЕК.'
                    }]

                # Припускаємо, що перший результат є найбільш релевантним
                result = data['results'][0]
                street_id = result['street_id']
                house_id = result['house_id']

        except Exception as e:
            logger.error(f"[Київ] Помилка пошуку адреси: {e}", exc_info=True)
            return [{
                'time': 'Помилка',
                'status': 'error',
                'message': 'Помилка з\'єднання з API ДТЕК.'
            }]

        # 2. Отримання графіку
        try:
            async with aiohttp.ClientSession() as session:
                response = await session.post(
                    api_url,
                    json={
                        "street_id": street_id,
                        "house_id": house_id,
                        "language": "ua"
                    },
                    headers={
                        'Content-Type': 'application/json',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                    }
                )
                data = await response.json()

                schedule_list = []
                raw_schedule = data.get('current_schedule', [])

                for item in raw_schedule:
                    schedule_list.append({
                        'time': item['time'],
                        # Статус має бути 'off', 'on', 'possible'
                        'status': item['status'].lower().replace('possible', 'maybe')
                    })

                if not schedule_list:
                    return [{
                        'time': 'Інформація',
                        'status': 'info',
                        'message': 'Графік відсутній, відключень наразі немає.'
                    }]

                return schedule_list

        except Exception as e:
            logger.error(f"[Київ] Помилка отримання графіку: {e}", exc_info=True)
            return [{
                'time': 'Помилка',
                'status': 'error',
                'message': 'Помилка обробки даних графіку ДТЕК.'
            }]

    @staticmethod
    async def _parse_image_based(city_data: dict, city: str) -> Optional[Dict[int, List[Dict]]]:
        """ОНОВЛЕНО: Парсер для міст з графіками у вигляді зображень (Хмельницький/Кам'янець)"""
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
                    images = soup.find_all('img', src=re.compile(r'(grafik|schedule|vidkl|pogod)', re.I))

                    if images:
                        img_url = images[0].get('src')
                        # Формуємо повний URL
                        if not img_url.startswith('http'):
                            img_url = urljoin(url, img_url)

                        logger.info(f"[{city}] Знайдено зображення графіку: {img_url}")

                        # ЗБЕРІГАЄМО URL у БД
                        ScheduleParser._save_image_url(city, img_url)

                        return {
                            0: [{
                                'time': 'Інформація',
                                'status': 'info',
                                'message': f'Актуальний графік у форматі зображення',
                                'image_url': img_url
                            }]
                        }

            logger.warning(f"[{city}] Зображення графіку не знайдено.")

            return {
                0: [{
                    'time': 'Помилка',
                    'status': 'error',
                    'message': 'Зображення графіку на сайті не знайдено.'
                }]
            }


        except Exception as e:
            logger.error(f"[{city}] Помилка парсингу зображення: {e}", exc_info=True)
            return None

    @staticmethod
    def _save_image_url(city: str, image_url: str):
        """Збереження URL зображення в окрему таблицю"""
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
        """Отримання збереженого URL зображення"""
        with get_db() as conn:
            row = conn.execute(
                "SELECT image_url FROM image_schedules WHERE city = ?", (city,)
            ).fetchone()
            return row['image_url'] if row else None

    # ... (Решта статичних методів парсингу залишаються без змін)
    @staticmethod
    def _parse_generic(city_data: dict, city: str) -> Optional[Dict[int, List[Dict]]]:
        # ... (ваш код)
        return None
    # ... (інші методи)


# ... (UserManager, init_db, format_schedule, клавіатури - без суттєвих змін)
# ...
# ...
# ...


# Бот
router = Router()


# ... (Всі роутери команд та callback-ів залишаються без змін)

# Scheduled tasks

async def update_schedules(bot: Bot):
    """Оновлення графіків для всіх міст"""
    logger.info("📅 Оновлення графіків для всіх міст...")

    today = datetime.now().strftime("%Y-%m-%d")

    for city_id, city_data in CITIES.items():
        try:
            parser_type = city_data.get('parser_type', 'default')

            # Якщо це парсер зображення, просто отримуємо його URL
            if parser_type == 'image_based':
                await ScheduleParser.fetch_schedule(city_id)  # Це оновить image_schedules
                logger.info(f"[{city_id}] URL зображення оновлено.")
                continue

            # Пропускаємо міста, що не мають загальної сітки
            if parser_type in ['kyiv_dtek_address']:
                logger.info(f"[{city_id}] Пропускаємо автооновлення ({parser_type})")
                continue

            logger.info(f"[{city_id}] Початок оновлення...")
            schedules = await ScheduleParser.fetch_schedule(city_id)

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
    """НОВИЙ ПЛАНУВАЛЬНИК: Перевірка змін у графіках-зображеннях"""
    logger.info("📸 Перевірка оновлень зображень графіків...")

    for city_id, city_data in CITIES.items():
        if city_data.get('parser_type') != 'image_based':
            continue

        city_name = city_data['name']
        old_url = ScheduleParser._get_image_url(city_id)

        # Виконуємо парсинг, щоб отримати новий URL і оновити його в БД
        new_schedules = await ScheduleParser.fetch_schedule(city_id)

        if not new_schedules or 0 not in new_schedules:
            continue

        new_url = new_schedules[0][0].get('image_url')

        if not new_url:
            continue

        # Порівнюємо старий та новий URL
        if old_url and old_url != new_url:
            logger.info(f"[{city_id}] ЗНАЙДЕНО ЗМІНУ ГРАФІКУ! {old_url} -> {new_url}")

            users_to_notify = UserManager.get_users_by_city(city_id)
            caption = f"⚠️ **ОНОВЛЕННЯ ГРАФІКУ!** ({city_name})\n\nАктуальний графік відключень змінився. Перевірте нове зображення."

            for user in users_to_notify:
                try:
                    # Надсилаємо нове зображення
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
    target_hour_minute = target_time.strftime("%H:%M")  # Наприклад, 10:30
    today = now.strftime("%Y-%m-%d")

    for city_id, city_data in CITIES.items():
        parser_type = city_data.get('parser_type')
        city_name = city_data['name']

        # Пропускаємо міста із зображенням
        if parser_type == 'image_based':
            continue

        # 1. Міста за ГРУПАМИ (Чернівці)
        if parser_type not in ['kyiv_dtek_address']:
            # ... (Логіка сповіщень для груп залишається без змін)
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
                address = user.get('address')
                if not address: continue

                # Отримуємо актуальний графік для цієї адреси
                schedule = await ScheduleParser._parse_kyiv_dtek(city_data, address)

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
        # Використовуємо новий спосіб для налаштувань за замовчуванням
        default=DefaultBotProperties(parse_mode='Markdown')
    )
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    # Scheduler
    scheduler = AsyncIOScheduler()

    # Оновлення графіків кожні 30 хвилин (парсинг)
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

    # НОВИЙ ПЛАНУВАЛЬНИК: Перевірка змін зображень кожну годину
    scheduler.add_job(
        check_and_notify_image_changes,
        CronTrigger(hour="*", minute="10"),  # О 10 хвилині кожної години
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