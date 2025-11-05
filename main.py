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
        "parser_type": "chernivtsi_html",
        "groups_count": 12
    },
    "kyiv": {
        "name": "Київ",
        "schedule_url": "https://www.dtek-kem.com.ua/ua/shutdowns",
        "search_url": "https://www.dtek-kem.com.ua/ua/shutdowns",
        "parser_type": "address_based",
        "note": "Потрібна адреса"
    },
    "khmelnytskyi": {
        "name": "Хмельницький",
        "schedule_url": "https://hoe.com.ua/page/pogodinni-vidkljuchennja",
        "parser_type": "image_based",
        "note": "Графік у форматі зображення"
    },
    "kamyanets": {
        "name": "Кам'янець-Подільський",
        "schedule_url": "https://hoe.com.ua/page/pogodinni-vidkljuchennja",
        "parser_type": "image_based",
        "note": "Графік у форматі зображення"
    }
}


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
    """Ініціалізація бази даних з оптимізованою структурою"""
    with get_db() as conn:
        # Таблиця користувачів
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                city TEXT DEFAULT 'chernivtsi',
                group_number INTEGER,
                address TEXT,
                notifications_enabled INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Таблиця міст користувача (multi-city support)
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

        # Таблиця графіків - зберігаємо спарсені дані
        conn.execute("""
            CREATE TABLE IF NOT EXISTS schedules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                city TEXT,
                group_number INTEGER,
                date TEXT,
                schedule_data TEXT,
                raw_html TEXT,
                parsed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(city, group_number, date)
            )
        """)

        # Таблиця часових інтервалів (для структурованого зберігання)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS schedule_intervals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                schedule_id INTEGER,
                time_start TEXT,
                time_end TEXT,
                status TEXT,
                FOREIGN KEY(schedule_id) REFERENCES schedules(id)
            )
        """)

        # Таблиця сповіщень
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

        # Таблиця логів парсингу
        conn.execute("""
            CREATE TABLE IF NOT EXISTS parse_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                city TEXT,
                status TEXT,
                message TEXT,
                groups_parsed INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Індекси для швидкого пошуку
        conn.execute("CREATE INDEX IF NOT EXISTS idx_schedules_city_group_date ON schedules(city, group_number, date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_users_city_group ON users(city, group_number)")

        conn.commit()
        logger.info("✅ База даних ініціалізована")


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
                    updated_at = CURRENT_TIMESTAMP
            """, (user_id, username, city, group_number))
            conn.commit()

    @staticmethod
    def update_city(user_id: int, city: str):
        with get_db() as conn:
            conn.execute(
                "UPDATE users SET city = ?, updated_at = CURRENT_TIMESTAMP WHERE user_id = ?",
                (city, user_id)
            )
            conn.commit()

    @staticmethod
    def update_group(user_id: int, group_number: int):
        with get_db() as conn:
            conn.execute(
                "UPDATE users SET group_number = ?, updated_at = CURRENT_TIMESTAMP WHERE user_id = ?",
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
    def get_user(user_id: int) -> Optional[Dict]:
        with get_db() as conn:
            row = conn.execute(
                "SELECT * FROM users WHERE user_id = ?", (user_id,)
            ).fetchone()
            return dict(row) if row else None

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
                    "UPDATE users SET notifications_enabled = ?, updated_at = CURRENT_TIMESTAMP WHERE user_id = ?",
                    (new_state, user_id)
                )
                conn.commit()
                return bool(new_state)
            return False


# Парсер графіків з HTML → БД
class ScheduleParser:

    @staticmethod
    async def fetch_and_parse(city: str) -> Dict:
        """
        Головна функція парсингу: HTML → Structured Data → БД
        Повертає статистику парсингу
        """
        city_data = CITIES.get(city)
        if not city_data:
            return {"success": False, "error": "City not found"}

        parser_type = city_data.get('parser_type')

        try:
            if parser_type == "chernivtsi_html":
                return await ScheduleParser._parse_chernivtsi_full(city, city_data)
            elif parser_type == "address_based":
                return {"success": False, "reason": "address_required"}
            elif parser_type == "image_based":
                return {"success": False, "reason": "image_format"}
            else:
                return {"success": False, "error": "Unknown parser type"}

        except Exception as e:
            logger.error(f"[{city}] Parse error: {e}", exc_info=True)
            return {"success": False, "error": str(e)}

    @staticmethod
    async def _parse_chernivtsi_full(city: str, city_data: dict) -> Dict:
        """
        Повний парсинг для Чернівців:
        1. Завантаження HTML
        2. Парсинг BeautifulSoup
        3. Витягування даних по групах
        4. Збереження в БД
        """
        url = city_data['schedule_url']
        logger.info(f"[{city}] 📡 Завантаження з {url}")

        # 1. Завантаження HTML
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'uk-UA,uk;q=0.9',
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=30) as response:
                if response.status != 200:
                    ScheduleParser._log_parse_result(city, "error", f"HTTP {response.status}", 0)
                    return {"success": False, "error": f"HTTP {response.status}"}

                html = await response.text()
                logger.info(f"[{city}] 📄 HTML завантажено ({len(html)} символів)")

        # 2. Парсинг BeautifulSoup
        soup = BeautifulSoup(html, 'html.parser')
        schedule_container = soup.find('div', {'id': 'gsv'})

        if not schedule_container:
            ScheduleParser._log_parse_result(city, "error", "Container not found", 0)
            return {"success": False, "error": "Schedule container not found"}

        # 3. Витягування часових інтервалів
        time_intervals = ScheduleParser._extract_time_intervals(schedule_container)
        logger.info(f"[{city}] ⏰ Знайдено {len(time_intervals)} часових інтервалів")

        # 4. Парсинг груп
        today = datetime.now().strftime("%Y-%m-%d")
        groups_parsed = 0
        all_schedules = {}

        for group_num in range(1, city_data.get('groups_count', 12) + 1):
            group_div = soup.find('div', {'id': f'inf{group_num}'})
            if not group_div:
                logger.warning(f"[{city}] Група {group_num} не знайдена")
                continue

            # Парсинг статусів
            schedule = ScheduleParser._parse_group_schedule(
                group_div,
                time_intervals,
                group_num
            )

            if schedule:
                # 5. Збереження в БД
                ScheduleParser._save_to_db(city, group_num, today, schedule, html)
                all_schedules[group_num] = schedule
                groups_parsed += 1
                logger.info(f"[{city}] ✅ Група {group_num}: {len(schedule)} інтервалів збережено")

        # 6. Логування результату
        ScheduleParser._log_parse_result(
            city,
            "success" if groups_parsed > 0 else "partial",
            f"Parsed {groups_parsed} groups",
            groups_parsed
        )

        return {
            "success": True,
            "city": city,
            "date": today,
            "groups_parsed": groups_parsed,
            "total_intervals": len(time_intervals),
            "schedules": all_schedules
        }

    @staticmethod
    def _extract_time_intervals(container) -> List[str]:
        """Витягування часових інтервалів з HTML"""
        time_intervals = []
        time_container = container.find('p')

        if time_container:
            for time_block in time_container.find_all('u', recursive=False):
                main_time = time_block.find('b')
                half_time = time_block.find('u')

                if main_time:
                    hour_text = main_time.get_text(strip=True)
                    hour_match = re.search(r'(\d{2})', hour_text)

                    if hour_match:
                        hour = int(hour_match.group(1))
                        next_hour = (hour + 1) % 24

                        # Додаємо обидва інтервали (00-30 та 30-60)
                        time_intervals.append(f"{hour:02d}:00-{hour:02d}:30")
                        time_intervals.append(f"{hour:02d}:30-{next_hour:02d}:00")

        return time_intervals

    @staticmethod
    def _parse_group_schedule(group_div, time_intervals: List[str], group_num: int) -> List[Dict]:
        """Парсинг графіку конкретної групи"""
        schedule = []
        cells = group_div.find_all(['u', 'o', 's'])

        for idx, cell in enumerate(cells):
            if idx >= len(time_intervals):
                break

            tag_name = cell.name
            cell_text = cell.get_text(strip=True).lower()

            # Визначення статусу на основі тегу
            if tag_name == 'o':  # <o> = червоний = відключення
                status = 'off'
            elif tag_name == 's':  # <s> = можливо
                status = 'maybe'
            elif tag_name == 'u':  # <u> = зелений = світло є
                status = 'on'
            else:
                status = 'on'

            # Додаткова перевірка по тексту
            if 'в' in cell_text or 'відкл' in cell_text:
                status = 'off'
            elif 'мз' in cell_text or 'можливо' in cell_text:
                status = 'maybe'

            schedule.append({
                'time': time_intervals[idx],
                'status': status,
                'raw': cell_text
            })

        return schedule

    @staticmethod
    def _save_to_db(city: str, group_number: int, date: str, schedule: List[Dict], raw_html: str):
        """Збереження спарсених даних в БД"""
        with get_db() as conn:
            # Зберігаємо JSON графіку
            schedule_json = json.dumps(schedule, ensure_ascii=False)

            cursor = conn.execute("""
                INSERT INTO schedules (city, group_number, date, schedule_data, raw_html)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(city, group_number, date) DO UPDATE SET
                    schedule_data = excluded.schedule_data,
                    raw_html = excluded.raw_html,
                    updated_at = CURRENT_TIMESTAMP
            """, (city, group_number, date, schedule_json, raw_html[:10000]))  # Обмежуємо HTML

            schedule_id = cursor.lastrowid

            # Зберігаємо структуровані інтервали
            conn.execute("DELETE FROM schedule_intervals WHERE schedule_id = ?", (schedule_id,))

            for interval in schedule:
                time_parts = interval['time'].split('-')
                if len(time_parts) == 2:
                    conn.execute("""
                        INSERT INTO schedule_intervals (schedule_id, time_start, time_end, status)
                        VALUES (?, ?, ?, ?)
                    """, (schedule_id, time_parts[0], time_parts[1], interval['status']))

            conn.commit()

    @staticmethod
    def _log_parse_result(city: str, status: str, message: str, groups_parsed: int):
        """Логування результатів парсингу"""
        with get_db() as conn:
            conn.execute("""
                INSERT INTO parse_logs (city, status, message, groups_parsed)
                VALUES (?, ?, ?, ?)
            """, (city, status, message, groups_parsed))
            conn.commit()

    @staticmethod
    def get_schedule_from_db(city: str, group_number: int, date: str) -> Optional[List[Dict]]:
        """Отримання графіку з БД"""
        with get_db() as conn:
            row = conn.execute(
                "SELECT schedule_data FROM schedules WHERE city = ? AND group_number = ? AND date = ?",
                (city, group_number, date)
            ).fetchone()

            if row and row['schedule_data']:
                try:
                    return json.loads(row['schedule_data'])
                except json.JSONDecodeError:
                    return None
            return None

    @staticmethod
    def get_schedule_metadata(city: str, group_number: int, date: str) -> Optional[Dict]:
        """Отримання метаданих графіку"""
        with get_db() as conn:
            row = conn.execute("""
                SELECT parsed_at, updated_at 
                FROM schedules 
                WHERE city = ? AND group_number = ? AND date = ?
            """, (city, group_number, date)).fetchone()

            return dict(row) if row else None


# Форматування для виводу в Telegram
def format_schedule(schedule: List[Dict], city_data: dict = None) -> str:
    """Форматування графіку для відображення в Telegram"""
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

    # Групуємо послідовні відключення
    current_block = None

    for item in schedule:
        emoji = {"off": "🔴", "on": "🟢", "maybe": "⚪"}.get(item['status'], "⚪")
        status_text = {"off": "Відключення", "on": "Світло є", "maybe": "Можливо"}.get(item['status'], "Невідомо")

        text += f"{emoji} {item['time']} - {status_text}\n"

    text += "\n━━━━━━━━━━━━━━━━━━\n"
    text += "🔴 - гарантоване відключення\n"
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
        [InlineKeyboardButton(text="📈 Статистика", callback_data="stats")],
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


def get_groups_keyboard(city: str = "chernivtsi") -> InlineKeyboardMarkup:
    groups_count = CITIES.get(city, {}).get('groups_count', 18)
    keyboard = []

    for i in range(0, groups_count, 3):
        row = []
        for j in range(3):
            group_num = i + j + 1
            if group_num <= groups_count:
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
        "🔹 Я автоматично:\n"
        "  • Парсю графіки з офіційних сайтів\n"
        "  • Зберігаю дані в базу\n"
        "  • Надсилаю сповіщення за 30 хв до відключення\n"
        "  • Показую актуальну інформацію\n\n"
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
    """Примусове оновлення графіків"""
    await message.answer("⏳ Парсю графіки з сайту та оновлюю базу даних...")

    bot = message.bot
    result = await update_schedules(bot, force=True)

    if result.get('success'):
        await message.answer(
            f"✅ Графіки оновлено!\n\n"
            f"📊 Міст: {result.get('cities_updated', 0)}\n"
            f"👥 Груп: {result.get('groups_updated', 0)}"
        )
    else:
        await message.answer("❌ Помилка оновлення. Спробуйте пізніше.")


@router.message(Command("stats"))
async def cmd_stats(message: Message):
    """Статистика парсингу"""
    with get_db() as conn:
        # Останні логи
        logs = conn.execute("""
            SELECT city, status, message, groups_parsed, created_at 
            FROM parse_logs 
            ORDER BY created_at DESC 
            LIMIT 10
        """).fetchall()

        # Загальна статистика
        stats = conn.execute("""
            SELECT 
                city,
                COUNT(DISTINCT date) as days_parsed,
                COUNT(DISTINCT group_number) as groups_count,
                MAX(updated_at) as last_update
            FROM schedules
            GROUP BY city
        """).fetchall()

    text = "📈 Статистика парсингу\n\n"

    if stats:
        text += "🗂 Збережено в БД:\n"
        for stat in stats:
            city_name = CITIES.get(stat['city'], {}).get('name', stat['city'])
            text += f"• {city_name}: {stat['groups_count']} груп, {stat['days_parsed']} днів\n"
            text += f"  Оновлено: {stat['last_update'][:16]}\n"

    text += "\n📋 Останні операції:\n"
    for log in logs[:5]:
        city_name = CITIES.get(log['city'], {}).get('name', log['city'])
        emoji = "✅" if log['status'] == 'success' else "⚠️" if log['status'] == 'partial' else "❌"
        text += f"{emoji} {city_name}: {log['groups_parsed']} груп\n"

    await message.answer(text)


@router.callback_query(F.data == "stats")
async def show_stats(callback: CallbackQuery):
    await callback.answer()
    await cmd_stats(callback.message)


@router.callback_query(F.data == "my_schedule")
async def show_schedule(callback: CallbackQuery):
    await callback.answer()

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

    # Отримання з БД
    today = datetime.now().strftime("%Y-%m-%d")
    schedule = ScheduleParser.get_schedule_from_db(city, group_num, today)
    metadata = ScheduleParser.get_schedule_metadata(city, group_num, today)

    if not schedule:
        # Спробуємо завантажити
        await callback.message.answer("⏳ Завантажую з сайту...")

        result = await ScheduleParser.fetch_and_parse(city)

        if result.get('success'):
            schedule = result['schedules'].get(group_num)
            metadata = {"updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

        if not schedule:
            await callback.message.answer(
                f"📊 Графік для {city_name}, група {group_num} недоступний.\n\n"
                f"Перевірте на сайті:\n{city_data['schedule_url']}",
                reply_markup=get_main_keyboard(city)
            )
            return

    # Форматування та відправка
    text = format_schedule(schedule, city_data)
    text = f"🏙 {city_name}\n⚡️ Група {group_num}\n\n" + text

    if metadata:
        text += f"\n\n🕐 Оновлено: {metadata['updated_at'][:16]}"

    await callback.message.answer(text, reply_markup=get_main_keyboard(city))


@router.callback_query(F.data == "refresh_schedule")
async def refresh_schedule(callback: CallbackQuery):
    await callback.answer("🔄 Оновлюю графік...")

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

    # Примусове оновлення
    result = await ScheduleParser.fetch_and_parse(city)

    if result.get('success'):
        schedule = result['schedules'].get(group_num)

        if schedule:
            text = format_schedule(schedule, city_data)
            text = f"🏙 {city_name}\n⚡️ Група {group_num}\n\n" + text
            text += f"\n\n🕐 Оновлено: {datetime.now().strftime('%H:%M:%S')}"

            await callback.message.answer(text, reply_markup=get_main_keyboard(city))
        else:
            await callback.message.answer(
                f"❌ Не вдалося отримати графік для групи {group_num}",
                reply_markup=get_main_keyboard(city)
            )
    else:
        await callback.message.answer(
            "❌ Помилка оновлення. Спробуйте пізніше.",
            reply_markup=get_main_keyboard(city)
        )


@router.callback_query(F.data == "change_city")
async def change_city(callback: CallbackQuery):
    await callback.answer()
    await callback.message.edit_text(
        "🏙 Оберіть ваше місто:",
        reply_markup=get_cities_keyboard()
    )


@router.callback_query(F.data.startswith("city_"))
async def select_city(callback: CallbackQuery):
    city_id = callback.data.split("_")[1]
    city_data = CITIES.get(city_id)

    if not city_data:
        await callback.answer("❌ Місто не знайдено")
        return

    await callback.answer()

    # Оновлюємо місто користувача
    UserManager.update_city(callback.from_user.id, city_id)

    city_name = city_data['name']

    if city_data.get('parser_type') == 'address_based':
        await callback.message.edit_text(
            f"🏙 Обрано: {city_name}\n\n"
            f"⚠️ Для {city_name} потрібно вказати адресу.\n"
            f"Використовуйте пошук на сайті:\n{city_data['schedule_url']}",
            reply_markup=get_main_keyboard(city_id)
        )
    elif city_data.get('parser_type') == 'image_based':
        await callback.message.edit_text(
            f"🏙 Обрано: {city_name}\n\n"
            f"⚠️ Графік у форматі зображення.\n"
            f"Перегляньте на сайті:\n{city_data['schedule_url']}",
            reply_markup=get_main_keyboard(city_id)
        )
    else:
        await callback.message.edit_text(
            f"🏙 Обрано: {city_name}\n\n"
            f"Тепер оберіть вашу групу відключень:",
            reply_markup=get_groups_keyboard(city_id)
        )


@router.callback_query(F.data == "change_group")
async def change_group(callback: CallbackQuery):
    await callback.answer()

    user = UserManager.get_user(callback.from_user.id)
    city = user.get('city', 'chernivtsi') if user else 'chernivtsi'

    await callback.message.edit_text(
        "⚡️ Оберіть вашу групу відключень:",
        reply_markup=get_groups_keyboard(city)
    )


@router.callback_query(F.data.startswith("group_"))
async def select_group(callback: CallbackQuery):
    group_num = int(callback.data.split("_")[1])

    await callback.answer()

    # Оновлюємо групу
    UserManager.update_group(callback.from_user.id, group_num)
    user = UserManager.get_user(callback.from_user.id)

    city = user.get('city', 'chernivtsi')
    city_name = CITIES.get(city, {}).get('name', 'Чернівці')

    await callback.message.edit_text(
        f"✅ Налаштування збережено!\n\n"
        f"🏙 Місто: {city_name}\n"
        f"⚡️ Група: {group_num}\n\n"
        f"Завантажую ваш графік...",
        reply_markup=get_main_keyboard(city)
    )

    # Завантажуємо графік
    today = datetime.now().strftime("%Y-%m-%d")
    schedule = ScheduleParser.get_schedule_from_db(city, group_num, today)

    if not schedule:
        result = await ScheduleParser.fetch_and_parse(city)
        if result.get('success'):
            schedule = result['schedules'].get(group_num)

    if schedule:
        text = format_schedule(schedule, CITIES.get(city))
        text = f"🏙 {city_name}\n⚡️ Група {group_num}\n\n" + text
        await callback.message.answer(text, reply_markup=get_main_keyboard(city))


@router.callback_query(F.data == "settings")
async def show_settings(callback: CallbackQuery):
    await callback.answer()

    user = UserManager.get_user(callback.from_user.id)

    if not user:
        await callback.message.answer("❌ Помилка: користувач не знайдений")
        return

    notifications_status = "✅ Увімкнено" if user.get('notifications_enabled', 1) else "❌ Вимкнено"
    city_name = CITIES.get(user.get('city', 'chernivtsi'), {}).get('name', 'Чернівці')

    text = (
        "⚙️ Налаштування\n\n"
        f"🏙 Місто: {city_name}\n"
        f"⚡️ Група: {user.get('group_number', 'не обрано')}\n"
        f"🔔 Сповіщення: {notifications_status}\n"
    )

    keyboard = [
        [InlineKeyboardButton(
            text="🔔 Увімкнути сповіщення" if not user.get('notifications_enabled', 1) else "🔕 Вимкнути сповіщення",
            callback_data="toggle_notifications"
        )],
        [InlineKeyboardButton(text="🏙 Змінити місто", callback_data="change_city")],
        [InlineKeyboardButton(text="⚡️ Змінити групу", callback_data="change_group")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_menu")]
    ]

    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))


@router.callback_query(F.data == "toggle_notifications")
async def toggle_notifications(callback: CallbackQuery):
    new_state = UserManager.toggle_notifications(callback.from_user.id)

    status = "увімкнено" if new_state else "вимкнено"
    await callback.answer(f"🔔 Сповіщення {status}")

    # Оновлюємо меню налаштувань
    await show_settings(callback)


@router.callback_query(F.data == "help")
async def show_help(callback: CallbackQuery):
    await callback.answer()

    help_text = (
        "❓ Довідка\n\n"
        "🔹 Команди:\n"
        "/start - Головне меню\n"
        "/update - Оновити графіки\n"
        "/stats - Статистика парсингу\n\n"
        "🔹 Як це працює:\n"
        "1. Бот автоматично парсить графіки з офіційних сайтів\n"
        "2. Дані зберігаються в базу даних\n"
        "3. Ви отримуєте сповіщення за 30 хв до відключення\n"
        "4. Графік оновлюється кожні 6 годин\n\n"
        "🔹 Підтримувані міста:\n"
    )

    for city_data in CITIES.values():
        help_text += f"• {city_data['name']}\n"

    help_text += "\n📞 Підтримка: @your_support"

    keyboard = [[InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_menu")]]
    await callback.message.edit_text(help_text, reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))


@router.callback_query(F.data == "back_to_menu")
async def back_to_menu(callback: CallbackQuery):
    await callback.answer()

    user = UserManager.get_user(callback.from_user.id)
    city = user.get('city', 'chernivtsi') if user else 'chernivtsi'

    await callback.message.edit_text(
        "📊 Головне меню",
        reply_markup=get_main_keyboard(city)
    )


# Автоматичне оновлення графіків
async def update_schedules(bot: Bot, force: bool = False) -> Dict:
    """
    Автоматичне оновлення графіків для всіх міст
    """
    logger.info("🔄 Початок автоматичного оновлення графіків")

    results = {
        'success': True,
        'cities_updated': 0,
        'groups_updated': 0,
        'errors': []
    }

    for city_id, city_data in CITIES.items():
        if city_data.get('parser_type') == 'chernivtsi_html':
            try:
                logger.info(f"[{city_id}] Оновлення графіків...")
                result = await ScheduleParser.fetch_and_parse(city_id)

                if result.get('success'):
                    results['cities_updated'] += 1
                    results['groups_updated'] += result.get('groups_parsed', 0)
                    logger.info(f"[{city_id}] ✅ Оновлено {result.get('groups_parsed', 0)} груп")
                else:
                    results['errors'].append(f"{city_id}: {result.get('error', 'Unknown error')}")
                    logger.error(f"[{city_id}] ❌ Помилка: {result.get('error')}")

            except Exception as e:
                results['errors'].append(f"{city_id}: {str(e)}")
                logger.error(f"[{city_id}] ❌ Exception: {e}", exc_info=True)

    if results['errors']:
        results['success'] = False

    logger.info(
        f"🔄 Оновлення завершено: "
        f"{results['cities_updated']} міст, "
        f"{results['groups_updated']} груп, "
        f"{len(results['errors'])} помилок"
    )

    return results


# Сповіщення користувачів
async def send_notifications(bot: Bot):
    """
    Надсилання сповіщень за 30 хвилин до відключення
    """
    logger.info("🔔 Перевірка сповіщень...")

    now = datetime.now()
    notification_time = (now + timedelta(minutes=30)).strftime("%H:%M")
    today = now.strftime("%Y-%m-%d")

    with get_db() as conn:
        # Отримуємо всі графіки на сьогодні
        schedules = conn.execute("""
            SELECT city, group_number, schedule_data
            FROM schedules
            WHERE date = ?
        """, (today,)).fetchall()

    notifications_sent = 0

    for schedule_row in schedules:
        city = schedule_row['city']
        group_num = schedule_row['group_number']

        try:
            schedule = json.loads(schedule_row['schedule_data'])
        except json.JSONDecodeError:
            continue

        # Перевіряємо чи є відключення в найближчі 30 хвилин
        for interval in schedule:
            if interval['status'] == 'off':
                time_start = interval['time'].split('-')[0]

                # Якщо відключення через ~30 хвилин
                if time_start <= notification_time <= interval['time'].split('-')[1]:
                    # Отримуємо користувачів цієї групи
                    users = UserManager.get_users_by_city_and_group(city, group_num)

                    for user in users:
                        # Перевіряємо чи не надсилали вже сьогодні
                        with get_db() as conn:
                            already_sent = conn.execute("""
                                SELECT id FROM notifications_sent
                                WHERE user_id = ? AND city = ? AND group_number = ? AND date = ? AND time = ?
                            """, (user['user_id'], city, group_num, today, interval['time'])).fetchone()

                        if not already_sent:
                            try:
                                city_name = CITIES.get(city, {}).get('name', city)
                                message = (
                                    f"⚠️ Попередження!\n\n"
                                    f"🏙 {city_name}\n"
                                    f"⚡️ Група {group_num}\n\n"
                                    f"🔴 Через 30 хвилин планове відключення:\n"
                                    f"⏰ {interval['time']}"
                                )

                                await bot.send_message(user['user_id'], message)

                                # Зберігаємо що відправили
                                with get_db() as conn:
                                    conn.execute("""
                                        INSERT INTO notifications_sent (user_id, city, group_number, date, time)
                                        VALUES (?, ?, ?, ?, ?)
                                    """, (user['user_id'], city, group_num, today, interval['time']))
                                    conn.commit()

                                notifications_sent += 1
                                logger.info(f"🔔 Надіслано сповіщення користувачу {user['user_id']}")

                            except Exception as e:
                                logger.error(f"❌ Помилка надсилання користувачу {user['user_id']}: {e}")

    logger.info(f"🔔 Надіслано {notifications_sent} сповіщень")


# Планувальник задач
def setup_scheduler(bot: Bot) -> AsyncIOScheduler:
    """Налаштування автоматичних задач"""
    scheduler = AsyncIOScheduler(timezone="Europe/Kiev")

    # Оновлення графіків кожні 6 годин
    scheduler.add_job(
        update_schedules,
        CronTrigger(hour="*/6"),
        args=[bot, False],
        id="update_schedules",
        replace_existing=True
    )

    # Перевірка сповіщень кожні 15 хвилин
    scheduler.add_job(
        send_notifications,
        CronTrigger(minute="*/15"),
        args=[bot],
        id="send_notifications",
        replace_existing=True
    )

    logger.info("⏰ Планувальник налаштовано")
    return scheduler


# Головна функція
async def main():
    # Ініціалізація
    init_db()

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    # Планувальник
    scheduler = setup_scheduler(bot)
    scheduler.start()

    # Перше оновлення при запуску
    logger.info("🚀 Перше оновлення графіків...")
    await update_schedules(bot, force=True)

    logger.info("🤖 Бот запущено!")

    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        await bot.session.close()
        scheduler.shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Бот зупинено")