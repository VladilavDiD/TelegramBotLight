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
SCHEDULE_URL = "https://oblenergo.cv.ua/shutdowns/"
SEARCH_URL = "https://oblenergo.cv.ua/shutdowns-search/"


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
                group_number INTEGER,
                notifications_enabled INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS schedules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_number INTEGER,
                date TEXT,
                schedule_data TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(group_number, date)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS notifications_sent (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
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
    def save_user(user_id: int, username: str, group_number: Optional[int] = None):
        with get_db() as conn:
            conn.execute("""
                INSERT INTO users (user_id, username, group_number)
                VALUES (?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    username = excluded.username,
                    group_number = COALESCE(excluded.group_number, group_number)
            """, (user_id, username, group_number))
            conn.commit()

    @staticmethod
    def update_group(user_id: int, group_number: int):
        with get_db() as conn:
            conn.execute(
                "UPDATE users SET group_number = ? WHERE user_id = ?",
                (group_number, user_id)
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
    def get_users_by_group(group_number: int) -> List[Dict]:
        with get_db() as conn:
            rows = conn.execute(
                "SELECT * FROM users WHERE group_number = ? AND notifications_enabled = 1",
                (group_number,)
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
    async def fetch_schedule() -> Optional[Dict[int, List[Dict]]]:
        """Парсинг графіків з сайту Чернівціобленерго"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'uk-UA,uk;q=0.9',
            }

            async with aiohttp.ClientSession() as session:
                async with session.get(SCHEDULE_URL, headers=headers, timeout=30) as response:
                    if response.status != 200:
                        logger.error(f"HTTP {response.status} при завантаженні графіку")
                        return None

                    html = await response.text()
                    logger.info(f"Завантажено HTML ({len(html)} символів)")

                    # Зберігаємо HTML для відладки
                    try:
                        with open('debug_page.html', 'w', encoding='utf-8') as f:
                            f.write(html)
                        logger.info("💾 HTML збережено у debug_page.html")
                    except:
                        pass

                    # Метод 1: Пошук JSON в JavaScript
                    schedule_data = ScheduleParser._parse_js_data(html)
                    if schedule_data:
                        logger.info("✅ Дані знайдено через JavaScript")
                        return schedule_data

                    # Метод 2: Парсинг HTML таблиці
                    schedule_data = ScheduleParser._parse_html_table(html)
                    if schedule_data:
                        logger.info("✅ Дані знайдено через HTML таблицю")
                        return schedule_data

                    # Метод 3: Альтернативні таблиці
                    schedule_data = ScheduleParser._parse_alternative_tables(html)
                    if schedule_data:
                        logger.info("✅ Дані знайдено через альтернативні таблиці")
                        return schedule_data

                    logger.warning("❌ Жоден метод парсингу не спрацював")
                    logger.info("📋 Перевірте файл debug_page.html щоб зрозуміти структуру сторінки")

                    # Повертаємо порожній графік замість None
                    return {}

        except Exception as e:
            logger.error(f"Критична помилка парсингу: {e}", exc_info=True)
            return None

    @staticmethod
    def _parse_js_data(html: str) -> Optional[Dict[int, List[Dict]]]:
        """Метод 1: Пошук JSON у JavaScript"""
        patterns = [
            r'var\s+schedule\s*=\s*(\{.+?\});',
            r'const\s+schedule\s*=\s*(\{.+?\});',
            r'let\s+schedule\s*=\s*(\{.+?\});',
            r'window\.schedule\s*=\s*(\{.+?\});',
            r'scheduleData\s*=\s*(\{.+?\});',
            r'var\s+groups\s*=\s*(\[.+?\]);',
        ]

        for pattern in patterns:
            match = re.search(pattern, html, re.DOTALL)
            if match:
                try:
                    json_str = match.group(1)
                    data = json.loads(json_str)
                    logger.info(f"📍 Знайдено JSON через pattern: {pattern}")

                    if isinstance(data, dict):
                        return ScheduleParser._normalize_schedule_data(data)
                    elif isinstance(data, list):
                        return ScheduleParser._normalize_schedule_list(data)
                except json.JSONDecodeError:
                    continue

        return None

    @staticmethod
    def _parse_html_table(html: str) -> Optional[Dict[int, List[Dict]]]:
        """Метод 2: Парсинг HTML таблиці"""
        soup = BeautifulSoup(html, 'html.parser')

        # Шукаємо таблицю
        table_selectors = [
            {'class': 'shutdowns-table'},
            {'class': 'schedule-table'},
            {'class': 'outages-table'},
            {'id': 'schedule'},
            {'class': 'table'},
        ]

        table = None
        for selector in table_selectors:
            table = soup.find('table', selector)
            if table:
                logger.info(f"📍 Знайдено таблицю з селектором: {selector}")
                break

        # Якщо не знайшли - беремо будь-яку таблицю з більш ніж 5 рядків
        if not table:
            tables = soup.find_all('table')
            for t in tables:
                rows = t.find_all('tr')
                if len(rows) > 5:
                    table = t
                    logger.info(f"📍 Знайдено таблицю з {len(rows)} рядками")
                    break

        if not table:
            logger.warning("⚠️ Таблицю не знайдено")
            return None

        return ScheduleParser._parse_table_element(table)

    @staticmethod
    def _parse_table_element(table) -> Optional[Dict[int, List[Dict]]]:
        """Парсинг елемента таблиці"""
        schedule_data = {}

        try:
            # Знаходимо заголовки (часові інтервали)
            headers = []
            header_row = table.find('thead') or table.find('tr')

            if header_row:
                for th in header_row.find_all(['th', 'td']):
                    text = th.get_text(strip=True)
                    # Шукаємо часові інтервали (00:00, 00-02, 00:00-02:00, тощо)
                    if re.search(r'\d{1,2}[:.\-]\d{1,2}', text):
                        headers.append(text)

            logger.info(f"📋 Заголовків: {len(headers)}, приклад: {headers[:3] if headers else 'немає'}")

            # Якщо заголовків немає - створюємо стандартні
            if not headers:
                headers = [f"{i:02d}:00-{i + 2:02d}:00" for i in range(0, 24, 2)]
                logger.info("📋 Створено стандартні заголовки")

            # Парсинг рядків
            tbody = table.find('tbody')
            rows = (tbody.find_all('tr') if tbody else table.find_all('tr'))[1:]

            logger.info(f"📋 Рядків для парсингу: {len(rows)}")

            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) < 2:
                    continue

                # Перша комірка - номер групи
                group_text = cells[0].get_text(strip=True)
                group_match = re.search(r'\d+', group_text)

                if not group_match:
                    continue

                group_num = int(group_match.group())
                if group_num < 1 or group_num > 20:
                    continue

                schedule_data[group_num] = []

                # Парсинг статусів
                for i, cell in enumerate(cells[1:]):
                    if i >= len(headers):
                        break

                    status = ScheduleParser._determine_cell_status(cell)

                    schedule_data[group_num].append({
                        'time': headers[i],
                        'status': status
                    })

            if schedule_data:
                logger.info(f"✅ Успішно спарсено {len(schedule_data)} груп")
                # Виводимо приклад для групи 1
                if 1 in schedule_data:
                    logger.info(f"📊 Приклад (група 1): {schedule_data[1][:3]}")
                return schedule_data

        except Exception as e:
            logger.error(f"❌ Помилка парсингу таблиці: {e}", exc_info=True)

        return None

    @staticmethod
    def _determine_cell_status(cell) -> str:
        """Визначення статусу комірки"""
        style = str(cell.get('style', '')).lower()
        classes = ' '.join(cell.get('class', [])).lower()
        text = cell.get_text(strip=True).lower()
        bgcolor = str(cell.get('bgcolor', '')).lower()

        # Логуємо перші 3 комірки для відладки
        if not hasattr(ScheduleParser._determine_cell_status, 'logged'):
            ScheduleParser._determine_cell_status.logged = 0

        if ScheduleParser._determine_cell_status.logged < 3:
            logger.info(f"🔍 Комірка #{ScheduleParser._determine_cell_status.logged}: "
                        f"style={style[:50]}, classes={classes[:50]}, text={text[:20]}")
            ScheduleParser._determine_cell_status.logged += 1

        # Червоний = відключення
        red_indicators = ['red', '#ff0000', '#f00', 'rgb(255,0,0)', 'danger', 'outage', 'off']
        if any(ind in style or ind in classes or ind in bgcolor for ind in red_indicators):
            return 'off'

        # Зелений = світло є
        green_indicators = ['green', '#00ff00', '#0f0', 'rgb(0,255,0)', 'success', 'on']
        if any(ind in style or ind in classes or ind in bgcolor for ind in green_indicators):
            return 'on'

        # Жовтий/сірий = можливо
        maybe_indicators = ['yellow', 'gray', 'grey', 'warning', 'maybe']
        if any(ind in style or ind in classes or ind in bgcolor for ind in maybe_indicators):
            return 'maybe'

        # Текстові індикатори
        if any(word in text for word in ['відключення', 'немає', 'off', 'виключ']):
            return 'off'
        if any(word in text for word in ['можливо', 'maybe', 'імовірно']):
            return 'maybe'
        if any(word in text for word in ['так', 'yes', 'світло', 'on']):
            return 'on'

        return 'on'  # За замовчуванням

    @staticmethod
    def _parse_alternative_tables(html: str) -> Optional[Dict[int, List[Dict]]]:
        """Метод 3: Пошук альтернативних структур (div, ul, тощо)"""
        soup = BeautifulSoup(html, 'html.parser')

        # Шукаємо div з класом schedule
        schedule_divs = soup.find_all('div', class_=re.compile(r'schedule|shutdowns|outages', re.I))

        for div in schedule_divs:
            logger.info(f"📍 Знайдено div: {div.get('class')}")
            # Тут можна додати парсинг специфічних структур

        return None

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
        """Нормалізація списку"""
        result = {}
        for item in data:
            if isinstance(item, dict) and 'group' in item:
                try:
                    result[int(item['group'])] = item.get('schedule', [])
                except:
                    continue
        return result if result else None

    @staticmethod
    def save_schedule(group_number: int, date: str, schedule_data: str):
        """Збереження графіку в БД"""
        with get_db() as conn:
            conn.execute("""
                INSERT INTO schedules (group_number, date, schedule_data)
                VALUES (?, ?, ?)
                ON CONFLICT(group_number, date) DO UPDATE SET
                    schedule_data = excluded.schedule_data,
                    updated_at = CURRENT_TIMESTAMP
            """, (group_number, date, schedule_data))
            conn.commit()

    @staticmethod
    def get_schedule(group_number: int, date: str) -> Optional[str]:
        """Отримання графіку з БД"""
        with get_db() as conn:
            row = conn.execute(
                "SELECT schedule_data FROM schedules WHERE group_number = ? AND date = ?",
                (group_number, date)
            ).fetchone()
            return row['schedule_data'] if row else None


# Форматування графіку
def format_schedule(schedule: List[Dict]) -> str:
    """Форматування графіку для відображення"""
    if not schedule:
        return "✅ Наразі планових відключень немає!"

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
def get_main_keyboard() -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton(text="📊 Мій графік", callback_data="my_schedule")],
        [InlineKeyboardButton(text="🔄 Оновити", callback_data="refresh")],
        [InlineKeyboardButton(text="⚙️ Змінити групу", callback_data="change_group")],
        [InlineKeyboardButton(text="🔔 Налаштування", callback_data="settings")],
        [InlineKeyboardButton(text="❓ Допомога", callback_data="help")]
    ]
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

    welcome_text = (
        "👋 Вітаю! Я бот для відстеження графіків відключення світла "
        "в Чернівецькій області.\n\n"
        "🔹 Функції:\n"
        "  • Актуальний графік відключень\n"
        "  • Сповіщення за 30 хв до відключення\n"
        "  • Інформація про зміни в графіку\n\n"
    )

    if user and user.get('group_number'):
        welcome_text = f"👋 З поверненням! Ваша група: {user['group_number']}"
    else:
        welcome_text += "Для початку оберіть свою групу відключень 👇"

    await message.answer(welcome_text, reply_markup=get_main_keyboard())


@router.message(Command("debug"))
async def cmd_debug(message: Message):
    """Відладка - показує стан бота"""
    user = UserManager.get_user(message.from_user.id)

    text = "🔍 Діагностика:\n\n"

    if user:
        text += f"👤 User ID: {user['user_id']}\n"
        text += f"📝 Username: {user.get('username', 'N/A')}\n"
        text += f"⚡️ Група: {user.get('group_number', 'не встановлено')}\n"
        text += f"🔔 Сповіщення: {'✅' if user.get('notifications_enabled') else '❌'}\n\n"

        if user.get('group_number'):
            today = datetime.now().strftime("%Y-%m-%d")
            schedule_data = ScheduleParser.get_schedule(user['group_number'], today)
            text += f"💾 Графік в БД: {'✅ Є' if schedule_data else '❌ Немає'}\n"
    else:
        text += "❌ Користувача не знайдено в БД\n"

    text += f"\n📅 Дата: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    text += f"\n🌐 URL: {SCHEDULE_URL}"

    await message.answer(text)


@router.message(Command("test"))
async def cmd_test(message: Message):
    """Тестове оновлення графіку"""
    await message.answer("⏳ Тестую парсинг...")

    schedules = await ScheduleParser.fetch_schedule()

    if schedules is None:
        await message.answer("❌ Помилка завантаження. Перевірте логи.")
    elif not schedules:
        await message.answer("✅ Графіки порожні (відключень немає)")
    else:
        text = f"✅ Успішно! Знайдено {len(schedules)} груп\n\n"
        text += "Приклад (група 1):\n"
        if 1 in schedules:
            for item in schedules[1][:5]:
                text += f"• {item['time']}: {item['status']}\n"
        await message.answer(text)


@router.callback_query(F.data == "my_schedule")
async def show_schedule(callback: CallbackQuery):
    await callback.answer()

    user = UserManager.get_user(callback.from_user.id)

    if not user or not user.get('group_number'):
        await callback.message.answer(
            "❌ Спочатку оберіть групу відключень",
            reply_markup=get_groups_keyboard()
        )
        return

    group_num = user['group_number']
    today = datetime.now().strftime("%Y-%m-%d")
    schedule_data = ScheduleParser.get_schedule(group_num, today)

    if not schedule_data:
        await callback.message.answer("⏳ Завантажую графік...")
        schedules = await ScheduleParser.fetch_schedule()

        if schedules and group_num in schedules:
            schedule_json = json.dumps(schedules[group_num], ensure_ascii=False)
            ScheduleParser.save_schedule(group_num, today, schedule_json)
            schedule_data = schedule_json
        elif schedules is not None and not schedules:
            schedule_json = json.dumps([], ensure_ascii=False)
            ScheduleParser.save_schedule(group_num, today, schedule_json)
            schedule_data = schedule_json

        if not schedule_data:
            await callback.message.answer(
                f"📊 Графік для групи {group_num} поки недоступний.\n\n"
                "Перевірте на сайті:\n"
                f"{SCHEDULE_URL}",
                reply_markup=get_main_keyboard()
            )
            return

    try:
        schedule = json.loads(schedule_data)
        text = format_schedule(schedule)
        text = f"⚡️ Група {group_num}\n\n" + text

        await callback.message.answer(text, reply_markup=get_main_keyboard())
    except json.JSONDecodeError:
        await callback.message.answer(
            "❌ Помилка читання графіку",
            reply_markup=get_main_keyboard()
        )


@router.callback_query(F.data == "refresh")
async def refresh_schedule(callback: CallbackQuery):
    await callback.answer("⏳ Оновлюю...")

    user = UserManager.get_user(callback.from_user.id)
    if not user or not user.get('group_number'):
        await callback.message.answer("❌ Спочатку оберіть групу")
        return

    schedules = await ScheduleParser.fetch_schedule()
    today = datetime.now().strftime("%Y-%m-%d")
    group_num = user['group_number']

    if schedules and group_num in schedules:
        schedule_json = json.dumps(schedules[group_num], ensure_ascii=False)
        ScheduleParser.save_schedule(group_num, today, schedule_json)

        text = format_schedule(schedules[group_num])
        text = f"⚡️ Група {group_num}\n\n" + text + "\n\n🕐 Оновлено щойно"
        await callback.message.answer(text, reply_markup=get_main_keyboard())
    elif schedules is not None and not schedules:
        await callback.message.answer(
            "✅ Сьогодні відключень немає!\n\n🕐 Оновлено щойно",
            reply_markup=get_main_keyboard()
        )
    else:
        await callback.message.answer(
            "❌ Не вдалося оновити. Спробуйте пізніше.",
            reply_markup=get_main_keyboard()
        )


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
    UserManager.update_group(callback.from_user.id, group_num)

    await callback.message.answer(
        f"✅ Групу {group_num} встановлено!\n\n"
        "Тепер ви будете отримувати сповіщення.",
        reply_markup=get_main_keyboard()
    )


@router.callback_query(F.data == "settings")
async def settings(callback: CallbackQuery):
    await callback.answer()

    user = UserManager.get_user(callback.from_user.id)
    enabled = user.get('notifications_enabled', 1) if user else 1
    status = "✅ Увімкнено" if enabled else "❌ Вимкнено"

    keyboard = [
        [InlineKeyboardButton(
            text="🔕 Вимкнути" if enabled else "🔔 Увімкнути",
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
        "/debug - Діагностика налаштувань\n"
        "/test - Тест парсингу графіку\n\n"
        "🔧 Як дізнатися свою групу?\n"
        f"Перейдіть на сайт:\n{SEARCH_URL}\n\n"
        "Введіть свою адресу і дізнайтесь номер групи.\n\n"
        "💡 Якщо графік не показується:\n"
        "1. Використайте /debug\n"
        "2. Використайте /test для перевірки\n"
        "3. Натисніть 🔄 Оновити\n"
        "4. Перевірте файл debug_page.html (якщо бот локально)"
    )

    await callback.message.answer(help_text, reply_markup=get_main_keyboard())


@router.callback_query(F.data == "back_to_menu")
async def back_to_menu(callback: CallbackQuery):
    await callback.answer()
    await callback.message.answer(
        "Головне меню:",
        reply_markup=get_main_keyboard()
    )


# Scheduled tasks
async def update_schedules(bot: Bot):
    """Оновлення графіків щогодини"""
    logger.info("📅 Оновлення графіків...")

    today = datetime.now().strftime("%Y-%m-%d")

    try:
        schedules = await ScheduleParser.fetch_schedule()

        if schedules is None:
            logger.warning("⚠️ Не вдалося отримати графіки")
            return

        if not schedules:
            logger.info("✅ Графіки порожні - відключень немає")
            # Зберігаємо порожній графік для всіх груп
            for group_num in range(1, 19):
                schedule_json = json.dumps([], ensure_ascii=False)
                ScheduleParser.save_schedule(group_num, today, schedule_json)
        else:
            for group_num, schedule in schedules.items():
                schedule_json = json.dumps(schedule, ensure_ascii=False)
                ScheduleParser.save_schedule(group_num, today, schedule_json)

            logger.info(f"✅ Графіки оновлено: {len(schedules)} груп")

    except Exception as e:
        logger.error(f"❌ Помилка оновлення графіків: {e}", exc_info=True)


async def send_notifications(bot: Bot):
    """Надсилання сповіщень про відключення"""
    logger.info("🔔 Перевірка сповіщень...")

    now = datetime.now()
    target_time = now + timedelta(minutes=30)
    target_hour = target_time.strftime("%H")
    today = now.strftime("%Y-%m-%d")

    with get_db() as conn:
        schedules = conn.execute(
            "SELECT DISTINCT group_number FROM schedules WHERE date = ?",
            (today,)
        ).fetchall()

    for row in schedules:
        group_num = row['group_number']
        users = UserManager.get_users_by_group(group_num)

        if not users:
            continue

        schedule_json = ScheduleParser.get_schedule(group_num, today)
        if not schedule_json:
            continue

        try:
            schedule = json.loads(schedule_json)

            if not schedule:
                continue

            # Шукаємо відключення через 30 хв
            for item in schedule:
                if item['status'] == 'off' and target_hour in item['time']:
                    for user in users:
                        try:
                            await bot.send_message(
                                user['user_id'],
                                f"⚠️ Увага! Через 30 хвилин (о {item['time']}) "
                                f"очікується відключення світла у групі {group_num}"
                            )
                            logger.info(f"✉️ Сповіщення надіслано користувачу {user['user_id']}")
                        except Exception as e:
                            logger.error(f"❌ Помилка надсилання: {e}")
        except json.JSONDecodeError:
            logger.error(f"❌ Помилка декодування JSON для групи {group_num}")
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
    logger.info(f"📍 URL для парсингу: {SCHEDULE_URL}")
    logger.info("📝 Використайте /test для перевірки парсингу")

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())