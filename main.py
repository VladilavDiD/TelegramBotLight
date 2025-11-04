
import asyncio
import logging
import os
from datetime import datetime, timedelta
from typing import Optional, Dict, List

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import aiohttp
from bs4 import BeautifulSoup
import sqlite3
from contextlib import contextmanager
import json

# Налаштування логування
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфігурація
BOT_TOKEN = os.getenv("BOT_TOKEN", "8264057355:AAGgZMq5_2lNJRs5JV8KudlPyiQv6iKj_Sk")
DATABASE_PATH = "bot_database.db"

# Міста та їх URL
CITIES = {
    "chernivtsi": {
        "name": "Чернівці",
        "schedule_url": "https://oblenergo.cv.ua/shutdowns/",
        "search_url": "https://oblenergo.cv.ua/shutdowns-search/"
    },
    "kyiv": {
        "name": "Київ",
        "schedule_url": "https://www.dtek-kem.com.ua/ua/shutdowns",
        "search_url": "https://www.dtek-kem.com.ua.ua/ua/shutdowns"
    },
    "khmelnytskyi": {
        "name": "Хмельницький",
        "schedule_url": "https://www.oe.km.ua/spozhyvacham/grafiky-vidklyuchen/",
        "search_url": "https://www.oe.km.ua/spozhyvacham/grafiky-vidklyuchen/"
    },
    "kamyanets": {
        "name": "Кам'янець-Подільський",
        "schedule_url": "https://www.oe.km.ua/spozhyvacham/grafiky-vidklyuchen/",
        "search_url": "https://www.oe.km.ua/spozhyvacham/grafiky-vidklyuchen/"
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
            # Оновлюємо основну групу
            conn.execute(
                "UPDATE users SET group_number = ? WHERE user_id = ?",
                (group_number, user_id)
            )

            # Додаємо/оновлюємо групу для поточного міста
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


# Парсер графіків
class ScheduleParser:
    @staticmethod
    async def fetch_schedule(city: str = "chernivtsi") -> Optional[Dict[int, List[Dict]]]:
        """Парсинг графіків з сайту"""
        try:
            city_data = CITIES.get(city)
            if not city_data:
                logger.error(f"Місто {city} не знайдено в конфігурації")
                return None

            url = city_data['schedule_url']

            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=30) as response:
                    if response.status != 200:
                        logger.error(f"Помилка при завантаженні графіку для {city}: {response.status}")
                        return None

                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')

                    schedule_data = {}

                    # Знаходимо таблицю
                    table = soup.find('table', class_='shutdowns-table') or \
                            soup.find('table') or \
                            soup.find('div', class_='schedule-table')

                    if not table:
                        logger.warning(f"Таблицю з графіками не знайдено для {city}")
                        # Повертаємо порожній графік замість None
                        return {}

                    # Парсинг заголовків
                    headers = []
                    header_row = table.find('thead') or table.find('tr')
                    if header_row:
                        for th in header_row.find_all(['th', 'td'])[1:]:  # Пропускаємо першу колонку
                            text = th.get_text(strip=True)
                            if text and (':' in text or '-' in text):
                                headers.append(text)

                    # Парсинг даних по групах
                    tbody = table.find('tbody') or table
                    rows = tbody.find_all('tr')[1:] if tbody == table else tbody.find_all('tr')

                    for row in rows:
                        cells = row.find_all(['td', 'th'])
                        if len(cells) < 2:
                            continue

                        # Перша комірка - номер групи
                        group_text = cells[0].get_text(strip=True)
                        try:
                            # Витягуємо число з тексту
                            group_num = int(''.join(filter(str.isdigit, group_text)))
                            if group_num < 1 or group_num > 20:
                                continue
                        except (ValueError, AttributeError):
                            continue

                        schedule_data[group_num] = []

                        # Решта комірок - статус
                        for i, cell in enumerate(cells[1:]):
                            if i >= len(headers):
                                break

                            status = 'on'
                            style = cell.get('style', '').lower()
                            classes = ' '.join(cell.get('class', [])).lower()
                            text = cell.get_text(strip=True).lower()

                            # Визначаємо статус
                            if any(word in style or word in classes for word in ['red', '#ff0000', '#f00', 'danger']):
                                status = 'off'
                            elif any(word in style or word in classes for word in ['green', '#00ff00', '#0f0', 'success']):
                                status = 'on'
                            elif any \
                                    (word in style or word in classes for word in ['gray', 'grey', 'yellow', 'warning']):
                                status = 'maybe'
                            elif 'відключення' in text or 'немає' in text:
                                status = 'off'
                            elif 'можливо' in text:
                                status = 'maybe'

                            schedule_data[group_num].append({
                                'time': headers[i] if i < len(headers) else f"{ i *2:02d}:00-{( i * 2 +2):02d}:00",
                                'status': status
                            })

                    if schedule_data:
                        logger.info(f"Успішно спарсено графіки для {city}: {len(schedule_data)} груп")
                    else:
                        logger.info(f"Графіки для {city} порожні - відключень немає")
                        schedule_data = {}  # Порожній словник означає відсутність відключень

                    return schedule_data

        except Exception as e:
            logger.error(f"Помилка парсингу графіку для {city}: {e}", exc_info=True)
            return None

    @staticmethod
    def save_schedule(city: str, group_number: int, date: str, schedule_data: str):
        """Збереження графіку в БД"""
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
        """Отримання графіку з БД"""
        with get_db() as conn:
            row = conn.execute(
                "SELECT schedule_data FROM schedules WHERE city = ? AND group_number = ? AND date = ?",
                (city, group_number, date)
            ).fetchone()
            return row['schedule_data'] if row else None


# Форматування графіку
def format_schedule(schedule: List[Dict]) -> str:
    """Форматування графіку для відображення"""
    if not schedule:
        return "✅ Наразі планових відключень немає!\n\nГрафік порожній або не оновлювався."

    # Перевіряємо чи є хоч одне відключення
    has_outages = any(item['status'] == 'off' for item in schedule)

    if not has_outages:
        return "✅ Чудові новини! Сьогодні планових відключень для вашої групи немає!"

    text = "📊 Графік відключень на сьогодні:\n\n"

    for item in schedule:
        time = item['time']
        status = item['status']

        if status == 'off':
            emoji = "🔴"
            status_text = "Відключення"
        elif status == 'on':
            emoji = "🟢"
            status_text = "Світло є"
        else:
            emoji = "⚪"
            status_text = "Можливо"

        text += f"{emoji} {time} - {status_text}\n"

    text += "\n🔴 - гарантоване відключення\n"
    text += "🟢 - гарантоване включення\n"
    text += "⚪ - можливе включення\n"

    return text


# Клавіатури
def get_main_keyboard(user_city: str = "chernivtsi") -> InlineKeyboardMarkup:
    city_name = CITIES.get(user_city, {}).get('name', 'Чернівці')
    keyboard = [
        [InlineKeyboardButton(text="📊 Мій графік", callback_data="my_schedule")],
        [InlineKeyboardButton(text=f"🏙 Місто: {city_name}", callback_data="change_city")],
        [InlineKeyboardButton(text="⚙️ Змінити групу", callback_data="change_group")],
        [InlineKeyboardButton(text="🔔 Налаштування", callback_data="settings")],
        [InlineKeyboardButton(text="❓ Допомога", callback_data="help")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def get_cities_keyboard() -> InlineKeyboardMarkup:
    keyboard = []
    for city_id, city_data in CITIES.items():
        keyboard.append([InlineKeyboardButton(
            text=city_data['name'],
            callback_data=f"city_{city_id}"
        )])
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

    await message.answer(welcome_text, reply_markup=get_main_keyboard(user.get('city', 'chernivtsi') if user else 'chernivtsi'))


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
    city_name = CITIES.get(city, {}).get('name', 'Чернівці')

    # Отримуємо графік
    today = datetime.now().strftime("%Y-%m-%d")
    schedule_data = ScheduleParser.get_schedule(city, group_num, today)

    if not schedule_data:
        await callback.message.answer(
            f"📊 Графік для {city_name}, група {group_num} поки недоступний.\n\n"
            "Можливо:\n"
            "• Графік ще не оновлено\n"
            "• Наразі відключень немає\n"
            "• Проблеми з сайтом енергопостачальника\n\n"
            f"Перевірте на сайті:\n{CITIES[city]['schedule_url']}"
        )
        return

    # Парсимо і форматуємо
    try:
        schedule = json.loads(schedule_data)
        text = format_schedule(schedule)
        text = f"🏙 {city_name}\n⚡️ Група {group_num}\n\n" + text
        await callback.message.answer(text, reply_markup=get_main_keyboard(city))
    except json.JSONDecodeError:
        await callback.message.answer(
            "❌ Помилка читання графіку. Спробуйте пізніше.",
            reply_markup=get_main_keyboard(city)
        )


@router.callback_query(F.data == "change_city")
async def change_city(callback: CallbackQuery):
    await callback.answer()
    await callback.message.answer(
        "🏙 Оберіть місто:",
        reply_markup=get_cities_keyboard()
    )


@router.callback_query(F.data.startswith("city_"))
async def select_city(callback: CallbackQuery):
    await callback.answer()

    city_id = callback.data.split("_")[1]
    city_name = CITIES.get(city_id, {}).get('name', 'Невідоме місто')

    UserManager.update_city(callback.from_user.id, city_id)

    await callback.message.answer(
        f"✅ Місто {city_name} встановлено!\n\n"
        "Тепер оберіть групу відключень:",
        reply_markup=get_groups_keyboard()
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
    user = UserManager.get_user(callback.from_user.id)
    city = user.get('city', 'chernivtsi') if user else 'chernivtsi'
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
        "Команди:\n"
        "/start - Головне меню\n\n"
        "🏙 Доступні міста:\n"
    )

    for city_data in CITIES.values():
        help_text += f"  • {city_data['name']}\n"

    help_text += (
        "\nЯк дізнатися свою групу?\n"
        "Перейдіть на сайт енергопостачальника\n"
        "вашого міста та введіть свою адресу."
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
    """Оновлення графіків щогодини"""
    logger.info("Оновлення графіків...")

    today = datetime.now().strftime("%Y-%m-%d")

    for city_id, city_data in CITIES.items():
        try:
            schedules = await ScheduleParser.fetch_schedule(city_id)

            if schedules is None:
                logger.warning(f"Не вдалося отримати графіки для {city_data['name']}")
                continue

            if not schedules:
                logger.info(f"Графіки для {city_data['name']} порожні - відключень немає")
                # Зберігаємо порожній графік для всіх груп
                for group_num in range(1, 19):
                    schedule_json = json.dumps([], ensure_ascii=False)
                    ScheduleParser.save_schedule(city_id, group_num, today, schedule_json)
            else:
                for group_num, schedule in schedules.items():
                    schedule_json = json.dumps(schedule, ensure_ascii=False)
                    ScheduleParser.save_schedule(city_id, group_num, today, schedule_json)

                logger.info(f"Графіки оновлено для {city_data['name']}: {len(schedules)} груп")

        except Exception as e:
            logger.error(f"Помилка оновлення графіків для {city_data['name']}: {e}", exc_info=True)


async def send_notifications(bot: Bot):
    """Надсилання сповіщень про відключення"""
    logger.info("Перевірка сповіщень...")

    now = datetime.now()
    target_time = now + timedelta(minutes=30)
    target_hour = target_time.strftime("%H")
    today = now.strftime("%Y-%m-%d")

    for city_id, city_data in CITIES.items():
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

                # Якщо графік порожній - відключень немає
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
                                    f"очікується відключення світла\n\n"
                                    f"🏙 {city_data['name']}\n"
                                    f"⚡️ Група {group_num}"
                                )
                            except Exception as e:
                                logger.error(f"Помилка надсилання повідомлення користувачу {user['user_id']}: {e}")
            except json.JSONDecodeError:
                logger.error(f"Помилка декодування JSON для {city_id}, група {group_num}")
                continue


async def main():
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

    # Перше оновлення
    await update_schedules(bot)

    logger.info("Бот запущено!")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())