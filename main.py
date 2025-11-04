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

# Налаштування логування
logging.basicConfig(level=logging.INFO)
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


# Парсер графіків
class ScheduleParser:
    @staticmethod
    async def fetch_schedule() -> Optional[Dict[int, List[Dict]]]:
        """Парсинг графіків з сайту Чернівціобленерго"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(SCHEDULE_URL, timeout=30) as response:
                    if response.status != 200:
                        logger.error(f"Помилка при завантаженні графіку: {response.status}")
                        return None

                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')

                    # Парсинг таблиці з графіками
                    schedule_data = {}

                    # Знаходимо таблицю з графіками
                    table = soup.find('table') or soup.find('div', class_='schedule-table')

                    if not table:
                        logger.warning("Таблицю з графіками не знайдено")
                        return None

                    # Парсинг заголовків (часові інтервали)
                    headers = []
                    header_row = table.find('tr')
                    if header_row:
                        for th in header_row.find_all(['th', 'td']):
                            text = th.get_text(strip=True)
                            if ':' in text:  # Це часовий інтервал
                                headers.append(text)

                    # Парсинг даних по групах
                    rows = table.find_all('tr')[1:]  # Пропускаємо заголовок

                    for row in rows:
                        cells = row.find_all(['td', 'th'])
                        if len(cells) < 2:
                            continue

                        # Перша комірка - номер групи
                        group_text = cells[0].get_text(strip=True)
                        if 'Група' in group_text or 'група' in group_text:
                            try:
                                group_num = int(''.join(filter(str.isdigit, group_text)))
                            except ValueError:
                                continue

                            schedule_data[group_num] = []

                            # Решта комірок - статус відключень
                            for i, cell in enumerate(cells[1:]):
                                if i >= len(headers):
                                    break

                                status = 'on'  # За замовчуванням світло є

                                # Перевіряємо колір/клас комірки
                                style = cell.get('style', '')
                                classes = ' '.join(cell.get('class', []))

                                if 'red' in style or 'red' in classes or 'background-color: red' in style:
                                    status = 'off'  # Відключення
                                elif 'green' in style or 'green' in classes or 'background-color: green' in style:
                                    status = 'on'  # Гарантоване включення
                                elif 'gray' in style or 'gray' in classes or 'grey' in classes:
                                    status = 'maybe'  # Можливе включення

                                schedule_data[group_num].append({
                                    'time': headers[i],
                                    'status': status
                                })

                    if schedule_data:
                        logger.info(f"Успішно спарсено графіки для {len(schedule_data)} груп")
                        return schedule_data
                    else:
                        logger.warning("Не вдалося спарсити дані з таблиці")
                        return None

        except Exception as e:
            logger.error(f"Помилка парсингу графіку: {e}")
            return None

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
        return "📊 Графік не знайдено"

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
def get_main_keyboard() -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton(text="📊 Мій графік", callback_data="my_schedule")],
        [InlineKeyboardButton(text="⚙️ Змінити групу", callback_data="change_group")],
        [InlineKeyboardButton(text="🔔 Налаштування", callback_data="settings")],
        [InlineKeyboardButton(text="❓ Допомога", callback_data="help")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def get_groups_keyboard() -> InlineKeyboardMarkup:
    keyboard = []
    for i in range(1, 13):
        row = []
        for j in range(3):
            group_num = i + j * 12
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
        "🔹 Я буду надсилати вам:\n"
        "  • Актуальний графік відключень\n"
        "  • Сповіщення за 30 хв до відключення\n"
        "  • Інформацію про зміни в графіку\n\n"
        "Для початку оберіть свою групу відключень 👇"
    )

    if user and user.get('group_number'):
        welcome_text = f"👋 З поверненням! Ваша група: {user['group_number']}"

    await message.answer(welcome_text, reply_markup=get_main_keyboard())


@router.callback_query(F.data == "my_schedule")
async def show_schedule(callback: CallbackQuery):
    await callback.answer()

    user = UserManager.get_user(callback.from_user.id)

    if not user or not user.get('group_number'):
        await callback.message.answer(
            "❌ Спочатку оберіть свою групу відключень",
            reply_markup=get_groups_keyboard()
        )
        return

    group_num = user['group_number']

    # Отримуємо графік
    today = datetime.now().strftime("%Y-%m-%d")
    schedule_data = ScheduleParser.get_schedule(group_num, today)

    if not schedule_data:
        await callback.message.answer(
            f"📊 Графік для групи {group_num} поки недоступний.\n"
            "Спробуйте пізніше або перевірте на сайті:\n"
            "https://oblenergo.cv.ua/shutdowns/"
        )
        return

    # Парсимо і форматуємо
    import json
    schedule = json.loads(schedule_data)
    text = format_schedule(schedule)
    text = f"Група {group_num}\n\n" + text

    await callback.message.answer(text, reply_markup=get_main_keyboard())


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
        "Тепер ви будете отримувати сповіщення про відключення.",
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
        "Як дізнатися свою групу?\n"
        "Перейдіть на сайт Чернівціобленерго:\n"
        "https://oblenergo.cv.ua/shutdowns-search/\n\n"
        "Введіть свою адресу і дізнайтесь номер групи."
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
    logger.info("Оновлення графіків...")

    schedules = await ScheduleParser.fetch_schedule()

    if schedules:
        today = datetime.now().strftime("%Y-%m-%d")
        import json

        for group_num, schedule in schedules.items():
            schedule_json = json.dumps(schedule, ensure_ascii=False)
            ScheduleParser.save_schedule(group_num, today, schedule_json)

        logger.info(f"Графіки оновлено для {len(schedules)} груп")


async def send_notifications(bot: Bot):
    """Надсилання сповіщень про відключення"""
    logger.info("Перевірка сповіщень...")

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

        import json
        schedule = json.loads(schedule_json)

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
                    except Exception as e:
                        logger.error(f"Помилка надсилання: {e}")


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