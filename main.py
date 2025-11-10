import asyncio
import logging
import os
import re
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
from contextlib import contextmanager
import json
import sqlite3
from urllib.parse import urljoin
import aiohttp
from bs4 import BeautifulSoup

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from aiogram.client.default import DefaultBotProperties

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфігурація
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    logger.warning("BOT_TOKEN не встановлено у змінних середовища. Використовується заглушка.")
    BOT_TOKEN = "8264057355:AAGgZMq5_2lNJRs5JV8KudlPyiQv6iKj_Sk"

DATABASE_PATH = "bot_database.db"

# ОНОВЛЕНО: Тільки Хмельницький та Кам'янець-Подільський
CITIES: Dict[str, Dict[str, Any]] = {
    "khmelnytskyi": {
        "name": "Хмельницький",
        "schedule_url": "https://hoe.com.ua/page/pogodinni-vidkljuchennja",
        "parser_type": "image_based"
    },
    "kamyanets": {
        "name": "Кам'янець-Подільський",
        "schedule_url": "https://hoe.com.ua/page/pogodinni-vidkljuchennja",
        "parser_type": "image_based"
    }
}


# FSM стани
class UserStates(StatesGroup):
    waiting_for_confirmation = State()


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
                city TEXT DEFAULT 'khmelnytskyi',
                notifications_enabled INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
                notification_type TEXT,
                sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()


# Користувачі
class UserManager:
    @staticmethod
    def save_user(user_id: int, username: str, city: str = "khmelnytskyi"):
        """Збереження або оновлення користувача"""
        with get_db() as conn:
            conn.execute("""
                INSERT INTO users (user_id, username, city)
                VALUES (?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    username = excluded.username,
                    city = excluded.city 
            """, (user_id, username, city))
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
    def get_user(user_id: int) -> Optional[Dict]:
        with get_db() as conn:
            row = conn.execute(
                "SELECT * FROM users WHERE user_id = ?", (user_id,)
            ).fetchone()
            return dict(row) if row else None

    @staticmethod
    def get_users_by_city(city: str) -> List[Dict]:
        """Отримання всіх користувачів міста з увімкненими сповіщеннями"""
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


# Парсер графіків
class ScheduleParser:
    @staticmethod
    async def fetch_image_schedule(city: str) -> Optional[str]:
        """ВИПРАВЛЕНО: Парсинг URL зображення графіку"""
        try:
            city_data = CITIES.get(city)
            if not city_data:
                logger.error(f"Місто {city} не знайдено в конфігурації")
                return None

            url = city_data['schedule_url']
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Cache-Control': 'no-cache'
            }

            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=30) as response:
                    if response.status != 200:
                        logger.warning(f"[{city}] Не вдалося завантажити сторінку (статус {response.status})")
                        return None

                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')

                    # Шукаємо зображення з графіком (розширений пошук)
                    images = soup.find_all('img', src=re.compile(
                        r'(grafik|schedule|vidkljuch|vidkl|pogod|відкл|графік|jpg|jpeg|png)',
                        re.I
                    ))

                    if not images:
                        logger.warning(f"[{city}] Зображення графіку не знайдено на сторінці")
                        return None

                    img_url = images[0].get('src')

                    # Формуємо повний URL
                    if not img_url.startswith('http'):
                        img_url = urljoin(url, img_url)

                    logger.info(f"[{city}] Знайдено зображення: {img_url}")
                    return img_url

        except Exception as e:
            logger.error(f"[{city}] Критична помилка парсингу: {e}", exc_info=True)
            return None

    @staticmethod
    def save_image_url(city: str, image_url: str):
        """Збереження URL зображення"""
        with get_db() as conn:
            conn.execute("""
                INSERT INTO image_schedules (city, image_url, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(city) DO UPDATE SET
                    image_url = excluded.image_url,
                    updated_at = CURRENT_TIMESTAMP
            """, (city, image_url))
            conn.commit()
            logger.info(f"[{city}] URL збережено в БД: {image_url}")

    @staticmethod
    def get_image_url(city: str) -> Optional[str]:
        """Отримання збереженого URL зображення"""
        with get_db() as conn:
            row = conn.execute(
                "SELECT image_url FROM image_schedules WHERE city = ?", (city,)
            ).fetchone()
            return row['image_url'] if row else None

    @staticmethod
    def get_last_update_time(city: str) -> Optional[str]:
        """Отримання часу останнього оновлення"""
        with get_db() as conn:
            row = conn.execute(
                "SELECT updated_at FROM image_schedules WHERE city = ?", (city,)
            ).fetchone()
            return row['updated_at'] if row else None


# Клавіатури
def get_main_keyboard(user_city: str = "khmelnytskyi") -> InlineKeyboardMarkup:
    city_name = CITIES.get(user_city, {}).get('name', 'Хмельницький')
    keyboard = [
        [InlineKeyboardButton(text="📊 Переглянути графік", callback_data="view_schedule")],
        [InlineKeyboardButton(text="🔄 Оновити графік", callback_data="refresh_schedule")],
        [InlineKeyboardButton(text=f"🏙 Місто: {city_name}", callback_data="change_city")],
        [InlineKeyboardButton(text="🔔 Налаштування сповіщень", callback_data="settings")],
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


# Бот
router = Router()


@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    user = UserManager.get_user(message.from_user.id)

    if not user:
        UserManager.save_user(message.from_user.id, message.from_user.username or "Unknown")
        user = UserManager.get_user(message.from_user.id)

    city = user.get('city', 'khmelnytskyi')
    city_name = CITIES.get(city, {}).get('name', 'Хмельницький')

    welcome_text = (
        "👋 Вітаю! Я бот для відстеження графіків відключення світла.\n\n"
        f"🏙 Ваше місто: **{city_name}**\n\n"
        "🔹 Я буду надсилати вам сповіщення, коли графік відключень оновиться на сайті облпостачальника.\n\n"
        "📷 Графік відображається у форматі зображення з офіційного сайту.\n\n"
        "Натисніть '📊 Переглянути графік' щоб побачити актуальну інформацію 👇"
    )

    await message.answer(welcome_text, reply_markup=get_main_keyboard(city))


@router.message(Command("debug"))
async def cmd_debug(message: Message):
    """Команда для відладки"""
    user = UserManager.get_user(message.from_user.id)
    if not user:
        await message.answer("❌ Користувача не знайдено в базі даних")
        return

    city = user.get('city', 'N/A')
    city_name = CITIES.get(city, {}).get('name', 'N/A')

    debug_text = "🔍 Ваші дані в системі:\n\n"
    debug_text += f"User ID: {user['user_id']}\n"
    debug_text += f"Username: {user.get('username', 'N/A')}\n"
    debug_text += f"Місто: {city_name} ({city})\n"
    debug_text += f"Сповіщення: {'✅ Увімкнено' if user.get('notifications_enabled') else '❌ Вимкнено'}\n\n"

    # Інформація про збережений графік
    saved_url = ScheduleParser.get_image_url(city)
    last_update = ScheduleParser.get_last_update_time(city)

    if saved_url:
        debug_text += f"📷 Графік у БД: Так\n"
        debug_text += f"🕐 Останнє оновлення: {last_update}\n"
    else:
        debug_text += f"📷 Графік у БД: Немає\n"

    await message.answer(debug_text)


@router.message(Command("test"))
async def cmd_test(message: Message):
    """Тестове оновлення для конкретного міста"""
    user = UserManager.get_user(message.from_user.id)
    city = user.get('city', 'khmelnytskyi') if user else 'khmelnytskyi'
    city_name = CITIES[city]['name']

    await message.answer(f"⏳ Тестую парсинг для {city_name}...")

    image_url = await ScheduleParser.fetch_image_schedule(city)

    if image_url:
        await message.answer(
            f"✅ Успішно!\n\n🏙 {city_name}\n📷 Зображення знайдено:\n{image_url}",
            disable_web_page_preview=False
        )
    else:
        await message.answer(f"❌ Помилка завантаження для {city_name}")


@router.callback_query(F.data == "view_schedule")
async def view_schedule(callback: CallbackQuery):
    await callback.answer()

    user = UserManager.get_user(callback.from_user.id)
    if not user:
        UserManager.save_user(callback.from_user.id, callback.from_user.username or "Unknown")
        user = UserManager.get_user(callback.from_user.id)

    city = user.get('city', 'khmelnytskyi')
    city_name = CITIES[city]['name']

    # Спочатку перевіряємо збережений графік
    saved_url = ScheduleParser.get_image_url(city)
    last_update = ScheduleParser.get_last_update_time(city)

    if saved_url:
        caption = f"📊 Графік відключень\n🏙 {city_name}"
        if last_update:
            caption += f"\n🕐 Оновлено: {last_update.split('.')[0]}"

        try:
            await callback.message.answer_photo(
                photo=saved_url,
                caption=caption,
                reply_markup=get_main_keyboard(city)
            )
            return
        except Exception as e:
            logger.error(f"Помилка відправки збереженого зображення: {e}")

    # Якщо немає збереженого графіку, завантажуємо новий
    await callback.message.answer("⏳ Завантажую актуальний графік...")

    image_url = await ScheduleParser.fetch_image_schedule(city)

    if image_url:
        ScheduleParser.save_image_url(city, image_url)

        caption = f"📊 Графік відключень\n🏙 {city_name}\n🕐 Оновлено щойно"

        try:
            await callback.message.answer_photo(
                photo=image_url,
                caption=caption,
                reply_markup=get_main_keyboard(city)
            )
        except Exception as e:
            logger.error(f"Помилка відправки зображення: {e}")
            await callback.message.answer(
                f"❌ Не вдалося завантажити зображення.\n\n"
                f"Перегляньте графік на сайті: {CITIES[city]['schedule_url']}",
                reply_markup=get_main_keyboard(city)
            )
    else:
        await callback.message.answer(
            f"❌ Не вдалося отримати графік.\n\n"
            f"Перегляньте його на сайті: {CITIES[city]['schedule_url']}",
            reply_markup=get_main_keyboard(city)
        )


@router.callback_query(F.data == "refresh_schedule")
async def refresh_schedule(callback: CallbackQuery):
    await callback.answer("🔄 Оновлюю...")

    user = UserManager.get_user(callback.from_user.id)
    city = user.get('city', 'khmelnytskyi') if user else 'khmelnytskyi'
    city_name = CITIES[city]['name']

    image_url = await ScheduleParser.fetch_image_schedule(city)

    if image_url:
        ScheduleParser.save_image_url(city, image_url)

        caption = f"✅ Графік оновлено!\n🏙 {city_name}\n🕐 Оновлено щойно"

        try:
            await callback.message.answer_photo(
                photo=image_url,
                caption=caption,
                reply_markup=get_main_keyboard(city)
            )
        except Exception as e:
            logger.error(f"Помилка відправки зображення: {e}")
            await callback.message.answer(
                f"✅ Графік оновлено, але не вдалося відобразити зображення.\n\n"
                f"Перегляньте на сайті: {CITIES[city]['schedule_url']}",
                reply_markup=get_main_keyboard(city)
            )
    else:
        await callback.message.answer(
            f"❌ Не вдалося оновити графік.\n\nСпробуйте пізніше.",
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
        f"Тепер ви отримуватимете сповіщення про оновлення графіку для цього міста.",
        reply_markup=get_main_keyboard(city_id)
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
        f"⚙️ Налаштування сповіщень\n\n"
        f"Статус: {status}\n\n"
        f"ℹ️ Коли сповіщення увімкнено, ви отримуватимете повідомлення кожного разу, "
        f"коли графік відключень оновиться на сайті.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
    )


@router.callback_query(F.data == "toggle_notifications")
async def toggle_notif(callback: CallbackQuery):
    enabled = UserManager.toggle_notifications(callback.from_user.id)
    status = "увімкнено ✅" if enabled else "вимкнено ❌"
    await callback.answer(f"Сповіщення {status}")
    await settings(callback)


@router.callback_query(F.data == "help")
async def show_help(callback: CallbackQuery):
    await callback.answer()

    user = UserManager.get_user(callback.from_user.id)
    city = user.get('city', 'khmelnytskyi') if user else 'khmelnytskyi'

    help_text = (
        "❓ **Допомога**\n\n"
        "📋 **Команди:**\n"
        "/start - Головне меню\n"
        "/debug - Показати збережені налаштування\n"
        "/test - Тестування парсингу графіку\n\n"
        "🏙 **Доступні міста:**\n"
        "  • Хмельницький\n"
        "  • Кам'янець-Подільський\n\n"
        "📷 **Як це працює:**\n"
        "Бот автоматично перевіряє сайт облпостачальника кожну годину. "
        "Коли графік оновлюється (з'являється нове зображення), ви отримуєте сповіщення.\n\n"
        "🔧 **Якщо графік не показується:**\n"
        "1. Використайте /debug для перевірки налаштувань\n"
        "2. Спробуйте /test для тестування парсингу\n"
        "3. Натисніть '🔄 Оновити графік' для примусового оновлення\n"
        "4. Переоберіть місто через меню\n\n"
        "💡 **Джерело даних:**\n"
        "Хмельницькобленерго - hoe.com.ua"
    )

    await callback.message.answer(help_text, reply_markup=get_main_keyboard(city))


@router.callback_query(F.data == "back_to_menu")
async def back_to_menu(callback: CallbackQuery):
    await callback.answer()
    user = UserManager.get_user(callback.from_user.id)
    city = user.get('city', 'khmelnytskyi') if user else 'khmelnytskyi'
    await callback.message.answer("🏠 Головне меню:", reply_markup=get_main_keyboard(city))


# ВИПРАВЛЕНА ФУНКЦІЯ: Перевірка та сповіщення про оновлення графіків
async def check_and_notify_updates(bot: Bot):
    """Перевірка оновлень графіків та надсилання сповіщень"""
    logger.info("🔍 Перевірка оновлень графіків...")

    for city_id, city_data in CITIES.items():
        city_name = city_data['name']

        try:
            # 1. ОТРИМУЄМО СТАРИЙ URL (ДО парсингу)
            old_url = ScheduleParser.get_image_url(city_id)
            logger.info(f"[{city_id}] Старий URL в БД: {old_url}")

            # 2. ПАРСИМО НОВИЙ URL (без збереження)
            new_url = await ScheduleParser.fetch_image_schedule(city_id)
            logger.info(f"[{city_id}] Новий URL з сайту: {new_url}")

            if not new_url:
                logger.warning(f"[{city_id}] Не вдалося отримати новий URL")
                continue

            # 3. ПОРІВНЮЄМО
            if old_url and old_url != new_url:
                logger.info(f"[{city_id}] 🔥 ЗНАЙДЕНО ЗМІНУ ГРАФІКУ!")
                logger.info(f"[{city_id}] Старий: {old_url}")
                logger.info(f"[{city_id}] Новий: {new_url}")

                # 4. ЗБЕРІГАЄМО НОВИЙ URL
                ScheduleParser.save_image_url(city_id, new_url)

                # 5. НАДСИЛАЄМО СПОВІЩЕННЯ
                users = UserManager.get_users_by_city(city_id)

                if not users:
                    logger.info(f"[{city_id}] Немає користувачів для сповіщення")
                    continue

                caption = (
                    f"⚠️ **ОНОВЛЕННЯ ГРАФІКУ!**\n\n"
                    f"🏙 {city_name}\n\n"
                    f"Графік відключень оновлено на сайті облпостачальника.\n"
                    f"Перегляньте актуальну інформацію на зображенні 👇"
                )

                success_count = 0
                for user in users:
                    try:
                        await bot.send_photo(
                            user['user_id'],
                            photo=new_url,
                            caption=caption
                        )
                        success_count += 1
                        logger.info(f"[{city_id}] ✅ Сповіщення надіслано користувачу {user['user_id']}")
                    except Exception as e:
                        logger.error(f"[{city_id}] ❌ Помилка надсилання користувачу {user['user_id']}: {e}")

                logger.info(f"[{city_id}] 📤 Надіслано {success_count}/{len(users)} сповіщень")

            elif not old_url:
                # Перше збереження (не надсилаємо сповіщення)
                logger.info(f"[{city_id}] Перше збереження графіку")
                ScheduleParser.save_image_url(city_id, new_url)

            else:
                # URL не змінився
                logger.info(f"[{city_id}] ✅ Графік без змін")

        except Exception as e:
            logger.error(f"[{city_id}] ❌ Помилка перевірки оновлень: {e}", exc_info=True)

    logger.info("✅ Перевірка оновлень завершена")


async def main():
    logger.info("🚀 Запуск бота...")

    init_db()

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode='Markdown')
    )
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    # Scheduler
    scheduler = AsyncIOScheduler()

    # Перевірка оновлень графіків кожну годину (о 5 хвилині)
    scheduler.add_job(
        check_and_notify_updates,
        CronTrigger(hour="*", minute="5"),
        args=[bot]
    )

    scheduler.start()
    logger.info("⏰ Scheduler запущено (перевірка щогодини о XX:05)")

    # Перше оновлення
    logger.info("📥 Виконую перше завантаження графіків...")
    try:
        await check_and_notify_updates(bot)
        logger.info("✅ Перше завантаження завершено")
    except Exception as e:
        logger.error(f"❌ Помилка при першому завантаженні: {e}", exc_info=True)

    logger.info("✅ Бот запущено!")
    logger.info(f"🏙 Підтримка міст: {', '.join([c['name'] for c in CITIES.values()])}")

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())