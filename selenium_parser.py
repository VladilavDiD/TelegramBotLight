"""
Альтернативний парсер через Selenium
Використовуйте якщо сайт генерує таблицю через JavaScript

Встановлення:
pip install selenium webdriver-manager
"""

import logging
from typing import Optional, Dict, List
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
import time

logger = logging.getLogger(__name__)


class SeleniumScheduleParser:
    """Парсер з використанням Selenium для JavaScript рендерингу"""

    def __init__(self):
        self.driver = None

    def _init_driver(self):
        """Ініціалізація Chrome драйвера"""
        if self.driver:
            return

        options = Options()
        options.add_argument('--headless')  # Без вікна
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')

        # Автоматичне завантаження ChromeDriver
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)
        logger.info("✅ Selenium драйвер ініціалізовано")

    def close(self):
        """Закриття драйвера"""
        if self.driver:
            self.driver.quit()
            self.driver = None
            logger.info("🔒 Selenium драйвер закрито")

    def fetch_schedule(self, url: str = "https://oblenergo.cv.ua/shutdowns/") -> Optional[Dict[int, List[Dict]]]:
        """
        Завантаження та парсинг графіку через Selenium
        """
        try:
            self._init_driver()

            logger.info(f"🌐 Завантаження {url}")
            self.driver.get(url)

            # Чекаємо поки завантажиться таблиця (максимум 15 сек)
            try:
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.TAG_NAME, "table"))
                )
                logger.info("✅ Таблиця завантажена")
            except:
                logger.warning("⚠️ Таймаут очікування таблиці")

            # Додатково чекаємо поки JavaScript виконається
            time.sleep(3)

            # Отримуємо HTML після виконання JavaScript
            html = self.driver.page_source

            # Зберігаємо для відладки
            with open('debug_selenium.html', 'w', encoding='utf-8') as f:
                f.write(html)
            logger.info("💾 HTML збережено у debug_selenium.html")

            # Парсимо HTML
            soup = BeautifulSoup(html, 'html.parser')

            # Спробуємо різні методи парсингу
            schedule_data = self._parse_table(soup)
            if schedule_data:
                logger.info(f"✅ Дані спарсено: {len(schedule_data)} груп")
                return schedule_data

            # Якщо таблиця не знайдена - шукаємо div структуру
            schedule_data = self._parse_divs(soup)
            if schedule_data:
                logger.info(f"✅ Дані спарсено через DIV: {len(schedule_data)} груп")
                return schedule_data

            # Спробуємо отримати дані з JavaScript
            schedule_data = self._extract_js_data()
            if schedule_data:
                logger.info(f"✅ Дані отримано з JavaScript: {len(schedule_data)} груп")
                return schedule_data

            logger.warning("❌ Не вдалося спарсити дані")
            return {}

        except Exception as e:
            logger.error(f"❌ Помилка Selenium парсингу: {e}", exc_info=True)
            return None
        finally:
            # Не закриваємо драйвер для повторного використання
            pass

    def _parse_table(self, soup) -> Optional[Dict[int, List[Dict]]]:
        """Парсинг HTML таблиці"""
        schedule_data = {}

        # Шукаємо таблицю
        table = soup.find('table')
        if not table:
            logger.warning("❌ Таблицю не знайдено")
            return None

        logger.info(f"📊 Таблиця знайдена: {table.get('class')} {table.get('id')}")

        try:
            # Знаходимо заголовки
            headers = []
            header_row = table.find('thead') or table.find('tr')

            if header_row:
                for th in header_row.find_all(['th', 'td']):
                    text = th.get_text(strip=True)
                    if ':' in text or '-' in text:
                        headers.append(text)

            if not headers:
                # Створюємо стандартні заголовки
                headers = [f"{i:02d}:00-{i + 2:02d}:00" for i in range(0, 24, 2)]

            logger.info(f"📋 Заголовків: {len(headers)}")

            # Парсинг рядків
            tbody = table.find('tbody') or table
            rows = tbody.find_all('tr')[1:]  # Пропускаємо заголовок

            logger.info(f"📋 Рядків: {len(rows)}")

            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) < 2:
                    continue

                # Перша комірка - номер групи
                group_text = cells[0].get_text(strip=True)

                # Витягуємо число
                import re
                match = re.search(r'\d+', group_text)
                if not match:
                    continue

                group_num = int(match.group())
                if group_num < 1 or group_num > 20:
                    continue

                schedule_data[group_num] = []

                # Парсинг статусів
                for i, cell in enumerate(cells[1:]):
                    if i >= len(headers):
                        break

                    status = self._determine_status(cell)

                    schedule_data[group_num].append({
                        'time': headers[i],
                        'status': status
                    })

            return schedule_data if schedule_data else None

        except Exception as e:
            logger.error(f"❌ Помилка парсингу таблиці: {e}")
            return None

    def _parse_divs(self, soup) -> Optional[Dict[int, List[Dict]]]:
        """Парсинг DIV структури"""
        # Шукаємо контейнер з графіком
        schedule_container = soup.find('div', class_=lambda x: x and 'schedule' in x.lower())

        if not schedule_container:
            return None

        logger.info("📦 Знайдено DIV контейнер з графіком")

        # Тут потрібна специфічна логіка для конкретної структури
        # Додайте після аналізу сайту

        return None

    def _determine_status(self, cell) -> str:
        """Визначення статусу комірки"""
        style = str(cell.get('style', '')).lower()
        classes = ' '.join(cell.get('class', [])).lower()
        text = cell.get_text(strip=True).lower()
        bgcolor = str(cell.get('bgcolor', '')).lower()

        # Червоний = відключення
        if any(x in style or x in classes or x in bgcolor for x in
               ['red', '#ff0000', '#f00', 'rgb(255,0,0)', 'danger', 'outage']):
            return 'off'

        # Зелений = світло є
        if any(x in style or x in classes or x in bgcolor for x in
               ['green', '#00ff00', '#0f0', 'rgb(0,255,0)', 'success', 'power']):
            return 'on'

        # Жовтий/сірий = можливо
        if any(x in style or x in classes or x in bgcolor for x in
               ['yellow', 'gray', 'grey', 'warning', 'maybe']):
            return 'maybe'

        # Текстові індикатори
        if any(word in text for word in ['відключення', 'немає', 'off']):
            return 'off'
        if any(word in text for word in ['можливо', 'maybe']):
            return 'maybe'

        return 'on'

    def _extract_js_data(self) -> Optional[Dict[int, List[Dict]]]:
        """Витягнення даних з JavaScript змінних"""
        try:
            # Спробуємо виконати JavaScript і отримати дані
            scripts = [
                "return window.schedule",
                "return window.scheduleData",
                "return window.groups",
                "return document.getElementById('schedule-data').textContent",
            ]

            for script in scripts:
                try:
                    result = self.driver.execute_script(script)
                    if result:
                        logger.info(f"✅ Дані знайдено через JavaScript: {script}")
                        return self._normalize_data(result)
                except:
                    continue

        except Exception as e:
            logger.error(f"❌ Помилка витягнення JS даних: {e}")

        return None

    def _normalize_data(self, data) -> Dict[int, List[Dict]]:
        """Нормалізація даних з різних форматів"""
        if isinstance(data, dict):
            result = {}
            for key, value in data.items():
                try:
                    import re
                    group_num = int(re.search(r'\d+', str(key)).group())
                    if isinstance(value, list):
                        result[group_num] = value
                except:
                    continue
            return result
        return {}


# Асинхронна обгортка для використання в основному боті
async def fetch_schedule_selenium() -> Optional[Dict[int, List[Dict]]]:
    """
    Асинхронна функція для використання в боті
    """
    parser = SeleniumScheduleParser()
    try:
        result = parser.fetch_schedule()
        return result
    finally:
        parser.close()


# Тестування
def test_selenium_parser():
    """Тестова функція"""
    parser = SeleniumScheduleParser()

    try:
        result = parser.fetch_schedule()

        if result is None:
            print("❌ Помилка завантаження")
        elif not result:
            print("✅ Графіки порожні (відключень немає)")
        else:
            print(f"✅ Успішно! Знайдено {len(result)} груп")

            # Показуємо приклад для групи 1
            if 1 in result:
                print("\n📊 Група 1:")
                for item in result[1][:5]:
                    print(f"  {item['time']}: {item['status']}")

            # Показуємо всі групи
            print(f"\n📋 Знайдені групи: {sorted(result.keys())}")

    finally:
        parser.close()


if __name__ == "__main__":
    test_selenium_parser()