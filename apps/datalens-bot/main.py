import os
import time
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from PIL import Image
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


# === НАСТРОЙКИ ===
# Токен берем из ENV
TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN")
CHAT_ID = os.environ.get("TG_CHAT_ID") 
DATALENS_URL = "https://datalens.ru/35o65aulrl0wo-kc-obshiy"

# Пути
SCREENSHOT_PATH = Path("/app/data/datalens_dashboard.png")
# Путь к профилю ВНУТРИ контейнера Chrome (через volume)
SELENIUM_PROFILE_PATH = "/opt/selenium/profile"
# Адрес Selenium (из переменной или дефолтный)
SELENIUM_HOST = os.environ.get("SELENIUM_HOST", "http://selenium-chrome:4444/wd/hub")


def now_moscow() -> datetime:
    """Возвращает текущее время в Москве (UTC+3), используя timezone-aware объекты."""
    # Берем текущее время в UTC и добавляем 3 часа
    return datetime.now(timezone.utc) + timedelta(hours=3)


def make_screenshot(first_login: bool = False) -> None:
    print("Подключаюсь к Selenium...")
    options = Options()
    
    # Подключаем профиль, чтобы сохранить логин
    options.add_argument(f"--user-data-dir={SELENIUM_PROFILE_PATH}")
    
    # Настройки для стабильности в Docker
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")

    try:
        driver = webdriver.Remote(
            command_executor=SELENIUM_HOST,
            options=options
        )
    except Exception as e:
        print(f"CRITICAL: Не могу подключиться к Selenium: {e}")
        return

    try:
        print(f"Открываю {DATALENS_URL}")
        driver.get(DATALENS_URL)

        if first_login:
            print("\n!!! РЕЖИМ ПЕРВОГО ВХОДА !!!")
            print(f"Перейди по адресу http://<IP_СЕРВЕРА>:7900 (пароль secret)")
            print("У тебя есть 300 секунд, чтобы залогиниться в Яндексе.")
            time.sleep(300)
        else:
            # Ждем прогрузки дашборда
            print("Жду загрузки страницы (15 сек)...")
            time.sleep(15)

        # Создаем папку, если вдруг её нет
        SCREENSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
        
        driver.save_screenshot(str(SCREENSHOT_PATH))
        print(f"Скриншот сохранён: {SCREENSHOT_PATH}")
        
    except Exception as e:
        print(f"Ошибка во время работы Selenium: {e}")
    finally:
        # Всегда закрываем сессию, чтобы не забить память
        driver.quit()


def crop_screenshot() -> None:
    try:
        if not SCREENSHOT_PATH.exists():
            print("Нет файла скриншота для обрезки.")
            return

        img = Image.open(SCREENSHOT_PATH)
        w, h = img.size
        
        # Настройки кропа (подбери под свой экран)
        top = int(h * 0.25)
        bottom = int(h * 0.4)
        
        cropped = img.crop((0, top, w, bottom))
        cropped.save(SCREENSHOT_PATH)
        print("Скриншот обрезан.")
    except Exception as e:
        print(f"Ошибка кропа: {e}")


def send_to_telegram(text=None, photo_path=None):
    if not TG_BOT_TOKEN or not CHAT_ID:
        print("Ошибка конфигурации: Нет токена или chat_id в ENV")
        return

    try:
        if photo_path and photo_path.exists():
            with open(photo_path, "rb") as photo:
                requests.post(
                    f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendPhoto",
                    data={"chat_id": CHAT_ID},
                    files={"photo": photo},
                    timeout=20
                )
        elif text:
            requests.post(
                f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
                data={"chat_id": CHAT_ID, "text": text},
                timeout=20
            )
    except Exception as e:
        print(f"Ошибка отправки в Telegram: {e}")


def main():
    # Проверка режима "первый вход" через переменную окружения
    is_first_run = os.environ.get("FIRST_RUN", "false").lower() == "true"
    
    if is_first_run:
        make_screenshot(first_login=True)
        print("Первичная настройка завершена. Останови контейнер и убери FIRST_RUN=true")
        return

    print("Бот запущен в рабочем режиме. Жду расписания...")

    # Делаем тестовый прогон при старте (чтобы убедиться, что работает), или удали эти 3 строки
    # make_screenshot()
    # crop_screenshot()
    # send_to_telegram(text="Бот перезапущен. Тестовая проверка.")

    while True:
        now = now_moscow()

        # Вычисляем время до следующего часа + 10 минут
        next_run = (now + timedelta(hours=1)).replace(
            minute=10, second=0, microsecond=0
        )
        sleep_seconds = (next_run - now).total_seconds()
        
        print(
            f"Сейчас {now.strftime('%H:%M:%S')} (МСК). "
            f"Сплю {int(sleep_seconds)} сек до {next_run.strftime('%H:%M:%S')}"
        )
        
        # Спим
        time.sleep(sleep_seconds)

        # Проснулись, проверяем время
        now = now_moscow()
        hour = now.hour

        # Если время меньше 9 утра (например, 8:10), пропускаем
        if hour < 9:
            print(f"Время {hour}:10 — рано для отчета. Жду следующего часа.")
            continue

        print(f"Время {hour}:10 — запускаю процесс.")
        
        make_screenshot()
        crop_screenshot()
        send_to_telegram(photo_path=SCREENSHOT_PATH)
        send_to_telegram(text=f"Отчет за {hour}:00")


if __name__ == "__main__":
    main()