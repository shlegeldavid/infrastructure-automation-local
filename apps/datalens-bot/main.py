import os
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from PIL import Image
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


# === НАСТРОЙКИ ===
TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN")
CHAT_ID = os.environ.get("TG_CHAT_ID") 
DATALENS_URL = "https://datalens.ru/35o65aulrl0wo-kc-obshiy"
YANDEX_PASSPORT_URL = "https://passport.yandex.ru"

SCREENSHOT_PATH = Path("/app/data/datalens_dashboard.png")
COOKIES_PATH = Path("/app/data/yandex_cookies.json")
SELENIUM_HOST = os.environ.get("SELENIUM_HOST", "http://selenium-chrome:4444/wd/hub")


def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def now_moscow() -> datetime:
    return datetime.now(timezone.utc) + timedelta(hours=3)


def save_cookies(driver):
    """Сохраняет cookies в файл."""
    cookies = driver.get_cookies()
    COOKIES_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(COOKIES_PATH, 'w') as f:
        json.dump(cookies, f)
    log(f"Cookies сохранены: {len(cookies)} шт.")


def load_cookies(driver):
    """Загружает cookies из файла."""
    if not COOKIES_PATH.exists():
        log("Файл cookies не найден")
        return False
    
    try:
        with open(COOKIES_PATH, 'r') as f:
            cookies = json.load(f)
        
        # Сначала открываем домен, чтобы можно было установить cookies
        driver.get(YANDEX_PASSPORT_URL)
        time.sleep(2)
        
        for cookie in cookies:
            # Убираем проблемные поля
            cookie.pop('sameSite', None)
            cookie.pop('expiry', None)
            try:
                driver.add_cookie(cookie)
            except Exception as e:
                log(f"Не удалось добавить cookie {cookie.get('name')}: {e}")
        
        log(f"Cookies загружены: {len(cookies)} шт.")
        return True
    except Exception as e:
        log(f"Ошибка загрузки cookies: {e}")
        return False


def create_driver():
    """Создаёт новый WebDriver."""
    options = Options()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-blink-features=AutomationControlled")
    
    try:
        driver = webdriver.Remote(
            command_executor=SELENIUM_HOST,
            options=options
        )
        log("WebDriver создан")
        return driver
    except Exception as e:
        log(f"CRITICAL: Не могу создать WebDriver: {e}")
        return None


def make_screenshot(first_login: bool = False) -> bool:
    driver = create_driver()
    if not driver:
        return False

    try:
        # Загружаем cookies если есть (и не первый логин)
        if not first_login and COOKIES_PATH.exists():
            load_cookies(driver)
        
        log(f"Открываю {DATALENS_URL}")
        driver.get(DATALENS_URL)
        
        if first_login:
            log("!!! РЕЖИМ ПЕРВОГО ВХОДА !!!")
            log("Перейди на VNC (порт 7900) для логина в Яндексе")
            log("У тебя есть 300 секунд...")
            time.sleep(300)
            
            # Сохраняем cookies после логина
            save_cookies(driver)
            log("Cookies сохранены после первого входа")
        else:
            log("Жду загрузки страницы (20 сек)...")
            time.sleep(20)
            
            # Обновляем cookies после каждого успешного захода
            save_cookies(driver)

        SCREENSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
        driver.save_screenshot(str(SCREENSHOT_PATH))
        
        if SCREENSHOT_PATH.exists():
            log(f"Скриншот сохранён: {SCREENSHOT_PATH.stat().st_size} байт")
            return True
        else:
            log("ОШИБКА: Файл скриншота не создался!")
            return False
        
    except Exception as e:
        log(f"Ошибка: {e}")
        return False
    finally:
        try:
            driver.quit()
            log("WebDriver закрыт")
        except:
            pass


def crop_screenshot() -> bool:
    log("Обрезаю скриншот...")
    try:
        if not SCREENSHOT_PATH.exists():
            log("Нет файла для обрезки")
            return False

        img = Image.open(SCREENSHOT_PATH)
        w, h = img.size
        
        top = int(h * 0.25)
        bottom = int(h * 0.4)
        
        cropped = img.crop((0, top, w, bottom))
        cropped.save(SCREENSHOT_PATH)
        log("Скриншот обрезан")
        return True
    except Exception as e:
        log(f"Ошибка кропа: {e}")
        return False


def send_to_telegram(text=None, photo_path=None) -> bool:
    if not TG_BOT_TOKEN or not CHAT_ID:
        log("Ошибка: Нет TG_BOT_TOKEN или CHAT_ID")
        return False

    try:
        if photo_path and photo_path.exists():
            log(f"Отправляю фото...")
            with open(photo_path, "rb") as photo:
                resp = requests.post(
                    f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendPhoto",
                    data={"chat_id": CHAT_ID},
                    files={"photo": photo},
                    timeout=30
                )
            log(f"Telegram ответ: {resp.status_code}")
            return resp.status_code == 200
        elif text:
            log(f"Отправляю текст: {text}")
            resp = requests.post(
                f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
                data={"chat_id": CHAT_ID, "text": text},
                timeout=30
            )
            return resp.status_code == 200
    except Exception as e:
        log(f"Ошибка Telegram: {e}")
        return False


def main():
    log("=== DATALENS BOT STARTED ===")
    log(f"SELENIUM_HOST: {SELENIUM_HOST}")
    log(f"Cookies файл: {COOKIES_PATH} (существует: {COOKIES_PATH.exists()})")
    
    is_first_run = os.environ.get("FIRST_RUN", "false").lower() == "true"
    
    if is_first_run:
        log("Режим первого входа")
        make_screenshot(first_login=True)
        log("Первичная настройка завершена. Убери FIRST_RUN=true")
        return

    log("Рабочий режим")

    while True:
        now = now_moscow()
        next_run = (now + timedelta(hours=1)).replace(minute=10, second=0, microsecond=0)
        sleep_seconds = (next_run - now).total_seconds()
        
        log(f"Сплю до {next_run.strftime('%H:%M:%S')} МСК")
        time.sleep(sleep_seconds)

        now = now_moscow()
        hour = now.hour

        if hour < 9:
            log(f"{hour}:10 — рано")
            continue

        log(f"=== Отчет за {hour}:00 ===")
        
        if make_screenshot():
            crop_screenshot()
            send_to_telegram(photo_path=SCREENSHOT_PATH)
            send_to_telegram(text=f"Отчет за {hour}:00")
        else:
            send_to_telegram(text=f"⚠️ Ошибка отчета за {hour}:00")


if __name__ == "__main__":
    main()