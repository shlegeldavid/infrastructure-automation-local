import os
import re
import time
import json
import tempfile
import urllib.parse
import random
from datetime import datetime, time as dtime, timedelta
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from urllib.parse import urlparse
import requests
import pandas as pd

from selenium.common.exceptions import TimeoutException as SelTimeoutException
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials as UserCredentials

# Логирование
def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)

# Конфигурация
TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN_YANDEX_PARSER_V2")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID_YANDEX_PARSER_V2")
MOSCOW_TZ = ZoneInfo("Europe/Moscow")

DRIVE_OAUTH_SCOPES = ["https://www.googleapis.com/auth/drive.file"]

CONFIG = {
    # Источник запросов
    "queries_source": "gsheets",
    "resolve_final_url": True,

    # Откуда читать запросы (первый лист, колонка B начиная с B2)
    "gsheets_queries_spreadsheet_id": "1JcUKxyTib-LPYgA-XFZd-HlCbhzlc4KzguVpGFGKRs4",
    # Куда писать результаты (лист создастся сам при отсутствии)
    "gsheets_results_spreadsheet_id": "1EEXVYmlDFPiCn4hcDVdDon9PrQJbcPP7P-2y4i6PknA",
    "gsheets_results_sheet": "Results",

    # Excel (если queries_source == "excel")
    "excel_path": "queries.xlsx",
    "excel_sheet_name": "Sheet1",
    "excel_column": "B",

    # Папка на Google Drive для скринов
    "gdrive_folder_id": "1VPtEC4JcuddvPJI5HUn3CmuypxdCuevv",

    # Selenium/режим 
    "headless": False,                      # headful уменьшает шанс капчи
    "use_undetected_chromedriver": False,   # можно включить при необходимости

    # Тайминги/паузы
    "page_load_timeout_sec": 25,
    "element_timeout_sec": 10,
    "post_load_sleep_sec": 1.0,
    "human_delay_sec": (1.5, 3.5),          # «человеческие» задержки
    "per_query_pause_sec": (35, 70),        # пауза между запросами
    "captcha_backoff_sec": [120, 300],      # бэкофф между ретраями (2 и 5 минут)
    "max_retries_per_query": 3,             # попыток на один запрос

    # Капча: ручной режим + ожидание
    "manual_captcha_mode": True,            # ждём пользователя для прохождения
    "manual_captcha_total_wait_sec": 300,   # ждём до 5 минут
    "manual_captcha_poll_sec": 2,           # проверяем каждые 2 сек

    # Ротация UA
    "rotate_user_agents": [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    ],

    # Парсинг рекламных меток
    "ad_labels": ["Реклама", "Промо"],
    "top_n": 5,

    # Google Service Account (для Sheets)
    "google_sa_json_path": "service_account.json",
    "cookies_path": "/app/data/yandex_search_cookies.json",
    "screenshot_dir": "/app/data/screenshots"
}


# Google auth helpers
SHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.file",
]

def send_telegram(text):
    """Отправляет сообщение в Telegram."""
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        log("[TG] Токен или chat_id не заданы")
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
            data={"chat_id": TG_CHAT_ID, "text": text},
            timeout=10
        )
        return r.status_code == 200
    except Exception as e:
        log(f"[TG] Ошибка отправки: {e}")
        return False

def send_telegram_photo(photo_path, caption=None):
    """Sends photo to tg"""
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        return False
    try:
        with open(photo_path, "rb") as f:
            data = {"chat_id": TG_CHAT_ID}
            if caption:
                data["caption"] = caption
            r = requests.post(
                f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendPhoto",
                data=data,
                files={"photo": f},
                timeout=30
            )
        return r.status_code == 200
    except Exception as e:
        log(f"[TG] Ошибка отправки фоточки: {e}")
        return False

def save_cookies(driver):
    """Сохраняет cookies в файл."""
    try:
        cookies = driver.get_cookies()
        cookies_path = CONFIG["cookies_path"]
        os.makedirs(os.path.dirname(cookies_path), exist_ok=True)
        with open(cookies_path, 'w') as f:
            json.dump(cookies, f)
        log(f"[COOKIES] Сохранено {len(cookies)} cookies")
        return True
    except Exception as e:
        log(f"[COOKIES] Ошибка сохранения: {e}")
        return False

def load_cookies(driver):
    """Загружает кукисы из файлика"""
    cookies_path = CONFIG["cookies_path"]
    if not os.path.exists(cookies_path):
        log("[COOKIES] файл не найден(-ы)")
        return False
    try:
        with open(cookies_path, 'r') as f:
            cookies = json.load(f)
        driver.get("https://ya.ru")
        time.sleep(2)

        loaded = 0
        for cookie in cookies:
            cookie.pop('sameSite', None)
            cookie.pop('expiry', None)
            try:
                driver.add_cookie(cookie)
                loaded += 1
            except:
                pass
        
        log(f"[COOKIES] Загружено {loaded} куки")
        return True
    except Exception as e:
        log(f"[COOKIES] Ошибка загрузки куки: {e}")
        return False

def get_google_creds():
    return Credentials.from_service_account_file(CONFIG["google_sa_json_path"], scopes=SHEETS_SCOPES)

def gsheet_client():
    return gspread.authorize(get_google_creds())

def get_user_drive_creds():
    token_path = "token_drive.json"
    creds = None
    if os.path.exists(token_path):
        creds = UserCredentials.from_authorized_user_file(token_path, DRIVE_OAUTH_SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("oauth_client.json", DRIVE_OAUTH_SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_path, "w", encoding="utf-8") as f:
            f.write(creds.to_json())
    return creds

def upload_to_drive(local_path, filename):
    try:
        drive = build("drive", "v3", credentials=get_user_drive_creds())
        file_metadata = {"name": filename, "parents": [CONFIG["gdrive_folder_id"]]}
        media = MediaFileUpload(local_path, mimetype="image/png", resumable=True)
        file = drive.files().create(body=file_metadata, media_body=media,
                                    fields="id,webViewLink").execute()
        return file["id"], file.get("webViewLink")
    except Exception as e:
        log(f"[DRIVE] Ошибка загрузки: {e}")
        return None, None

def assert_is_google_sheet(spreadsheet_id):
    creds = get_google_creds()
    drive = build("drive", "v3", credentials=creds)
    meta = drive.files().get(fileId=spreadsheet_id, fields="id, name, mimeType").execute()
    if meta["mimeType"] != "application/vnd.google-apps.spreadsheet":
        raise ValueError(
            f"Документ '{meta['name']}' не является Google Таблицей "
            f"(mimeType={meta['mimeType']}). Конвертируй: Файл → Сохранить как Google Таблицы."
        )

def ensure_results_worksheet(gc):
    assert_is_google_sheet(CONFIG["gsheets_results_spreadsheet_id"])
    sh = gc.open_by_key(CONFIG["gsheets_results_spreadsheet_id"])
    try:
        ws = sh.worksheet(CONFIG["gsheets_results_sheet"])
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(CONFIG["gsheets_results_sheet"], rows=1000, cols=10)
        ws.append_row(["timestamp", "query", "position", "label", "title", "url", "domain"])
    return ws

def read_queries():
    if CONFIG.get("queries_source") == "excel":
        df = pd.read_excel(CONFIG["excel_path"], sheet_name=CONFIG["excel_sheet_name"])
        col_idx = ord(CONFIG["excel_column"].upper()) - ord('A')
        return [str(x).strip() for x in df.iloc[:, col_idx].dropna().tolist() if str(x).strip()]
    else:
        gc = gsheet_client()
        sh = gc.open_by_key(CONFIG["gsheets_queries_spreadsheet_id"])
        ws = sh.sheet1  # первый лист
        values = ws.col_values(2)  # колонка B
        return [v.strip() for v in values[1:] if v and v.strip()]  # пропускаем B1

def write_run_timestamp():
    gc = gsheet_client()
    sh = gc.open_by_key(CONFIG["gsheets_queries_spreadsheet_id"])
    ws = sh.sheet1
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    ws.update_acell('A2', ts)

# Selenium helpers
def resolve_final_url_via_selenium(driver, href, timeout=10):
    """
    Пытаемся получить конечный URL, перейдя по ссылке в новой вкладке.
    Если что-то пошло не так — возвращаем исходный href.
    Основано на предположении, что после всех редиректов в браузере будет
    реально открыта целевая страница сайта.
    """
    if not href:
        return href

    try:
        original_handle = driver.current_window_handle
    except Exception:
        return href

    try:
        # Пытаемся открыть новую вкладку (Selenium 4)
        try:
            driver.switch_to.new_window('tab')
        except Exception:
            # Фолбэк для старых версий Selenium
            driver.execute_script("window.open('about:blank','_blank');")
            driver.switch_to.window(driver.window_handles[-1])

        driver.get(href)
        try:
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
        except SelTimeoutException:
            pass

        final_url = driver.current_url or href
    except Exception:
        final_url = href
    finally:
        try:
            driver.close()
        except Exception:
            pass
        try:
            driver.switch_to.window(original_handle)
        except Exception:
            pass

    return final_url

def create_driver(user_agent=None):
    opts = Options()

    if CONFIG.get("headless", False):
        opts.add_argument("--headless=new")
        opts.add_argument("--disable-gpu")
    
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    
    if user_agent:
        opts.add_argument(f"--user-agent={user_agent}")
    
    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(CONFIG.get("page_load_timeout_sec", 25))

    load_cookies(driver)

    if not CONFIG.get("headless", False):
        try:
            driver.maximize_window()
        except:
            pass
    
    return driver

def safe_quit_driver(driver):
    try:
        driver.quit()
    except Exception:
        pass
    # НИЧЕГО не удаляем — куки живут


def fullpage_screenshot(driver, path_png):
    """
    Делаем скриншот всей страницы, масштабируя окно,
    и сохраняем в указанный путь.
    """
    try:
        total_width = driver.execute_script(
            "return Math.max(document.body.scrollWidth, document.documentElement.scrollWidth);"
        )
        total_height = driver.execute_script(
            "return Math.max(document.body.scrollHeight, document.documentElement.scrollHeight);"
        )
        driver.set_window_size(max(1280, total_width), max(800, total_height))
        time.sleep(1)
    except Exception:
        # если вдруг скрипты не отработали — всё равно пробуем просто сделать скрин
        pass

    driver.save_screenshot(path_png)

def timestamp_str():
    return datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

def text_or_empty(el):
    try:
        return el.text.strip()
    except Exception:
        return ""

def normalize_domain(u):
    try:
        netloc = urlparse(u).netloc
        return netloc.lower().lstrip("www.")
    except Exception:
        return ""

# UX helpers (cookie/поиск)
def accept_cookies_if_any(driver):
    xpaths = [
        "//*[self::button or self::a][contains(.,'Понятно') or contains(.,'Согласен') or contains(.,'Принять')]",
        "//*[self::button or self::a][contains(.,'Allow all') or contains(.,'Accept all') or contains(.,'Accept')]",
        "//button[contains(.,'Allow essential cookies')]",
    ]
    for xp in xpaths:
        try:
            el = WebDriverWait(driver, 2).until(EC.element_to_be_clickable((By.XPATH, xp)))
            el.click()
            time.sleep(random.uniform(*CONFIG.get("human_delay_sec",(1.5,3.5))))
            break
        except Exception:
            pass

def find_search_box(driver, wait_sec=6):
    selectors = [
        (By.NAME, "text"),
        (By.CSS_SELECTOR, "input#text"),
        (By.CSS_SELECTOR, "input[type='search']"),
        (By.CSS_SELECTOR, "input.input__control"),
    ]
    for by, sel in selectors:
        try:
            el = WebDriverWait(driver, wait_sec).until(EC.presence_of_element_located((by, sel)))
            if el and el.is_enabled():
                return el
        except TimeoutException:
            continue
    return None

def human_like_search_flow(driver, query):
    # Порядок: ya.ru → yandex.ru → фолбэк на search/?text=
    for start_url in ["https://ya.ru/", "https://yandex.ru/"]:
        try:
            driver.get(start_url)
            WebDriverWait(driver, CONFIG.get("element_timeout_sec",10)).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(random.uniform(*CONFIG.get("human_delay_sec",(1.5,3.5))))
            accept_cookies_if_any(driver)
            if is_yandex_captcha(driver):
                return "captcha"

            box = find_search_box(driver, wait_sec=6)
            if not box:
                continue

            for chunk in query.split():
                box.send_keys(chunk + " ")
                time.sleep(random.uniform(0.15, 0.35))
            box.submit()
            time.sleep(random.uniform(*CONFIG.get("human_delay_sec",(1.5,3.5))))
            return "ok" if not is_yandex_captcha(driver) else "captcha"
        except Exception:
            continue

    # Фолбэк: прямой переход на страницу выдачи
    q = urllib.parse.quote_plus(query)
    driver.get(f"https://yandex.ru/search/?text={q}")
    time.sleep(random.uniform(*CONFIG.get("human_delay_sec",(1.5,3.5))))
    accept_cookies_if_any(driver)
    return "ok" if not is_yandex_captcha(driver) else "captcha"

# CAPTCHA detect & manual wait
def is_yandex_captcha(driver):
    html = (driver.page_source or "").lower()
    if ("smartcaptcha" in html or "я не робот" in html
        or "подтвердите, что запросы отправляли вы" in html):
        return True
    try:
        driver.find_element(By.XPATH, "//*[contains(text(),'Я не робот') or contains(text(),'SmartCaptcha')]")
        return True
    except Exception:
        return False

def notify_user_captcha(query):
    """Уведомляет о капче."""
    msg = f"🔐 КАПЧА!\n\nЗапрос: {query}\n\nОткрой VNC (порт 7900) и реши капчу.\nОжидание: до 5 минут."
    log(f"[CAPTCHA] {msg}")
    send_telegram(msg)

def wait_user_to_solve_captcha(driver, query):
    """Ждёт пока пользователь решит капчу."""
    notify_user_captcha(query)
    
    total = CONFIG.get("manual_captcha_total_wait_sec", 300)
    poll = CONFIG.get("manual_captcha_poll_sec", 2)
    deadline = time.time() + total
    
    while time.time() < deadline:
        try:
            driver.execute_script("window.scrollBy(0, 50);")
        except:
            pass
        
        if not is_yandex_captcha(driver):
            # Капча решена — сохраняем cookies
            save_cookies(driver)
            send_telegram(f"✅ Капча решена: {query}")
            log(f"[CAPTCHA] Решена для: {query}")
            return True
        
        time.sleep(poll)
    
    send_telegram(f"❌ Таймаут капчи: {query}")
    log(f"[CAPTCHA] Таймаут для: {query}")
    return False

# Core: parse Yandex SERP
def parse_ads_positions(driver):
    limit = int(CONFIG.get("top_n", 5))

    def has_ad_marker(block) -> bool:
        try:
            for lbl in CONFIG.get("ad_labels", ["Реклама", "Промо"]):
                nodes = block.find_elements(
                    By.XPATH,
                    f".//*[(self::span or self::div or self::b or self::small) "
                    f"and contains(normalize-space(.), '{lbl}')]"
                )
                for n in nodes:
                    t = (n.text or "").strip()
                    if 0 < len(t) <= 20:
                        return True
        except Exception:
            pass

        # иногда реклама помечается атрибутами/классами
        try:
            fast = (block.get_attribute("data-fast-name") or "").lower()
            cls = (block.get_attribute("class") or "").lower()
            if "adv" in fast or fast in ("ad", "ads"):
                return True
            if "serp-item" in cls and "adv" in cls:
                return True
        except Exception:
            pass

        return False

    def extract_best_link(block):
        xps = [
            ".//a[@href][.//h2 or .//h3]",
            ".//a[@href][@role='link']",
            ".//a[@href]",
        ]
        for xp in xps:
            try:
                links = block.find_elements(By.XPATH, xp)
                for a in links:
                    href = a.get_attribute("href")
                    if not href:
                        continue
                    if href.startswith("javascript"):
                        continue
                    if len((a.text or "").strip()) == 0:
                        continue
                    return a
            except Exception:
                continue
        return None

    blocks = driver.find_elements(
        By.XPATH,
        "//li[contains(@class,'serp-item')] | //div[contains(@class,'serp-item')]"
    )

    out = []
    seen = set()
    pos = 0  # позиция в выдаче среди результатов (1..limit)

    for block in blocks:
        if pos >= limit:
            break

        try:
            if not block.is_displayed():
                continue
        except Exception:
            pass

        link = extract_best_link(block)
        if not link:
            continue

        href = link.get_attribute("href")
        if not href:
            continue

        # это считается "результатом", значит увеличиваем позицию
        pos += 1

        # если не реклама — просто пропускаем, но позиция уже учтена
        if not has_ad_marker(block):
            continue

        # если реклама — добавляем 
        if href in seen:
            continue
        seen.add(href)

        title = text_or_empty(link)
        if not title:
            try:
                title = text_or_empty(block.find_element(By.XPATH, ".//h2 | .//h3"))
            except Exception:
                title = ""

        out.append({
            "position": pos,
            "label": "AD",
            "title": title,
            "url": href,
            "domain": normalize_domain(href),
        })

    return out

# Main per-query with manual-captcha + retries
def run_for_query(query, ws_results):
    log(f"[QUERY] Начинаю: {query}")

    retries = CONFIG.get("max_retries_per_query", 3)
    backoffs = CONFIG.get("captcha_backoff_sec", [120, 300])
    ua_list = CONFIG.get("rotate_user_agents", [])

    for attempt in range(1, retries + 1):
        log(f"[QUERY] Попытка {attempt}/{retries}")
        
        ua = random.choice(ua_list) if ua_list else None
        driver = create_driver(user_agent=ua)
        
        try:
            status = human_like_search_flow(driver, query)

            # Капча на входе
            if status == "captcha":
                if CONFIG.get("manual_captcha_mode", True):
                    solved = wait_user_to_solve_captcha(driver, query)
                    if not solved:
                        backoff = backoffs[min(attempt - 1, len(backoffs) - 1)]
                        log(f"[QUERY] Бэкофф {backoff} сек")
                        time.sleep(backoff)
                        continue
                else:
                    backoff = backoffs[min(attempt - 1, len(backoffs) - 1)]
                    time.sleep(backoff)
                    continue

            # Ждём загрузки
            try:
                WebDriverWait(driver, CONFIG.get("element_timeout_sec", 10)).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
            except:
                pass
            time.sleep(CONFIG.get("post_load_sleep_sec", 1.0))

            # Проверяем капчу ещё раз
            if is_yandex_captcha(driver):
                if CONFIG.get("manual_captcha_mode", True):
                    solved = wait_user_to_solve_captcha(driver, query)
                    if not solved:
                        backoff = backoffs[min(attempt - 1, len(backoffs) - 1)]
                        time.sleep(backoff)
                        continue
                else:
                    backoff = backoffs[min(attempt - 1, len(backoffs) - 1)]
                    time.sleep(backoff)
                    continue

            # Парсим рекламу
            ads = parse_ads_positions(driver)
            log(f"[QUERY] Найдено {len(ads)} рекламных позиций")

            # Резолвим URL если нужно
            if ads and CONFIG.get("resolve_final_url", False):
                for it in ads:
                    try:
                        raw = it.get("url")
                        if raw:
                            it["url"] = resolve_final_url_via_selenium(driver, raw)
                            it["domain"] = normalize_domain(it["url"])
                    except:
                        pass

            # Скриншот
            ts = timestamp_str()
            safe_name = re.sub(r'[^А-Яа-яA-Za-z0-9_\- ]+', '_', query)[:50]
            screenshots_dir = CONFIG.get("screenshots_dir", "/app/data/screenshots")
            os.makedirs(screenshots_dir, exist_ok=True)
            local_png = os.path.join(screenshots_dir, f"{safe_name}_{ts}.png")
            fullpage_screenshot(driver, local_png)

            # Загружаем на Drive
            drive_link = None
            try:
                _, drive_link = upload_to_drive(local_png, os.path.basename(local_png))
            except Exception as e:
                log(f"[DRIVE] Не удалось загрузить: {e}")

            # Запись в Results
            if not ads:
                ws_results.append_row([ts, query, "", "SUCCESS_NO_ADS", "", driver.current_url, "yandex.ru"])
                log(f"[QUERY] Реклама не найдена")
            else:
                rows = [
                    [ts, query, it["position"], "SUCCESS", it["title"], it["url"], it["domain"]]
                    for it in ads
                ]
                ws_results.append_rows(rows, value_input_option="USER_ENTERED")
                log(f"[QUERY] Записано {len(rows)} строк")

            # Сохраняем cookies после успешного запроса
            save_cookies(driver)

            # Пауза между запросами
            pause = random.uniform(*CONFIG.get("per_query_pause_sec", (30, 60)))
            log(f"[QUERY] Пауза {pause:.0f} сек")
            time.sleep(pause)
            return

        except Exception as e:
            log(f"[QUERY] Ошибка: {e}")
        finally:
            safe_quit_driver(driver)

    log(f"[QUERY] Все попытки исчерпаны для: {query}")

MOSCOW_TZ = ZoneInfo("Europe/Moscow")

def seconds_until_next_run(now=None):
    """
    Считает, сколько секунд осталось до ближайшего запуска
    в понедельник или пятницу в 10:00 по МСК.
    """
    if now is None:
        now = datetime.now(MOSCOW_TZ)

    target_time = dtime(10, 0)  # 10:00
    target_weekdays = {0, 4}    # 0 = понедельник, 4 = пятница

    # Ищем ближайший понедельник/пятницу (включая сегодня)
    for days_ahead in range(0, 8):
        candidate_date = now.date() + timedelta(days=days_ahead)
        if candidate_date.weekday() in target_weekdays:
            candidate_dt = datetime.combine(candidate_date, target_time, tzinfo=MOSCOW_TZ)
            if candidate_dt > now:
                delta = candidate_dt - now
                return delta.total_seconds()

    # Теоретически сюда не дойдём, но на всякий случай — сутки ожидания
    return 24 * 3600

def main_once():
    log("=== ЗАПУСК ПАРСЕРА ===")
    send_telegram("🚀 Yandex Parser запущен")
    
    try:
        gc = gsheet_client()
        ws_results = ensure_results_worksheet(gc)
        write_run_timestamp()
        queries = read_queries()
        
        log(f"Загружено {len(queries)} запросов")
        
        for i, q in enumerate(queries, 1):
            log(f"[{i}/{len(queries)}] {q}")
            run_for_query(q, ws_results)
        
        send_telegram(f"✅ Парсер завершён. Обработано {len(queries)} запросов.")
        log("=== ПАРСЕР ЗАВЕРШЁН ===")
        
    except Exception as e:
        log(f"[ERROR] {e}")
        send_telegram(f"❌ Ошибка парсера: {e}")

def scheduler_loop():
    """Бесконечный цикл планировщика."""
    log("=== YANDEX PARSER STARTED ===")

    while True:
        now = datetime.now(MOSCOW_TZ)
        wait_sec = seconds_until_next_run(now)
        hours = wait_sec / 3600

        log(f"[SCHEDULER] Ждём {hours:.2f} ч до следующего запуска")
        time.sleep(wait_sec)

        log(f"[SCHEDULER] Запуск в {datetime.now(MOSCOW_TZ)}")
        try:
            main_once()
        except Exception as e:
            log(f"[SCHEDULER] Ошибка: {e}")


if __name__ == "__main__":
    if os.environ.get("SMOKE_TEST", "").lower() == "true":
        log("=== SMOKE TEST MODE ===")
        log("Проверяю импорты и конфигурацию...")
        
        # Проверяем что Chrome запускается
        try:
            driver = create_driver()
            driver.get("https://ya.ru")
            log(f"✅ Chrome работает, страница: {driver.title}")
            safe_quit_driver(driver)
        except Exception as e:
            log(f"❌ Ошибка Chrome: {e}")
            exit(1)
        
        log("✅ Smoke test пройден")
        exit(0)
    
    scheduler_loop()
