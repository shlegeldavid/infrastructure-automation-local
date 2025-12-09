import os
import re
import time
import json
import shutil
import tempfile
import urllib.parse
import random
from datetime import datetime, time as dtime, timedelta
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from urllib.parse import urlparse
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

    # === Excel (если queries_source == "excel") ===
    "excel_path": "queries.xlsx",
    "excel_sheet_name": "Sheet1",
    "excel_column": "B",

    # === Папка на Google Drive для скринов ===
    "gdrive_folder_id": "1VPtEC4JcuddvPJI5HUn3CmuypxdCuevv",

    # === Selenium/режим ===
    "headless": False,                      # headful уменьшает шанс капчи
    "use_undetected_chromedriver": False,   # можно включить при необходимости
    "strict_fresh_profile_per_query": False, # не чистим куки на каждый запрос
    "chrome_profile_path": "./chrome_profile",  # куда сохранять профиль (куки и т.д.)

    # Тайминги/паузы
    "page_load_timeout_sec": 25,
    "element_timeout_sec": 10,
    "post_load_sleep_sec": 1.0,
    "human_delay_sec": (1.5, 3.5),          # «человеческие» задержки
    "per_query_pause_sec": (30, 60),        # пауза между запросами
    "captcha_backoff_sec": [120, 300],      # бэкофф между ретраями (2 и 5 минут)
    "max_retries_per_query": 3,             # попыток на один запрос

    # Капча: ручной режим + ожидание
    "manual_captcha_mode": True,            # ждём пользователя для прохождения
    "manual_captcha_total_wait_sec": 300,   # ждём до 5 минут
    "manual_captcha_poll_sec": 2,           # проверяем каждые 2 сек
    "beep_on_captcha": True,                # звуковой сигнал (Windows)

    # Ротация UA
    "rotate_user_agents": [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    ],

    # Парсинг рекламных меток
    "ad_labels": ["Реклама", "Промо"],
    "top_n": 10,

    # Google Service Account (для Sheets)
    "google_sa_json_path": "service_account.json",
}

# Подстраховка (если где-то конфиг перезапишут)
CONFIG.setdefault("resolve_final_url", False)
CONFIG.setdefault("page_load_timeout_sec", 25)
CONFIG.setdefault("element_timeout_sec", 10)
CONFIG.setdefault("post_load_sleep_sec", 1.0)
CONFIG.setdefault("human_delay_sec", (1.5, 3.5))
CONFIG.setdefault("per_query_pause_sec", (30, 60 ))
CONFIG.setdefault("captcha_backoff_sec", [120, 300])
CONFIG.setdefault("max_retries_per_query", 3)
CONFIG.setdefault("rotate_user_agents", [])
CONFIG.setdefault("ad_labels", ["Реклама", "Промо"])
CONFIG.setdefault("top_n", 10)
CONFIG.setdefault("headless", False)
CONFIG.setdefault("use_undetected_chromedriver", False)
CONFIG.setdefault("manual_captcha_mode", True)
CONFIG.setdefault("manual_captcha_total_wait_sec", 300)
CONFIG.setdefault("manual_captcha_poll_sec", 2)
CONFIG.setdefault("beep_on_captcha", True)

# =========================
# Google auth helpers
# =========================
SHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.file",
]

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
    drive = build("drive", "v3", credentials=get_user_drive_creds())
    file_metadata = {"name": filename, "parents": [CONFIG["gdrive_folder_id"]]}
    media = MediaFileUpload(local_path, mimetype="image/png", resumable=True)
    file = drive.files().create(body=file_metadata, media_body=media,
                                fields="id,webViewLink").execute()
    return file["id"], file.get("webViewLink")

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

# =========================
# Selenium helpers
# =========================
def resolve_final_url_via_selenium(driver, href, timeout=10):
    """
    Получаем конечный URL, открывая ссылку в новой вкладке.
    ВАЖНО: это реальные клики по рекламе.
    """
    if not href:
        return href

    try:
        original_handle = driver.current_window_handle
    except Exception:
        return href

    final_url = href

    # Открываем новую вкладку
    try:
        try:
            driver.switch_to.new_window('tab')
        except Exception:
            driver.execute_script("window.open('about:blank','_blank');")
            driver.switch_to.window(driver.window_handles[-1])
    except Exception:
        return href

    try:
        driver.get(href)
        try:
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
        except TimeoutException:
            pass

        current = driver.current_url
        if current:
            final_url = current
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


def new_driver_with_fresh_profile(user_agent=None):
    # либо каждый раз новый профиль, либо постоянный — по конфигу
    if CONFIG.get("strict_fresh_profile_per_query", True):
        tmp_profile = tempfile.mkdtemp(prefix="chrome_prof_")
    else:
        tmp_profile = os.path.abspath(CONFIG.get("chrome_profile_path", "./chrome_profile"))
        os.makedirs(tmp_profile, exist_ok=True)

    opts = Options()
    if CONFIG.get("headless", False):
        opts.add_argument("--headless=new")
        opts.add_argument("--disable-gpu")

    # здесь все твои остальные аргументы как было
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--disable-infobars")
    opts.add_argument("--disable-notifications")

    if user_agent:
        opts.add_argument(f"--user-agent={user_agent}")

    opts.add_argument(f"--user-data-dir={tmp_profile}")

    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(CONFIG.get("page_load_timeout_sec", 25))

    if not CONFIG.get("headless", False):
        try:
            driver.maximize_window()
        except Exception:
            pass

    return driver, tmp_profile


def safe_quit_driver(driver, tmp_profile):
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

# =========================
# UX helpers (cookie/поиск)
# =========================
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

# =========================
# CAPTCHA detect & manual wait
# =========================
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

def log_screenshot_to_drive_and_row(driver, ws_results, query, label_text):
    ts = timestamp_str()
    safe_name = re.sub(r'[^А-Яа-яA-Za-z0-9_\- ]+', '_', query)[:100]

    screens_dir = os.path.join(os.getcwd(), "screenshots")
    os.makedirs(screens_dir, exist_ok=True)

    local_png = os.path.abspath(os.path.join(screens_dir, f"{safe_name}_{ts}_{label_text}.png"))
    fullpage_screenshot(driver, local_png)
    _, drive_link = upload_to_drive(local_png, os.path.basename(local_png))
    try:
        os.remove(local_png)
    except Exception:
        pass

    ws_results.append_row([
        ts, query, "", label_text, "", driver.current_url, "yandex.ru"
    ])

def notify_user_captcha(query):
    print(f"[CAPTCHA] Для запроса: {query}. Пройди капчу в открытом окне браузера. "
          f"Скрипт продолжит автоматически, как только капча исчезнет.")
    if CONFIG.get("beep_on_captcha", True) and os.name == "nt":
        try:
            import winsound
            winsound.Beep(1000, 300)
            winsound.Beep(850, 300)
        except Exception:
            pass

def wait_user_to_solve_captcha(driver, query):
    notify_user_captcha(query)
    total = CONFIG.get("manual_captcha_total_wait_sec", 300)
    poll = CONFIG.get("manual_captcha_poll_sec", 2)
    deadline = time.time() + total
    while time.time() < deadline:
        try:
            # Лёгкий скролл, чтобы не засыпало
            try:
                driver.execute_script("window.scrollBy(0, 50);")
            except Exception:
                pass
            if not is_yandex_captcha(driver):
                return True
        except Exception:
            pass
        time.sleep(poll)
    return False

# =========================
# Core: parse Yandex SERP
# =========================
def parse_ads_positions(driver):
    labels_xpath = "//span[normalize-space()='Реклама' or normalize-space()='Промо']"
    label_nodes = driver.find_elements(By.XPATH, labels_xpath)

    items = []
    for lab in label_nodes:
        try:
            container = None
            for xpath in [
                "ancestor::article[1]",
                "ancestor::li[contains(@class,'serp')][1]",
                "ancestor::div[contains(@class,'serp')][1]",
                "ancestor::*[contains(@class,'serp-item')][1]"
            ]:
                try:
                    container = lab.find_element(By.XPATH, xpath)
                    if container:
                        break
                except Exception:
                    pass
            if not container:
                continue

            link = None
            for lx in [
                ".//a[@href and (starts-with(@href,'http') or starts-with(@href,'https'))][1]",
                ".//h2//a[@href][1]",
                ".//a[@href][1]"
            ]:
                try:
                    link = container.find_element(By.XPATH, lx)
                    if link:
                        break
                except Exception:
                    pass
            if not link:
                continue

            href = link.get_attribute("href") or ""
            title = text_or_empty(link)
            if not title:
                try:
                    title = text_or_empty(container.find_element(By.XPATH, ".//h2|.//h3"))
                except Exception:
                    pass

            y = container.location.get("y", 10**9)
            label_text = text_or_empty(lab)
            if label_text not in CONFIG["ad_labels"]:
                continue

            items.append({
                "y": y,
                "label": label_text,
                "title": title,
                "url": href,
                "domain": normalize_domain(href),
            })
        except Exception:
            continue

    items.sort(key=lambda x: x["y"])
    out = []
    for i, it in enumerate(items, start=1):
        out.append({
            "position": i,
            "label": it["label"],
            "title": it["title"],
            "url": it["url"],
            "domain": it["domain"],
        })
    return out[:CONFIG["top_n"]]


# =========================
# Main per-query with manual-captcha + retries
# =========================
def run_for_query(query, ws_results):
    retries = CONFIG.get("max_retries_per_query", 3)
    backoffs = CONFIG.get("captcha_backoff_sec", [120, 300])
    ua_list = CONFIG.get("rotate_user_agents", [])

    for attempt in range(1, retries + 1):
        ua = random.choice(ua_list) if ua_list else None
        driver, tmp_prof = new_driver_with_fresh_profile(user_agent=ua)
        try:
            status = human_like_search_flow(driver, query)

            # Капча на входе
            if status == "captcha":
                if CONFIG.get("manual_captcha_mode", True):
                    solved = wait_user_to_solve_captcha(driver, query)
                    if not solved:
                        backoff = backoffs[min(attempt - 1, len(backoffs) - 1)]
                        time.sleep(backoff)
                        continue  # следующий ретрай без логов и скринов
                    # капча решена вручную — продолжаем парсинг
                else:
                    backoff = backoffs[min(attempt - 1, len(backoffs) - 1)]
                    time.sleep(backoff)
                    continue

            # Подстраховка по DOM
            try:
                WebDriverWait(driver, CONFIG.get("element_timeout_sec",10)).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
            except Exception:
                pass
            time.sleep(CONFIG.get("post_load_sleep_sec", 1.0))

            # Если во время ожиданий появилась капча — обрабатываем как выше
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

            # Парсим рекламные/промо блоки
            ads = parse_ads_positions(driver)

            # При необходимости резолвим конечный URL
            if ads and CONFIG.get("resolve_final_url", False):
                for it in ads:
                    try:
                        raw = it.get("url")
                        if not raw:
                            continue
                        final = resolve_final_url_via_selenium(driver, raw)
                        it["url"] = final
                        it["domain"] = normalize_domain(final)
                    except Exception:
                        continue

            # Скрин нормальной выдачи
            ts = timestamp_str()
            safe_name = re.sub(r'[^А-Яа-яA-Za-z0-9_\- ]+', '_', query)[:100]
            local_png = os.path.abspath(f"{safe_name}_{ts}.png")
            fullpage_screenshot(driver, local_png)
            _, drive_link = upload_to_drive(local_png, os.path.basename(local_png))
            try:
                os.remove(local_png)
            except Exception:
                pass

            # Запись в Results
            if not ads:
                ws_results.append_row([ts, query, "", "SUCCESS_NO_ADS", "", driver.current_url, "yandex.ru"])
            else:
                rows = []
                for it in ads:
                    rows.append([
                        ts, query, it["position"], "SUCCESS", it["title"], it["url"], it["domain"]
                    ])
                ws_results.append_rows(rows, value_input_option="USER_ENTERED")

            # Пауза между запросами — и выходим (успех)
            time.sleep(random.uniform(*CONFIG.get("per_query_pause_sec",(30,60))))
            return

        finally:
            safe_quit_driver(driver, tmp_prof)

    return

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


def scheduler_loop():
    """
    Бесконечный цикл: ждём до ближайшего Пн/Пт 10:00 по МСК и запускаем main_once().
    """
    while True:
        now = datetime.now(MOSCOW_TZ)
        wait_sec = seconds_until_next_run(now)
        hours = wait_sec / 3600
        print(f"[SCHEDULER] Сейчас (МСК): {now}. Ждём ~{hours:.2f} ч до следующего запуска.")
        time.sleep(wait_sec)
        run_time = datetime.now(MOSCOW_TZ)
        print(f"[SCHEDULER] Запускаем main_once() в {run_time} (МСК).")
        try:
            main_once()
        except Exception as e:
            # Логируем ошибку, но планировщик живёт дальше
            print(f"[SCHEDULER] Ошибка в main_once: {e}")


def main_once():
    gc = gsheet_client()
    ws_results = ensure_results_worksheet(gc)
    write_run_timestamp()
    queries = read_queries()
    for q in queries:
        run_for_query(q, ws_results)

if __name__ == "__main__":
    scheduler_loop()
