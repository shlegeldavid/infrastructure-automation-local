import requests
import datetime
import logging
from telegram import Bot
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import asyncio
import os

# ---------------------------
# CONFIG
# ---------------------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN_RUVDS")
CHAT_ID = os.getenv("CHAT_ID_RUVDS")
RUVDS_TOKEN = os.getenv("RUVDS_TOKEN")

API_URL = "https://api.ruvds.com/v2"

bot = Bot(token=TELEGRAM_TOKEN)

logging.basicConfig(level=logging.INFO)


# ---------------------------
# API CALLS
# ---------------------------
def api_get(path: str):
    headers = {"Authorization": f"Bearer {RUVDS_TOKEN}"}
    r = requests.get(API_URL + path, headers=headers)
    r.raise_for_status()
    return r.json()


def get_servers():
    return api_get("/servers?get_paid_till=true")["servers"]


def get_cost(server_id: int):
    return api_get(f"/servers/{server_id}/cost")["cost_rub"]

def get_ip(server_id: int):
    data = api_get(f"/servers/{server_id}/networks")
    v4 = data.get("v4", [])
    if not v4:
        return "нет IP"
    return v4[0]["ip_address"]


# ---------------------------
# MAIN LOGIC
# ---------------------------
async def check_servers():
    
    servers = get_servers()
    today = datetime.datetime.utcnow().date()

    print("DEBUG SERVERS:", servers)

    for s in servers:
        server_id = s["virtual_server_id"]
        paid_till_raw = s.get("paid_till")

        if not paid_till_raw:
            continue

        paid_till = datetime.datetime.fromisoformat(
            paid_till_raw.replace("Z", "+00:00")
        ).date()

        cost = get_cost(server_id)
        days_left = (paid_till - today).days
        ip = get_ip(server_id)

        print("DEBUG PAY DATE:", paid_till, type(paid_till))
        print("DAYS LEFT:", days_left)
        print("DEBUG IP:", ip)

        # 1) За день до конца оплаты
        if (0 < days_left <= 5):
            msg = (
                f"⚠️ Через {days_left} дней, ({paid_till.strftime('%d.%m.%Y')}) "
                f"у сервера с IP {ip} заканчивается оплата.\n"
                f"Необходимо пополнить баланс на {cost} ₽."
            )
            await bot.send_message(chat_id=CHAT_ID, text=msg)

        # 2) Просроченный сервер → пишем каждый день
        if days_left < 0:
            overdue_days = abs(days_left)
            msg = (
                f"❗ Оплата сервера #{server_id} истекла {paid_till.strftime('%d.%m.%Y')}.\n"
                f"Сервер не оплачен уже {overdue_days} дн.\n"
                f"Стоимость продления: {cost} ₽."
            )
            await bot.send_message(chat_id=CHAT_ID, text=msg)


# ---------------------------
# SCHEDULER — запуск каждый день в 07:00 UTC (10:00 МСК)
# ---------------------------
async def main():
    scheduler = AsyncIOScheduler()
    scheduler.add_job(check_servers, "cron", hour=10, minute=00) 
    scheduler.start()

    print("Bot started.")
    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    asyncio.run(main())
