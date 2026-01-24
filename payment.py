"""
Модуль для интеграции с ЮKassa
"""
import uuid
import logging
from yookassa import Configuration, Payment
from yookassa.domain.notification import WebhookNotification
import os
from dotenv import load_dotenv

load_dotenv()

# Настройка ЮKassa
YOOKASSA_SHOP_ID = os.getenv("YOOKASSA_SHOP_ID")
YOOKASSA_SECRET_KEY = os.getenv("YOOKASSA_SECRET_KEY")

if YOOKASSA_SHOP_ID and YOOKASSA_SECRET_KEY:
    Configuration.account_id = YOOKASSA_SHOP_ID
    Configuration.secret_key = YOOKASSA_SECRET_KEY
else:
    logging.warning("YooKassa credentials not set in .env file")

# Тарифы
TARIFFS = {
    "week": {
        "amount": "199.00",
        "description": "Подписка на 1 неделю",
        "days": 7
    },
    "month": {
        "amount": "499.00", 
        "description": "Подписка на 1 месяц",
        "days": 30
    },
    "3months": {
        "amount": "1199.00",
        "description": "Подписка на 3 месяца",
        "days": 90
    }
}


def create_payment(user_id: int, tariff: str, return_url: str = None, payment_method_type: str = None) -> dict:
    if tariff not in TARIFFS:
        raise ValueError(f"Unknown tariff: {tariff}")
    
    tariff_info = TARIFFS[tariff]
    idempotence_key = str(uuid.uuid4())
    
    payment_data = {
        "amount": {"value": tariff_info["amount"], "currency": "RUB"},
        "confirmation": {"type": "redirect", "return_url": return_url or "https://t.me/ContragentCheckBot"},
        "capture": True,
        "description": tariff_info["description"],
        "metadata": {"user_id": str(user_id), "tariff": tariff}
    }

    # Если указан конкретный метод оплаты (sbp или bank_card)
    if payment_method_type:
        payment_data["payment_method_data"] = {
            "type": payment_method_type
        }
    
    logging.info(f"Creating payment with data: {payment_data}")

    try:
        payment = Payment.create(payment_data, idempotence_key)
        return {
            "success": True,
            "payment_id": payment.id,
            "confirmation_url": payment.confirmation.confirmation_url,
            "amount": tariff_info["amount"],
            "tariff": tariff
        }
    except Exception as e:
        logging.error(f"Error creating payment: {e}")
        return {"success": False, "error": str(e)}


def check_payment_status(payment_id: str) -> dict:
    try:
        payment = Payment.find_one(payment_id)
        return {"success": True, "status": payment.status, "paid": payment.paid, "metadata": payment.metadata}
    except Exception as e:
        logging.error(f"Error checking payment: {e}")
        return {"success": False, "error": str(e)}


def get_tariff_days(tariff: str) -> int:
    return TARIFFS.get(tariff, {}).get("days", 0)


def get_tariff_info(tariff: str) -> dict:
    return TARIFFS.get(tariff, {})
