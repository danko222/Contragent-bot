import asyncio
import logging
import os
import json
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, FSInputFile, ReplyKeyboardMarkup, KeyboardButton
from dotenv import load_dotenv
from dadata import Dadata
from database import (
    init_db, try_consume_check, is_admin, get_or_create_user,
    add_check_history, get_check_history, get_user_stats,
    update_last_activity, get_all_active_users, get_clients_stats,
    mark_user_blocked, log_broadcast, increment_api_usage, get_api_usage,
    reset_api_usage, ADMIN_USERNAMES, save_payment, update_payment_status,
    get_payment_by_id, set_premium, add_favorite, remove_favorite, get_favorites, is_favorite
)
from risk_analyzer import format_risk_report, analyze_risks
from affiliates import find_affiliated_companies, format_affiliates_report
from pdf_generator import generate_pdf_report
from api_assist import check_company_extended, format_extended_report
from payment import create_payment, check_payment_status, get_tariff_days, TARIFFS

load_dotenv()
logging.basicConfig(level=logging.INFO)
bot = Bot(token=os.getenv("BOT_TOKEN"))
dp = Dispatcher()

# Хранилище данных для PDF (временное, по user_id)
pdf_data_cache = {}  # {cache_key: {'data': data, 'affiliates': affs}}


# Постоянная клавиатура внизу экрана
def get_persistent_menu():
    """ Клавиатура которая всегда видна внизу чата """
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Проверить ИНН"), KeyboardButton(text="👤 Профиль")],
            [KeyboardButton(text="📜 История"), KeyboardButton(text="⭐ Избранное")],
            [KeyboardButton(text="💎 Подписка"), KeyboardButton(text="❓ Помощь")]
        ],
        resize_keyboard=True,
        is_persistent=True
    )


# === FSM для рассылки ===
class BroadcastStates(StatesGroup):
    waiting_for_message = State()
    confirm = State()


# === Главное меню ===
def get_main_keyboard(username: str = None):
    buttons = [
        [InlineKeyboardButton(text="👤 Мой профиль", callback_data="profile")],
        [InlineKeyboardButton(text="📜 История проверок", callback_data="history"),
         InlineKeyboardButton(text="⭐ Избранное", callback_data="favorites")],
        [InlineKeyboardButton(text="💎 Подписка", callback_data="subscribe")],
        [InlineKeyboardButton(text="❓ Помощь", callback_data="help")]
    ]
    # Админ-кнопки
    if username and is_admin(username):
        buttons.insert(0, [
            InlineKeyboardButton(text="👥 Клиенты", callback_data="admin_clients"),
            InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast")
        ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


@dp.message(Command("start"))
async def cmd_start(msg: Message):
    user = get_or_create_user(msg.from_user.id, msg.from_user.username, msg.from_user.first_name)
    update_last_activity(msg.from_user.id)
    name = msg.from_user.first_name or "друг"
    
    # Отправляем приветствие с постоянной клавиатурой
    await msg.answer(
        f"👋 Привет, **{name}**!\n\n"
        "Я проверяю контрагентов по ИНН и показываю:\n"
        "• 🚦 Светофор рисков\n"
        "• 💰 Финансы компании\n"
        "• 🔗 Связанные компании\n"
        "• 📄 PDF-отчет\n\n"
        f"📊 Осталось проверок: **{user['checks_left']}**\n\n"
        "Отправь **ИНН компании** (10-12 цифр) для начала!",
        parse_mode="Markdown",
        reply_markup=get_persistent_menu()
    )
    # Также отправляем inline-меню
    await msg.answer(
        "📱 **Главное меню:**",
        parse_mode="Markdown",
        reply_markup=get_main_keyboard(msg.from_user.username)
    )


# === Обработчики текстовых кнопок (постоянная клавиатура) ===
@dp.message(lambda m: m.text == "📊 Проверить ИНН")
async def btn_check_inn(msg: Message):
    await msg.answer(
        "🔍 **Проверка контрагента**\n\n"
        "Отправьте **ИНН компании** (10 или 12 цифр):",
        parse_mode="Markdown"
    )


@dp.message(lambda m: m.text == "👤 Профиль")
async def btn_profile(msg: Message):
    await cmd_profile(msg)


@dp.message(lambda m: m.text == "📜 История")
async def btn_history(msg: Message):
    await cmd_history(msg)


@dp.message(lambda m: m.text == "⭐ Избранное")
async def btn_favorites(msg: Message):
    await show_favorites(msg, msg.from_user.id)


@dp.message(lambda m: m.text == "💎 Подписка")
async def btn_subscribe(msg: Message):
    await show_subscribe(msg)


@dp.message(lambda m: m.text == "❓ Помощь")
async def btn_help(msg: Message):
    await msg.answer(
        "❓ **Помощь**\n\n"
        "**Как проверить компанию:**\n"
        "Просто отправьте ИНН (10-12 цифр)\n\n"
        "**Команды:**\n"
        "/start — Главное меню\n"
        "/profile — Ваш профиль\n"
        "/history — История проверок\n"
        "/subscribe — Подписка\n\n"
        "**Связь:** @zegnas",
        parse_mode="Markdown"
    )


@dp.message(Command("profile"))
async def cmd_profile(msg: Message):
    await show_profile(msg)


@dp.callback_query(lambda c: c.data == "profile")
async def cb_profile(callback: CallbackQuery):
    await callback.answer()
    await show_profile(callback.message, callback.from_user.id, callback.from_user.username, callback.from_user.first_name)


async def show_profile(msg: Message, user_id: int = None, username: str = None, first_name: str = None):
    if user_id is None:
        user_id = msg.from_user.id
        username = msg.from_user.username
        first_name = msg.from_user.first_name
    
    user = get_or_create_user(user_id, username, first_name)
    stats = get_user_stats(user_id)
    admin = is_admin(username)
    
    status_emoji = "👑" if admin else ("💎" if user["is_premium"] else "👤")
    status_text = "Администратор" if admin else ("Премиум" if user["is_premium"] else "Базовый")
    
    text = (
        f"**{status_emoji} Ваш профиль**\n\n"
        f"**Статус**: {status_text}\n"
        f"**Осталось проверок**: {'∞ Безлимит' if admin or user['is_premium'] else user['checks_left']}\n"
    )
    
    if user.get("premium_until") and user["is_premium"]:
        text += f"**Подписка до**: {user['premium_until']}\n"
    
    text += (
        f"\n**📊 Статистика**\n"
        f"• Всего проверок: {stats['total_checks']}\n"
        f"• Сегодня: {stats['today_checks']}\n"
    )
    
    if user.get("created_at"):
        try:
            created = datetime.fromisoformat(user["created_at"]).strftime("%d.%m.%Y")
            text += f"• С нами с: {created}\n"
        except:
            pass
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💎 Купить подписку", callback_data="subscribe")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_menu")]
    ])
    
    await msg.answer(text, parse_mode="Markdown", reply_markup=keyboard)


@dp.message(Command("history"))
async def cmd_history(msg: Message):
    await show_history(msg)


@dp.callback_query(lambda c: c.data == "history")
async def cb_history(callback: CallbackQuery):
    await callback.answer()
    await show_history(callback.message, callback.from_user.id)


async def show_history(msg: Message, user_id: int = None):
    if user_id is None:
        user_id = msg.from_user.id
    
    history = get_check_history(user_id, 10)
    
    if not history:
        await msg.answer(
            "📜 **История проверок**\n\n"
            "У вас пока нет проверок.\n"
            "Отправьте ИНН компании, чтобы начать!",
            parse_mode="Markdown"
        )
        return
    
    text = "📜 **Последние проверки:**\n\n"
    for i, (inn, name, risk, checked_at) in enumerate(history, 1):
        try:
            date = datetime.fromisoformat(checked_at).strftime("%d.%m %H:%M")
        except:
            date = checked_at[:16] if checked_at else ""
        
        risk_emoji = {"low": "🟢", "medium": "🟡", "high": "🔴"}.get(risk, "⚪")
        short_name = name[:25] + "..." if len(name) > 25 else name
        text += f"{i}. {risk_emoji} **{short_name}**\n   ИНН: `{inn}` | {date}\n\n"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_menu")]
    ])
    
    await msg.answer(text, parse_mode="Markdown", reply_markup=keyboard)


# === Избранное ===
@dp.callback_query(lambda c: c.data == "favorites")
async def cb_favorites(callback: CallbackQuery):
    await callback.answer()
    await show_favorites(callback.message, callback.from_user.id)


async def show_favorites(msg: Message, user_id: int):
    """Показывает список избранных компаний."""
    favorites_list = get_favorites(user_id, 10)
    
    if not favorites_list:
        await msg.answer(
            "⭐ **Избранные компании**\n\n"
            "У вас пока нет избранных компаний.\n\n"
            "После проверки компании нажмите ⭐ чтобы добавить её в избранное.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_menu")]
            ])
        )
        return
    
    text = "⭐ **Избранные компании:**\n\n"
    buttons = []
    
    for inn, name, added_at in favorites_list:
        short_name = name[:25] + "..." if len(name) > 25 else name
        text += f"• **{short_name}**\n  ИНН: `{inn}`\n\n"
        buttons.append([
            InlineKeyboardButton(text=f"🔍 {short_name}", callback_data=f"recheck_{inn}"),
            InlineKeyboardButton(text="❌", callback_data=f"unfav_{inn}")
        ])
    
    buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_menu")])
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    await msg.answer(text, parse_mode="Markdown", reply_markup=keyboard)


@dp.callback_query(lambda c: c.data.startswith("fav_") and not c.data.startswith("favorites"))
async def cb_add_favorite(callback: CallbackQuery):
    """Добавляет компанию в избранное."""
    inn = callback.data.replace("fav_", "")
    user_id = callback.from_user.id
    
    # Получаем название компании из кеша
    cache_key = f"{user_id}_{inn}"
    cached = pdf_data_cache.get(cache_key, {})
    company_name = cached.get('company_name', 'Компания')
    
    if add_favorite(user_id, inn, company_name):
        await callback.answer("⭐ Добавлено в избранное!", show_alert=False)
    else:
        await callback.answer("Уже в избранном", show_alert=False)


@dp.callback_query(lambda c: c.data.startswith("unfav_"))
async def cb_remove_favorite(callback: CallbackQuery):
    """Удаляет компанию из избранного."""
    inn = callback.data.replace("unfav_", "")
    user_id = callback.from_user.id
    
    if remove_favorite(user_id, inn):
        await callback.answer("❌ Удалено из избранного")
        await show_favorites(callback.message, user_id)
    else:
        await callback.answer("Ошибка удаления")


@dp.callback_query(lambda c: c.data.startswith("recheck_"))
async def cb_recheck(callback: CallbackQuery):
    """Быстрая перепроверка компании из избранного или истории."""
    inn = callback.data.replace("recheck_", "")
    await callback.answer(f"⏳ Проверяю {inn}...")
    
    # Симулируем сообщение с ИНН для повторной проверки
    # Создаём фейковый объект с нужными данными
    from types import SimpleNamespace
    fake_msg = SimpleNamespace()
    fake_msg.text = inn
    fake_msg.from_user = callback.from_user
    fake_msg.answer = callback.message.answer
    
    # Вызываем обычную проверку
    from aiogram.fsm.context import FSMContext
    # Просто отправляем инструкцию пользователю
    await callback.message.answer(
        f"🔍 Для проверки отправьте ИНН:\n\n`{inn}`",
        parse_mode="Markdown"
    )


# === Валидация ИНН ===
@dp.message(lambda m: m.text and m.text.isdigit() and len(m.text) not in [10, 12] and 5 <= len(m.text) <= 15)
async def invalid_inn_handler(msg: Message, state: FSMContext):
    """Обработчик неправильного количества цифр в ИНН."""
    current_state = await state.get_state()
    if current_state is not None:
        return
    
    digit_count = len(msg.text)
    await msg.answer(
        f"❌ **Неверный формат ИНН**\n\n"
        f"Вы ввели: {digit_count} цифр\n"
        f"ИНН должен содержать **10** (юрлицо) или **12** (ИП) цифр.\n\n"
        f"Проверьте и отправьте корректный ИНН.",
        parse_mode="Markdown"
    )


@dp.message(Command("subscribe"))
async def cmd_subscribe(msg: Message):
    await show_subscribe(msg)


@dp.callback_query(lambda c: c.data == "subscribe")
async def cb_subscribe(callback: CallbackQuery):
    await callback.answer()
    await show_subscribe(callback.message)


async def show_subscribe(msg: Message):
    text = (
        "💎 **Премиум подписка**\n\n"
        "**Что даёт подписка:**\n"
        "• ♾️ Безлимитные проверки\n"
        "• 📄 Подробные PDF-отчёты\n"
        "• ⚡ Приоритетная скорость\n"
        "• 🆕 Ранний доступ к новым функциям\n\n"
        "**💰 Стоимость:**\n"
        "• 1 неделя — 199 ₽\n"
        "• 1 месяц — 499 ₽\n"
        "• 3 месяца — 1199 ₽ _(экономия 20%)_\n"
        "• 1 год — 3499 ₽ _(экономия 42%!)_ 🔥\n\n"
        "Выберите тариф для оплаты:"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 1 неделя — 199₽", callback_data="pay_week")],
        [InlineKeyboardButton(text="💳 1 месяц — 499₽", callback_data="pay_month")],
        [InlineKeyboardButton(text="💳 3 месяца — 1199₽", callback_data="pay_3months")],
        [InlineKeyboardButton(text="💳 1 год — 3499₽ 🔥", callback_data="pay_year")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_menu")]
    ])
    
    await msg.answer(text, parse_mode="Markdown", reply_markup=keyboard)


@dp.callback_query(lambda c: c.data.startswith("pay_") and not c.data.startswith("pay_check_") and not c.data.startswith("pay_method_"))
async def cb_pay(callback: CallbackQuery):
    tariff = callback.data.replace("pay_", "")
    
    if tariff not in TARIFFS:
        await callback.answer("❌ Неизвестный тариф", show_alert=True)
        return
    
    # Предлагаем выбрать способ оплаты
    tariff_info = TARIFFS[tariff]
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Банковская карта", callback_data=f"pay_method_{tariff}_bank_card")],
        [InlineKeyboardButton(text="💠 СБП (Быстрый платёж)", callback_data=f"pay_method_{tariff}_sbp")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="subscribe")]
    ])
    
    await callback.message.edit_text(
        f"💎 **Оформление подписки**\n\n"
        f"**Тариф:** {tariff_info['description']}\n"
        f"**Сумма:** {tariff_info['amount']} ₽\n\n"
        "Выберите удобный способ оплаты:",
        parse_mode="Markdown",
        reply_markup=keyboard
    )


@dp.callback_query(lambda c: c.data.startswith("pay_method_"))
async def cb_payment_method(callback: CallbackQuery):
    try:
        # формат: pay_method_{tariff}_{method_type}
        data = callback.data.replace("pay_method_", "")
        parts = data.split("_")
        
        # Обработка sbp и bank_card (у bank_card sbp_card состоит из 2 частей)
        if "sbp" in data:
            tariff = data.replace("_sbp", "")
            method_type = "sbp"
        elif "bank_card" in data:
            tariff = data.replace("_bank_card", "")
            method_type = "bank_card"
        else:
            await callback.answer("❌ Ошибка выбора метода", show_alert=True)
            return

        if tariff not in TARIFFS:
            await callback.answer("❌ Неизвестный тариф", show_alert=True)
            return

        await callback.answer("⏳ Создаю платёж...")
        
        user_id = callback.from_user.id
        tariff_info = TARIFFS[tariff]
        
        # Создаём платёж в ЮKassa с указанным методом
        result = create_payment(user_id, tariff, payment_method_type=method_type)
        
        if not result.get("success"):
            # Не используем Markdown для ошибок, так как текст ошибки может содержать спецсимволы
            await callback.message.answer(
                f"❌ Ошибка создания платежа: {result.get('error', 'Неизвестная ошибка')}\n\n"
                "Попробуйте позже или обратитесь в поддержку: @zegnas"
            )
            return
        
        # Сохраняем платёж в БД
        save_payment(user_id, result["payment_id"], tariff, result["amount"])
        
        # Отправляем ссылку на оплату
        method_name = "СБП" if method_type == "sbp" else "Банковская карта"
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"💳 Оплатить через {method_name}", url=result["confirmation_url"])],
            [InlineKeyboardButton(text="✅ Я оплатил", callback_data=f"pay_check_{result['payment_id']}")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="subscribe")]
        ])
        
        await callback.message.edit_text(
            f"💳 **Оплата подписки**\n\n"
            f"**Тариф:** {tariff_info['description']}\n"
            f"**Сумма:** {result['amount']} ₽\n"
            f"**Способ:** {method_name}\n\n"
            f"Нажмите кнопку ниже для перехода к оплате.\n"
            f"После оплаты нажмите **\"Я оплатил\"** для активации подписки.",
            parse_mode="Markdown",
            reply_markup=keyboard
        )
    except Exception as e:
        logging.error(f"Error in cb_payment_method: {e}")
        await callback.message.answer(f"❌ Произошла ошибка: {str(e)}")


@dp.callback_query(lambda c: c.data.startswith("pay_check_"))
async def cb_check_payment(callback: CallbackQuery):
    payment_id = callback.data.replace("pay_check_", "")
    await callback.answer("⏳ Проверяю оплату...")
    
    user_id = callback.from_user.id
    
    # Проверяем статус платежа в ЮKassa
    result = check_payment_status(payment_id)
    
    if not result.get("success"):
        await callback.message.answer(
            "❌ Ошибка проверки платежа. Попробуйте ещё раз.",
            parse_mode="Markdown"
        )
        return
    
    if result.get("paid") and result.get("status") == "succeeded":
        # Платёж успешен — активируем подписку
        payment_data = get_payment_by_id(payment_id)
        
        if payment_data and payment_data["status"] != "succeeded":
            tariff = payment_data["tariff"]
            days = get_tariff_days(tariff)
            
            # Устанавливаем премиум
            until_date = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d")
            set_premium(user_id, until_date)
            
            # Обновляем статус платежа
            update_payment_status(payment_id, "succeeded")
            
            # Кнопки после успешной оплаты с призывом к действию
            success_keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔍 Проверить ИНН", callback_data="prompt_inn")],
                [InlineKeyboardButton(text="👤 Мой профиль", callback_data="profile")],
                [InlineKeyboardButton(text="📜 История проверок", callback_data="history")]
            ])
            
            await callback.message.answer(
                f"🎉 **Оплата прошла успешно!**\n\n"
                f"Ваша подписка активирована до **{until_date}**\n\n"
                f"Теперь вам доступны:\n"
                f"• ♾️ Безлимитные проверки\n"
                f"• 📄 Все PDF-отчёты\n\n"
                f"Спасибо за покупку! 💎\n\n"
                f"**Отправьте ИНН компании** для проверки:",
                parse_mode="Markdown",
                reply_markup=success_keyboard
            )
        else:
            await callback.message.answer(
                "✅ Эта подписка уже была активирована ранее.",
                parse_mode="Markdown",
                reply_markup=get_main_keyboard(callback.from_user.username)
            )
    elif result.get("status") == "pending":
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Проверить ещё раз", callback_data=f"pay_check_{payment_id}")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="subscribe")]
        ])
        await callback.message.answer(
            "⏳ **Ожидание оплаты**\n\n"
            "Оплата ещё не поступила. Если вы уже оплатили, подождите минуту и нажмите **\"Проверить ещё раз\"**.",
            parse_mode="Markdown",
            reply_markup=keyboard
        )
    elif result.get("status") == "canceled":
        await callback.message.answer(
            "❌ **Платёж отменён**\n\n"
            "Попробуйте оформить подписку заново.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💎 Подписка", callback_data="subscribe")]
            ])
        )
    else:
        await callback.message.answer(
            f"⚠️ Статус платежа: {result.get('status', 'неизвестен')}\n\n"
            "Если возникли проблемы, обратитесь в поддержку: @zegnas",
            parse_mode="Markdown"
        )


@dp.callback_query(lambda c: c.data == "help")
async def cb_help(callback: CallbackQuery):
    await callback.answer()
    await callback.message.answer(
        "❓ **Помощь**\n\n"
        "**Как проверить компанию:**\n"
        "Просто отправьте ИНН (10-12 цифр)\n\n"
        "**Команды:**\n"
        "/start — Главное меню\n"
        "/profile — Ваш профиль\n"
        "/history — История проверок\n"
        "/subscribe — Подписка\n\n"
        "**Связь:** @zegnas",
        parse_mode="Markdown"
    )


@dp.callback_query(lambda c: c.data == "back_to_menu")
async def cb_back(callback: CallbackQuery):
    await callback.answer()
    await callback.message.answer(
        "📱 **Главное меню**\n\nОтправьте ИНН для проверки или выберите действие:",
        parse_mode="Markdown",
        reply_markup=get_main_keyboard(callback.from_user.username)
    )


@dp.callback_query(lambda c: c.data == "prompt_inn")
async def cb_prompt_inn(callback: CallbackQuery):
    """Обработчик кнопки 'Проверить ИНН' — приглашает пользователя ввести ИНН."""
    await callback.answer()
    await callback.message.answer(
        "🔍 **Проверка контрагента**\n\n"
        "Отправьте **ИНН компании** (10 или 12 цифр) для проверки:",
        parse_mode="Markdown"
    )


# === Админ-панель ===
@dp.message(Command("clients"))
async def cmd_clients(msg: Message):
    if not is_admin(msg.from_user.username):
        return
    await show_clients_stats(msg)


@dp.callback_query(lambda c: c.data == "admin_clients")
async def cb_admin_clients(callback: CallbackQuery):
    if not is_admin(callback.from_user.username):
        await callback.answer("⛔ Только для администраторов", show_alert=True)
        return
    await callback.answer()
    await show_clients_stats(callback.message)


async def show_clients_stats(msg: Message):
    stats = get_clients_stats()
    text = (
        "👥 **Статистика клиентов**\n\n"
        f"📊 **Всего пользователей:** {stats['total']}\n"
        f"🟢 **Активных за 7 дней:** {stats['active_7d']}\n"
        f"🔵 **Активных за 30 дней:** {stats['active_30d']}\n"
        f"💎 **Premium:** {stats['premium']}\n"
        f"🚫 **Заблокировали бота:** {stats['blocked']}\n"
    )
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="📊 API баланс", callback_data="admin_api_stats")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_menu")]
    ])
    await msg.answer(text, parse_mode="Markdown", reply_markup=keyboard)


@dp.message(Command("api_stats"))
async def cmd_api_stats(msg: Message):
    if not is_admin(msg.from_user.username):
        return
    await show_api_stats(msg)


@dp.callback_query(lambda c: c.data == "admin_api_stats")
async def cb_admin_api_stats(callback: CallbackQuery):
    if not is_admin(callback.from_user.username):
        await callback.answer("⛔ Только для администраторов", show_alert=True)
        return
    await callback.answer()
    await show_api_stats(callback.message)


async def show_api_stats(msg: Message):
    usage = get_api_usage()
    if not usage:
        await msg.answer("❌ Нет данных об использовании API")
        return
    
    # Определяем цвет статуса
    remaining = usage['remaining']
    if remaining <= usage['alert_threshold']:
        status = "🔴 КРИТИЧЕСКИ МАЛО!"
    elif remaining <= usage['alert_threshold'] * 5:
        status = "🟡 Внимание"
    else:
        status = "🟢 Нормально"
    
    # Прогресс-бар
    used_percent = usage['usage_percent']
    bar_length = 10
    filled = int(bar_length * used_percent / 100)
    bar = "█" * filled + "░" * (bar_length - filled)
    
    text = (
        f"📊 **Баланс API: За Честный Бизнес**\n\n"
        f"**Статус:** {status}\n\n"
        f"**Лимит:** {usage['total_limit']:,} запросов\n"
        f"**Использовано:** {usage['used_count']:,} ({used_percent}%)\n"
        f"**Осталось:** {remaining:,}\n\n"
        f"[{bar}] {used_percent}%\n\n"
        f"⚠️ **Порог оповещения:** {usage['alert_threshold']:,}\n"
        f"📅 **Дата сброса:** {usage['reset_date'] or 'Не установлена'}"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Сбросить счётчик", callback_data="reset_api_usage")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_clients")]
    ])
    await msg.answer(text, parse_mode="Markdown", reply_markup=keyboard)


@dp.callback_query(lambda c: c.data == "reset_api_usage")
async def cb_reset_api_usage(callback: CallbackQuery):
    if not is_admin(callback.from_user.username):
        await callback.answer("⛔ Только для администраторов", show_alert=True)
        return
    
    reset_api_usage()
    await callback.answer("✅ Счётчик сброшен!")
    await show_api_stats(callback.message)


@dp.message(Command("broadcast"))
async def cmd_broadcast(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.username):
        return
    await start_broadcast(msg, state)


@dp.callback_query(lambda c: c.data == "admin_broadcast")
async def cb_admin_broadcast(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.username):
        await callback.answer("⛔ Только для администраторов", show_alert=True)
        return
    await callback.answer()
    await start_broadcast(callback.message, state)


async def start_broadcast(msg: Message, state: FSMContext):
    await state.set_state(BroadcastStates.waiting_for_message)
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_broadcast")]
    ])
    await msg.answer(
        "📢 **Рассылка сообщений**\n\n"
        "Введите текст сообщения, которое будет отправлено всем пользователям.\n"
        "Поддерживается Markdown форматирование.",
        parse_mode="Markdown",
        reply_markup=keyboard
    )


@dp.callback_query(lambda c: c.data == "cancel_broadcast")
async def cb_cancel_broadcast(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.answer("Рассылка отменена")
    await callback.message.answer(
        "📱 **Главное меню**",
        parse_mode="Markdown",
        reply_markup=get_main_keyboard(callback.from_user.username)
    )


@dp.message(BroadcastStates.waiting_for_message)
async def process_broadcast_message(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.username):
        await state.clear()
        return
    
    users = get_all_active_users()
    await state.update_data(message_text=msg.text, user_count=len(users))
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Отправить", callback_data="confirm_broadcast")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_broadcast")]
    ])
    
    await msg.answer(
        f"📢 **Подтверждение рассылки**\n\n"
        f"Получателей: **{len(users)}** пользователей\n\n"
        f"───────────────\n"
        f"{msg.text}\n"
        f"───────────────\n\n"
        "Отправить?",
        parse_mode="Markdown",
        reply_markup=keyboard
    )
    await state.set_state(BroadcastStates.confirm)


@dp.callback_query(lambda c: c.data == "confirm_broadcast", BroadcastStates.confirm)
async def confirm_broadcast(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.username):
        await state.clear()
        return
    
    await callback.answer()
    data = await state.get_data()
    message_text = data.get("message_text", "")
    
    users = get_all_active_users()
    total = len(users)
    success = 0
    failed = 0
    
    progress_msg = await callback.message.answer(f"⏳ Рассылка... (0/{total})")
    
    for i, (user_id, username, first_name) in enumerate(users):
        try:
            await bot.send_message(user_id, message_text, parse_mode="Markdown")
            success += 1
        except Exception as e:
            failed += 1
            if "blocked" in str(e).lower() or "deactivated" in str(e).lower():
                mark_user_blocked(user_id)
        
        # Обновляем прогресс каждые 10 пользователей
        if (i + 1) % 10 == 0:
            try:
                await progress_msg.edit_text(f"⏳ Рассылка... ({i + 1}/{total})")
            except:
                pass
        
        # Небольшая задержка чтобы не превышать лимиты Telegram
        await asyncio.sleep(0.05)
    
    # Логируем рассылку
    log_broadcast(message_text, total, success, failed)
    
    await progress_msg.delete()
    await callback.message.answer(
        f"✅ **Рассылка завершена!**\n\n"
        f"• Успешно: {success}\n"
        f"• Не доставлено: {failed}",
        parse_mode="Markdown",
        reply_markup=get_main_keyboard(callback.from_user.username)
    )
    await state.clear()


# === Обработчик PDF ===
@dp.callback_query(lambda c: c.data.startswith("pdf_"))
async def cb_download_pdf(callback: CallbackQuery):
    await callback.answer("📄 Генерирую PDF...")
    
    inn = callback.data.replace("pdf_", "")
    user_id = callback.from_user.id
    
    # Получаем закешированные данные
    cache_key = f"{user_id}_{inn}"
    if cache_key not in pdf_data_cache:
        await callback.message.answer("❌ Данные устарели. Отправьте ИНН повторно.")
        return
    
    cached = pdf_data_cache[cache_key]
    data = cached.get('data', cached)  # Обратная совместимость
    affiliates = cached.get('affiliates', None)
    extended_data = cached.get('extended', None)
    
    try:
        filepath = generate_pdf_report(data, user_id, affiliates, extended_data)
        pdf_file = FSInputFile(filepath)
        await callback.message.answer_document(
            pdf_file,
            caption=f"📄 Отчет о проверке ИНН {inn}"
        )
        # Удаляем файл после отправки
        os.remove(filepath)
    except Exception as e:
        logging.error(f"PDF generation error: {e}")
        await callback.message.answer(f"❌ Ошибка генерации PDF: {str(e)[:100]}")


# === Проверка компании ===
@dp.message(lambda m: m.text and m.text.isdigit() and len(m.text) in [10, 12])
async def check_company(msg: Message, state: FSMContext):
    # Пропускаем если пользователь в FSM состоянии (например, рассылка)
    current_state = await state.get_state()
    if current_state is not None:
        return
    
    uid = msg.from_user.id
    uname = msg.from_user.username
    admin = is_admin(uname)
    
    if not admin and not try_consume_check(uid):
        await msg.answer(
            "🚫 **Лимит исчерпан!**\n\n"
            "У вас закончились бесплатные проверки.\n"
            "Оформите подписку для безлимитного доступа!",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💎 Купить подписку", callback_data="subscribe")]
            ])
        )
        return
    
    user = get_or_create_user(uid, uname, msg.from_user.first_name)
    
    # Определяем статус проверок
    if admin:
        left = "👑 Безлимит"
    elif user['is_premium']:
        left = "💎 Безлимит"
    else:
        left = f"Осталось: {user['checks_left']}"
    
    await msg.answer(f"⏳ Ищу компанию... ({left})")
    
    try:
        d = Dadata(os.getenv("DADATA_API_KEY"))
        result = d.find_by_id("party", msg.text)
        
        if not result:
            await msg.answer("❌ Компания с таким ИНН не найдена.")
            return
        
        data = result[0]["data"]
        inn = data.get("inn", msg.text)
        company_name = data.get("name", {}).get("short_with_opf", "Неизвестно")
        
        # Анализ рисков
        risk_emoji, risk_text, factors = analyze_risks(data)
        risk_level = "high" if "Высокий" in risk_text else ("medium" if "Средний" in risk_text else "low")
        
        # Сохраняем в историю
        add_check_history(uid, inn, company_name, risk_level)
        
        # Базовый отчёт (название, светофор, финансы)
        report = format_risk_report(data)
        
        # Получаем связанные компании
        mgr = data.get("management", {}).get("name", "")
        affs = []
        if mgr:
            affs = find_affiliated_companies(mgr, exclude_inn=inn)
        
        # Расширенная проверка (ФССП, Арбитраж, ФНС)
        extended_data = check_company_extended(inn, mgr)
        extended_report = format_extended_report(extended_data)
        
        # Добавляем расширенные данные ПОСЛЕ финансов
        report += extended_report
        
        # Добавляем связанные компании
        if affs:
            report += format_affiliates_report(mgr, affs)
        
        # Добавляем директора, адрес, ОКВЭД и дату в конце
        from okved import get_okved_name
        address = data.get("address", {}).get("value", "Не указан") if isinstance(data.get("address"), dict) else "Не указан"
        okved_code = data.get("okved", "Н/Д")
        okved_name = get_okved_name(okved_code)
        okved_full = f"{okved_code}" + (f" - {okved_name}" if okved_name else "")
        
        from datetime import datetime
        report += f"\n\n**👤 Руководитель:** {mgr or 'Не указан'}"
        report += f"\n**📍 Адрес:** {address}"
        report += f"\n**🏭 ОКВЭД:** {okved_full}"
        report += f"\n\n_Отчет сформирован: {datetime.now().strftime('%d.%m.%Y %H:%M')}_"
        
        # Кешируем данные для PDF (включая affiliates и extended)
        cache_key = f"{uid}_{inn}"
        pdf_data_cache[cache_key] = {'data': data, 'affiliates': affs, 'extended': extended_data, 'company_name': company_name}
        
        # Кнопки для PDF и избранного
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📄 Скачать PDF-отчет", callback_data=f"pdf_{inn}")],
            [InlineKeyboardButton(text="⭐ В избранное", callback_data=f"fav_{inn}")]
        ])
        
        await msg.answer(report, parse_mode="Markdown", reply_markup=keyboard)
        
    except Exception as e:
        logging.error(f"Error checking company: {e}")
        await msg.answer(f"❌ Ошибка при проверке: {str(e)[:100]}")


async def main():
    init_db()
    print("--- Бот запущен ---")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
