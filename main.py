import asyncio
import logging
import os
import json
from datetime import datetime
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, FSInputFile
from dotenv import load_dotenv
from dadata import Dadata
from database import (
    init_db, try_consume_check, is_admin, get_or_create_user,
    add_check_history, get_check_history, get_user_stats,
    update_last_activity, get_all_active_users, get_clients_stats,
    mark_user_blocked, log_broadcast
)
from risk_analyzer import format_risk_report, analyze_risks
from affiliates import find_affiliated_companies, format_affiliates_report
from pdf_generator import generate_pdf_report
from api_assist import check_company_extended, format_extended_report

load_dotenv()
logging.basicConfig(level=logging.INFO)
bot = Bot(token=os.getenv("BOT_TOKEN"))
dp = Dispatcher()

# Хранилище данных для PDF (временное, по user_id)
pdf_data_cache = {}  # {cache_key: {'data': data, 'affiliates': affs}}


# === FSM для рассылки ===
class BroadcastStates(StatesGroup):
    waiting_for_message = State()
    confirm = State()


# === Главное меню ===
def get_main_keyboard(username: str = None):
    buttons = [
        [InlineKeyboardButton(text="👤 Мой профиль", callback_data="profile")],
        [InlineKeyboardButton(text="📜 История проверок", callback_data="history")],
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
        reply_markup=get_main_keyboard(msg.from_user.username)
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
        "• 3 месяца — 999 ₽\n\n"
        "_Оплата через ЮKassa (скоро)_"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Оплатить 1 месяц — 499₽", callback_data="pay_month")],
        [InlineKeyboardButton(text="💳 Оплатить 3 месяца — 999₽", callback_data="pay_3months")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_menu")]
    ])
    
    await msg.answer(text, parse_mode="Markdown", reply_markup=keyboard)


@dp.callback_query(lambda c: c.data.startswith("pay_"))
async def cb_pay(callback: CallbackQuery):
    await callback.answer("⏳ Платежи скоро будут доступны!", show_alert=True)


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
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_menu")]
    ])
    await msg.answer(text, parse_mode="Markdown", reply_markup=keyboard)


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
    left = "👑 Безлимит" if admin else f"Осталось: {user['checks_left']}"
    
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
        pdf_data_cache[cache_key] = {'data': data, 'affiliates': affs, 'extended': extended_data}
        
        # Кнопка для PDF
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📄 Скачать PDF-отчет", callback_data=f"pdf_{inn}")]
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
