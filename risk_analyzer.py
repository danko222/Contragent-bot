"""
Модуль анализа рисков контрагента.
Формирует "светофор" по нескольким факторам.
"""

from datetime import datetime
from typing import Dict, Any, List, Tuple


def calculate_age_days(timestamp_ms) -> int:
    """Вычисляет количество дней с даты (timestamp в миллисекундах)."""
    if not timestamp_ms:
        return 0
    try:
        date = datetime.fromtimestamp(int(timestamp_ms) / 1000)
        return (datetime.now() - date).days
    except:
        return 0


def format_date_from_timestamp(timestamp_ms) -> str:
    """Форматирует timestamp в читаемую дату."""
    if not timestamp_ms:
        return "Неизвестно"
    try:
        date = datetime.fromtimestamp(int(timestamp_ms) / 1000)
        return date.strftime('%d.%m.%Y')
    except:
        return "Неизвестно"


def analyze_risks(data: Dict[str, Any]) -> Tuple[str, str, List[Dict[str, Any]]]:
    """
    Анализирует данные компании и возвращает:
    - emoji светофора (🟢/🟡/🔴)
    - текстовый статус
    - список факторов с их оценками
    """
    factors = []
    critical_issues = 0
    warnings = 0
    
    # 1. Статус компании
    status = data.get('state', {}).get('status', 'UNKNOWN')
    if status == 'ACTIVE':
        factors.append({"name": "Статус", "value": "Действующая", "emoji": "✓"})
    elif status == 'LIQUIDATING':
        factors.append({"name": "Статус", "value": "В процессе ликвидации", "emoji": "✗"})
        critical_issues += 1
    else:
        factors.append({"name": "Статус", "value": "Ликвидирована/Банкрот", "emoji": "✗"})
        critical_issues += 1
    
    # 2. Возраст компании
    reg_date = data.get('state', {}).get('registration_date')
    age_days = calculate_age_days(reg_date)
    age_years = age_days // 365
    
    if age_days < 180:  # Меньше 6 месяцев
        factors.append({"name": "Возраст", "value": f"{age_days} дней", "emoji": "✗"})
        critical_issues += 1
    elif age_days < 365:  # Меньше года
        factors.append({"name": "Возраст", "value": f"{age_days} дней", "emoji": "⚠"})
        warnings += 1
    else:
        factors.append({"name": "Возраст", "value": f"{age_years} лет", "emoji": "✓"})
    
    # 3. Недостоверные сведения (общий флаг)
    invalid = data.get('invalid')
    if invalid:
        factors.append({"name": "Достоверность", "value": "Есть недостоверные сведения!", "emoji": "✗"})
        critical_issues += 1
    else:
        factors.append({"name": "Достоверность", "value": "Сведения достоверны", "emoji": "✓"})
    
    # 4. Проверка адреса
    address_data = data.get('address', {})
    if isinstance(address_data, dict):
        address_qc = address_data.get('data', {}).get('qc') if isinstance(address_data.get('data'), dict) else None
        if address_qc is not None and address_qc != 0:
            factors.append({"name": "Адрес", "value": "Проблемы с адресом", "emoji": "⚠"})
            warnings += 1
        else:
            factors.append({"name": "Адрес", "value": "Адрес подтвержден", "emoji": "✓"})
    
    # 5. Уставный капитал
    capital = data.get('capital', {})
    if isinstance(capital, dict):
        capital_value = capital.get('value', 0) or 0
        if capital_value < 10000:
            factors.append({"name": "Уставный капитал", "value": f"{capital_value:,.0f} ₽".replace(",", " "), "emoji": "⚠"})
            warnings += 1
        else:
            factors.append({"name": "Уставный капитал", "value": f"{capital_value:,.0f} ₽".replace(",", " "), "emoji": "✓"})
    
    # 6. Руководитель и дата назначения
    manager = data.get('management', {})
    if manager and manager.get('name'):
        # Проверяем дату назначения, если доступна
        # DaData может не возвращать эту дату напрямую, используем state.actuality_date как приближение
        # или ищем в managers если есть
        managers_list = data.get('managers', [])
        manager_date = None
        
        if managers_list:
            for m in managers_list:
                if m.get('fio', {}).get('surname') in manager.get('name', ''):
                    manager_date = m.get('date')
                    break
        
        if not manager_date:
            # Пробуем получить из других полей
            manager_date = data.get('state', {}).get('actuality_date')
        
        if manager_date:
            manager_days = calculate_age_days(manager_date)
            date_str = format_date_from_timestamp(manager_date)
            
            if manager_days < 90:  # Меньше 3 месяцев
                factors.append({"name": "Руководитель", "value": f"Назначен {date_str} (недавно!)", "emoji": "⚠"})
                warnings += 1
            elif manager_days < 365:  # Меньше года
                factors.append({"name": "Руководитель", "value": f"Назначен {date_str}", "emoji": "✓"})
            else:
                years = manager_days // 365
                factors.append({"name": "Руководитель", "value": f"Назначен {date_str} ({years} лет)", "emoji": "✓"})
        else:
            factors.append({"name": "Руководитель", "value": "Указан (дата неизвестна)", "emoji": "✓"})
    else:
        factors.append({"name": "Руководитель", "value": "Не указан", "emoji": "⚠"})
        warnings += 1
    
    # Итоговый индикатор риска (галочки на цветном фоне)
    if critical_issues > 0:
        overall_emoji = "❌"
        overall_text = "Высокий риск"
    elif warnings >= 2:
        overall_emoji = "⚠️"
        overall_text = "Средний риск"
    else:
        overall_emoji = "✅"
        overall_text = "Низкий риск"
    
    return overall_emoji, overall_text, factors


def format_money(value) -> str:
    """Форматирует денежную сумму."""
    if value is None:
        return "Н/Д"
    try:
        val = float(value)
        if val >= 1_000_000_000:
            return f"{val/1_000_000_000:.1f} млрд ₽"
        elif val >= 1_000_000:
            return f"{val/1_000_000:.1f} млн ₽"
        elif val >= 1_000:
            return f"{val/1_000:.0f} тыс ₽"
        else:
            return f"{val:.0f} ₽"
    except:
        return "Данных нет"


def get_financial_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Извлекает финансовые данные из ответа DaData."""
    finance = data.get('finance', {})
    if not finance:
        return {"revenue": None, "income": None, "expense": None, "profit": None, "year": None}
    
    return {
        "revenue": finance.get('revenue'),  # Выручка (код 2110)
        "income": finance.get('income'),    # Доходы
        "expense": finance.get('expense'),  # Расходы
        "profit": finance.get('profit'),    # Прибыль = income - expense
        "year": finance.get('year'),        # Год отчетности
    }


def format_risk_report(data: Dict[str, Any]) -> str:
    """Форматирует отчет о рисках для отправки в Telegram."""
    name = data.get('name', {}).get('full_with_opf') or data.get('name', {}).get('short_with_opf') or 'Неизвестно'
    inn = data.get('inn', 'Н/Д')
    address = data.get('address', {}).get('value', 'Не указан') if isinstance(data.get('address'), dict) else 'Не указан'
    manager_name = data.get('management', {}).get('name', 'Не указан')
    
    # ОКВЭД с расшифровкой из локального справочника
    from okved import get_okved_name
    okved_code = data.get('okved', 'Н/Д')
    okved_name = get_okved_name(okved_code)
    okved_full = f"{okved_code}" + (f" - {okved_name}" if okved_name else "")
    
    overall_emoji, overall_text, factors = analyze_risks(data)
    
    # Получаем финансовые данные
    finance = get_financial_data(data)
    
    # Формируем сообщение
    lines = [
        f"{overall_emoji} **{overall_text.upper()}**",
        f"",
        f"**{name}**",
        f"ИНН: `{inn}`",
        f"",
        f"**📊 Светофор рисков:**",
    ]
    
    for factor in factors:
        lines.append(f"  {factor['emoji']} {factor['name']}: {factor['value']}")
    
    # Добавляем финансовые данные
    lines.append(f"")
    lines.append(f"**💰 Финансы" + (f" ({finance['year']} г.):**" if finance['year'] else ":**"))
    
    if finance['revenue'] is not None:
        lines.append(f"  📈 Выручка: {format_money(finance['revenue'])}")
    else:
        lines.append(f"  📈 Выручка: Данных нет")
    
    # Расчет прибыли
    if finance['income'] is not None and finance['expense'] is not None:
        profit = finance['income'] - finance['expense']
        profit_emoji = "📉" if profit < 0 else "📈"
        lines.append(f"  {profit_emoji} Прибыль: {format_money(profit)}")
    elif finance['profit'] is not None:
        profit = finance['profit']
        profit_emoji = "📉" if profit < 0 else "📈"
        lines.append(f"  {profit_emoji} Прибыль: {format_money(profit)}")
    else:
        lines.append(f"  📊 Прибыль: Данных нет")
    
    # Возвращаем базовый отчёт (affiliates и footer добавляются в main.py)
    return "\n".join(lines)

