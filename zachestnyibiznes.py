"""
Модуль для работы с API ЗАЧЕСТНЫЙБИЗНЕС
https://zachestnyibiznesapi.ru/docs

Получает полные данные о компании: карточка, финансы, ФССП, арбитраж, рейтинг.
"""

import os
import requests
from typing import Dict, Any, Optional
from datetime import datetime

BASE_URL = "https://zachestnyibiznesapi.ru/paid/data"

# Оптимальный набор методов
DEFAULT_METHODS = "card,fs-fns,fssp-list,rating,court-arbitration,affilation-company,contacts"


def get_api_key() -> str:
    """Получает API ключ (ленивая загрузка после load_dotenv)."""
    return os.getenv("ZACHESTNYIBIZNES_API_KEY", "")


def get_company_data(inn: str, methods: str = None) -> Dict[str, Any]:
    """
    Получает полные данные о компании одним запросом.
    
    Args:
        inn: ИНН компании (10 или 12 цифр)
        methods: Список методов через запятую (по умолчанию оптимальный набор)
    
    Returns:
        Словарь с данными компании или ошибкой
    """
    api_key = get_api_key()
    if not api_key:
        return {"error": "API key not configured", "success": False}
    
    methods = methods or DEFAULT_METHODS
    
    try:
        url = f"{BASE_URL}/multiple-methods"
        params = {
            "id": inn,
            "api_key": api_key,
            "list": methods,
            "_format": "json"
        }
        
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Проверяем общую ошибку (например, "Данные не найдены")
        if data.get("status") == "260":
            return {"error": "Компания не найдена", "success": False}
        if data.get("status") == "error" or (data.get("message") and "ошибка" in data.get("message", "").lower()):
            return {"error": data.get("message", "Unknown error"), "success": False}
        
        # multiple-methods возвращает каждый метод как ключ на верхнем уровне
        # Например: {"card": {...}, "fs-fns": {...}, "rating": {...}}
        
        return {
            "success": True,
            "status": "found",
            "data": data,  # Передаём весь ответ, парсеры сами извлекут нужные данные
            "raw": data
        }
        
    except requests.exceptions.Timeout:
        return {"error": "Превышено время ожидания", "success": False}
    except requests.exceptions.RequestException as e:
        return {"error": f"Ошибка запроса: {str(e)}", "success": False}
    except Exception as e:
        return {"error": f"Неожиданная ошибка: {str(e)}", "success": False}


def get_single_method(inn: str, method: str) -> Dict[str, Any]:
    """Получает данные одним методом (для отладки или экономии)."""
    api_key = get_api_key()
    if not api_key:
        return {"error": "API key not configured", "success": False}
    
    try:
        url = f"{BASE_URL}/{method}"
        params = {
            "id": inn,
            "api_key": api_key,
            "_format": "json"
        }
        
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        return {"success": True, "data": response.json()}
        
    except Exception as e:
        return {"error": str(e), "success": False}


# ============ Парсеры данных ============

def parse_card(data: Dict) -> Dict[str, Any]:
    """Парсит основные сведения из card."""
    # Обрабатываем разные форматы ответа
    card_data = data.get("card", {})
    
    # Если card содержит body.docs (формат single method)
    if "body" in card_data:
        body = card_data.get("body", {})
        if "docs" in body and isinstance(body["docs"], list) and len(body["docs"]) > 0:
            card = body["docs"][0]
        else:
            card = body
    else:
        card = card_data
    
    # Получаем руководителя из массива Руководители
    directors = card.get("Руководители", [])
    director_name = ""
    director_inn = ""
    director_date = ""
    if directors and isinstance(directors, list) and len(directors) > 0:
        first_director = directors[0]
        director_name = first_director.get("fl", "") or first_director.get("fio", "")
        director_inn = first_director.get("inn", "")
        director_date = first_director.get("date", "")
    
    # Получаем адрес
    address_data = card.get("Адрес", {})
    if isinstance(address_data, dict):
        address = address_data.get("АдресПолн", "") or address_data.get("value", "")
    elif isinstance(address_data, str):
        address = address_data
    else:
        address = ""
    
    return {
        "name": card.get("НаимЮЛСокр", "") or card.get("name", ""),
        "full_name": card.get("НаимЮЛПолн", "") or card.get("fullName", ""),
        "inn": card.get("ИНН", ""),
        "ogrn": card.get("ОГРН", ""),
        "kpp": card.get("КПП", ""),
        "status": card.get("Активность", "") or card.get("СвСтатус", ""),
        "reg_date": card.get("ДатаОГРН", "") or card.get("ОбрДата", ""),
        "address": address,
        "director": director_name,
        "director_inn": director_inn,
        "director_date": director_date,
        "okved": card.get("КодОКВЭД", ""),
        "okved_name": card.get("НаимОКВЭД", ""),
        "capital": card.get("СумКап", 0),
        "employees": card.get("ЧислСотруд", 0),
    }


def parse_finances(data: Dict) -> Dict[str, Any]:
    """Парсит финансовые данные из fs-fns (бухотчётность ФНС)."""
    fs = data.get("fs-fns", {}).get("body", {})
    if not fs:
        fs = data.get("fs-fns", {})
    
    # Новая структура: Документ.ФинРез
    doc = fs.get("Документ", {})
    if not doc:
        # Старая структура на случай другого формата
        years = fs.get("Года", [])
        if years and isinstance(years, list):
            latest = years[0]
            return {
                "has_data": True,
                "year": latest.get("Год", ""),
                "revenue": latest.get("Выручка", 0),
                "profit": latest.get("Прибыль", 0),
                "taxes_paid": latest.get("УплНалога", 0),
                "tax_debt": latest.get("ЗадолжНалога", 0),
                "employees": latest.get("СрЧислРаб", 0),
            }
        return {"has_data": False}
    
    # Получаем год отчёта
    attrs = doc.get("@attributes", {})
    year = attrs.get("ОтчетГод", "")
    
    # Финансовые результаты
    fin_res = doc.get("ФинРез", {})
    
    # Выручка (в тыс. рублей)
    revenue_data = fin_res.get("Выруч", {}).get("@attributes", {})
    revenue = float(revenue_data.get("СумОтч", 0) or 0) * 1000  # тыс -> рубли
    revenue_prev = float(revenue_data.get("СумПред", 0) or 0) * 1000
    
    # Чистая прибыль
    profit_data = fin_res.get("ЧистПрибУб", {}).get("@attributes", {})
    profit = float(profit_data.get("СумОтч", 0) or 0) * 1000
    profit_prev = float(profit_data.get("СумПред", 0) or 0) * 1000
    
    # Налог на прибыль (уплачено)
    tax_data = fin_res.get("ТекНалПриб", {}).get("@attributes", {})
    taxes_paid = float(tax_data.get("СумОтч", 0) or 0) * 1000
    
    # Сотрудники из СвНП если есть
    sv_np = doc.get("СвНП", {})
    employees = sv_np.get("@attributes", {}).get("СрЧислРаб", 0) or 0
    
    return {
        "has_data": revenue > 0 or profit != 0,
        "year": year,
        "revenue": revenue,
        "revenue_prev": revenue_prev,
        "profit": profit,
        "profit_prev": profit_prev,
        "taxes_paid": taxes_paid,
        "tax_debt": 0,
        "employees": employees,
    }


def parse_fssp(data: Dict) -> Dict[str, Any]:
    """Парсит данные ФССП."""
    fssp = data.get("fssp-list", {}).get("body", {})
    if not fssp:
        fssp = data.get("fssp-list", {})
    
    items = fssp.get("Записи", [])
    total_sum = sum(float(item.get("СуммаДолга", 0) or 0) for item in items)
    
    return {
        "count": len(items),
        "total_sum": total_sum,
        "items": items[:5],  # Первые 5 для отображения
    }


def parse_rating(data: Dict) -> Dict[str, Any]:
    """Парсит официальный рейтинг ЗСК (За Честный Бизнес)."""
    rating = data.get("rating", {}).get("body", {})
    if not rating:
        rating = data.get("rating", {})
    
    # Официальные поля ЗСК API
    return {
        "rating_category": rating.get("rating_category", ""),  # низкий/средний/высокий
        "risk_level": rating.get("risk_level", ""),  # уровень риска
        "point": rating.get("point", 0),  # балл (1-5)
        "tax_burn": rating.get("tax_burn", ""),  # налоговая нагрузка
        "stop": rating.get("stop", False),  # стоп-фактор
        # Старые поля для совместимости
        "index": rating.get("Индекс", ""),
        "reliability": rating.get("Надежность", ""),
    }


def parse_arbitration(data: Dict) -> Dict[str, Any]:
    """Парсит арбитражные дела."""
    arb = data.get("court-arbitration", {}).get("body", {})
    if not arb:
        arb = data.get("court-arbitration", {})
    
    cases = arb.get("Дела", [])
    as_plaintiff = sum(1 for c in cases if c.get("Роль") == "Истец")
    as_defendant = sum(1 for c in cases if c.get("Роль") == "Ответчик")
    
    return {
        "total": len(cases),
        "as_plaintiff": as_plaintiff,
        "as_defendant": as_defendant,
        "cases": cases[:5],
    }


def parse_affiliates(data: Dict) -> list:
    """Парсит связанные компании."""
    aff = data.get("affilation-company", {})
    
    # API возвращает данные в body.docs
    body = aff.get("body", {})
    if isinstance(body, dict):
        companies = body.get("docs", [])
    else:
        companies = []
    
    result = []
    for comp in companies[:10]:
        result.append({
            "name": comp.get("НаимЮЛСокр", comp.get("НаимЮЛПолн", "")),
            "inn": comp.get("ИНН", ""),
            "status": comp.get("Активность", ""),
            "address": comp.get("Адрес", ""),
            "okved": comp.get("КодОКВЭД", ""),
        })
    
    return result


def parse_contacts(data: Dict) -> Dict[str, Any]:
    """Парсит контактные данные."""
    contacts = data.get("contacts", {}).get("body", {})
    if not contacts:
        contacts = data.get("contacts", {})
    
    phones_raw = contacts.get("ТелВсе", "")
    emails_raw = contacts.get("EmailВсе", "")
    sites_raw = contacts.get("СайтВсе", "")
    
    # Разбираем телефоны (берём первые 3 уникальных)
    phones = []
    if phones_raw:
        for p in phones_raw.split(";")[:5]:
            p = p.strip()
            if p and len(p) > 5 and p not in phones:
                phones.append(p)
    
    # Разбираем email
    emails = []
    if emails_raw:
        for e in emails_raw.split(";")[:3]:
            e = e.strip()
            if e and "@" in e and e not in emails:
                emails.append(e)
    
    # Разбираем сайты
    sites = []
    if sites_raw:
        for s in sites_raw.split(";")[:3]:
            s = s.strip()
            if s and "." in s and s not in sites:
                sites.append(s)
    
    return {
        "phones": phones[:3],
        "emails": emails[:2],
        "sites": sites[:2],
        "has_data": bool(phones or emails or sites)
    }




# ============ Форматирование отчёта ============

def format_number(num) -> str:
    """Форматирует число с разделителями."""
    try:
        num = float(num)
        if num >= 1_000_000_000:
            return f"{num/1_000_000_000:.1f} млрд ₽"
        elif num >= 1_000_000:
            return f"{num/1_000_000:.1f} млн ₽"
        elif num >= 1_000:
            return f"{num/1_000:.0f} тыс ₽"
        else:
            return f"{num:.0f} ₽"
    except:
        return "Н/Д"


def format_company_report(result: Dict[str, Any]) -> str:
    """
    Форматирует полный отчёт о компании для Telegram.
    Включает светофор рисков, финансы, ФССП, арбитраж, связи.
    """
    if not result.get("success"):
        return f"❌ Ошибка: {result.get('error', 'Unknown error')}"
    
    data = result.get("data", {})
    
    # Парсим все секции
    card = parse_card(data)
    finances = parse_finances(data)
    fssp = parse_fssp(data)
    rating = parse_rating(data)
    arb = parse_arbitration(data)
    affiliates = parse_affiliates(data)
    
    # === СВЕТОФОР РИСКОВ ===
    risk_factors = []
    overall_risk = "low"
    
    # 1. Статус компании
    status = card.get("status", "")
    if "Действующ" in status:
        risk_factors.append(("✅", "Статус", "Действующая"))
    elif "Ликвид" in status:
        risk_factors.append(("🔴", "Статус", "Ликвидирована"))
        overall_risk = "high"
    elif status:
        risk_factors.append(("🟡", "Статус", status))
    else:
        risk_factors.append(("⚠️", "Статус", "Неизвестен"))
    
    # 2. Возраст компании
    reg_date = card.get("reg_date", "")
    if reg_date:
        try:
            if "." in reg_date:
                reg_dt = datetime.strptime(reg_date, "%d.%m.%Y")
            elif "-" in reg_date:
                reg_dt = datetime.strptime(reg_date[:10], "%Y-%m-%d")
            else:
                reg_dt = None
            if reg_dt:
                age_years = (datetime.now() - reg_dt).days // 365
                if age_years >= 5:
                    risk_factors.append(("✅", "Возраст", f"{age_years} лет"))
                elif age_years >= 2:
                    risk_factors.append(("🟡", "Возраст", f"{age_years} года"))
                else:
                    risk_factors.append(("🔴", "Возраст", f"Менее 2 лет (молодая)"))
                    overall_risk = "medium" if overall_risk == "low" else overall_risk
        except:
            pass
    
    # 3. Руководитель
    director = card.get("director", "")
    director_date = card.get("director_date", "")
    if director:
        if director_date:
            risk_factors.append(("✅", "Руководитель", f"Назначен {director_date[:10]}"))
        else:
            risk_factors.append(("✅", "Руководитель", "Назначен"))
    else:
        risk_factors.append(("🔴", "Руководитель", "Не указан"))
        overall_risk = "medium" if overall_risk == "low" else overall_risk
    
    # 4. Адрес
    address = card.get("address", "")
    if address and len(str(address)) > 10:
        risk_factors.append(("✅", "Адрес", "Указан"))
    else:
        risk_factors.append(("⚠️", "Адрес", "Не указан"))
    
    # 5. ФССП
    if fssp["count"] > 0:
        if fssp["total_sum"] > 500000:
            risk_factors.append(("🔴", "ФССП", f"{fssp['count']} производств ({format_number(fssp['total_sum'])})"))
            overall_risk = "high"
        else:
            risk_factors.append(("🟡", "ФССП", f"{fssp['count']} производств"))
            overall_risk = "medium" if overall_risk == "low" else overall_risk
    else:
        risk_factors.append(("✅", "ФССП", "Исполнительных производств нет"))
    
    # 6. Арбитраж
    if arb["total"] > 0:
        if arb["as_defendant"] > 5:
            risk_factors.append(("🔴", "Арбитраж", f"{arb['total']} дел (ответчик: {arb['as_defendant']})"))
            overall_risk = "high"
        elif arb["as_defendant"] > 0:
            risk_factors.append(("🟡", "Арбитраж", f"{arb['total']} дел"))
        else:
            risk_factors.append(("✅", "Арбитраж", f"{arb['total']} дел (только истец)"))
    else:
        risk_factors.append(("✅", "Арбитраж", "Дел не найдено"))
    
    # === ОФИЦИАЛЬНЫЙ РЕЙТИНГ ЗСК ===
    # Используем rating_category от API ЗАЧЕСТНЫЙБИЗНЕС
    zsk_rating = rating.get("rating_category", "").lower()
    zsk_risk = rating.get("risk_level", "").lower()
    zsk_point = rating.get("point", 0)
    zsk_tax = rating.get("tax_burn", "")
    
    # Определяем общий риск по официальному рейтингу ЗСК
    if zsk_rating == "высокий" or "высок" in zsk_risk:
        risk_emoji = "🔴"
        risk_text = "ВЫСОКИЙ РИСК (ЗСК)"
    elif zsk_rating == "средний" or "средн" in zsk_risk:
        risk_emoji = "🟡"
        risk_text = "СРЕДНИЙ РИСК (ЗСК)"
    elif zsk_rating == "низкий" or "низк" in zsk_risk:
        risk_emoji = "🟢"
        risk_text = "НИЗКИЙ РИСК (ЗСК)"
    else:
        # Fallback на нашу логику если ЗСК не вернул рейтинг
        risk_map = {"low": ("🟢", "НИЗКИЙ РИСК"), "medium": ("🟡", "СРЕДНИЙ РИСК"), "high": ("🔴", "ВЫСОКИЙ РИСК")}
        risk_emoji, risk_text = risk_map[overall_risk]
    
    
    # === ФОРМИРУЕМ ОТЧЁТ ===
    lines = [
        f"{risk_emoji} **{risk_text}**",
        f"",
        f"**{card.get('name') or card.get('full_name', 'Компания')}**",
        f"ИНН: {card.get('inn', 'Н/Д')}",
        f"",
        f"📊 **Светофор рисков:**",
    ]
    
    for emoji, name, value in risk_factors:
        lines.append(f"  {emoji} {name}: {value}")
    
    # Финансы
    fin_year = finances.get("year", "")
    year_suffix = f" ({fin_year})" if fin_year else ""
    
    lines.append(f"\n💰 **Финансы{year_suffix}:**")
    if card.get("capital") and float(card.get("capital") or 0) > 0:
        lines.append(f"  💵 Уставный капитал: {format_number(card['capital'])}")
    if finances.get("has_data"):
        lines.append(f"  📈 Выручка: {format_number(finances['revenue'])}")
        lines.append(f"  💹 Прибыль: {format_number(finances['profit'])}")
        if finances.get("taxes_paid") and float(finances.get("taxes_paid") or 0) > 0:
            lines.append(f"  🏛 Уплачено налогов: {format_number(finances['taxes_paid'])}")
        if finances.get("tax_debt") and float(finances.get("tax_debt") or 0) > 0:
            lines.append(f"  ⚠️ Долг по налогам: {format_number(finances['tax_debt'])}")
        if finances.get("employees"):
            lines.append(f"  👥 Сотрудников: {finances['employees']}")
    else:
        # Если нет данных за текущий год
        lines.append(f"  📈 Выручка: Данных нет")
        lines.append(f"  💹 Прибыль: Данных нет")
    
    # === РЕЙТИНГ ЗСК с причинами риска ===
    zsk_stop = rating.get("stop", False)
    
    if zsk_rating or zsk_risk or zsk_point or zsk_stop:
        lines.append(f"\n🔍 **Рейтинг ЗСК (За Честный Бизнес):**")
        
        # Балл надёжности
        if zsk_point and zsk_point > 0:
            stars = "⭐" * int(zsk_point)
            lines.append(f"  {stars} Балл надёжности: {zsk_point}/5")
        elif zsk_stop:
            lines.append(f"  ❌ Балл надёжности: 0/5 (стоп-фактор)")
        
        # Генерируем причины риска на основе данных
        risk_reasons = []
        
        # Проверяем стоп-фактор
        if zsk_stop:
            risk_reasons.append("⛔ Обнаружен стоп-фактор ЗСК")
        
        # Проверяем ФССП
        if fssp["count"] > 0:
            if fssp["total_sum"] > 500000:
                risk_reasons.append(f"⚠️ ФССП: {fssp['count']} производств на {format_number(fssp['total_sum'])}")
            else:
                risk_reasons.append(f"⚠️ ФССП: {fssp['count']} производств")
        
        # Проверяем арбитраж
        if arb["as_defendant"] > 3:
            risk_reasons.append(f"⚠️ Арбитраж: ответчик в {arb['as_defendant']} делах")
        elif arb["as_defendant"] > 0:
            risk_reasons.append(f"ℹ️ Арбитраж: ответчик в {arb['as_defendant']} делах")
        
        # Проверяем возраст компании (молодая = риск)
        if reg_date:
            try:
                if "." in reg_date:
                    reg_dt = datetime.strptime(reg_date, "%d.%m.%Y")
                elif "-" in reg_date:
                    reg_dt = datetime.strptime(reg_date[:10], "%Y-%m-%d")
                else:
                    reg_dt = None
                if reg_dt:
                    age_years = (datetime.now() - reg_dt).days // 365
                    if age_years < 2:
                        risk_reasons.append(f"⚠️ Молодая компания ({age_years} года)")
            except:
                pass
        
        # Выводим причины
        if risk_reasons:
            lines.append(f"  📋 Причины риска:")
            for reason in risk_reasons[:4]:
                lines.append(f"    {reason}")
        elif "высок" in zsk_risk.lower():
            lines.append(f"  ⚠️ Высокий риск по данным ЗСК")
        elif "средн" in zsk_risk.lower():
            lines.append(f"  ℹ️ Средний уровень риска")
        else:
            lines.append(f"  ✅ Существенных рисков не выявлено")
        
        # Налоги
        if finances.get("taxes_paid") and float(finances.get("taxes_paid") or 0) > 0:
            if fin_year == "2024":
                lines.append(f"  🏛 Налоги: 2025 нет — {format_number(finances['taxes_paid'])} ({fin_year})")
            else:
                lines.append(f"  🏛 Налоги ({fin_year}): {format_number(finances['taxes_paid'])}")
    
    # Связанные компании (фильтруем пустые)
    valid_affiliates = [a for a in affiliates if a.get("name") and a.get("inn")]
    if valid_affiliates:
        lines.append(f"\n🔗 **Связанные компании:**")
        lines.append(f"Руководитель связан еще с {len(valid_affiliates)} компаниями:")
        for comp in valid_affiliates[:5]:
            status_emoji = "🟢" if "Действующ" in comp.get("status", "") else "🔴"
            name_short = comp['name'][:35] if len(comp.get('name', '')) > 35 else comp.get('name', '?')
            lines.append(f"  {status_emoji} {name_short} (ИНН: {comp.get('inn', '?')})")
        if len(valid_affiliates) > 5:
            lines.append(f"  ... и еще {len(valid_affiliates) - 5} компаний")
    
    # Контакты
    contacts = parse_contacts(data)
    if contacts.get("has_data"):
        lines.append(f"\n📞 **Контакты:**")
        if contacts.get("phones"):
            lines.append(f"  ☎️ {', '.join(contacts['phones'][:2])}")
        if contacts.get("emails"):
            lines.append(f"  ✉️ {', '.join(contacts['emails'])}")
        if contacts.get("sites"):
            lines.append(f"  🌐 {', '.join(contacts['sites'])}")
    
    # Реквизиты
    lines.append(f"\n📋 **Реквизиты:**")
    lines.append(f"  ОГРН: {card.get('ogrn', 'Н/Д')}")
    if director:
        lines.append(f"  👤 Руководитель: {director}")
    if address:
        addr_short = str(address)[:55] + "..." if len(str(address)) > 55 else address
        lines.append(f"  📍 Адрес: {addr_short}")
    if card.get("okved"):
        okved_name = card.get('okved_name', '')[:30]
        lines.append(f"  🏭 ОКВЭД: {card['okved']} - {okved_name}")
    
    
    lines.append(f"\n_Отчёт: {datetime.now().strftime('%d.%m.%Y %H:%M')}_")
    
    return "\n".join(lines)
