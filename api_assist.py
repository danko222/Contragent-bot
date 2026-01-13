"""
Модуль для работы с API api-assist.com
Включает: ФССП, pb.nalog.ru, kad.arbitr.ru
"""

import os
import requests
from typing import Dict, Any, List, Optional
from urllib.parse import quote

API_ASSIST_KEY = os.getenv("API_ASSIST_KEY", "")
BASE_URL = "https://service.api-assist.com/parser"


def _make_request(endpoint: str, params: Dict[str, str]) -> Dict[str, Any]:
    """Выполняет HTTP запрос к API."""
    params["key"] = API_ASSIST_KEY
    try:
        response = requests.get(f"{BASE_URL}/{endpoint}", params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        return {"error": str(e), "success": 0}
    except Exception as e:
        return {"error": str(e), "success": 0}


# ============ ФССП API ============

def get_fssp_by_inn(inn: str) -> Dict[str, Any]:
    """
    Поиск исполнительных производств по ИНН юр.лица.
    Возвращает список долгов и общую сумму.
    """
    result = _make_request("fssp_api/search_ur_by_inn", {"inn": inn})
    
    if result.get("done") != 1:
        return {"found": False, "total": 0, "sum": 0, "items": [], "error": result.get("error")}
    
    items = result.get("result", [])
    total_sum = 0
    
    for item in items:
        subjects = item.get("subjects", [])
        for subj in subjects:
            try:
                sum_str = subj.get("sum", "0").replace(" ", "").replace(",", ".")
                total_sum += float(sum_str) if sum_str else 0
            except:
                pass
    
    return {
        "found": True,
        "total": len(items),
        "sum": total_sum,
        "items": items[:5],  # Первые 5 для отображения
        "url": result.get("url", "")
    }


def format_fssp_report(data: Dict[str, Any]) -> str:
    """Форматирует отчет ФССП для Telegram."""
    if not data.get("found") or data.get("total", 0) == 0:
        return "\n📋 **ФССП:** Исполнительных производств нет ✅"
    
    total = data.get("total", 0)
    total_sum = data.get("sum", 0)
    
    # Форматируем сумму
    if total_sum >= 1_000_000:
        sum_str = f"{total_sum/1_000_000:.1f} млн ₽"
    elif total_sum >= 1_000:
        sum_str = f"{total_sum/1_000:.0f} тыс ₽"
    else:
        sum_str = f"{total_sum:.0f} ₽"
    
    emoji = "🔴" if total_sum > 100000 else ("🟡" if total_sum > 0 else "🟢")
    
    lines = [f"\n{emoji} **ФССП:** {total} производств на {sum_str}"]
    
    # Показываем до 3 записей
    for item in data.get("items", [])[:3]:
        subjects = item.get("subjects", [])
        for subj in subjects[:1]:
            title = subj.get("title", "Задолженность")[:40]
            lines.append(f"  • {title}")
    
    return "\n".join(lines)


# ============ pb.nalog.ru API ============

def get_nalog_org(inn: str) -> Dict[str, Any]:
    """Получает информацию об организации из pb.nalog.ru."""
    result = _make_request("nalog_pb_api/", {"type": "TYPE_SEARCH_ORG", "inn": inn})
    
    if result.get("success") != 1:
        return {"found": False, "error": result.get("error")}
    
    orgs = result.get("org", [])
    if not orgs:
        return {"found": False}
    
    org = orgs[0]
    return {
        "found": True,
        "name": org.get("name", ""),
        "name_short": org.get("name_short", ""),
        "inn": org.get("inn", ""),
        "okved": org.get("okved", ""),
        "okved_name": org.get("okved_name", ""),
        "address": org.get("address", ""),
        "status": org.get("status", "")
    }


def get_nalog_director_limits(inn: str) -> Dict[str, Any]:
    """Проверяет ограничения по ИНН физлица (директора)."""
    result = _make_request("nalog_pb_api/", {"type": "TYPE_SEARCH_LIMIT_ORG", "inn": inn})
    
    if result.get("success") != 1:
        return {"found": False, "limits": []}
    
    limits = result.get("limit_org", [])
    return {
        "found": len(limits) > 0,
        "limits": limits
    }


def check_disqualified(fio: str) -> Dict[str, Any]:
    """Проверяет, дисквалифицировано ли лицо."""
    result = _make_request("nalog_pb_api/", {"type": "TYPE_SEARCH_DIS", "fio": fio})
    
    if result.get("success") != 1:
        return {"found": False, "items": []}
    
    items = result.get("dis", [])
    return {
        "found": len(items) > 0,
        "items": items
    }


def format_nalog_report(org_data: Dict, limits_data: Dict = None, disq_data: Dict = None) -> str:
    """Форматирует отчет ФНС для Telegram."""
    lines = ["\n📊 **Данные ФНС:**"]
    
    # Статус организации
    if org_data.get("found"):
        status = org_data.get("status", "Неизвестно")
        emoji = "🟢" if "Действующее" in status else "🔴"
        lines.append(f"  {emoji} Статус: {status}")
        
        if org_data.get("okved_name"):
            lines.append(f"  🏭 ОКВЭД: {org_data.get('okved')} - {org_data.get('okved_name')[:50]}")
    
    # Ограничения
    if limits_data and limits_data.get("found"):
        lines.append(f"  🔴 Ограничения ФНС: {len(limits_data.get('limits', []))} записей")
    
    # Дисквалификация
    if disq_data:
        if disq_data.get("found"):
            lines.append(f"  🔴 Директор ДИСКВАЛИФИЦИРОВАН!")
        else:
            lines.append(f"  🟢 Дисквалификация: нет")
    
    return "\n".join(lines) if len(lines) > 1 else ""


# ============ kad.arbitr.ru API ============

def get_arbitr_cases(inn: str) -> Dict[str, Any]:
    """
    Поиск арбитражных дел по ИНН.
    Возвращает количество дел и краткую информацию.
    """
    result = _make_request("arbitr_api/search", {"Inn": inn})
    
    if result.get("Success") != 1:
        return {"found": False, "total": 0, "cases": [], "error": result.get("error")}
    
    cases = result.get("Cases", [])
    pages = result.get("PagesCount", 1)
    
    # Подсчитываем роли
    as_plaintiff = 0
    as_respondent = 0
    bankruptcy = 0
    
    for case in cases:
        case_type = case.get("CaseType", "")
        if case_type == "Б":
            bankruptcy += 1
        
        # Проверяем роль
        plaintiffs = case.get("Plaintiffs", [])
        respondents = case.get("Respondents", [])
        
        for p in plaintiffs:
            if p.get("Inn") == inn:
                as_plaintiff += 1
                break
        
        for r in respondents:
            if r.get("Inn") == inn:
                as_respondent += 1
                break
    
    return {
        "found": len(cases) > 0,
        "total": len(cases),
        "total_pages": pages,
        "as_plaintiff": as_plaintiff,
        "as_respondent": as_respondent,
        "bankruptcy": bankruptcy,
        "cases": cases[:5]  # Первые 5
    }


def format_arbitr_report(data: Dict[str, Any]) -> str:
    """Форматирует отчет по арбитражным делам для Telegram."""
    if not data.get("found") or data.get("total", 0) == 0:
        return "\n⚖️ **Арбитраж:** Дел не найдено ✅"
    
    total = data.get("total", 0)
    plaintiff = data.get("as_plaintiff", 0)
    respondent = data.get("as_respondent", 0)
    bankruptcy = data.get("bankruptcy", 0)
    
    # Определяем уровень риска
    if bankruptcy > 0:
        emoji = "🔴"
        risk_note = " (БАНКРОТСТВО!)"
    elif respondent > 3:
        emoji = "🔴"
        risk_note = ""
    elif respondent > 0:
        emoji = "🟡"
        risk_note = ""
    else:
        emoji = "🟢"
        risk_note = ""
    
    lines = [f"\n{emoji} **Арбитраж:** {total} дел{risk_note}"]
    
    if plaintiff > 0:
        lines.append(f"  📤 Истец: {plaintiff} дел")
    if respondent > 0:
        lines.append(f"  📥 Ответчик: {respondent} дел")
    if bankruptcy > 0:
        lines.append(f"  💀 Банкротство: {bankruptcy} дел")
    
    # Показываем до 2 последних дел
    for case in data.get("cases", [])[:2]:
        number = case.get("CaseNumber", "")
        court = case.get("Court", "")[:25]
        lines.append(f"  • {number} ({court})")
    
    return "\n".join(lines)


# ============ Комплексная проверка ============

def check_company_extended(inn: str, director_name: str = None) -> Dict[str, Any]:
    """
    Полная проверка компании по всем API.
    Возвращает данные для отчета.
    """
    result = {
        "fssp": get_fssp_by_inn(inn),
        "nalog_org": get_nalog_org(inn),
        "arbitr": get_arbitr_cases(inn),
        "disqualified": None
    }
    
    # Проверяем дисквалификацию директора если есть ФИО
    if director_name and director_name != "Не указан":
        result["disqualified"] = check_disqualified(director_name)
    
    return result


def format_extended_report(data: Dict[str, Any]) -> str:
    """Форматирует полный расширенный отчет."""
    parts = []
    
    # ФССП
    if data.get("fssp"):
        parts.append(format_fssp_report(data["fssp"]))
    
    # Арбитраж
    if data.get("arbitr"):
        parts.append(format_arbitr_report(data["arbitr"]))
    
    # ФНС - статус организации
    nalog = data.get("nalog_org")
    if nalog and nalog.get("found"):
        status = nalog.get("status", "")
        if status:
            emoji = "🟢" if "Действующее" in status else "🔴"
            parts.append(f"\n{emoji} **ФНС:** {status}")
    
    # Дисквалификация директора
    if data.get("disqualified"):
        if data["disqualified"].get("found"):
            parts.append("\n🔴 **Директор ДИСКВАЛИФИЦИРОВАН!**")
        else:
            parts.append("\n🟢 **Дисквалификация директора:** нет")
    
    return "".join(parts)
