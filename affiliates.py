"""
Модуль поиска аффилированных компаний.
Находит все компании, связанные с директором или учредителем.
"""

import os
import requests
from typing import Dict, Any, List, Optional

DADATA_API_URL = "https://suggestions.dadata.ru/suggestions/api/4_1/rs/suggest/party"


def find_affiliated_companies(manager_name: str, exclude_inn: str = None, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Ищет компании, где указанное лицо является руководителем или учредителем.
    
    Args:
        manager_name: ФИО руководителя/учредителя
        exclude_inn: ИНН текущей компании (чтобы исключить её из результатов)
        limit: Максимальное количество результатов
    
    Returns:
        Список компаний с базовой информацией
    """
    api_key = os.getenv("DADATA_API_KEY")
    if not api_key:
        return []
    
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Token {api_key}"
    }
    
    # Ищем компании по ФИО руководителя
    payload = {
        "query": manager_name,
        "count": limit + 5,  # Берём с запасом, т.к. одну исключим
        "type": "LEGAL"  # Только юрлица
    }
    
    try:
        response = requests.post(DADATA_API_URL, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        companies = []
        for suggestion in data.get("suggestions", []):
            company_data = suggestion.get("data", {})
            inn = company_data.get("inn", "")
            
            # Пропускаем текущую компанию
            if exclude_inn and inn == exclude_inn:
                continue
            
            # Проверяем, что это действительно связь через руководителя
            management = company_data.get("management", {})
            manager = management.get("name", "") if management else ""
            
            # Проверяем совпадение ФИО (частичное)
            name_parts = manager_name.lower().split()
            is_match = any(part in manager.lower() for part in name_parts if len(part) > 2)
            
            if is_match:
                status = company_data.get("state", {}).get("status", "UNKNOWN")
                companies.append({
                    "name": company_data.get("name", {}).get("short_with_opf", suggestion.get("value", "Неизвестно")),
                    "inn": inn,
                    "status": status,
                    "status_emoji": "🟢" if status == "ACTIVE" else "🔴",
                    "position": management.get("post", "Руководитель") if management else "Связь"
                })
            
            if len(companies) >= limit:
                break
        
        return companies
    
    except Exception as e:
        print(f"Ошибка поиска аффилированных компаний: {e}")
        return []


def format_affiliates_report(manager_name: str, affiliates: List[Dict[str, Any]]) -> str:
    """Форматирует отчет об аффилированных компаниях."""
    if not affiliates:
        return ""
    
    count = len(affiliates)
    
    lines = [
        f"",
        f"**🔗 Связанные компании:**",
        f"Руководитель связан еще с **{count}** компаниями:",
    ]
    
    # Показываем первые 5
    for company in affiliates[:5]:
        lines.append(f"  • {company['name']} (ИНН: {company['inn']})")
    
    if count > 5:
        lines.append(f"  _...и еще {count - 5} компаний_")
    
    return "\n".join(lines)
