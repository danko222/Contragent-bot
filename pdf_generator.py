"""
Модуль генерации PDF-отчетов о контрагентах.
С поддержкой кириллицы через шрифт DejaVuSans.
Включает: риски, финансы, связанные компании.
"""

import os
from datetime import datetime
from typing import Dict, Any, List
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle

from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

from risk_analyzer import analyze_risks, get_financial_data
from affiliates import find_affiliated_companies

# Путь для сохранения отчетов
REPORTS_DIR = os.path.join(os.path.dirname(__file__), "reports")
os.makedirs(REPORTS_DIR, exist_ok=True)

# Регистрируем шрифт с поддержкой кириллицы
FONT_PATH = os.path.join(os.path.dirname(__file__), "DejaVuSans.ttf")
FONT_BOLD_PATH = os.path.join(os.path.dirname(__file__), "DejaVuSans-Bold.ttf")

if os.path.exists(FONT_PATH):
    pdfmetrics.registerFont(TTFont('DejaVuSans', FONT_PATH))
if os.path.exists(FONT_BOLD_PATH):
    pdfmetrics.registerFont(TTFont('DejaVuSans-Bold', FONT_BOLD_PATH))


def format_money(value) -> str:
    """Форматирует денежную сумму."""
    if value is None:
        return "Данных нет"
    try:
        v = float(value)
        if v >= 1_000_000_000:
            return f"{v/1_000_000_000:.1f} млрд ₽"
        elif v >= 1_000_000:
            return f"{v/1_000_000:.1f} млн ₽"
        elif v >= 1_000:
            return f"{v/1_000:.0f} тыс ₽"
        else:
            return f"{v:.0f} ₽"
    except:
        return "Данных нет"


def generate_pdf_report(data: Dict[str, Any], user_id: int, affiliates_list: List[Dict] = None) -> str:
    """
    Генерирует PDF-отчет о компании.
    Возвращает путь к созданному файлу.
    """
    inn = data.get('inn', 'unknown')
    filename = f"report_{inn}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
    filepath = os.path.join(REPORTS_DIR, filename)
    
    doc = SimpleDocTemplate(
        filepath,
        pagesize=A4,
        rightMargin=1.5*cm,
        leftMargin=1.5*cm,
        topMargin=1.5*cm,
        bottomMargin=1.5*cm
    )
    
    # Используем DejaVuSans для кириллицы
    font_name = 'DejaVuSans' if os.path.exists(FONT_PATH) else 'Helvetica'
    font_bold = 'DejaVuSans-Bold' if os.path.exists(FONT_BOLD_PATH) else 'Helvetica-Bold'
    
    # Кастомные стили
    title_style = ParagraphStyle('CustomTitle', fontName=font_bold, fontSize=14, spaceAfter=20, alignment=1)
    heading_style = ParagraphStyle('CustomHeading', fontName=font_bold, fontSize=11, spaceAfter=8, spaceBefore=15)
    normal_style = ParagraphStyle('CustomNormal', fontName=font_name, fontSize=9, spaceAfter=4)
    small_style = ParagraphStyle('SmallText', fontName=font_name, fontSize=8, textColor=colors.grey)
    
    # Собираем данные
    name = data.get('name', {}).get('full_with_opf') or data.get('name', {}).get('short_with_opf') or 'Неизвестно'
    ogrn = data.get('ogrn', 'Н/Д')
    kpp = data.get('kpp', 'Н/Д')
    address = data.get('address', {}).get('value', 'Не указан') if isinstance(data.get('address'), dict) else 'Не указан'
    manager_name = data.get('management', {}).get('name', 'Не указан') if data.get('management') else 'Не указан'
    manager_post = data.get('management', {}).get('post', '') if data.get('management') else ''
    
    # ОКВЭД с расшифровкой
    okved_code = data.get('okved', 'Н/Д')
    okved_name = data.get('okved_type', '')
    if not okved_name:
        # Пробуем получить из okveds
        okveds = data.get('okveds', [])
        if okveds and isinstance(okveds, list) and len(okveds) > 0:
            okved_name = okveds[0].get('name', '')
    okved = f"{okved_code}" + (f" - {okved_name}" if okved_name else "")
    
    # Анализ рисков
    overall_emoji, overall_text, factors = analyze_risks(data)
    
    # Финансы
    finance = get_financial_data(data)
    
    # Связанные компании (если не переданы, ищем)
    if affiliates_list is None and manager_name and manager_name != 'Не указан':
        affiliates_list = find_affiliated_companies(manager_name, exclude_inn=inn)
    
    elements = []
    
    # === ЗАГОЛОВОК ===
    elements.append(Paragraph("ОТЧЕТ О ПРОВЕРКЕ КОНТРАГЕНТА", title_style))
    elements.append(Paragraph(f"Дата: {datetime.now().strftime('%d.%m.%Y %H:%M')}", small_style))
    elements.append(Spacer(1, 15))
    
    # === ОБЩАЯ ОЦЕНКА ===
    risk_color = colors.green if "Низкий" in overall_text else (colors.orange if "Средний" in overall_text else colors.red)
    elements.append(Paragraph(f"<b>ОБЩАЯ ОЦЕНКА: {overall_text.upper()}</b>", heading_style))
    elements.append(Spacer(1, 8))
    
    # === ОСНОВНЫЕ СВЕДЕНИЯ ===
    elements.append(Paragraph("<b>ОСНОВНЫЕ СВЕДЕНИЯ</b>", heading_style))
    
    info_data = [
        ["Наименование:", name],
        ["ИНН:", inn],
        ["ОГРН:", ogrn],
        ["КПП:", kpp],
        ["Адрес:", address],
        ["Руководитель:", f"{manager_name}" + (f" ({manager_post})" if manager_post else "")],
        ["Основной ОКВЭД:", okved],
    ]
    
    info_table = Table(info_data, colWidths=[4*cm, 13*cm])
    info_table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (-1, -1), font_name),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('TEXTCOLOR', (0, 0), (0, -1), colors.grey),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
    ]))
    elements.append(info_table)
    
    # === СВЕТОФОР РИСКОВ ===
    elements.append(Paragraph("<b>АНАЛИЗ РИСКОВ</b>", heading_style))
    
    risk_data = [["Показатель", "Значение", "Статус"]]
    for factor in factors:
        status = "OK" if factor['emoji'] == "🟢" else ("ВНИМАНИЕ" if factor['emoji'] == "🟡" else "РИСК")
        risk_data.append([factor['name'], factor['value'], status])
    
    risk_table = Table(risk_data, colWidths=[4*cm, 9*cm, 4*cm])
    risk_table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (-1, -1), font_name),
        ('FONTNAME', (0, 0), (-1, 0), font_bold),
        ('FONTSIZE', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ('TOPPADDING', (0, 0), (-1, -1), 5),
    ]))
    elements.append(risk_table)
    
    # === ФИНАНСЫ ===
    elements.append(Paragraph("<b>ФИНАНСОВЫЕ ПОКАЗАТЕЛИ</b>", heading_style))
    
    revenue = format_money(finance.get('revenue'))
    profit = format_money(finance.get('profit'))
    year = finance.get('year', 'Н/Д')
    
    fin_data = [
        ["Показатель", "Значение", "Период"],
        ["Выручка", revenue, f"{year} год" if year != 'Н/Д' else "Н/Д"],
        ["Прибыль", profit, f"{year} год" if year != 'Н/Д' else "Н/Д"],
    ]
    
    fin_table = Table(fin_data, colWidths=[5*cm, 7*cm, 5*cm])
    fin_table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (-1, -1), font_name),
        ('FONTNAME', (0, 0), (-1, 0), font_bold),
        ('FONTSIZE', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ('TOPPADDING', (0, 0), (-1, -1), 5),
    ]))
    elements.append(fin_table)
    
    # === СВЯЗАННЫЕ КОМПАНИИ ===
    elements.append(Paragraph("<b>СВЯЗАННЫЕ КОМПАНИИ</b>", heading_style))
    
    if affiliates_list and len(affiliates_list) > 0:
        count = len(affiliates_list)
        risk_text = "МАССОВЫЙ ДИРЕКТОР" if count >= 10 else ("Много связей" if count >= 5 else "Норма")
        elements.append(Paragraph(f"Руководитель связан еще с {count} компаниями. Оценка: {risk_text}", normal_style))
        
        aff_data = [["Компания", "ИНН", "Статус"]]
        for aff in affiliates_list[:10]:  # Максимум 10
            status = "Действует" if aff.get('status_emoji') == "🟢" or aff.get('status') == "ACTIVE" else "Не действует"
            company_name_aff = aff.get('name', '?')
            if len(company_name_aff) > 35:
                company_name_aff = company_name_aff[:35] + "..."
            aff_data.append([company_name_aff, aff.get('inn', '?'), status])
        
        if count > 10:
            aff_data.append([f"... и еще {count - 10} компаний", "", ""])
        
        aff_table = Table(aff_data, colWidths=[9*cm, 4*cm, 4*cm])
        aff_table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (-1, -1), font_name),
            ('FONTNAME', (0, 0), (-1, 0), font_bold),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
        ]))
        elements.append(aff_table)
    else:
        elements.append(Paragraph("Связанных компаний не найдено", normal_style))
    
    # === ПОДПИСЬ ===
    elements.append(Spacer(1, 30))
    elements.append(Paragraph("_" * 70, normal_style))
    footer_style = ParagraphStyle('Footer', fontName=font_name, fontSize=7, textColor=colors.grey)
    elements.append(Paragraph("Отчет сформирован автоматически ботом @contragent111_bot", footer_style))
    elements.append(Paragraph(f"Telegram: t.me/contragent111_bot", footer_style))
    
    # Генерируем PDF
    doc.build(elements)
    
    return filepath
