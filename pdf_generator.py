"""
Модуль генерации PDF-отчетов о контрагентах.
Обновлён для работы с API ЗАЧЕСТНЫЙБИЗНЕС.
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


def generate_pdf_report(
    data: Dict[str, Any], 
    user_id: int, 
    affiliates_list: List[Dict] = None, 
    extended_data: Dict = None,
    # Новые параметры для ZaChestnyiBiznes
    card: Dict = None,
    fssp: Dict = None,
    arbitration: Dict = None,
    finances: Dict = None,
    contacts: Dict = None
) -> str:
    """
    Генерирует PDF-отчет о компании.
    Поддерживает как старый формат (DaData), так и новый (ZaChestnyiBiznes).
    """
    
    # Определяем источник данных (новый API или старый)
    use_new_api = card is not None
    
    if use_new_api:
        # Новый формат ZaChestnyiBiznes
        inn = card.get('inn', 'unknown')
        name = card.get('name') or card.get('full_name', 'Неизвестно')
        ogrn = card.get('ogrn', 'Н/Д')
        kpp = card.get('kpp', 'Н/Д')
        address = card.get('address', 'Не указан')
        if isinstance(address, dict):
            address = address.get('АдресПолн', 'Не указан')
        manager_name = card.get('director', 'Не указан')
        manager_post = ''
        okved = card.get('okved', 'Н/Д')
        okved_name = card.get('okved_name', '')
        okved_full = f"{okved}" + (f" - {okved_name}" if okved_name else "")
        capital = card.get('capital', 0)
        employees = card.get('employees', 0)
        status = card.get('status', '')
    else:
        # Старый формат DaData
        inn = data.get('inn', 'unknown')
        name = data.get('name', {}).get('full_with_opf') or data.get('name', {}).get('short_with_opf') or 'Неизвестно'
        ogrn = data.get('ogrn', 'Н/Д')
        kpp = data.get('kpp', 'Н/Д')
        address = data.get('address', {}).get('value', 'Не указан') if isinstance(data.get('address'), dict) else 'Не указан'
        manager_name = data.get('management', {}).get('name', 'Не указан') if data.get('management') else 'Не указан'
        manager_post = data.get('management', {}).get('post', '') if data.get('management') else ''
        from okved import get_okved_name
        okved = data.get('okved', 'Н/Д')
        okved_name = get_okved_name(okved)
        okved_full = f"{okved}" + (f" - {okved_name}" if okved_name else "")
        capital = 0
        employees = 0
        status = data.get('state', {}).get('status', '')
    
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
    
    # Шрифты
    font_name = 'DejaVuSans' if os.path.exists(FONT_PATH) else 'Helvetica'
    font_bold = 'DejaVuSans-Bold' if os.path.exists(FONT_BOLD_PATH) else 'Helvetica-Bold'
    
    # Стили
    title_style = ParagraphStyle('CustomTitle', fontName=font_bold, fontSize=14, spaceAfter=20, alignment=1)
    heading_style = ParagraphStyle('CustomHeading', fontName=font_bold, fontSize=11, spaceAfter=8, spaceBefore=15)
    normal_style = ParagraphStyle('CustomNormal', fontName=font_name, fontSize=9, spaceAfter=4)
    small_style = ParagraphStyle('SmallText', fontName=font_name, fontSize=8, textColor=colors.grey)
    
    # Определяем уровень риска
    if use_new_api and fssp and arbitration:
        fssp_count = fssp.get('count', 0)
        fssp_sum = fssp.get('total_sum', 0)
        arb_defendant = arbitration.get('as_defendant', 0)
        
        if fssp_count > 3 or fssp_sum > 500000:
            risk_level = "ВЫСОКИЙ РИСК"
            risk_color = colors.red
        elif arb_defendant > 5 or fssp_count > 0:
            risk_level = "СРЕДНИЙ РИСК"
            risk_color = colors.orange
        else:
            risk_level = "НИЗКИЙ РИСК"
            risk_color = colors.green
    else:
        # Старый анализ через risk_analyzer
        from risk_analyzer import analyze_risks
        overall_emoji, overall_text, factors = analyze_risks(data)
        risk_level = overall_text.upper()
        risk_color = colors.green if "Низкий" in overall_text else (colors.orange if "Средний" in overall_text else colors.red)
    
    elements = []
    
    # === ЗАГОЛОВОК ===
    elements.append(Paragraph("ОТЧЕТ О ПРОВЕРКЕ КОНТРАГЕНТА", title_style))
    elements.append(Paragraph(f"Дата: {datetime.now().strftime('%d.%m.%Y %H:%M')}", small_style))
    elements.append(Spacer(1, 15))
    
    # === ОБЩАЯ ОЦЕНКА ===
    elements.append(Paragraph(f"<b>ОБЩАЯ ОЦЕНКА: {risk_level}</b>", heading_style))
    elements.append(Spacer(1, 8))
    
    # === ОСНОВНЫЕ СВЕДЕНИЯ ===
    elements.append(Paragraph("<b>ОСНОВНЫЕ СВЕДЕНИЯ</b>", heading_style))
    
    info_data = [
        ["Наименование:", name],
        ["ИНН:", inn],
        ["ОГРН:", ogrn],
        ["КПП:", kpp],
        ["Статус:", status or "Н/Д"],
        ["Адрес:", address[:70] + "..." if len(str(address)) > 70 else address],
        ["Руководитель:", f"{manager_name}" + (f" ({manager_post})" if manager_post else "")],
        ["Основной ОКВЭД:", okved_full[:60]],
    ]
    
    if capital and float(capital) > 0:
        info_data.append(["Уставный капитал:", format_money(capital)])
    if employees and int(employees) > 0:
        info_data.append(["Сотрудников:", str(employees)])
    
    info_table = Table(info_data, colWidths=[4.5*cm, 12.5*cm])
    info_table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (-1, -1), font_name),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('TEXTCOLOR', (0, 0), (0, -1), colors.grey),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
    ]))
    elements.append(info_table)
    
    # === ФИНАНСОВЫЕ ПОКАЗАТЕЛИ ===
    elements.append(Paragraph("<b>ФИНАНСОВЫЕ ПОКАЗАТЕЛИ</b>", heading_style))
    
    if use_new_api and finances:
        if finances.get('has_data'):
            fin_data = [
                ["Показатель", "Значение", "Период"],
                ["Выручка", format_money(finances.get('revenue', 0)), f"{finances.get('year', 'Н/Д')} год"],
                ["Прибыль", format_money(finances.get('profit', 0)), f"{finances.get('year', 'Н/Д')} год"],
            ]
            if finances.get('taxes_paid') and float(finances.get('taxes_paid', 0)) > 0:
                fin_data.append(["Уплачено налогов", format_money(finances['taxes_paid']), f"{finances.get('year', 'Н/Д')} год"])
            if finances.get('tax_debt') and float(finances.get('tax_debt', 0)) > 0:
                fin_data.append(["Задолженность по налогам", format_money(finances['tax_debt']), "⚠️"])
            if finances.get('employees') and int(finances.get('employees', 0)) > 0:
                fin_data.append(["Сотрудников (ФНС)", str(finances['employees']), f"{finances.get('year', 'Н/Д')} год"])
        else:
            fin_data = [["Показатель", "Значение", "Период"], ["Данные", "Отсутствуют", "-"]]
    else:
        # Старый формат
        from risk_analyzer import get_financial_data
        finance = get_financial_data(data)
        fin_data = [
            ["Показатель", "Значение", "Период"],
            ["Выручка", format_money(finance.get('revenue')), f"{finance.get('year', 'Н/Д')} год"],
            ["Прибыль", format_money(finance.get('profit')), f"{finance.get('year', 'Н/Д')} год"],
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
    
    # === ФССП (Исполнительные производства) ===
    elements.append(Paragraph("<b>ИСПОЛНИТЕЛЬНЫЕ ПРОИЗВОДСТВА (ФССП)</b>", heading_style))
    
    if use_new_api and fssp:
        fssp_count = fssp.get('count', 0)
        fssp_sum = fssp.get('total_sum', 0)
        
        if fssp_count > 0:
            elements.append(Paragraph(
                f"Найдено производств: {fssp_count}, общая сумма: {format_money(fssp_sum)}", 
                normal_style
            ))
            
            if fssp.get('items'):
                fssp_data = [["Предмет взыскания", "Сумма"]]
                for item in fssp['items'][:5]:
                    subject = item.get('СодИП', item.get('Предмет', 'Задолженность'))[:50]
                    amount = format_money(item.get('СуммаДолга', 0))
                    fssp_data.append([subject, amount])
                
                fssp_table = Table(fssp_data, colWidths=[12*cm, 5*cm])
                fssp_table.setStyle(TableStyle([
                    ('FONTNAME', (0, 0), (-1, -1), font_name),
                    ('FONTNAME', (0, 0), (-1, 0), font_bold),
                    ('FONTSIZE', (0, 0), (-1, -1), 8),
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                    ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
                ]))
                elements.append(fssp_table)
        else:
            elements.append(Paragraph("✓ Исполнительных производств не найдено", normal_style))
    elif extended_data and extended_data.get("fssp"):
        # Старый формат
        fssp_old = extended_data["fssp"]
        if fssp_old.get("found") and fssp_old.get("total", 0) > 0:
            elements.append(Paragraph(f"Найдено производств: {fssp_old.get('total', 0)}", normal_style))
        else:
            elements.append(Paragraph("Исполнительных производств не найдено", normal_style))
    else:
        elements.append(Paragraph("Данные недоступны", normal_style))
    
    # === АРБИТРАЖНЫЕ ДЕЛА ===
    elements.append(Paragraph("<b>АРБИТРАЖНЫЕ ДЕЛА</b>", heading_style))
    
    if use_new_api and arbitration:
        arb_total = arbitration.get('total', 0)
        arb_plaintiff = arbitration.get('as_plaintiff', 0)
        arb_defendant = arbitration.get('as_defendant', 0)
        
        if arb_total > 0:
            summary = f"Всего дел: {arb_total}"
            if arb_plaintiff > 0:
                summary += f", истец: {arb_plaintiff}"
            if arb_defendant > 0:
                summary += f", ответчик: {arb_defendant}"
            elements.append(Paragraph(summary, normal_style))
            
            if arbitration.get('cases'):
                arb_data = [["Номер дела", "Статус"]]
                for case in arbitration['cases'][:5]:
                    number = case.get('НомерДела', case.get('number', '?'))
                    status_case = case.get('Статус', case.get('status', ''))[:30]
                    arb_data.append([number, status_case])
                
                arb_table = Table(arb_data, colWidths=[8*cm, 9*cm])
                arb_table.setStyle(TableStyle([
                    ('FONTNAME', (0, 0), (-1, -1), font_name),
                    ('FONTNAME', (0, 0), (-1, 0), font_bold),
                    ('FONTSIZE', (0, 0), (-1, -1), 8),
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                    ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
                ]))
                elements.append(arb_table)
        else:
            elements.append(Paragraph("✓ Арбитражных дел не найдено", normal_style))
    elif extended_data and extended_data.get("arbitr"):
        arbitr = extended_data["arbitr"]
        if arbitr.get("found") and arbitr.get("total", 0) > 0:
            elements.append(Paragraph(f"Всего дел: {arbitr.get('total', 0)}", normal_style))
        else:
            elements.append(Paragraph("Арбитражных дел не найдено", normal_style))
    else:
        elements.append(Paragraph("Данные недоступны", normal_style))
    
    # === СВЯЗАННЫЕ КОМПАНИИ ===
    elements.append(Paragraph("<b>СВЯЗАННЫЕ КОМПАНИИ</b>", heading_style))
    
    if affiliates_list and len(affiliates_list) > 0:
        count = len(affiliates_list)
        risk_text = "МАССОВЫЙ ДИРЕКТОР" if count >= 10 else ("Много связей" if count >= 5 else "Норма")
        elements.append(Paragraph(f"Руководитель связан еще с {count} компаниями. Оценка: {risk_text}", normal_style))
        
        aff_data = [["Компания", "ИНН", "Статус"]]
        for aff in affiliates_list[:10]:
            if isinstance(aff, dict):
                aff_name = aff.get('name', aff.get('Наименование', '?'))
                aff_inn = aff.get('inn', aff.get('ИНН', '?'))
                aff_status = aff.get('status', aff.get('Статус', ''))
                status_text = "Действует" if "Действ" in str(aff_status) else "Не действует"
            else:
                continue
            
            if len(aff_name) > 35:
                aff_name = aff_name[:35] + "..."
            aff_data.append([aff_name, aff_inn, status_text])
        
        if len(aff_data) > 1:
            aff_table = Table(aff_data, colWidths=[9*cm, 4*cm, 4*cm])
            aff_table.setStyle(TableStyle([
                ('FONTNAME', (0, 0), (-1, -1), font_name),
                ('FONTNAME', (0, 0), (-1, 0), font_bold),
                ('FONTSIZE', (0, 0), (-1, -1), 8),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
            ]))
            elements.append(aff_table)
    else:
        elements.append(Paragraph("Связанных компаний не найдено или данные недоступны", normal_style))
    
    # === КОНТАКТЫ ===
    if contacts and contacts.get("has_data"):
        elements.append(Paragraph("<b>КОНТАКТНЫЕ ДАННЫЕ</b>", heading_style))
        
        contact_info = []
        if contacts.get("phones"):
            contact_info.append(["Телефоны:", ", ".join(contacts["phones"][:3])])
        if contacts.get("emails"):
            contact_info.append(["Email:", ", ".join(contacts["emails"][:2])])
        if contacts.get("sites"):
            contact_info.append(["Веб-сайт:", ", ".join(contacts["sites"][:2])])
        
        if contact_info:
            contact_table = Table(contact_info, colWidths=[4*cm, 13*cm])
            contact_table.setStyle(TableStyle([
                ('FONTNAME', (0, 0), (-1, -1), font_name),
                ('FONTSIZE', (0, 0), (-1, -1), 9),
                ('TEXTCOLOR', (0, 0), (0, -1), colors.grey),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
            ]))
            elements.append(contact_table)
    
    # === ПОДПИСЬ ===
    elements.append(Spacer(1, 30))
    elements.append(Paragraph("_" * 70, normal_style))
    footer_style = ParagraphStyle('Footer', fontName=font_name, fontSize=7, textColor=colors.grey)
    elements.append(Paragraph("Отчет сформирован автоматически ботом @contragent111_bot", footer_style))
    elements.append(Paragraph(f"Источник данных: API ЗАЧЕСТНЫЙБИЗНЕС | Telegram: t.me/contragent111_bot", footer_style))
    
    # Генерируем PDF
    doc.build(elements)
    
    return filepath
