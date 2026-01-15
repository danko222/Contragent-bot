import sqlite3
import os
from datetime import datetime, timedelta

DB_PATH = os.path.join(os.path.dirname(__file__), "bot.db")

# Список username администраторов с безлимитным доступом (без @)
ADMIN_USERNAMES = ["zegnas"]

def init_db():
    """Инициализирует базу данных."""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        # Таблица пользователей
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                phone TEXT,
                checks_left INTEGER DEFAULT 3,
                is_premium BOOLEAN DEFAULT 0,
                premium_until TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                last_activity TEXT,
                is_blocked BOOLEAN DEFAULT 0
            )
        """)
        # Таблица истории проверок
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS check_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                inn TEXT,
                company_name TEXT,
                risk_level TEXT,
                checked_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        """)
        # Таблица рассылок
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS broadcasts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_text TEXT,
                sent_at TEXT DEFAULT CURRENT_TIMESTAMP,
                total_users INTEGER,
                success_count INTEGER,
                failed_count INTEGER
            )
        """)
        # Миграция: добавляем новые колонки если их нет
        try:
            cursor.execute("ALTER TABLE users ADD COLUMN last_name TEXT")
        except sqlite3.OperationalError:
            pass
        try:
            cursor.execute("ALTER TABLE users ADD COLUMN phone TEXT")
        except sqlite3.OperationalError:
            pass
        try:
            cursor.execute("ALTER TABLE users ADD COLUMN last_activity TEXT")
        except sqlite3.OperationalError:
            pass
        try:
            cursor.execute("ALTER TABLE users ADD COLUMN is_blocked BOOLEAN DEFAULT 0")
        except sqlite3.OperationalError:
            pass
        conn.commit()


def is_admin(username: str) -> bool:
    """Проверяет, является ли пользователь администратором."""
    if not username:
        return False
    return username.lower() in [u.lower() for u in ADMIN_USERNAMES]


def get_or_create_user(user_id: int, username: str = None, first_name: str = None):
    """Возвращает информацию о пользователе или создает нового."""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT checks_left, is_premium, premium_until, created_at FROM users WHERE user_id = ?", (user_id,))
        result = cursor.fetchone()
        
        if result:
            # Обновляем username если изменился
            if username:
                cursor.execute("UPDATE users SET username = ?, first_name = ? WHERE user_id = ?", (username, first_name, user_id))
                conn.commit()
            return {
                "checks_left": result[0], 
                "is_premium": bool(result[1]),
                "premium_until": result[2],
                "created_at": result[3]
            }
        else:
            # Новый пользователь - 3 проверки
            cursor.execute(
                "INSERT INTO users (user_id, username, first_name) VALUES (?, ?, ?)", 
                (user_id, username, first_name)
            )
            conn.commit()
            return {"checks_left": 3, "is_premium": False, "premium_until": None, "created_at": datetime.now().isoformat()}


def try_consume_check(user_id: int) -> bool:
    """Пытается списать 1 проверку. Возвращает True если разрешено."""
    user = get_or_create_user(user_id)
    
    if user["is_premium"]:
        return True
        
    if user["checks_left"] > 0:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET checks_left = checks_left - 1 WHERE user_id = ?", (user_id,))
            conn.commit()
        return True
    
    return False


def add_check_history(user_id: int, inn: str, company_name: str, risk_level: str):
    """Добавляет запись в историю проверок."""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO check_history (user_id, inn, company_name, risk_level) VALUES (?, ?, ?, ?)",
            (user_id, inn, company_name, risk_level)
        )
        conn.commit()


def get_check_history(user_id: int, limit: int = 10):
    """Получает историю проверок пользователя."""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT inn, company_name, risk_level, checked_at 
            FROM check_history 
            WHERE user_id = ? 
            ORDER BY checked_at DESC 
            LIMIT ?
        """, (user_id, limit))
        return cursor.fetchall()


def get_user_stats(user_id: int):
    """Получает статистику пользователя."""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        # Общее количество проверок
        cursor.execute("SELECT COUNT(*) FROM check_history WHERE user_id = ?", (user_id,))
        total_checks = cursor.fetchone()[0]
        
        # Проверок за сегодня
        cursor.execute("""
            SELECT COUNT(*) FROM check_history 
            WHERE user_id = ? AND DATE(checked_at) = DATE('now')
        """, (user_id,))
        today_checks = cursor.fetchone()[0]
        
        return {"total_checks": total_checks, "today_checks": today_checks}


def set_premium(user_id: int, until_date: str = None):
    """Устанавливает премиум статус пользователю."""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE users SET is_premium = 1, premium_until = ? WHERE user_id = ?",
            (until_date, user_id)
        )
        conn.commit()


# === Функции для управления клиентами и рассылок ===

def update_last_activity(user_id: int):
    """Обновляет время последней активности пользователя."""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE users SET last_activity = ?, is_blocked = 0 WHERE user_id = ?",
            (datetime.now().isoformat(), user_id)
        )
        conn.commit()


def mark_user_blocked(user_id: int):
    """Помечает пользователя как заблокировавшего бота."""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE users SET is_blocked = 1 WHERE user_id = ?",
            (user_id,)
        )
        conn.commit()


def get_all_active_users():
    """Получает всех активных пользователей для рассылки."""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT user_id, username, first_name 
            FROM users 
            WHERE is_blocked = 0 OR is_blocked IS NULL
        """)
        return cursor.fetchall()


def get_clients_stats():
    """Получает статистику по клиентам для администратора."""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        
        # Всего пользователей
        cursor.execute("SELECT COUNT(*) FROM users")
        total = cursor.fetchone()[0]
        
        # Активных за 7 дней
        week_ago = (datetime.now() - timedelta(days=7)).isoformat()
        cursor.execute(
            "SELECT COUNT(*) FROM users WHERE last_activity > ?", 
            (week_ago,)
        )
        active_7d = cursor.fetchone()[0]
        
        # Активных за 30 дней
        month_ago = (datetime.now() - timedelta(days=30)).isoformat()
        cursor.execute(
            "SELECT COUNT(*) FROM users WHERE last_activity > ?",
            (month_ago,)
        )
        active_30d = cursor.fetchone()[0]
        
        # Premium пользователей
        cursor.execute("SELECT COUNT(*) FROM users WHERE is_premium = 1")
        premium = cursor.fetchone()[0]
        
        # Заблокировавших бота
        cursor.execute("SELECT COUNT(*) FROM users WHERE is_blocked = 1")
        blocked = cursor.fetchone()[0]
        
        return {
            "total": total,
            "active_7d": active_7d,
            "active_30d": active_30d,
            "premium": premium,
            "blocked": blocked
        }


def log_broadcast(message_text: str, total: int, success: int, failed: int):
    """Сохраняет лог рассылки."""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """INSERT INTO broadcasts (message_text, total_users, success_count, failed_count) 
               VALUES (?, ?, ?, ?)""",
            (message_text, total, success, failed)
        )
        conn.commit()

