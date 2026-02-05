import sqlite3
import datetime

DB_NAME = "bot_users.db"

def get_connection():
    # check_same_thread=False agar aman saat diakses oleh banyak user (async)
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            full_name TEXT,
            username TEXT,
            api_key TEXT,
            is_allowed INTEGER DEFAULT 0,
            usage_count INTEGER DEFAULT 0,
            joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expiry_date TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS user_apikeys (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            api_key TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS active_tasks (
            task_id TEXT PRIMARY KEY,
            user_id INTEGER,
            chat_id INTEGER,
            model TEXT,
            prompt TEXT,
            start_time REAL,
            status TEXT
        )
    """)
    conn.commit()
    conn.close()

# Jalankan init saat file ini di-load
init_db()

def ensure_columns():
    """Pastikan kolom username ada (untuk update dari db versi lama)."""
    conn = get_connection()
    try:
        cols = [r["name"] for r in conn.execute("PRAGMA table_info(users)").fetchall()]
        if "username" not in cols:
            conn.execute("ALTER TABLE users ADD COLUMN username TEXT")
            conn.commit()
    except Exception as e:
        print(f"Error ensure_columns: {e}")
    finally:
        conn.close()

ensure_columns()

def register_user(user_id: int, full_name: str = "Unknown", username: str = None):
    conn = get_connection()
    try:
        conn.execute(
            "INSERT OR IGNORE INTO users (user_id, full_name, username) VALUES (?, ?, ?)",
            (user_id, full_name, username)
        )
        # Update data hanya jika tidak None
        conn.execute(
            "UPDATE users SET full_name = COALESCE(?, full_name), username = COALESCE(?, username) WHERE user_id = ?",
            (full_name, username, user_id)
        )
        conn.commit()
    except Exception as e:
        print(f"Error register_user: {e}")
    finally:
        conn.close()

    return None

def add_apikey(user_id: int, api_key: str) -> bool:
    conn = get_connection()
    try:
        # Cek jumlah key saat ini
        count = conn.execute("SELECT COUNT(*) FROM user_apikeys WHERE user_id = ?", (user_id,)).fetchone()[0]
        if count >= 10: return False
        
        # Cek duplikat
        exist = conn.execute("SELECT 1 FROM user_apikeys WHERE user_id = ? AND api_key = ?", (user_id, api_key)).fetchone()
        if exist: return True # Anggap sukses jika sudah ada
        
        conn.execute("INSERT INTO user_apikeys (user_id, api_key) VALUES (?, ?)", (user_id, api_key))
        conn.commit()
        return True
    finally:
        conn.close()

def get_all_apikeys(user_id: int):
    conn = get_connection()
    try:
        rows = conn.execute("SELECT id, api_key FROM user_apikeys WHERE user_id = ? ORDER BY id ASC", (user_id,)).fetchall()
        return [{"id": r["id"], "api_key": r["api_key"]} for r in rows]
    finally:
        conn.close()

def delete_apikey(user_id: int, key_id: int):
    conn = get_connection()
    try:
        conn.execute("DELETE FROM user_apikeys WHERE user_id = ? AND id = ?", (user_id, key_id))
        conn.commit()
    finally:
        conn.close()

def migrate_keys():
    """Pindahkan key dari tabel users ke user_apikeys (One-time run)"""
    conn = get_connection()
    try:
        users = conn.execute("SELECT user_id, api_key FROM users WHERE api_key IS NOT NULL AND api_key != ''").fetchall()
        for u in users:
            # Cek apakah sudah ada di tabel baru
            exist = conn.execute("SELECT 1 FROM user_apikeys WHERE user_id = ? AND api_key = ?", (u["user_id"], u["api_key"])).fetchone()
            if not exist:
                conn.execute("INSERT INTO user_apikeys (user_id, api_key) VALUES (?, ?)", (u["user_id"], u["api_key"]))
        conn.commit()
    except Exception as e:
        print(f"Migrate error: {e}")
    finally:
        conn.close()

# Auto migrate saat start
migrate_keys()

def check_access(user_id: int) -> bool:
    conn = get_connection()
    try:
        row = conn.execute("SELECT is_allowed, expiry_date FROM users WHERE user_id = ?", (user_id,)).fetchone()
    finally:
        conn.close()

    if not row or int(row["is_allowed"] or 0) == 0:
        return False

    if row["expiry_date"]:
        # Support dua format timestamp sqlite
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
            try:
                exp = datetime.datetime.strptime(str(row["expiry_date"]), fmt)
                if datetime.datetime.now() > exp:
                    # Opsional: Auto disable jika expired
                    # disable_user(user_id) 
                    return False
                break
            except:
                continue

    return True

def grant_access(user_id: int, days: int = 30):
    # Pastikan user terdaftar dulu
    register_user(user_id, "Added by Admin", None)
    
    expiry = datetime.datetime.now() + datetime.timedelta(days=days)
    conn = get_connection()
    try:
        conn.execute("UPDATE users SET is_allowed = 1, expiry_date = ? WHERE user_id = ?", (expiry, user_id))
        conn.commit()
    finally:
        conn.close()

def delete_user(user_id: int):
    conn = get_connection()
    try:
        conn.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
        conn.commit()
    finally:
        conn.close()

def increment_usage(user_id: int):
    conn = get_connection()
    try:
        conn.execute("UPDATE users SET usage_count = usage_count + 1 WHERE user_id = ?", (user_id,))
        conn.commit()
    finally:
        conn.close()

def get_all_users():
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT user_id, full_name, username, is_allowed, usage_count, joined_at, expiry_date
            FROM users
            ORDER BY joined_at DESC
        """).fetchall()
        return rows
    finally:
        conn.close()

def add_task(task_id: str, user_id: int, chat_id: int, model: str, prompt: str, start_time: float):
    conn = get_connection()
    try:
        conn.execute("""
            INSERT OR IGNORE INTO active_tasks (task_id, user_id, chat_id, model, prompt, start_time, status)
            VALUES (?, ?, ?, ?, ?, ?, 'PENDING')
        """, (task_id, user_id, chat_id, model, prompt, start_time))
        conn.commit()
    except Exception as e:
        print(f"Error add_task: {e}")
    finally:
        conn.close()

def remove_task(task_id: str):
    conn = get_connection()
    try:
        conn.execute("DELETE FROM active_tasks WHERE task_id = ?", (task_id,))
        conn.commit()
    except Exception as e:
        print(f"Error remove_task: {e}")
    finally:
        conn.close()

def get_active_tasks():
    conn = get_connection()
    try:
        return conn.execute("SELECT * FROM active_tasks").fetchall()
    finally:
        conn.close()
