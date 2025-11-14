import aiosqlite
import json
from datetime import datetime

DB_FILE = "manga_cache.db"

# --- Инициализация и создание таблиц ---
async def init_db():
    """Инициализирует базу данных и создает таблицы, если они не существуют."""
    async with aiosqlite.connect(DB_FILE) as db:
        # Кэш для готовых глав (PDF/Telegraph)
        await db.execute('''
            CREATE TABLE IF NOT EXISTS chapters_cache (
                manga_id INTEGER NOT NULL,
                chapter_num TEXT NOT NULL,
                format_type TEXT NOT NULL,
                file_id TEXT NOT NULL,
                file_unique_id TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (manga_id, chapter_num, format_type)
            )
        ''')

        # Кэш для изображений (обложек)
        await db.execute('''
            CREATE TABLE IF NOT EXISTS images_cache (
                image_url TEXT PRIMARY KEY,
                file_id TEXT NOT NULL,
                file_unique_id TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Хранилище для токена Telegraph
        await db.execute('''
            CREATE TABLE IF NOT EXISTS key_value_store (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        ''')
        await db.commit()

# --- Работа с Telegraph токеном ---
async def save_telegraph_token(token: str):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "INSERT OR REPLACE INTO key_value_store (key, value) VALUES (?, ?)",
            ("telegraph_token", token)
        )
        await db.commit()

async def load_telegraph_token() -> str | None:
    async with aiosqlite.connect(DB_FILE) as db:
        cursor = await db.execute("SELECT value FROM key_value_store WHERE key = ?", ("telegraph_token",))
        row = await cursor.fetchone()
        return row[0] if row else None


# --- Функции для кэша глав ---
async def add_chapter_to_cache(manga_id: int, chapter_num: str, format_type: str, file_id: str, file_unique_id: str):
    """Добавляет главу в кэш."""
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            '''
            INSERT OR REPLACE INTO chapters_cache (manga_id, chapter_num, format_type, file_id, file_unique_id)
            VALUES (?, ?, ?, ?, ?)
            ''',
            (manga_id, str(chapter_num), format_type, file_id, file_unique_id)
        )
        await db.commit()

async def get_chapter_from_cache(manga_id: int, chapter_num: str, format_type: str) -> dict | None:
    """Получает данные главы из кэша."""
    async with aiosqlite.connect(DB_FILE) as db:
        cursor = await db.execute(
            "SELECT file_id, file_unique_id FROM chapters_cache WHERE manga_id = ? AND chapter_num = ? AND format_type = ?",
            (manga_id, str(chapter_num), format_type)
        )
        row = await cursor.fetchone()
        if row:
            return {"file_id": row[0], "file_unique_id": row[1]}
        return None

# --- Функции для кэша изображений ---
async def add_image_to_cache(image_url: str, file_id: str, file_unique_id: str):
    """Добавляет изображение в кэш."""
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            '''
            INSERT OR REPLACE INTO images_cache (image_url, file_id, file_unique_id)
            VALUES (?, ?, ?)
            ''',
            (image_url, file_id, file_unique_id)
        )
        await db.commit()

async def get_image_from_cache(image_url: str) -> dict | None:
    """Получает данные изображения из кэша по URL."""
    async with aiosqlite.connect(DB_FILE) as db:
        cursor = await db.execute(
            "SELECT file_id, file_unique_id FROM images_cache WHERE image_url = ?",
            (image_url,)
        )
        row = await cursor.fetchone()
        if row:
            return {"file_id": row[0], "file_unique_id": row[1]}
        return None
