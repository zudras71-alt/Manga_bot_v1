import os
import time
import requests
import json
from io import BytesIO
from aiogram import Bot, Dispatcher, types, F
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.filters import Command, StateFilter, CommandStart
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, BufferedInputFile, LabeledPrice,
    PreCheckoutQuery, SuccessfulPayment
)
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import img2pdf
from http.client import IncompleteRead
from requests.exceptions import RequestException
from datetime import datetime, timedelta, timezone
import asyncio
import math
from functools import wraps
from PIL import Image
from telegraph import Telegraph
from telegraph.exceptions import TelegraphException

# --- ИЗМЕНЕНО: Импортируем logging и db ---
import logging
import db

# --- ИЗМЕНЕНО: Настройка логирования ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bot.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Конфигурация ---
TOKEN = "7674848541:AAE_BIB_50rQbrGs33RAeeSjG68fcpYo3g8"
BASE_URL = 'https://desu.city/manga/api'
ADMIN_IDS = [6311102512, 390443177]

# --- Файлы данных ---
FAVORITES_FILE = "favorites.json"
CHANNELS_FILE = "channels.json"
USERS_FILE = "users.json"
STATS_FILE = "stats.json"
SETTINGS_FILE = "user_settings.json"
PREMIUM_USERS_FILE = "premium_users.json"
CHANNEL_ID = "@houuak"

# --- Инициализация ---
session = requests.Session()
session.headers.update({
    'User-Agent': 'AniMangaBot/1.0 (contact: @Dao12g)',
    'Referer': 'https://desu.city/'
})

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

telegraph = Telegraph()


# --- Состояния FSM ---
class MangaStates(StatesGroup):
    main_menu = State()
    selecting_manga = State()
    viewing_manga_chapters = State()
    waiting_for_search_query = State()
    selecting_genres = State()
    selecting_kinds = State()
    settings_menu = State()
    premium_menu = State()


class AdminStates(StatesGroup):
    panel = State()
    adding_channel = State()
    removing_channel = State()
    mailing_get_content = State()
    mailing_get_buttons = State()
    mailing_confirm = State()


# --- Константы ---
MANGAS_PER_PAGE = 10
CHAPTERS_PER_PAGE = 25
API_LIMIT = 50

# --- ПЛАНЫ VIP-ПОДПИСКИ ---
VIP_PLANS = {
    "vip_1m": {"stars": 150, "days": 30, "title": "VIP на 1 месяц"},
    "vip_3m": {"stars": 400, "days": 90, "title": "VIP на 3 месяца"},
    "vip_6m": {"stars": 700, "days": 180, "title": "VIP на 6 месяцев"},
    "vip_12m": {"stars": 1100, "days": 365, "title": "VIP на 1 год"},
}

MANGA_GENRES = [
    {"id": 56, "text": "Action", "russian": "Экшен"}, {"id": 49, "text": "Comedy", "russian": "Комедия"},
    {"id": 51, "text": "Ecchi", "russian": "Этти"}, {"id": 57, "text": "Fantasy", "russian": "Фэнтези"},
    {"id": 62, "text": "Romance", "russian": "Романтика"}, {"id": 60, "text": "School", "russian": "Школа"},
    {"id": 48, "text": "Supernatural", "russian": "Сверхъестественное"},
    {"id": 69, "text": "Seinen", "russian": "Сэйнэн"}, {"id": 71, "text": "Shounen", "russian": "Сёнэн"},
    {"id": 73, "text": "Shoujo", "russian": "Сёдзё"}, {"id": 78, "text": "Drama", "russian": "Драма"},
    {"id": 82, "text": "Adventure", "russian": "Приключения"},
    {"id": 83, "text": "Sci-Fi", "russian": "Научная фантастика"}, {"id": 85, "text": "Horror", "russian": "Ужасы"},
    {"id": 88, "text": "Slice of Life", "russian": "Повседневность"},
    {"id": 74, "text": "yaoi", "russian": "Яой"}, {"id": 75, "text": "yuri", "russian": "Юри"},
    {"id": 70, "text": "shounen-ai", "russian": "Сёнен-ай"}, {"id": 72, "text": "shoujo-ai", "russian": "Сёдзё-ай"}
]
MANGA_KINDS = [
    {"id": "manga", "russian": "Манга"},
    {"id": "manhwa", "russian": "Манхва (Корейская)"},
    {"id": "manhua", "russian": "Маньхуа (Китайская)"}
]


# --- УЛУЧШЕННЫЕ ФУНКЦИИ ДЛЯ VIP-ДОСТУПА ---
def grant_vip_access(user_id: int, plan_key: str):
    if plan_key not in VIP_PLANS:
        logger.error(f"Ошибка: Неизвестный план '{plan_key}' для пользователя {user_id}")
        return
    users_data = load_data(PREMIUM_USERS_FILE, {})
    user_id_str = str(user_id)
    duration_days = VIP_PLANS[plan_key]["days"]
    start_date = datetime.now(timezone.utc)
    current_expiry_str = users_data.get(user_id_str, {}).get("vip_expires_at")
    if current_expiry_str:
        try:
            current_expiry_date = datetime.fromisoformat(current_expiry_str)
            if current_expiry_date.tzinfo is None:
                current_expiry_date = current_expiry_date.replace(tzinfo=timezone.utc)
            if current_expiry_date > start_date:
                start_date = current_expiry_date
        except (ValueError, TypeError):
            pass
    new_expiry_date = start_date + timedelta(days=duration_days)
    if user_id_str not in users_data:
        users_data[user_id_str] = {}
    users_data[user_id_str]["vip_expires_at"] = new_expiry_date.isoformat()
    save_data(PREMIUM_USERS_FILE, users_data)
    logger.info(f"Пользователю {user_id} предоставлен/продлен VIP до {new_expiry_date.strftime('%Y-%m-%d %H:%M %Z')}.")


def check_vip_access(user_id: int) -> bool:
    users_data = load_data(PREMIUM_USERS_FILE, {})
    user_info = users_data.get(str(user_id))
    if not user_info or "vip_expires_at" not in user_info:
        return False
    try:
        expiry_date = datetime.fromisoformat(user_info["vip_expires_at"])
        if expiry_date.tzinfo is None:
            expiry_date = expiry_date.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) < expiry_date
    except (ValueError, TypeError):
        return False


def get_vip_expiry_date(user_id: int) -> str | None:
    users_data = load_data(PREMIUM_USERS_FILE, {})
    user_info = users_data.get(str(user_id))
    if not user_info or "vip_expires_at" not in user_info:
        return None
    try:
        expiry_date = datetime.fromisoformat(user_info["vip_expires_at"])
        if expiry_date.tzinfo is None:
            expiry_date = expiry_date.replace(tzinfo=timezone.utc)
        if datetime.now(timezone.utc) >= expiry_date:
            return None
        return expiry_date.strftime("%d.%m.%Y в %H:%M UTC")
    except (ValueError, TypeError):
        return None


# --- Функции для работы с данными ---
def load_data(file_path, default_data):
    if not os.path.exists(file_path):
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(default_data, f, indent=2)
        return default_data
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return default_data


def save_data(file_path, data):
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except IOError as e:
        logger.error(f"Ошибка сохранения файла {file_path}: {e}")


def add_user_to_db(user_id):
    users = load_data(USERS_FILE, {"users": []})
    if user_id not in users["users"]:
        users["users"].append(user_id)
        save_data(USERS_FILE, users)


def get_display_name(manga_data: dict) -> str:
    return manga_data.get('russian') or manga_data.get('name', 'Неизвестно')


def increment_download_count():
    stats = load_data(STATS_FILE, {"downloads": 0})
    stats["downloads"] += 1
    save_data(STATS_FILE, stats)


# --- Функции для избранного ---
def add_to_favorites(user_id, manga_info):
    favorites = load_data(FAVORITES_FILE, {})
    user_id_str = str(user_id)
    if user_id_str not in favorites: favorites[user_id_str] = []
    if not any(str(m['id']) == str(manga_info['id']) for m in favorites[user_id_str]):
        simplified_manga = {'id': manga_info['id'], 'name': manga_info.get('name'),
                            'russian': manga_info.get('russian')}
        favorites[user_id_str].append(simplified_manga)
        save_data(FAVORITES_FILE, favorites)
        return True
    return False


def remove_from_favorites(user_id, manga_id):
    favorites = load_data(FAVORITES_FILE, {})
    user_id_str = str(user_id)
    if user_id_str in favorites:
        initial_len = len(favorites[user_id_str])
        favorites[user_id_str] = [m for m in favorites[user_id_str] if str(m['id']) != str(manga_id)]
        if len(favorites[user_id_str]) < initial_len:
            save_data(FAVORITES_FILE, favorites)
            return True
    return False


def get_user_favorites(user_id):
    return load_data(FAVORITES_FILE, {}).get(str(user_id), [])


def is_in_favorites(user_id, manga_id):
    return any(str(m['id']) == str(manga_id) for m in get_user_favorites(user_id))


# --- Настройки пользователя ---
def get_user_settings(user_id: int) -> dict:
    all_settings = load_data(SETTINGS_FILE, {})
    default_settings = {"batch_size": 5, "output_format": "pdf"}
    user_settings = all_settings.get(str(user_id), {})
    default_settings.update(user_settings)
    return default_settings


def save_user_settings(user_id: int, new_settings: dict):
    all_settings = load_data(SETTINGS_FILE, {})
    user_id_str = str(user_id)
    if user_id_str not in all_settings:
        all_settings[user_id_str] = {}
    all_settings[user_id_str].update(new_settings)
    save_data(SETTINGS_FILE, all_settings)


# --- Функции проверки подписки ---
async def check_subscription(user_id: int):
    channels = load_data(CHANNELS_FILE, {"channels": []})["channels"]
    if not channels: return True
    for channel in channels:
        try:
            member = await bot.get_chat_member(chat_id=channel, user_id=user_id)
            if member.status not in ['member', 'administrator', 'creator']: return False
        except TelegramBadRequest:
            logger.warning(f"Ошибка: Неверный ID канала '{channel}' или бот не админ в нем.")
            return False
        except Exception as e:
            logger.error(f"Неожиданная ошибка при проверке подписки на {channel}: {e}")
            return False
    return True


async def get_subscribe_keyboard():
    channels = load_data(CHANNELS_FILE, {"channels": []})["channels"]
    keyboard = []
    for channel in channels:
        try:
            chat_info = await bot.get_chat(channel)
            invite_link = chat_info.invite_link or f"https://t.me/{chat_info.username}"
            keyboard.append([InlineKeyboardButton(text=f"➡️ {chat_info.title}", url=invite_link)])
        except Exception as e:
            logger.error(f"Не удалось получить информацию о канале {channel}: {e}")
    keyboard.append([InlineKeyboardButton(text="✅ Я подписался", callback_data="check_subscription_again")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def subscription_wrapper(func):
    @wraps(func)
    async def wrapper(event: types.Message | CallbackQuery, **kwargs):
        user_id = event.from_user.id
        if not await check_subscription(user_id):
            keyboard = await get_subscribe_keyboard()
            text = "Для использования бота, пожалуйста, подпишитесь на наши каналы:"
            if isinstance(event, CallbackQuery):
                await event.message.answer(text, reply_markup=keyboard)
                await event.answer()
            else:
                await event.answer(text, reply_markup=keyboard)
            return
        return await func(event, **kwargs)

    return wrapper


# --- Основные функции API и скачивания ---
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2),
       retry=retry_if_exception_type((IncompleteRead, RequestException)))
def download_image(img_url: str) -> bytes:
    logger.info(f"API Request: download_image (URL: {img_url[:50]}...)")
    r = session.get(img_url, timeout=15)
    r.raise_for_status()
    return r.content


def get_mangas(query: str = "", api_page: int = 1, order_by: str = "popular"):
    try:
        url = f'{BASE_URL}/?search={query}&limit={API_LIMIT}&page={api_page}&order_by={order_by}'
        logger.info(f"API Request: get_mangas (URL: {url})")
        resp = session.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return data.get('response', []), data.get('pageNavParams', {})
    except Exception as e:
        logger.error(f"Ошибка в get_mangas: {e}")
        return [], {}


def get_manga_info(manga_id: str):
    try:
        url = f'{BASE_URL}/{manga_id}'
        logger.info(f"API Request: get_manga_info (manga_id: {manga_id})")
        resp = session.get(url, timeout=15)
        resp.raise_for_status()
        return resp.json().get('response', {})
    except Exception as e:
        logger.error(f"Ошибка в get_manga_info (manga_id: {manga_id}): {e}")
        return {}


def get_mangas_by_genres_and_kinds(genres, kinds="", search="", api_page=1, order_by="popular"):
    try:
        url = f'{BASE_URL}/?limit={API_LIMIT}&page={api_page}&order_by={order_by}'
        if genres: url += f"&genres={genres}"
        if kinds: url += f"&kinds={kinds}"
        if search: url += f"&search={search}"
        logger.info(f"API Request: get_mangas_by_genres_and_kinds (URL: {url})")
        resp = session.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return data.get('response', []), data.get('pageNavParams', {})
    except Exception as e:
        logger.error(f"Ошибка в get_mangas_by_genres_and_kinds: {e}")
        return [], {}


# --- ИЗМЕНЕНО: Полностью переработана функция для корректной загрузки изображений ---
async def upload_to_telegraph(manga_name: str, chapter: dict, pages: list, callback: CallbackQuery) -> str | None:
    progress_message = await bot.send_message(callback.from_user.id,
                                              f"Загружаю главу {chapter['ch']} в Telegraph (0/{len(pages)})...")
    try:
        image_html_tags = []
        for i, page in enumerate(pages, 1):
            try:
                # 1. Скачиваем картинку
                img_data = download_image(page['img'])
                # 2. Загружаем ее в Telegraph
                # Выполняем синхронную блокирующую операцию в отдельном потоке
                uploaded_files = await asyncio.to_thread(telegraph.upload_file, src=BytesIO(img_data))
                # 3. Добавляем тег с новой ссылкой
                image_html_tags.append(f"<img src='{uploaded_files[0]['src']}'/>")

                if i % 5 == 0 or i == len(pages):
                    await bot.edit_message_text(
                        f"Загружаю главу {chapter['ch']} в Telegraph ({i}/{len(pages)})...",
                        chat_id=callback.from_user.id,
                        message_id=progress_message.message_id
                    )
            except Exception as e:
                logger.error(f"Не удалось загрузить страницу {i} в Telegraph: {e}")
                image_html_tags.append(f"<p><i>[Ошибка загрузки страницы {i}]</i></p>")

        content = "".join(image_html_tags)
        title = f"{manga_name} - Глава {chapter['ch']}"
        author_name = "AniMangaBot"

        response = await asyncio.to_thread(
            telegraph.create_page,
            title=title,
            html_content=content,
            author_name=author_name
        )

        await bot.delete_message(chat_id=callback.from_user.id, message_id=progress_message.message_id)
        return response['url']
    except TelegraphException as e:
        logger.error(f"Ошибка Telegraph API при создании страницы: {e}")
        await bot.edit_message_text("❌ Ошибка при создании страницы Telegraph.",
                                    chat_id=callback.from_user.id, message_id=progress_message.message_id)
        return None
    except Exception as e:
        logger.error(f"Критическая ошибка в upload_to_telegraph: {e}")
        if progress_message:
            await bot.edit_message_text("❌ Произошла ошибка при загрузке в Telegraph.",
                                        chat_id=callback.from_user.id, message_id=progress_message.message_id)
        return None


async def download_chapter(manga_id: str, chapter: dict, callback: CallbackQuery) -> bytes | None:
    url = f"{BASE_URL}/{manga_id}/chapter/{chapter['id']}"
    progress_message = None
    try:
        logger.info(f"API Request: download_chapter (manga_id: {manga_id}, chapter: {chapter.get('id')})")
        resp = session.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json().get('response')
        if not data or 'pages' not in data or 'list' not in data['pages']:
            logger.warning(f"Нет данных о страницах: manga_id {manga_id}, chapter_id {chapter.get('id')}")
            await bot.send_message(callback.from_user.id,
                                   f"❌ Ошибка: нет данных о страницах для главы {chapter['ch']}.")
            return None

        pages, total_pages = data['pages']['list'], len(data['pages']['list'])
        progress_message = await bot.send_message(callback.from_user.id,
                                                  f"Скачиваю главу {chapter['ch']} (0/{total_pages} страниц)...")

        images_for_pdf = []
        for i, page in enumerate(pages, 1):
            try:
                img_data = download_image(page['img'])
                img = Image.open(BytesIO(img_data))
                if img.mode != 'RGB':
                    img = img.convert('RGB')
                output_buffer = BytesIO()
                img.save(output_buffer, format='JPEG', quality=85)
                images_for_pdf.append(output_buffer.getvalue())

                if i % 5 == 0 or i == total_pages:
                    await bot.edit_message_text(
                        f"Скачиваю и сжимаю главу {chapter['ch']} ({i}/{total_pages} страниц)...",
                        chat_id=callback.from_user.id,
                        message_id=progress_message.message_id)
            except Exception as e:
                logger.error(f"Ошибка при скачивании/сжатии страницы {i} для PDF: {e}")

        if not images_for_pdf:
            logger.warning(
                f"Не удалось скачать ни одной страницы для PDF: manga_id {manga_id}, chapter {chapter['ch']}")
            await bot.edit_message_text("❌ Ошибка: не удалось скачать ни одной страницы.",
                                        chat_id=callback.from_user.id, message_id=progress_message.message_id)
            return None

        await bot.edit_message_text(f"⚙️ Конвертирую {len(images_for_pdf)} страниц в PDF...",
                                    chat_id=callback.from_user.id, message_id=progress_message.message_id)

        pdf_bytes = img2pdf.convert(images_for_pdf)

        if len(pdf_bytes) > 50 * 1024 * 1024:
            logger.warning(f"Глава {chapter['ch']} слишком большая (> 50 МБ)")
            await bot.delete_message(chat_id=callback.from_user.id, message_id=progress_message.message_id)
            await bot.send_message(callback.from_user.id,
                                   f"❌ Ошибка: Глава {chapter['ch']} слишком большая даже после сжатия (> 50 МБ). Невозможно отправить.")
            return None

        await bot.delete_message(chat_id=callback.from_user.id, message_id=progress_message.message_id)
        return pdf_bytes

    except Exception as e:
        logger.error(f"Ошибка в download_chapter: {e}")
        if progress_message:
            await bot.edit_message_text("❌ Произошла ошибка при скачивании главы.",
                                        chat_id=callback.from_user.id, message_id=progress_message.message_id)
        return None


async def run_batch_download(callback: CallbackQuery, state: FSMContext, start_index: int):
    user_id = callback.from_user.id
    settings = get_user_settings(user_id)
    batch_size = settings.get('batch_size', 5)

    data = await state.get_data()
    all_chapters = data.get('chapters', [])
    end_index = min(start_index + batch_size, len(all_chapters))
    chapters_to_process = all_chapters[start_index:end_index]

    if not chapters_to_process:
        try:
            await callback.answer("Больше глав для скачивания нет.", show_alert=True)
        except TelegramBadRequest:
            await bot.send_message(user_id, "Больше глав для скачивания нет.")
        return

    try:
        await callback.answer(f"Начинаю VIP-загрузку {len(chapters_to_process)} глав...", show_alert=False)
    except TelegramBadRequest:
        logger.warning("Не удалось ответить на callback в начале batch_download.")

    for i, chapter in enumerate(chapters_to_process):
        is_last = (i == len(chapters_to_process) - 1)
        await send_chapter_or_telegraph(callback, state, float(chapter['ch']), is_last_in_batch=is_last)
        await asyncio.sleep(0.4)


# --- Клавиатуры ---
def create_main_inline_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Поиск манги", callback_data="main_search"),
         InlineKeyboardButton(text="🌟 Premium", callback_data="main_premium")],
        [InlineKeyboardButton(text="💓 Избранное", callback_data="main_favorites"),
         InlineKeyboardButton(text="🚀 Топ рейтинга", callback_data="main_top")],
        [InlineKeyboardButton(text="📋 Поиск по жанрам", callback_data="main_genres"),
         InlineKeyboardButton(text="⚙️ Настройки", callback_data="main_settings")]
    ])


def create_admin_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_mailing")],
        [InlineKeyboardButton(text="➕ Добавить канал", callback_data="admin_add_channel")],
        [InlineKeyboardButton(text="➖ Удалить канал", callback_data="admin_remove_channel")],
        [InlineKeyboardButton(text="📄 Список каналов", callback_data="admin_list_channels")],
        [InlineKeyboardButton(text="⬅️ Выйти", callback_data="admin_exit")]
    ])


def create_settings_keyboard(user_id: int) -> InlineKeyboardMarkup:
    is_vip = check_vip_access(user_id)
    keyboard = []

    if is_vip:
        settings = get_user_settings(user_id)
        current_batch_size = settings.get('batch_size', 5)
        current_format = settings.get('output_format', 'pdf')

        sizes = [3, 5, 10]
        batch_buttons = [InlineKeyboardButton(
            text=f"✅ {size} глав" if size == current_batch_size else f"{size} глав",
            callback_data=f"set_batch_{size}"
        ) for size in sizes]
        keyboard.append([InlineKeyboardButton(text="Кол-во глав в пакете:", callback_data="ignore")])
        keyboard.append(batch_buttons)

        format_buttons = [
            InlineKeyboardButton(
                text="✅ PDF" if current_format == 'pdf' else "PDF",
                callback_data="set_format_pdf"
            ),
            InlineKeyboardButton(
                text="✅ Telegraph" if current_format == 'telegraph' else "Telegraph",
                callback_data="set_format_telegraph"
            )
        ]
        keyboard.append([InlineKeyboardButton(text="Формат выдачи:", callback_data="ignore")])
        keyboard.append(format_buttons)
    else:
        keyboard.append(
            [InlineKeyboardButton(text="🌟 Купить Premium для доступа к настройкам", callback_data="main_premium")])

    keyboard.append([InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def create_document_navigation_keyboard(chapters: list, current_chapter_num: float,
                                        user_id: int) -> InlineKeyboardMarkup:
    if not check_vip_access(user_id):
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🌟 Навигация доступна с Premium", callback_data="main_premium")],
            [InlineKeyboardButton(text="📖 К списку глав", callback_data="back_to_grid")]
        ])

    keyboard = []
    chapter_nums = [float(ch['ch']) for ch in chapters]
    try:
        current_index = chapter_nums.index(current_chapter_num)
    except ValueError:
        return InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="Ошибка навигации", callback_data="ignore")]])

    single_nav_row = []
    if current_index > 0:
        single_nav_row.append(
            InlineKeyboardButton(text="⬅️ Пред.", callback_data=f"doc_nav_{chapter_nums[current_index - 1]}"))
    single_nav_row.append(InlineKeyboardButton(text=f"Гл. {current_chapter_num}", callback_data="ignore"))
    if current_index < len(chapter_nums) - 1:
        single_nav_row.append(
            InlineKeyboardButton(text="След. ➡️", callback_data=f"doc_nav_{chapter_nums[current_index + 1]}"))
    if single_nav_row: keyboard.append(single_nav_row)

    settings = get_user_settings(user_id)
    batch_size = settings.get('batch_size', 5)
    batch_nav_row = []
    if current_index > 0:
        prev_batch_start_index = max(0, current_index - batch_size)
        batch_nav_row.append(
            InlineKeyboardButton(text=f"⬅️ Пред. {batch_size}", callback_data=f"batch_dl_{prev_batch_start_index}"))
    if current_index < len(chapter_nums) - 1:
        next_batch_start_index = current_index + 1
        batch_nav_row.append(
            InlineKeyboardButton(text=f"След. {batch_size} ➡️", callback_data=f"batch_dl_{next_batch_start_index}"))
    if batch_nav_row: keyboard.append(batch_nav_row)

    keyboard.append([InlineKeyboardButton(text="📖 К списку глав", callback_data="back_to_grid")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def create_premium_keyboard() -> InlineKeyboardMarkup:
    keyboard = [[InlineKeyboardButton(
        text=f"{plan_data['title']} - {plan_data['stars']} 🌟",
        callback_data=f"buy_{plan_key}"
    )] for plan_key, plan_data in VIP_PLANS.items()]
    keyboard.append([InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def create_manga_list_keyboard(mangas: list, page: int, total_pages: int):
    keyboard = [[InlineKeyboardButton(text=get_display_name(manga), callback_data=f"manga_{manga['id']}")] for manga in
                mangas[page * MANGAS_PER_PAGE:(page + 1) * MANGAS_PER_PAGE]]
    nav_row = []
    if page > 0: nav_row.append(InlineKeyboardButton(text="◀️ Назад", callback_data=f"list_page_{page - 1}"))
    if page < total_pages - 1: nav_row.append(
        InlineKeyboardButton(text="Вперед ▶️", callback_data=f"list_page_{page + 1}"))
    if nav_row: keyboard.append(nav_row)
    keyboard.append([InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def create_chapter_grid_keyboard(manga_id: str, chapters: list, is_fav: bool, page: int = 0):
    keyboard = []
    total_pages = math.ceil(len(chapters) / CHAPTERS_PER_PAGE)
    start_index = page * CHAPTERS_PER_PAGE
    end_index = start_index + CHAPTERS_PER_PAGE
    page_chapters = chapters[start_index:end_index]
    for i in range(0, len(page_chapters), 5):
        row = [InlineKeyboardButton(text=str(ch['ch']), callback_data=f"dl_{ch['ch']}") for ch in
               page_chapters[i:i + 5]]
        keyboard.append(row)
    nav_row = []
    if page > 0: nav_row.append(InlineKeyboardButton(text="◀️", callback_data=f"grid_page_{page - 1}"))
    if total_pages > 1: nav_row.append(InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="ignore"))
    if page < total_pages - 1: nav_row.append(InlineKeyboardButton(text="▶️", callback_data=f"grid_page_{page + 1}"))
    if nav_row: keyboard.append(nav_row)
    fast_nav_row = []
    if page > 0: fast_nav_row.append(InlineKeyboardButton(text="⏪ В начало", callback_data="grid_page_0"))
    if page < total_pages - 1: fast_nav_row.append(
        InlineKeyboardButton(text="В конец ⏩", callback_data=f"grid_page_{total_pages - 1}"))
    if fast_nav_row: keyboard.append(fast_nav_row)
    fav_text = "❌ Убрать из избранного" if is_fav else "⭐️ Добавить в избранное"
    keyboard.append([InlineKeyboardButton(text=fav_text, callback_data=f"toggle_fav_{manga_id}")])
    keyboard.append([InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def create_manga_caption_for_grid(info: dict, chapters_count: int) -> str:
    title = f"<b>{get_display_name(info)}</b>"
    details = []
    if info.get('score'): details.append(f"<b>📊 Рейтинг:</b> {info['score']}")
    if info.get('issue_year'): details.append(f"<b>📅 Год выпуска:</b> {info['issue_year']}")
    if info.get('kind'):
        kind_rus = next((k['russian'] for k in MANGA_KINDS if k['id'] == info['kind']), info['kind'])
        details.append(f"<b>📘 Тип:</b> {kind_rus}")
    if info.get('status'):
        details.append(
            f"<b>⏳ Статус:</b> {info['status'].replace('ongoing', 'выпускается').replace('released', 'выпущен')}")
    details.append(f"<b>📖 Глав:</b> {chapters_count}")
    genres = info.get('genres', [])
    if genres:
        genre_names = [g.get('russian', g.get('name', '')) for g in genres]
        details.append(f"<b>🎭 Жанры:</b> {', '.join(filter(None, genre_names))}")

    description = info.get('description', 'Нет описания').strip()
    details_text = "\n".join(details)
    base_text = f"{title}\n\n{details_text}\n\n"
    footer_text = "\n\n📚 <b>Выберите главу для скачивания:</b>"
    remaining_space = 1024 - len(base_text) - len(footer_text) - 20

    final_description = ""
    if remaining_space > 0 and description:
        if len(description) > remaining_space:
            description = description[:remaining_space] + '...'
        final_description = f"<i>{description}</i>"

    full_caption = base_text + final_description + footer_text
    if len(full_caption) > 1024:
        full_caption = full_caption[:1021] + '...'

    return full_caption


def create_genres_keyboard(selected_genres=None):
    if selected_genres is None: selected_genres = []
    keyboard = []
    row = []
    for genre in MANGA_GENRES:
        prefix = "✅ " if genre["id"] in selected_genres else ""
        btn = InlineKeyboardButton(text=f"{prefix}{genre['russian']}", callback_data=f"genre_{genre['id']}")
        row.append(btn)
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row: keyboard.append(row)
    action_row = []
    if selected_genres:
        action_row.append(InlineKeyboardButton(text="🔍 Найти мангу", callback_data="search_by_genres"))
        action_row.append(InlineKeyboardButton(text="❌ Очистить выбор", callback_data="clear_genres"))
    if action_row: keyboard.append(action_row)
    keyboard.append([InlineKeyboardButton(text="📚 Выбрать тип манги", callback_data="select_kinds")])
    keyboard.append([InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def create_kinds_keyboard(selected_kinds=None):
    if selected_kinds is None: selected_kinds = []
    keyboard = []
    for kind in MANGA_KINDS:
        prefix = "✅ " if kind["id"] in selected_kinds else ""
        keyboard.append([InlineKeyboardButton(text=f"{prefix}{kind['russian']}", callback_data=f"kind_{kind['id']}")])
    if selected_kinds: keyboard.append([InlineKeyboardButton(text="❌ Очистить выбор", callback_data="clear_kinds")])
    keyboard.append([InlineKeyboardButton(text="⬅️ Назад к жанрам", callback_data="back_to_genres")])
    keyboard.append([InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


# --- Основные обработчики ---
@dp.message(CommandStart())
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    add_user_to_db(message.from_user.id)
    if not await check_subscription(message.from_user.id):
        await message.answer("Для использования бота, пожалуйста, подпишитесь на наши каналы:",
                             reply_markup=await get_subscribe_keyboard())
        return
    await show_main_menu(message, state)


async def show_main_menu(message_or_callback: types.Message | CallbackQuery, state: FSMContext):
    text = (
        "<b>👋 Главное меню AniMangaBot!</b>\n\n"
        "Здесь ты можешь найти и читать свою любимую мангу 📚.\n\n"
        "▫️ /start — Перезапуск бота\n"
        "▫️ /premium — Узнать о преимуществах и купить VIP"
    )
    markup = create_main_inline_keyboard()
    if isinstance(message_or_callback, types.Message):
        await message_or_callback.answer(text, reply_markup=markup)
    else:
        try:
            await message_or_callback.message.edit_text(text, reply_markup=markup)
        except TelegramBadRequest:
            await message_or_callback.message.delete()
            await message_or_callback.message.answer(text, reply_markup=markup)
        finally:
            await message_or_callback.answer()
    await state.set_state(MangaStates.main_menu)


@dp.callback_query(F.data == "back_to_main_menu", StateFilter("*"))
async def back_to_main_menu_handler(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await show_main_menu(callback, state)


@dp.callback_query(F.data == "check_subscription_again")
async def check_subscription_again_handler(callback: CallbackQuery, state: FSMContext):
    if await check_subscription(callback.from_user.id):
        await callback.answer("✅ Спасибо за подписку!", show_alert=True)
        await callback.message.delete()
        await cmd_start(callback.message, state)
    else:
        await callback.answer("❌ Вы еще не подписались на все каналы.", show_alert=True)


@dp.callback_query(MangaStates.main_menu)
@subscription_wrapper
async def handle_main_menu_buttons(callback: types.CallbackQuery, state: FSMContext):
    action = callback.data
    await callback.answer()
    if action == "main_search":
        await callback.message.edit_text("Введите название манги для поиска:")
        await state.set_state(MangaStates.waiting_for_search_query)
    elif action in ["main_favorites", "main_top"]:
        source = "favorites" if action == "main_favorites" else "top"
        if source == "favorites":
            manga_list = get_user_favorites(callback.from_user.id)
            if not manga_list:
                await bot.answer_callback_query(callback.id, "📭 Ваше избранное пусто.", show_alert=True)
                return
            title = "⭐️ Ваше избранное:"
        else:
            await callback.message.edit_text("🏆 Загружаю топ манг...")
            manga_list, _ = get_mangas(order_by="popular")
            if not manga_list:
                await callback.message.edit_text("❌ Не удалось загрузить топ.")
                return
            title = "🏆 Топ манг по популярности:"
        await state.set_state(MangaStates.selecting_manga)
        await state.update_data(source=source, manga_list=manga_list, list_page=0)
        total_pages = math.ceil(len(manga_list) / MANGAS_PER_PAGE)
        await callback.message.edit_text(title, reply_markup=create_manga_list_keyboard(manga_list, 0, total_pages))
    elif action == "main_genres":
        await show_genres_menu(callback, state)
    elif action == "main_settings":
        await show_settings_menu(callback, state)
    elif action == "main_premium":
        await show_premium_menu(callback.message, state, is_callback=True)


async def show_settings_menu(callback: CallbackQuery, state: FSMContext):
    await state.set_state(MangaStates.settings_menu)
    await callback.message.edit_text(
        "⚙️ <b>Настройки VIP</b>\n\nЗдесь вы можете настроить дополнительные функции, доступные по подписке.",
        reply_markup=create_settings_keyboard(callback.from_user.id)
    )


@dp.callback_query(MangaStates.settings_menu, F.data.startswith("set_batch_"))
async def handle_set_batch_size(callback: CallbackQuery, state: FSMContext):
    if not check_vip_access(callback.from_user.id):
        await callback.answer("Эта функция доступна только для VIP-пользователей.", show_alert=True)
        return
    new_size = int(callback.data.split("_")[2])
    save_user_settings(callback.from_user.id, {"batch_size": new_size})
    await callback.answer(f"✅ Установлено скачивание по {new_size} глав.", show_alert=True)
    await callback.message.edit_reply_markup(reply_markup=create_settings_keyboard(callback.from_user.id))


@dp.callback_query(MangaStates.settings_menu, F.data.startswith("set_format_"))
async def handle_set_output_format(callback: CallbackQuery, state: FSMContext):
    if not check_vip_access(callback.from_user.id):
        await callback.answer("Эта функция доступна только для VIP-пользователей.", show_alert=True)
        return
    new_format = callback.data.split("_")[2]
    save_user_settings(callback.from_user.id, {"output_format": new_format})
    format_name = "PDF" if new_format == "pdf" else "Telegraph"
    await callback.answer(f"✅ Формат выдачи изменен на {format_name}.", show_alert=True)
    await callback.message.edit_reply_markup(reply_markup=create_settings_keyboard(callback.from_user.id))


@dp.message(Command("premium"))
@subscription_wrapper
async def cmd_premium(message: types.Message, state: FSMContext):
    await show_premium_menu(message, state)


async def show_premium_menu(message: types.Message, state: FSMContext, is_callback: bool = False):
    await state.set_state(MangaStates.premium_menu)
    user_id = message.chat.id
    text = ("🌟 <b>Premium доступ</b> 🌟\n\n"
            "Получите максимум от бота с VIP-подпиской!\n\n"
            "<b>Что вы получаете:</b>\n"
            "✅ <b>Пакетная загрузка</b> — скачивайте сразу по несколько глав.\n"
            "✅ <b>Быстрая навигация</b> — переключайтесь между главами прямо под файлом.\n"
            "✅ <b>Настройка скачивания</b> — выберите, сколько глав скачивать за раз.\n"
            "✅ <b>Формат Telegraph</b> — читайте мангу прямо в браузере без скачивания файлов.\n\n")
    if check_vip_access(user_id):
        expiry_date = get_vip_expiry_date(user_id)
        text += (f"✅ <b>У вас уже есть активная подписка!</b>\n"
                 f"     <i>Она действует до: {expiry_date}</i>\n\n"
                 f"Вы можете продлить её, выбрав один из планов ниже:")
    else:
        text += "Выберите подходящий план:"
    markup = create_premium_keyboard()
    if is_callback:
        await message.edit_text(text, reply_markup=markup)
    else:
        await message.answer(text, reply_markup=markup)


@dp.callback_query(MangaStates.settings_menu, F.data == "main_premium")
async def handle_premium_from_settings(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await show_premium_menu(callback.message, state, is_callback=True)


@dp.callback_query(F.data == "main_premium", F.message.document)
async def handle_premium_from_document(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await show_premium_menu(callback.message, state, is_callback=False)


@dp.callback_query(MangaStates.premium_menu, F.data.startswith("buy_"))
async def handle_buy_premium(callback: CallbackQuery):
    plan_key = callback.data.split("_", 1)[1]
    if plan_key not in VIP_PLANS:
        await callback.answer("Неизвестный тарифный план.", show_alert=True)
        return
    plan = VIP_PLANS[plan_key]

    await bot.send_invoice(
        chat_id=callback.from_user.id,
        title=plan["title"],
        description=f"VIP-доступ к функциям бота на {plan['days']} дней.",
        payload=plan_key,
        provider_token="",
        currency="XTR",
        prices=[LabeledPrice(label=plan["title"], amount=plan["stars"])]
    )
    await callback.answer()


@dp.pre_checkout_query()
async def pre_checkout_query_handler(pre_checkout_query: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)


@dp.message(F.successful_payment)
async def successful_payment_handler(message: types.Message):
    user_id = message.from_user.id
    payment_info = message.successful_payment
    plan_key = payment_info.invoice_payload
    grant_vip_access(user_id, plan_key)
    plan_title = VIP_PLANS.get(plan_key, {}).get("title", "услугу")
    expiry_date = get_vip_expiry_date(user_id)
    await bot.send_message(user_id, f"🎉 <b>Спасибо за покупку!</b>\n\n"
                                    f"Вам предоставлен «{plan_title}».\n"
                                    f"Ваша подписка активна до: <b>{expiry_date}</b>.\n\n"
                                    "Все VIP-функции теперь доступны!")


async def show_genres_menu(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "📋 Выберите жанры для поиска манги.\nМожно выбрать несколько жанров одновременно:",
        reply_markup=create_genres_keyboard()
    )
    await state.set_state(MangaStates.selecting_genres)
    await state.update_data(selected_genres=[], selected_kinds=[])


@dp.callback_query(MangaStates.selecting_genres)
async def handle_genre_selection(callback: CallbackQuery, state: FSMContext):
    action = callback.data
    await callback.answer()
    if action == "clear_genres":
        await state.update_data(selected_genres=[])
        await callback.message.edit_reply_markup(reply_markup=create_genres_keyboard())
    elif action == "search_by_genres":
        await search_by_genres(callback, state)
    elif action == "select_kinds":
        data = await state.get_data()
        selected_kinds = data.get('selected_kinds', [])
        await callback.message.edit_text("📚 Выберите тип манги:", reply_markup=create_kinds_keyboard(selected_kinds))
        await state.set_state(MangaStates.selecting_kinds)
    elif action.startswith("genre_"):
        genre_id = int(action.split("_")[1])
        data = await state.get_data()
        selected_genres = data.get('selected_genres', [])
        if genre_id in selected_genres:
            selected_genres.remove(genre_id)
        else:
            selected_genres.append(genre_id)
        await state.update_data(selected_genres=selected_genres)
        await callback.message.edit_reply_markup(reply_markup=create_genres_keyboard(selected_genres))


@dp.callback_query(MangaStates.selecting_kinds)
async def handle_kind_selection(callback: CallbackQuery, state: FSMContext):
    action = callback.data
    await callback.answer()
    if action == "back_to_genres":
        data = await state.get_data()
        selected_genres = data.get('selected_genres', [])
        await callback.message.edit_text("📋 Выберите жанры...", reply_markup=create_genres_keyboard(selected_genres))
        await state.set_state(MangaStates.selecting_genres)
    elif action == "clear_kinds":
        await state.update_data(selected_kinds=[])
        await callback.message.edit_reply_markup(reply_markup=create_kinds_keyboard())
    elif action.startswith("kind_"):
        kind_id = action.split("_")[1]
        data = await state.get_data()
        selected_kinds = data.get('selected_kinds', [])
        if kind_id in selected_kinds:
            selected_kinds.remove(kind_id)
        else:
            selected_kinds.append(kind_id)
        await state.update_data(selected_kinds=selected_kinds)
        await callback.message.edit_reply_markup(reply_markup=create_kinds_keyboard(selected_kinds))


async def search_by_genres(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    selected_genres = data.get('selected_genres', [])
    selected_kinds = data.get('selected_kinds', [])
    if not selected_genres and not selected_kinds:
        await bot.answer_callback_query(callback.id, "Пожалуйста, выберите хотя бы один жанр или тип", show_alert=True)
        return
    selected_genre_names = [g['russian'] for g in MANGA_GENRES if g['id'] in selected_genres]
    selected_kind_names = [k['russian'] for k in MANGA_KINDS if k['id'] in selected_kinds]
    genres_text = ', '.join(selected_genre_names) if selected_genres else "любые"
    kinds_text = ', '.join(selected_kind_names) if selected_kinds else "любые"
    search_message = await callback.message.edit_text(f"🔍 Ищу мангу...\n\nЖанры: {genres_text}\nТипы: {kinds_text}")
    genres_param = ','.join([g['text'] for g in MANGA_GENRES if g['id'] in selected_genres])
    kinds_param = ','.join(selected_kinds)
    try:
        mangas, page_nav = get_mangas_by_genres_and_kinds(genres_param, kinds_param, api_page=1)
        if not mangas:
            await search_message.edit_text(f"❌ Манга не найдена.", reply_markup=create_genres_keyboard(selected_genres))
            await state.set_state(MangaStates.selecting_genres)
            return
        await state.set_state(MangaStates.selecting_manga)
        await state.update_data(source="genres", manga_list=mangas, list_page=0, selected_genres=selected_genres,
                                selected_kinds=selected_kinds)
        total_pages = math.ceil(len(mangas) / MANGAS_PER_PAGE)
        await search_message.edit_text(f"🔍 Найдено манги: {page_nav.get('count', len(mangas))}",
                                       reply_markup=create_manga_list_keyboard(mangas, 0, total_pages))
    except Exception as e:
        logger.error(f"Ошибка при поиске по жанрам: {e}")
        await search_message.edit_text(f"❌ Произошла ошибка при поиске.",
                                       reply_markup=create_genres_keyboard(selected_genres))
        await state.set_state(MangaStates.selecting_genres)


async def show_manga_chapter_grid(manga_id: str, source: types.Message | CallbackQuery, state: FSMContext,
                                  page: int = 0):
    message = source.message if isinstance(source, CallbackQuery) else source
    user_id = source.from_user.id
    try:
        if isinstance(source, CallbackQuery): await source.answer("Загружаю информацию о манге...")

        info = get_manga_info(manga_id)
        if not info:
            await message.edit_text("❌ Не удалось получить информацию об этой манге.")
            return

        all_chapters = info.get('chapters', {}).get('list', [])
        unique_chapters, seen_chapter_nums = [], set()
        for chapter in all_chapters:
            ch_num = chapter.get('ch')
            if ch_num and ch_num not in seen_chapter_nums:
                unique_chapters.append(chapter)
                seen_chapter_nums.add(ch_num)
        chapters_sorted = sorted(unique_chapters, key=lambda x: float(x['ch']))

        cover_url = info.get('image', {}).get('original', 'https://via.placeholder.com/200x300.png?text=No+Image')
        caption = create_manga_caption_for_grid(info, len(chapters_sorted))
        is_fav = is_in_favorites(user_id, manga_id)
        keyboard = create_chapter_grid_keyboard(manga_id, chapters_sorted, is_fav, page=page)

        cached_image = await db.get_image_from_cache(cover_url)
        photo_to_send = ""
        if cached_image:
            photo_to_send = cached_image['file_id']
            logger.info(f"Cache HIT: Обложка для {manga_id} взята из кэша.")
        else:
            photo_to_send = cover_url
            logger.info(f"Cache MISS: Обложка для {manga_id} будет загружена по URL.")

        current_message = message
        sent_message = None

        if isinstance(source, CallbackQuery) and source.message.photo:
            try:
                sent_message = await current_message.edit_caption(caption=caption, reply_markup=keyboard)
            except TelegramBadRequest as e:
                if 'wrong file identifier' in str(e) or 'PHOTO_INVALID' in str(e):
                    logger.warning(f"Невалидный file_id для обложки {manga_id}. Переотправляю.")
                    await current_message.delete()
                    sent_message = await bot.send_photo(chat_id=message.chat.id, photo=photo_to_send, caption=caption,
                                                        reply_markup=keyboard)
                else:
                    raise e
        else:
            try:
                await current_message.delete()
            except TelegramBadRequest:
                pass
            sent_message = await bot.send_photo(chat_id=message.chat.id, photo=photo_to_send, caption=caption,
                                                reply_markup=keyboard)

        if not cached_image and sent_message and sent_message.photo:
            photo = sent_message.photo[-1]
            await db.add_image_to_cache(cover_url, photo.file_id, photo.file_unique_id)
            logger.info(f"Cache SAVE: Обложка для {manga_id} ({cover_url[:50]}...) сохранена в кэш.")

        await state.set_state(MangaStates.viewing_manga_chapters)
        await state.update_data(manga_id=manga_id, info=info, chapters=chapters_sorted, grid_page=page,
                                photo_msg_id=sent_message.message_id)
    except Exception as e:
        logger.error(f"Ошибка в show_manga_chapter_grid: {e}", exc_info=True)
        await message.answer("Произошла ошибка при загрузке манги. Попробуйте позже.")


@dp.message(MangaStates.waiting_for_search_query)
@subscription_wrapper
async def process_search_query(message: types.Message, state: FSMContext):
    search_query = message.text.strip()
    try:
        await message.delete()
    except TelegramBadRequest:
        pass
    if not search_query:
        await message.answer("Пожалуйста, введите поисковый запрос.")
        return
    search_msg = await message.answer(f"🔍 Ищу '{search_query}'...")
    mangas, _ = get_mangas(query=search_query, api_page=1)
    if not mangas:
        await search_msg.edit_text("❌ Ничего не найдено.")
        await asyncio.sleep(3)
        await search_msg.delete()
        await show_main_menu(message, state)
        return
    await state.set_state(MangaStates.selecting_manga)
    await state.update_data(source="search", manga_list=mangas, list_page=0)
    total_pages = math.ceil(len(mangas) / MANGAS_PER_PAGE)
    await search_msg.edit_text("🔍 Результаты поиска:", reply_markup=create_manga_list_keyboard(mangas, 0, total_pages))


@dp.callback_query(MangaStates.selecting_manga)
async def handle_manga_selection(callback: types.CallbackQuery, state: FSMContext):
    if callback.data.startswith("manga_"):
        manga_id = str(callback.data.split("_")[1])
        await show_manga_chapter_grid(manga_id, callback, state)
    elif callback.data.startswith("list_page_"):
        page = int(callback.data.split("_")[2])
        data = await state.get_data()
        manga_list = data.get('manga_list', [])
        total_pages = math.ceil(len(manga_list) / MANGAS_PER_PAGE)
        await callback.message.edit_text("🔍 Результаты:",
                                         reply_markup=create_manga_list_keyboard(manga_list, page, total_pages))
        await callback.answer()


async def send_chapter_or_telegraph(callback: types.CallbackQuery, state: FSMContext, chapter_num_to_dl: float,
                                    is_last_in_batch: bool = True):
    user_id = callback.from_user.id
    settings = get_user_settings(user_id)
    output_format = 'telegraph' if settings.get('output_format') == 'telegraph' and check_vip_access(user_id) else 'pdf'

    data = await state.get_data()
    manga_id = data.get('manga_id')
    if not manga_id or not data.get('chapters'):
        await bot.send_message(user_id, "❌ Ошибка сессии. Пожалуйста, выберите мангу заново из главного меню.")
        return
    chapter_to_dl = next((ch for ch in data['chapters'] if float(ch['ch']) == chapter_num_to_dl), None)
    if not chapter_to_dl:
        await bot.send_message(user_id, f"❌ Ошибка: Глава {chapter_num_to_dl} не найдена.")
        return

    last_doc_msg_id = data.get('last_doc_msg_id')
    if last_doc_msg_id:
        try:
            await bot.edit_message_reply_markup(chat_id=user_id, message_id=last_doc_msg_id, reply_markup=None)
        except TelegramBadRequest:
            pass

    keyboard = create_document_navigation_keyboard(data['chapters'], chapter_num_to_dl,
                                                   user_id) if is_last_in_batch else None

    cached_chapter = await db.get_chapter_from_cache(manga_id, str(chapter_num_to_dl), output_format)
    sent_msg = None

    if cached_chapter:
        logger.info(f"Cache HIT: Глава {manga_id}/{chapter_num_to_dl} ({output_format}) найдена в кэше.")
        try:
            if output_format == 'pdf':
                sent_msg = await bot.send_document(user_id, document=cached_chapter['file_id'], reply_markup=keyboard)
            else:  # telegraph
                sent_msg = await bot.send_message(user_id,
                                                  f"📖 <b>{get_display_name(data['info'])} - Глава {chapter_num_to_dl}</b>\n\n<a href='{cached_chapter['file_id']}'>Читать в Telegraph</a>",
                                                  reply_markup=keyboard, disable_web_page_preview=False)
            if sent_msg and is_last_in_batch: await state.update_data(last_doc_msg_id=sent_msg.message_id)
            return
        except (TelegramBadRequest, TelegramForbiddenError) as e:
            logger.warning(
                f"Кэшированный file_id для главы {chapter_num_to_dl} невалиден (Ошибка: {e}). Файл НЕ будет скачиваться заново.")
            await bot.send_message(user_id, "Кэш для этой главы устарел. Попробуйте запросить её ещё раз.",
                                   reply_markup=keyboard)
            return

    logger.info(
        f"Cache MISS: Глава {manga_id}/{chapter_num_to_dl} ({output_format}) не найдена в кэше. Начинаю загрузку.")
    if output_format == 'pdf':
        pdf_bytes = await download_chapter(manga_id, chapter_to_dl, callback)
        if pdf_bytes:
            filename = f"{get_display_name(data['info']).replace(' ', '_')}_ch_{chapter_to_dl['ch']}.pdf"
            try:
                file_to_send_user = BufferedInputFile(pdf_bytes, filename)
                sent_msg = await bot.send_document(user_id, document=file_to_send_user, reply_markup=keyboard)

                if CHANNEL_ID and sent_msg and sent_msg.document:
                    pdf_bytes_rewound = BytesIO(pdf_bytes)
                    file_to_send_cache = BufferedInputFile(pdf_bytes_rewound.read(), filename)
                    sent_to_channel_msg = await bot.send_document(CHANNEL_ID, file_to_send_cache)
                    if sent_to_channel_msg.document:
                        cache_doc = sent_to_channel_msg.document
                        await db.add_chapter_to_cache(manga_id, str(chapter_num_to_dl), 'pdf', cache_doc.file_id,
                                                      cache_doc.file_unique_id)
                        logger.info(f"Cache SAVE: Глава {manga_id}/{chapter_num_to_dl} (PDF) сохранена в кэш.")
            except Exception as e:
                logger.error(f"Ошибка при отправке/кэшировании PDF {chapter_num_to_dl}: {e}")
                await bot.send_message(user_id, f"❌ Ошибка при отправке главы {chapter_num_to_dl}.")

    else:  # output_format == 'telegraph'
        url_api = f"{BASE_URL}/{manga_id}/chapter/{chapter_to_dl['id']}"
        logger.info(f"API Request: get pages for Telegraph (manga_id: {manga_id}, chapter: {chapter_to_dl['id']})")
        resp_api = session.get(url_api).json()
        pages = resp_api.get('response', {}).get('pages', {}).get('list', [])

        if not pages:
            logger.warning(
                f"Не удалось получить страницы для Telegraph: manga_id {manga_id}, chapter {chapter_to_dl['ch']}")
            await bot.send_message(user_id, "Не удалось получить страницы для создания Telegraph-статьи.")
            return

        telegraph_url = await upload_to_telegraph(get_display_name(data['info']), chapter_to_dl, pages, callback)
        if telegraph_url:
            sent_msg = await bot.send_message(user_id,
                                              f"📖 <b>{get_display_name(data['info'])} - Глава {chapter_num_to_dl}</b>\n\n<a href='{telegraph_url}'>Читать в Telegraph</a>",
                                              reply_markup=keyboard, disable_web_page_preview=False)
            await db.add_chapter_to_cache(manga_id, str(chapter_num_to_dl), 'telegraph', telegraph_url,
                                          f"telegraph_{manga_id}_{chapter_num_to_dl}")
            logger.info(f"Cache SAVE: Глава {manga_id}/{chapter_num_to_dl} (Telegraph) сохранена в кэш.")

    if sent_msg and is_last_in_batch:
        await state.update_data(last_doc_msg_id=sent_msg.message_id)


@dp.callback_query(StateFilter(MangaStates.viewing_manga_chapters, None), F.data.startswith(("doc_nav_", "batch_dl_")))
async def handle_vip_navigation(callback: CallbackQuery, state: FSMContext):
    if not check_vip_access(callback.from_user.id):
        await callback.answer("Эта функция доступна только для Premium-пользователей.", show_alert=True)
        return
    await callback.answer()
    await state.update_data(last_doc_msg_id=callback.message.message_id)
    action_full = callback.data
    if action_full.startswith("doc_nav_"):
        chapter_num_to_send = float(action_full.split("_")[2])
        await send_chapter_or_telegraph(callback, state, chapter_num_to_send)
    elif action_full.startswith("batch_dl_"):
        start_index = int(action_full.split("_")[2])
        asyncio.create_task(run_batch_download(callback, state, start_index))


@dp.callback_query(MangaStates.viewing_manga_chapters)
async def handle_chapter_grid_actions(callback: types.CallbackQuery, state: FSMContext):
    action_full = callback.data
    action = action_full.split("_")[0]
    data = await state.get_data()
    manga_id = data.get('manga_id')
    if not manga_id:
        await callback.answer("Ошибка сессии, выберите мангу заново.", show_alert=True)
        return
    if action == "grid":
        page = int(action_full.split("_")[2])
        await callback.answer()
        await show_manga_chapter_grid(manga_id, callback, state, page=page)
    elif action == "toggle":
        is_fav = is_in_favorites(callback.from_user.id, manga_id)
        if is_fav:
            remove_from_favorites(callback.from_user.id, manga_id)
            await callback.answer("🗑 Удалено из избранного.")
        else:
            add_to_favorites(callback.from_user.id, data['info'])
            await callback.answer("⭐️ Добавлено в избранное!")
        await show_manga_chapter_grid(manga_id, callback, state, page=data.get('grid_page', 0))
    elif action == "dl":
        await callback.answer("Начинаю загрузку...")
        chapter_num = float(action_full.split("_")[1])
        await state.update_data(last_doc_msg_id=None)
        await send_chapter_or_telegraph(callback, state, chapter_num)
    elif action_full == "back_to_grid":
        await callback.answer()
        try:
            await callback.message.delete()
        except TelegramBadRequest:
            pass
        await state.update_data(last_doc_msg_id=None)
        grid_page = data.get('grid_page', 0)
        await show_manga_chapter_grid(manga_id, callback.message, state, page=grid_page)


# --- Админ-панель ---
@dp.message(Command("admin"))
async def cmd_admin(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS: return
    await state.clear()
    await state.set_state(AdminStates.panel)
    await message.answer("Добро пожаловать в админ-панель!", reply_markup=create_admin_keyboard())


@dp.callback_query(AdminStates.panel)
async def handle_admin_panel(callback: CallbackQuery, state: FSMContext):
    action = callback.data
    await callback.answer()

    if action == "admin_stats":
        users_count = len(load_data(USERS_FILE, {"users": []})["users"])
        downloads_count = load_data(STATS_FILE, {"downloads": 0})["downloads"]
        text = f"<b>📊 Статистика бота:</b>\n\n👤 Уникальных пользователей: {users_count}\n📥 Всего скачано глав: {downloads_count}"
        await callback.message.edit_text(text, reply_markup=create_admin_keyboard())
    elif action == "admin_mailing":
        await state.set_state(AdminStates.mailing_get_content)
        await callback.message.edit_text("Пришлите сообщение, которое хотите разослать.")
    elif action == "admin_add_channel":
        await state.set_state(AdminStates.adding_channel)
        await callback.message.edit_text("Отправьте ID канала (например, @channelname или -100123456789).")
    elif action == "admin_remove_channel":
        await state.set_state(AdminStates.removing_channel)
        await callback.message.edit_text("Отправьте ID канала для удаления.")
    elif action == "admin_list_channels":
        channels = load_data(CHANNELS_FILE, {"channels": []})["channels"]
        text = "<b>Каналы для обязательной подписки:</b>\n\n" + "\n".join(
            f"<code>{ch}</code>" for ch in channels) if channels else "Список каналов пуст."
        await callback.message.edit_text(text, reply_markup=create_admin_keyboard())
    elif action == "admin_exit":
        await callback.message.delete()
        await state.clear()


@dp.message(AdminStates.adding_channel)
async def process_adding_channel(message: types.Message, state: FSMContext):
    channel_id = message.text.strip()
    channels_data = load_data(CHANNELS_FILE, {"channels": []})
    if channel_id not in channels_data["channels"]:
        channels_data["channels"].append(channel_id)
        save_data(CHANNELS_FILE, channels_data)
        await message.answer(f"✅ Канал <code>{channel_id}</code> успешно добавлен.")
    else:
        await message.answer(f"⚠️ Канал <code>{channel_id}</code> уже есть в списке.")
    await state.set_state(AdminStates.panel)
    await message.answer("Админ-панель:", reply_markup=create_admin_keyboard())


@dp.message(AdminStates.removing_channel)
async def process_removing_channel(message: types.Message, state: FSMContext):
    channel_id = message.text.strip()
    channels_data = load_data(CHANNELS_FILE, {"channels": []})
    if channel_id in channels_data["channels"]:
        channels_data["channels"].remove(channel_id)
        save_data(CHANNELS_FILE, channels_data)
        await message.answer(f"🗑 Канал <code>{channel_id}</code> удален.")
    else:
        await message.answer(f"❌ Канал <code>{channel_id}</code> не найден в списке.")
    await state.set_state(AdminStates.panel)
    await message.answer("Админ-панель:", reply_markup=create_admin_keyboard())


# --- ЛОГИКА РАССЫЛКИ ---
@dp.message(AdminStates.mailing_get_content, F.media_group_id)
@dp.message(AdminStates.mailing_get_content)
async def handle_mailing_content(message: types.Message, state: FSMContext):
    mailing_data = {}
    if message.text:
        mailing_data = {"type": "text", "text": message.html_text}
    elif message.photo:
        mailing_data = {"type": "photo", "file_id": message.photo[-1].file_id, "caption": message.html_text}
    elif message.video:
        mailing_data = {"type": "video", "file_id": message.video.file_id, "caption": message.html_text}
    elif message.document:
        mailing_data = {"type": "document", "file_id": message.document.file_id, "caption": message.html_text}
    elif message.audio:
        mailing_data = {"type": "audio", "file_id": message.audio.file_id, "caption": message.html_text}
    else:
        await message.answer("❌ Неподдерживаемый тип сообщения.");
        return

    await state.update_data(mailing_data=mailing_data)
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Пропустить кнопки", callback_data="mailing_skip_buttons")]])
    await message.answer(
        "📎 Контент сохранен! Теперь отправьте кнопки в формате:\n\n<code>Текст - https://ссылка</code>\n\nИли нажмите 'Пропустить'",
        reply_markup=keyboard)
    await state.set_state(AdminStates.mailing_get_buttons)


@dp.message(AdminStates.mailing_get_buttons)
async def handle_mailing_buttons(message: types.Message, state: FSMContext):
    try:
        buttons = []
        for line in message.text.strip().split('\n'):
            if ' - ' in line:
                text, url = line.split(' - ', 1)
                buttons.append([InlineKeyboardButton(text=text.strip(), url=url.strip())])
        await state.update_data(mailing_buttons=buttons)
        await show_mailing_preview(message.from_user.id, state)
    except Exception as e:
        await message.answer(f"❌ Ошибка в формате кнопок: {e}\nПопробуйте еще раз:")


@dp.callback_query(AdminStates.mailing_get_buttons, F.data == "mailing_skip_buttons")
async def skip_mailing_buttons(callback: CallbackQuery, state: FSMContext):
    await state.update_data(mailing_buttons=[])
    await callback.message.delete()
    await show_mailing_preview(callback.from_user.id, state)
    await callback.answer()


async def send_broadcast_message(chat_id: int, data: dict):
    mailing_data = data.get('mailing_data', {})
    buttons = data.get('mailing_buttons', [])
    reply_markup = InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None
    message_type = mailing_data.get('type')
    try:
        if message_type == 'text':
            await bot.send_message(chat_id=chat_id, text=mailing_data['text'], reply_markup=reply_markup,
                                   disable_web_page_preview=True)
        elif message_type == 'photo':
            await bot.send_photo(chat_id=chat_id, photo=mailing_data['file_id'], caption=mailing_data.get('caption'),
                                 reply_markup=reply_markup)
        elif message_type == 'video':
            await bot.send_video(chat_id=chat_id, video=mailing_data['file_id'], caption=mailing_data.get('caption'),
                                 reply_markup=reply_markup)
        elif message_type == 'document':
            await bot.send_document(chat_id=chat_id, document=mailing_data['file_id'],
                                    caption=mailing_data.get('caption'), reply_markup=reply_markup)
        elif message_type == 'audio':
            await bot.send_audio(chat_id=chat_id, audio=mailing_data['file_id'], caption=mailing_data.get('caption'),
                                 reply_markup=reply_markup)
        return True
    except Exception as e:
        if "bot was blocked by the user" in str(e):
            logger.warning(f"Рассылка: Пользователь {chat_id} заблокировал бота.")
        elif "chat not found" in str(e):
            logger.warning(f"Рассылка: Чат с пользователем {chat_id} не найден.")
        else:
            logger.error(f"Рассылка: Ошибка отправки пользователю {chat_id}: {e}")
        return False


async def show_mailing_preview(admin_id: int, state: FSMContext):
    data = await state.get_data()
    await bot.send_message(admin_id, "👀 Предпросмотр сообщения:")
    await send_broadcast_message(admin_id, data)
    confirm_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Начать рассылку", callback_data="mailing_confirm_send")],
        [InlineKeyboardButton(text="❌ Отменить", callback_data="mailing_confirm_cancel")]])
    await bot.send_message(admin_id, "Начать рассылку?", reply_markup=confirm_keyboard)
    await state.set_state(AdminStates.mailing_confirm)


@dp.callback_query(AdminStates.mailing_confirm)
async def handle_mailing_confirmation(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    if callback.data == "mailing_confirm_send":
        await callback.message.edit_text("🔄 Начинаю рассылку...")
        asyncio.create_task(start_broadcast(callback.from_user.id, state))
    else:
        await callback.message.edit_text("❌ Рассылка отменена.")
        await state.set_state(AdminStates.panel)
        await callback.message.answer("Админ-панель:", reply_markup=create_admin_keyboard())


async def start_broadcast(admin_id: int, state: FSMContext):
    data = await state.get_data()
    users = load_data(USERS_FILE, {"users": []})["users"]
    total_users = len(users)
    successful, failed = 0, 0
    start_time = time.time()
    logger.info(f"Начинаю рассылку для {total_users} пользователей.")

    progress_msg = await bot.send_message(admin_id, f"📤 Рассылка начата... 0/{total_users}")

    for i, user_id in enumerate(users):
        if await send_broadcast_message(user_id, data):
            successful += 1
        else:
            failed += 1

        if (i + 1) % 25 == 0 or (i + 1) == total_users:
            try:
                await bot.edit_message_text(
                    chat_id=admin_id,
                    message_id=progress_msg.message_id,
                    text=f"📤 Рассылка... {i + 1}/{total_users}\n✅ Успешно: {successful}\n❌ Ошибок: {failed}"
                )
            except TelegramBadRequest:
                pass
        await asyncio.sleep(0.04)

    end_time = time.time()
    duration = round(end_time - start_time)

    final_text = (f"✅ Рассылка завершена за {duration} сек.!\n\n"
                  f"👥 Всего: {total_users}\n"
                  f"✅ Успешно: {successful}\n"
                  f"❌ Ошибок: {failed}")
    logger.info(final_text)
    await bot.send_message(admin_id, final_text)

    await state.set_state(AdminStates.panel)
    await bot.send_message(admin_id, "Админ-панель:", reply_markup=create_admin_keyboard())


async def main():
    global telegraph

    await db.init_db()
    logger.info("База данных инициализирована.")

    access_token = await db.load_telegraph_token()
    if not access_token:
        try:
            account = await asyncio.to_thread(Telegraph().create_account, short_name='AniMangaBot')
            access_token = account['access_token']
            await db.save_telegraph_token(access_token)
            logger.info("Создан новый аккаунт Telegraph и сохранен токен в БД.")
        except Exception as e:
            logger.critical(f"Не удалось создать аккаунт Telegraph: {e}", exc_info=True)
            return

    telegraph = Telegraph(access_token=access_token)

    try:
        await asyncio.to_thread(telegraph.get_account_info)
        logger.info("Аккаунт Telegraph успешно подключен.")
    except Exception as e:
        logger.error(f"Ошибка подключения к Telegraph: {e}")

    logger.info("Бот запущен...")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот остановлен.")
