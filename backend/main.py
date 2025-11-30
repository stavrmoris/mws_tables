import asyncio
import requests
from bs4 import BeautifulSoup
import re
import os
import requests
import json
import uvicorn
import logging
import vk_api
from datetime import datetime
from typing import List, Optional
from fastapi import Query
from fastapi.responses import StreamingResponse
import csv
import io
import pandas as pd

# Библиотеки для сбора данных
from telethon import TelegramClient
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Библиотеки для API и Бота
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.fsm.storage.memory import MemoryStorage

# --- НАСТРОЙКИ ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

# KEYS - TELEGRAM
TG_API_ID = os.getenv('TG_API_ID')
TG_API_HASH = os.getenv('TG_API_HASH')
TARGET_CHANNEL = os.getenv('TARGET_CHANNEL')
TG_BOT_TOKEN = os.getenv('TG_BOT_TOKEN')

# KEYS - VK
VK_ACCESS_TOKEN = os.getenv('VK_ACCESS_TOKEN')
VK_GROUP_DOMAIN = os.getenv('VK_GROUP_DOMAIN')

# KEYS - YOUTUBE & RUTUBE
YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')
YOUTUBE_CHANNEL_HANDLE = os.getenv('YOUTUBE_CHANNEL_ID')
RUTUBE_CHANNEL_NAME = os.getenv('RUTUBE_CHANNEL_NAME', 'mts')

# KEYS - HABR
HABR_TARGET_COMPANIES = os.getenv('HABR_TARGET_COMPANIES', 'mts_ai,telegram,vk').split(',')

# KEYS - MWS & AI
MWS_TOKEN = os.getenv('MWS_TOKEN')
MWS_TABLE_ID = os.getenv('MWS_TABLE_ID')
MWS_VIEW_ID = os.getenv('MWS_VIEW_ID')
MWS_CHANNELS_TABLE_ID = os.getenv('MWS_CHANNELS_TABLE_ID')
MWS_CHANNELS_VIEW_ID = os.getenv('MWS_CHANNELS_VIEW_ID')
OPENROUTER_API_KEY = os.getenv('OPENROUTER_API_KEY')
MWS_API_URL = "https://tables.mws.ru/fusion/v1/datasheets"

# --- ИНИЦИАЛИЗАЦИЯ ---
app = FastAPI(title="MTS ANALYZER", version="2.0")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

bot = Bot(token=TG_BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)


class ChatRequest(BaseModel):
    question: str


class ChatResponse(BaseModel):
    answer: str


# --- MWS HELPERS ---
class MWSTablesAPI:
    def __init__(self, token, table_id, view_id):
        self.base_url = f"{MWS_API_URL}/{table_id}/records"
        self.headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        self.view_id = view_id

    def get_existing_links(self):
        try:
            params = {"viewId": self.view_id, "fieldKey": "name", "fields": ["Ссылка"]}
            response = requests.get(self.base_url, headers=self.headers, params=params)
            if response.status_code == 200:
                records = response.json().get('data', {}).get('records', [])
                return {r['fields'].get('Ссылка') for r in records if r['fields'].get('Ссылка')}
            return set()
        except Exception as e:
            logger.error(f"Ошибка проверки дублей: {e}")
            return set()

    def add_records(self, records_data):
        if not records_data: return
        params = {"viewId": self.view_id, "fieldKey": "name"}
        payload = {"records": [{"fields": rec} for rec in records_data], "fieldKey": "name"}
        try:
            response = requests.post(self.base_url, headers=self.headers, params=params, json=payload)
            response.raise_for_status()
            logger.info(f"✅ Успешно добавлено {len(records_data)} записей в MWS")
        except Exception as e:
            logger.error(f"Ошибка MWS: {e}")


def get_mws_data():
    """Получает все данные для аналитики"""
    try:
        url = f"{MWS_API_URL}/{MWS_TABLE_ID}/records"
        headers = {"Authorization": f"Bearer {MWS_TOKEN}", "Content-Type": "application/json"}
        # Берем 1000 записей (максимум API)
        params = {"viewId": MWS_VIEW_ID, "fieldKey": "name", "pageSize": 1000}
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json().get('data', {}).get('records', [])
        return []
    except Exception as e:
        logger.error(f"Ошибка чтения MWS: {e}")
        return []


# --- AI HELPERS ---
def analyze_text_with_llm(text):
    if not OPENROUTER_API_KEY or len(text) < 5: return "Neutral", "Авто-саммари"
    url = "https://openrouter.ai/api/v1/chat/completions"
    headers = {"Authorization": f"Bearer {OPENROUTER_API_KEY}"}
    prompt = f"Проанализируй текст. 1. Тональность (Positive/Negative/Neutral). 2. Саммари (1 предложение).\nТекст: {text[:800]}\nВерни JSON: {{\"sentiment\": \"...\", \"summary\": \"...\"}}"
    try:
        data = {"model": "meta-llama/llama-3.3-70b-instruct:free", "messages": [{"role": "user", "content": prompt}]}
        res = requests.post(url, headers=headers, json=data, timeout=10)
        if res.status_code == 200:
            content = res.json()['choices'][0]['message']['content']
            import re
            json_match = re.search(r'\{.*\}', content, re.DOTALL)
            if json_match:
                parsed = json.loads(json_match.group(0))
                return parsed.get("sentiment", "Neutral"), parsed.get("summary", "")
    except Exception:
        pass
    return "Neutral", "Ошибка анализа"


def get_smart_answer(question: str) -> str:
    try:
        records = get_mws_data()
        if not records:
            return "У меня пока нет данных для анализа."

        # --- ШАГ 1: Математика на Python (Точная статистика) ---
        # Сортируем и ищем лидеров Python-ом, чтобы не полагаться на LLM в математике

        # Самый популярный по лайкам
        top_like = max(records, key=lambda x: x.get('fields', {}).get('Лайки', 0))
        top_like_title = top_like['fields'].get('Название', 'Без названия')
        max_likes = top_like['fields'].get('Лайки', 0)

        # Самый популярный по просмотрам
        top_view = max(records, key=lambda x: x.get('fields', {}).get('Просмотры', 0))
        top_view_title = top_view['fields'].get('Название', 'Без названия')
        max_views = top_view['fields'].get('Просмотры', 0)

        # Общая сумма
        total_views = sum(r.get('fields', {}).get('Просмотры', 0) for r in records)

        # --- ШАГ 2: Формируем контекст ---
        # Мы явно говорим нейросети правильные ответы на популярные вопросы
        stats_summary = f"""
        ВАЖНАЯ СТАТИСТИКА (Используй эти цифры для ответов):
        - Всего постов в базе: {len(records)}
        - Общее число просмотров: {total_views}
        - РЕКОРД ПО ЛАЙКАМ: "{top_like_title}" ({max_likes} лайков)
        - РЕКОРД ПО ПРОСМОТРАМ: "{top_view_title}" ({max_views} просмотров)
        """

        # Добавляем список последних постов для контекста (увеличим до 20)
        last_posts_context = "ПОСЛЕДНИЕ ПУБЛИКАЦИИ:\n"
        for r in records[-20:]:
            f = r.get("fields", {})
            title = f.get('Название', 'Без названия')[:50]
            last_posts_context += f"- [{f.get('Источник')}] {title} | Лайков: {f.get('Лайки')} | Тон: {f.get('Тональность')}\n"

        # --- ШАГ 3: Запрос к LLM ---
        prompt = f"""
        Ты аналитик данных. Твоя задача - отвечать на вопросы пользователя, используя предоставленную статистику.

        {stats_summary}

        {last_posts_context}

        ВОПРОС ПОЛЬЗОВАТЕЛЯ: {question}
        """

        ai_url = "https://openrouter.ai/api/v1/chat/completions"
        ai_headers = {
            "Authorization": f"Bearer {OPENROUTER_API_KEY}",
            "HTTP-Referer": "https://github.com/mws-hack",
        }
        ai_data = {
            "model": "meta-llama/llama-3.3-70b-instruct:free",
            "messages": [{"role": "user", "content": prompt}]
        }

        response = requests.post(ai_url, headers=ai_headers, json=ai_data, timeout=30)

        if response.status_code == 200:
            return response.json()['choices'][0]['message']['content']
        else:
            logger.error(f"LLM Error: {response.text}")
            return "Нейросеть временно недоступна, но я знаю, что топ по лайкам: " + top_like_title

    except Exception as e:
        logger.error(f"Smart Answer Error: {e}")
        return f"Ошибка при анализе: {e}"


# --- SCRAPERS ---
def get_real_channel_id(youtube, input_str):
    if input_str.startswith("UC"): return input_str
    handle = input_str if input_str.startswith("@") else f"@{input_str}"
    try:
        resp = youtube.channels().list(part="id", forHandle=handle).execute()
        if resp["items"]: return resp["items"][0]["id"]
    except Exception:
        pass
    return None


async def fetch_telegram(existing_links, targets):
    """targets: список каналов ['durov', 'mts_news']"""
    if not TG_API_ID or not targets: return []

    logger.info(f"📡 TG: Парсим каналы: {targets}")
    client = TelegramClient('anon_session', int(TG_API_ID), TG_API_HASH)
    await client.start()

    new_posts = []

    for channel in targets:
        try:
            async for message in client.iter_messages(channel, limit=5):
                if message.text:
                    link = f"https://t.me/{channel}/{message.id}"
                    if link in existing_links: continue

                    # (Опционально) Игнорируем посты без текста
                    if len(message.text) < 5: continue

                    sentiment, summary = analyze_text_with_llm(message.text)
                    likes = sum(r.count for r in
                                message.reactions.results) if message.reactions and message.reactions.results else 0

                    new_posts.append({
                        "Название": message.text[:50].replace('\n', ' ') + "...",
                        "Текст поста": message.text, "Дата": message.date.strftime('%Y-%m-%d'),
                        "Просмотры": message.views or 0, "Источник": "Telegram", "Ссылка": link,
                        "Лайки": likes, "Репосты": getattr(message, 'forwards', 0) or 0,
                        "Тональность": sentiment, "AI Саммари": summary
                    })
        except Exception as e:
            logger.error(f"Ошибка TG канала {channel}: {e}")

    await client.disconnect()
    return new_posts


def fetch_vk(existing_links, targets):
    """targets: список доменов ['mts', 'durov']"""
    if not VK_ACCESS_TOKEN or not targets: return []
    logger.info(f"🔵 VK: Парсим группы: {targets}")

    new_posts = []
    try:
        vk_session = vk_api.VkApi(token=VK_ACCESS_TOKEN)
        vk = vk_session.get_api()

        for domain in targets:
            try:
                response = vk.wall.get(domain=domain, count=5)
                for post in response['items']:
                    link = f"https://vk.com/wall{post['owner_id']}_{post['id']}"
                    if link in existing_links: continue

                    text = post.get('text', '')
                    if not text: continue

                    sentiment, summary = analyze_text_with_llm(text)
                    date_str = datetime.fromtimestamp(post['date']).strftime('%Y-%m-%d')

                    new_posts.append({
                        "Название": text[:50].replace('\n', ' ') + "...",
                        "Текст поста": text, "Дата": date_str,
                        "Просмотры": post.get('views', {}).get('count', 0), "Источник": "VK", "Ссылка": link,
                        "Лайки": post.get('likes', {}).get('count', 0),
                        "Репосты": post.get('reposts', {}).get('count', 0),
                        "Тональность": sentiment, "AI Саммари": summary
                    })
            except Exception as e:
                logger.error(f"Ошибка VK домена {domain}: {e}")

    except Exception as e:
        logger.error(f"Ошибка авторизации VK: {e}")

    return new_posts


def fetch_youtube(existing_links, targets):
    """targets: список handle ['@mts', '@google']"""
    if not YOUTUBE_API_KEY or not targets: return []
    logger.info(f"📺 YT: Парсим каналы: {targets}")

    youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
    new_vids = []

    for handle in targets:
        cid = get_real_channel_id(youtube, handle)
        if not cid: continue

        try:
            req = youtube.search().list(part="snippet", channelId=cid, maxResults=5, order="date", type="video")
            res = req.execute()

            for item in res.get('items', []):
                vid = item['id']['videoId']
                link = f"https://www.youtube.com/watch?v={vid}"
                if link in existing_links: continue

                snippet = item['snippet']
                stats = youtube.videos().list(part="statistics", id=vid).execute()['items'][0]['statistics']
                sentiment, summary = analyze_text_with_llm(snippet['title'])

                new_vids.append({
                    "Название": snippet['title'], "Текст поста": snippet['description'],
                    "Дата": snippet['publishedAt'][:10], "Просмотры": int(stats.get('viewCount', 0)),
                    "Источник": "YouTube", "Ссылка": link, "Лайки": int(stats.get('likeCount', 0)),
                    "Репосты": int(stats.get('commentCount', 0)), "Тональность": sentiment, "AI Саммари": summary
                })
        except Exception as e:
            logger.error(f"Ошибка YT канала {handle}: {e}")

    return new_vids


def fetch_rutube_data(existing_links, targets):
    """targets: список ID каналов"""
    if not targets: return []
    logger.info(f"🔴 Rutube: Парсим каналы: {targets}")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    rutube_posts = []
    for identifier in targets:
        try:
            # 1. Очистка ID
            if "rutube.ru" in identifier:
                if "/channel/" in identifier:
                    identifier = identifier.split("/channel/")[1].split("/")[0]
                elif "/u/" in identifier:
                    identifier = identifier.split("/u/")[1].split("/")[0]
            identifier = identifier.strip()

            if not identifier: continue

            # 2. Используем правильный эндпоинт для видео канала

            videos_url = f"https://rutube.ru/api/video/person/{identifier}/"

            response = requests.get(videos_url, headers=headers, timeout=10)

            if response.status_code == 404:
                logger.warning(f"⚠️ Rutube: Канал {identifier} не найден (404). Проверь ID.")
                continue
            if response.status_code != 200:
                logger.error(f"⚠️ Rutube API Error: {response.status_code}")
                continue

            data = response.json()
            results = data.get('results', [])

            for video in results[:5]:

                video_uuid = video.get('id')
                link = f"https://rutube.ru/video/{video_uuid}/"

                if link in existing_links: continue

                desc = video.get('description', '') or video.get('title', '')
                sentiment, summary = analyze_text_with_llm(desc)

                rutube_posts.append({
                    "Название": video.get('title', 'Без названия')[:50] + "...",
                    "Текст поста": desc,
                    "Дата": video.get('created_ts', '').split('T')[0],
                    "Просмотры": video.get('hits', 0),  # hits = просмотры
                    "Лайки": 0,  # В общей ленте лайки не отдаются
                    "Репосты": 0,
                    "Источник": "Rutube",
                    "Ссылка": link,
                    "Тональность": sentiment,
                    "AI Саммари": summary
                })
        except Exception as e:
            logger.error(f"Ошибка Rutube {identifier}: {e}")

    return rutube_posts


def parse_habr_metric(value_str):
    """Преобразует строки типа '1.5k', '+10', '120' в числа"""
    if not value_str:
        return 0
    try:
        value_str = value_str.strip().replace('+', '').replace(',', '.')
        if 'k' in value_str.lower():
            return int(float(value_str.lower().replace('k', '')) * 1000)
        if 'm' in value_str.lower():  # Миллионы (редко, но бывает)
            return int(float(value_str.lower().replace('m', '')) * 1000000)
        return int(float(value_str))
    except ValueError:
        return 0


def parse_habr_post(post_url):
    """Парсинг конкретного поста на Habr"""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7"
        }

        response = requests.get(post_url, headers=headers, timeout=10)
        if response.status_code != 200:
            logger.warning(f"Habr post error {response.status_code}: {post_url}")
            return None

        soup = BeautifulSoup(response.content, 'html.parser')

        # 1. Заголовок
        title_elem = soup.find('h1', class_='tm-title')
        title = title_elem.get_text(strip=True) if title_elem else "Без названия"

        # 2. Содержимое поста (Хабр использует id="post-content-body")
        content_elem = soup.find(id='post-content-body')
        if not content_elem:
            # Запасной вариант по классу
            content_elem = soup.find('div', class_='tm-article-body')

        # Берем текст, разделяя абзацы пробелами
        content = content_elem.get_text(separator=' ', strip=True) if content_elem else ""

        # 3. Дата (ISO формат внутри тега time)
        date_elem = soup.find('time')
        date = date_elem.get('datetime', '')[:10] if date_elem else datetime.now().strftime('%Y-%m-%d')

        # 4. Статистика
        views = 0
        likes = 0
        comments = 0

        # Рейтинг (Лайки) - ищем счетчик рейтинга
        # Он может быть в .tm-votes-meter__value
        likes_elem = soup.find('span', class_='tm-votes-meter__value')
        if likes_elem:
            likes = parse_habr_metric(likes_elem.get_text())

        # Просмотры - ищем иконку глаза и соседний текст
        # Обычно это класс tm-icon-counter__value
        # Но на странице статьи блок статистики может быть другим
        stats_blocks = soup.find_all('span', class_='tm-icon-counter__value')
        if stats_blocks:
            # Обычно просмотры - это первый или второй счетчик с большим числом
            # Попробуем найти тот, который похож на просмотры (обычно нет явного класса 'views')
            # Часто просмотры идут после рейтинга.
            for stat in stats_blocks:
                val = parse_habr_metric(stat.get_text())
                if val > views:  # Берем самое большое число, обычно это просмотры
                    views = val

        # Комментарии
        comments_elem = soup.find('span', class_='tm-article-comments-counter-link__value')
        if comments_elem:
            comments = parse_habr_metric(comments_elem.get_text())

        return {
            'title': title,
            'content': content,
            'date': date,
            'views': views,
            'likes': likes,
            'comments': comments,
            'shares': 0
        }

    except Exception as e:
        logger.error(f"Ошибка парсинга поста Habr {post_url}: {e}")
        return None


def fetch_habr_data(existing_links, targets):
    """Парсинг постов с Хабра по списку компаний"""
    if not targets:
        return []

    logger.info(f"📝 Habr: Парсим компании: {targets}")
    habr_posts = []

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    for company in targets:
        try:
            # Очистка имени компании от URL если случайно попал
            company = company.strip()
            if "habr.com" in company:
                company = company.split('/companies/')[-1].replace('/articles/', '').replace('/', '')

            search_url = f"https://habr.com/ru/companies/{company}/articles/"

            response = requests.get(search_url, headers=headers, timeout=10)
            if response.status_code != 200:
                logger.warning(f"Habr: Ошибка доступа для {company} (Code: {response.status_code})")
                continue

            soup = BeautifulSoup(response.content, 'html.parser')

            # Ищем статьи в списке
            articles = soup.find_all('article', class_='tm-articles-list__item')

            # Берем первые 5
            for post in articles[:5]:
                try:
                    title_elem = post.find('h2', class_='tm-title')
                    if not title_elem: continue

                    link_elem = title_elem.find('a')
                    if not link_elem: continue

                    relative_link = link_elem.get('href', '')
                    full_link = f"https://habr.com{relative_link}"

                    if full_link in existing_links: continue

                    logger.info(f"Habr: Обработка статьи {full_link}")

                    # Проваливаемся внутрь статьи за полными данными
                    post_data = parse_habr_post(full_link)

                    if not post_data:
                        logger.warning(f"Не удалось получить детали поста {full_link}")
                        continue

                    # Анализ AI (берем первые 1500 символов, чтобы не перегружать контекст)
                    sentiment, summary = analyze_text_with_llm(post_data['content'][:1500])

                    habr_posts.append({
                        "Название": post_data['title'][:100],  # MWS может иметь лимит на длину заголовка
                        "Текст поста": post_data['content'][:2000] + "...",  # Обрезаем слишком длинные статьи
                        "Дата": post_data['date'],
                        "Просмотры": post_data['views'],
                        "Лайки": post_data['likes'],
                        "Репосты": post_data['shares'],  # Хабр не отдает шеры в паблик
                        "Комментарии": post_data['comments'],
                        "Источник": "Habr",
                        "Ссылка": full_link,
                        "Тональность": sentiment,
                        "AI Саммари": summary
                    })

                except Exception as e:
                    logger.error(f"Ошибка обработки элемента списка Habr: {e}")
                    continue

        except Exception as e:
            logger.error(f"Ошибка Habr компании {company}: {e}")
            continue

    return habr_posts


async def update_data_logic():
    mws = MWSTablesAPI(MWS_TOKEN, MWS_TABLE_ID, MWS_VIEW_ID)

    # 1. Получаем список старых постов (чтобы не дублировать)
    existing = mws.get_existing_links()

    # 2. Получаем список каналов ИЗ ТАБЛИЦЫ MWS
    channels = get_monitored_channels()

    if not channels:
        logger.warning("⚠️ Список каналов пуст или не удалось загрузить.")
        return

    logger.info(f"📋 Найдены каналы для мониторинга: {channels}")

    # 3. Запускаем парсеры с динамическими списками
    data_tg = await fetch_telegram(existing, channels.get('Telegram', []))
    data_yt = fetch_youtube(existing, channels.get('YouTube', []))
    data_ru = fetch_rutube_data(existing, channels.get('Rutube', []))
    data_vk = fetch_vk(existing, channels.get('VK', []))
    data_habr = fetch_habr_data(existing, channels.get('Habr', []))

    logger.info(
        f"📊 Найдено постов: TG={len(data_tg)}, YT={len(data_yt)}, RU={len(data_ru)}, VK={len(data_vk)}, Habr={len(data_habr)}")

    all_data = data_tg + data_yt + data_ru + data_vk + data_habr

    if all_data:
        mws.add_records(all_data)
        logger.info(f"🎉 Успех! Загружено {len(all_data)} новых постов.")
    else:
        logger.info("😴 Свежих постов не найдено.")


def get_monitored_channels():
    """
    Получает список каналов из MWS и группирует их по источникам.
    Возвращает словарь: {'Telegram': ['durov', ...], 'VK': ['mts', ...], ...}
    """
    try:
        url = f"{MWS_API_URL}/{MWS_CHANNELS_TABLE_ID}/records"
        headers = {"Authorization": f"Bearer {MWS_TOKEN}", "Content-Type": "application/json"}
        params = {"viewId": MWS_CHANNELS_VIEW_ID, "fieldKey": "name", "pageSize": 1000}

        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            logger.error(f"Ошибка получения списка каналов: {response.status_code}")
            return {}

        records = response.json().get('data', {}).get('records', [])
        channels = {"Telegram": [], "VK": [], "YouTube": [], "Rutube": [], "Habr": []}

        for r in records:
            fields = r.get('fields', {})

            # 1. Проверяем, нужно ли смотреть этот канал
            if fields.get('Тип активности') != 'Смотреть':
                continue

            source = fields.get('Источник')
            raw_link = fields.get('Имя канала', '').strip()

            if not source or not raw_link:
                continue

            # 2. Очищаем ссылку до ID/Handle
            clean_id = raw_link

            if source == "Telegram":
                clean_id = raw_link.replace("https://t.me/", "").replace("@", "")
            elif source == "VK":
                clean_id = raw_link.replace("https://vk.com/", "").replace("https://m.vk.com/", "")
            elif source == "YouTube":
                clean_id = raw_link.replace("https://www.youtube.com/", "").replace("https://youtube.com/", "")
            elif source == "Rutube":
                clean_id = raw_link.replace("https://rutube.ru/channel/", "").replace("/", "")
            elif source == "Habr":
                # Для Habr берем название компании как есть (без URL)
                if "habr.com" in raw_link:
                    clean_id = raw_link.replace("https://habr.com/ru/company/", "").replace("/", "")
                else:
                    clean_id = raw_link  # Если это просто название компании

            # Добавляем в список, удаляя пустые значения
            if source in channels and clean_id:
                channels[source].append(clean_id)

        # Если в таблице нет каналов Habr, используем значения из .env
        if not channels["Habr"] and HABR_TARGET_COMPANIES:
            channels["Habr"] = [company.strip() for company in HABR_TARGET_COMPANIES if company.strip()]

        logger.info(f"📋 Загружены каналы для мониторинга: {channels}")
        return channels

    except Exception as e:
        logger.error(f"Critical error getting channels: {e}")
        # Возвращаем каналы из .env если произошла ошибка
        return {"Telegram": [], "VK": [], "YouTube": [], "Rutube": [], "Habr": HABR_TARGET_COMPANIES}


# === FRONTEND ANALYTICS ENDPOINTS ===

@app.get("/api/info", summary="Информация о системе")
async def get_system_info():
    """
    Получить информацию о системе сбора данных и доступных метриках
    """
    return {
        "project": "MTS Content Analyzer",
        "description": "Умная система анализа контента из социальных сетей с AI-аналитикой",
        "version": "1.0",
        "features": [
            "Автоматический сбор данных из соцсетей",
            "AI-анализ тональности контента",
            "Генерация саммари через LLM",
            "Визуализация эффективности контента"
        ],
        "data_sources": [
            {
                "name": "Telegram",
                "status": "active",
                "collected_data": [
                    "Текст постов", "Просмотры", "Лайки", "Репосты",
                    "Дата публикации", "Тональность", "AI-саммари"
                ]
            },
            {
                "name": "VK",
                "status": "planned",
                "collected_data": [
                    "Посты", "Просмотры", "Лайки", "Комментарии",
                    "Репосты", "Тональность", "AI-анализ"
                ]
            },
            {
                "name": "Rutube",
                "status": "active",
                "collected_data": [
                    "Название видео", "Описание", "Просмотры", "Лайки",
                    "Комментарии", "Тональность", "AI-саммари"
                ]
            },
            {
                "name": "Habr",
                "status": "active",
                "collected_data": [
                    "Технические статьи", "Просмотры", "Лайки", "Комментарии",
                    "Дата публикации", "Тональность", "AI-анализ"
                ]
            }
        ],
        "ai_capabilities": [
            "Анализ тональности (Positive/Negative/Neutral)",
            "Автоматическое саммари контента",
            "Ответы на вопросы о контенте",
            "Анализ эффективности публикаций"
        ],
        "total_records": len(get_mws_data())
    }


@app.get("/api/data", summary="Получить все данные")
async def get_all_data(
        limit: int = Query(100, description="Количество записей"),
        offset: int = Query(0, description="Смещение"),
        source: Optional[str] = Query(None, description="Фильтр по источнику"),
        sentiment: Optional[str] = Query(None, description="Фильтр по тональности")
):
    """
    Получить данные из таблицы с фильтрацией и пагинацией
    """
    try:
        all_data = get_mws_data()

        # Применяем фильтры
        filtered_data = all_data
        if source:
            filtered_data = [r for r in filtered_data if r.get('fields', {}).get('Источник') == source]
        if sentiment:
            filtered_data = [r for r in filtered_data if r.get('fields', {}).get('Тональность') == sentiment]

        # Пагинация
        total = len(filtered_data)
        paginated_data = filtered_data[offset:offset + limit]

        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "filters": {
                "source": source,
                "sentiment": sentiment
            },
            "data": [
                {
                    "id": f"{record.get('fields', {}).get('Источник', 'unknown')}_{idx + offset}",
                    "fields": record.get('fields', {}),
                    "metadata": {
                        "text_length": len(record.get('fields', {}).get('Текст поста', '')),
                        "has_ai_summary": bool(record.get('fields', {}).get('AI Саммари'))
                    }
                }
                for idx, record in enumerate(paginated_data)
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка получения данных: {str(e)}")


@app.get("/api/stats/overview", summary="Общая статистика")
async def get_overview_stats():
    """
    Получить общую статистику по всем данным
    """
    data = get_mws_data()

    if not data:
        return {"message": "Нет данных для анализа"}

    # Базовая статистика
    total_posts = len(data)
    sources = {}
    sentiments = {"Positive": 0, "Negative": 0, "Neutral": 0}

    total_views = 0
    total_likes = 0
    total_comments = 0

    for record in data:
        fields = record.get('fields', {})
        source = fields.get('Источник', 'Unknown')
        sentiment = fields.get('Тональность', 'Neutral')

        # Статистика по источникам
        if source not in sources:
            sources[source] = {"count": 0, "views": 0, "likes": 0}
        sources[source]["count"] += 1
        sources[source]["views"] += fields.get('Просмотры', 0)
        sources[source]["likes"] += fields.get('Лайки', 0)

        # Статистика по тональности
        if sentiment in sentiments:
            sentiments[sentiment] += 1

        # Общие метрики
        total_views += fields.get('Просмотры', 0)
        total_likes += fields.get('Лайки', 0)
        total_comments += fields.get('Комментарии', 0)

    # Расчет средних значений
    avg_views = total_views / total_posts if total_posts > 0 else 0
    avg_likes = total_likes / total_posts if total_posts > 0 else 0
    engagement_rate = (total_likes / total_views * 100) if total_views > 0 else 0

    return {
        "summary": {
            "total_posts": total_posts,
            "total_views": total_views,
            "total_likes": total_likes,
            "total_comments": total_comments,
            "average_views": round(avg_views, 2),
            "average_likes": round(avg_likes, 2),
            "engagement_rate": round(engagement_rate, 2)
        },
        "sources": sources,
        "sentiments": sentiments,
        "content_effectiveness": {
            "most_engaging_source": max(sources.items(), key=lambda x: x[1]["likes"])[0] if sources else "N/A",
            "positive_content_ratio": round(sentiments["Positive"] / total_posts * 100, 2) if total_posts > 0 else 0,
            "top_performing_metric": "Просмотры" if total_views > total_likes else "Лайки"
        }
    }


@app.get("/api/analytics/sentiment", summary="Анализ тональности")
async def get_sentiment_analytics():
    """
    Детальный анализ тональности контента
    """
    data = get_mws_data()

    sentiment_stats = {"Positive": 0, "Negative": 0, "Neutral": 0}
    source_sentiment = {}
    sentiment_engagement = {"Positive": 0, "Negative": 0, "Neutral": 0}

    for record in data:
        fields = record.get('fields', {})
        sentiment = fields.get('Тональность', 'Neutral')
        source = fields.get('Источник', 'Unknown')
        views = fields.get('Просмотры', 0)
        likes = fields.get('Лайки', 0)

        # Общая статистика тональности
        if sentiment in sentiment_stats:
            sentiment_stats[sentiment] += 1

        # Тональность по источникам
        if source not in source_sentiment:
            source_sentiment[source] = {"Positive": 0, "Negative": 0, "Neutral": 0, "total": 0}
        if sentiment in source_sentiment[source]:
            source_sentiment[source][sentiment] += 1
            source_sentiment[source]["total"] += 1

        # Вовлеченность по тональности
        if sentiment in sentiment_engagement:
            sentiment_engagement[sentiment] += views + likes

    # Расчет процентов
    total_posts = len(data)
    sentiment_percentages = {
        sentiment: round((count / total_posts) * 100, 2)
        for sentiment, count in sentiment_stats.items()
    } if total_posts > 0 else {}

    return {
        "overall": {
            "counts": sentiment_stats,
            "percentages": sentiment_percentages,
            "dominant_sentiment": max(sentiment_stats.items(), key=lambda x: x[1])[0] if sentiment_stats else "Neutral"
        },
        "by_source": source_sentiment,
        "engagement_by_sentiment": sentiment_engagement,
        "insights": {
            "total_analyzed": total_posts,
            "most_positive_source": max(source_sentiment.items(), key=lambda x: x[1]["Positive"])[
                0] if source_sentiment else "N/A",
            "engagement_trend": "Positive" if sentiment_engagement["Positive"] > sentiment_engagement[
                "Negative"] else "Neutral"
        }
    }


@app.get("/api/top/content", summary="Топ контента")
async def get_top_content(
        metric: str = Query("Просмотры", description="Метрика для сортировки"),
        limit: int = Query(10, description="Количество записей"),
        source: Optional[str] = Query(None, description="Фильтр по источнику")
):
    """
    Получить самый популярный контент по выбранной метрике
    """
    valid_metrics = ["Просмотры", "Лайки", "Репосты", "Комментарии"]
    if metric not in valid_metrics:
        raise HTTPException(
            status_code=400,
            detail=f"Недопустимая метрика. Допустимые значения: {', '.join(valid_metrics)}"
        )

    data = get_mws_data()

    # Фильтрация по источнику
    if source:
        data = [r for r in data if r.get('fields', {}).get('Источник') == source]

    # Сортировка по выбранной метрике
    sorted_data = sorted(
        data,
        key=lambda x: x.get('fields', {}).get(metric, 0),
        reverse=True
    )[:limit]

    return {
        "metric": metric,
        "source_filter": source,
        "top_content": [
            {
                "rank": idx + 1,
                "title": record.get('fields', {}).get('Название', 'Без названия'),
                "source": record.get('fields', {}).get('Источник', 'Unknown'),
                "metric_value": record.get('fields', {}).get(metric, 0),
                "sentiment": record.get('fields', {}).get('Тональность', 'Neutral'),
                "date": record.get('fields', {}).get('Дата', ''),
                "link": record.get('fields', {}).get('Ссылка', ''),
                "ai_summary": record.get('fields', {}).get('AI Саммари', '')
            }
            for idx, record in enumerate(sorted_data)
        ]
    }


@app.get("/api/sources/performance", summary="Эффективность источников")
async def get_sources_performance():
    """
    Сравнение эффективности разных источников контента
    """
    data = get_mws_data()

    sources = {}

    for record in data:
        fields = record.get('fields', {})
        source = fields.get('Источник', 'Unknown')

        if source not in sources:
            sources[source] = {
                "posts_count": 0,
                "total_views": 0,
                "total_likes": 0,
                "total_comments": 0,
                "sentiments": {"Positive": 0, "Negative": 0, "Neutral": 0},
                "posts": []
            }

        # Основные метрики
        sources[source]["posts_count"] += 1
        sources[source]["total_views"] += fields.get('Просмотры', 0)
        sources[source]["total_likes"] += fields.get('Лайки', 0)
        sources[source]["total_comments"] += fields.get('Комментарии', 0)

        # Тональность
        sentiment = fields.get('Тональность', 'Neutral')
        if sentiment in sources[source]["sentiments"]:
            sources[source]["sentiments"][sentiment] += 1

        # Сохраняем пост для деталей
        sources[source]["posts"].append({
            "title": fields.get('Название', ''),
            "views": fields.get('Просмотры', 0),
            "likes": fields.get('Лайки', 0),
            "sentiment": sentiment,
            "date": fields.get('Дата', '')
        })

    # Расчет производных метрик
    for source, stats in sources.items():
        stats["average_views"] = round(stats["total_views"] / stats["posts_count"], 2) if stats[
                                                                                              "posts_count"] > 0 else 0
        stats["average_likes"] = round(stats["total_likes"] / stats["posts_count"], 2) if stats[
                                                                                              "posts_count"] > 0 else 0
        stats["engagement_rate"] = round((stats["total_likes"] / stats["total_views"] * 100), 2) if stats[
                                                                                                        "total_views"] > 0 else 0
        stats["positive_ratio"] = round((stats["sentiments"]["Positive"] / stats["posts_count"] * 100), 2) if stats[
                                                                                                                  "posts_count"] > 0 else 0

    return {
        "sources": sources,
        "comparison": {
            "best_engagement": max(sources.items(), key=lambda x: x[1]["engagement_rate"])[0] if sources else "N/A",
            "most_active": max(sources.items(), key=lambda x: x[1]["posts_count"])[0] if sources else "N/A",
            "most_positive": max(sources.items(), key=lambda x: x[1]["positive_ratio"])[0] if sources else "N/A"
        }
    }


@app.get("/api/health", summary="Проверка здоровья системы")
async def health_check():
    """
    Проверка доступности системы и данных
    """
    try:
        data = get_mws_data()
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "data_available": len(data) > 0,
            "total_records": len(data),
            "sources_available": list(set(
                record.get('fields', {}).get('Источник', 'Unknown')
                for record in data
            ))
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "timestamp": datetime.now().isoformat(),
            "error": str(e)
        }


# --- ЭКСПОРТ CSV
# --- ЭКСПОРТ CSV
@app.get("/api/export/csv")
async def export_csv(
        source: Optional[str] = Query(None, description="Фильтр по источнику"),
        sentiment: Optional[str] = Query(None, description="Фильтр по тональности")
):
    try:
        data = get_mws_data()
        if source:
            data = [r for r in data if r.get('fields', {}).get('Источник') == source]
        if sentiment:
            data = [r for r in data if r.get('fields', {}).get('Тональность') == sentiment]
        if not data:
            raise HTTPException(status_code=404, detail='Нет данных для экспорта')

        output = io.StringIO()
        writer = csv.writer(output)
        headers = [
            "Название", "Текст поста", "Дата", "Просмотры", "Лайки",
            "Репосты", "Комментарии", "Источник", "Ссылка", "Тональность", "AI Саммари"
        ]
        writer.writerow(headers)

        for record in data:
            fields = record.get('fields', {})
            writer.writerow([
                fields.get('Название', ''),
                fields.get('Текст поста', ''),
                fields.get('Дата', ''),
                fields.get('Просмотры', 0),
                fields.get('Лайки', 0),
                fields.get('Репосты', 0),
                fields.get('Комментарии', 0),
                fields.get('Источник', ''),
                fields.get('Ссылка', ''),
                fields.get('Тональность', ''),
                fields.get('AI Саммари', '')
            ])

        # Создаем StreamingResponse после завершения цикла
        response = StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename=content_analysis_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
            }
        )
        return response

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Ошибка экспорта: {str(e)}')


@dp.message(F.text == "/start")
async def start_menu(message: types.Message):
    buttons = [
        [KeyboardButton(text="📊 Топ постов"), KeyboardButton(text="🔮 Прогноз")],
        [KeyboardButton(text="📥 Экспорт данных в CSV")]
    ]

    keyboard = ReplyKeyboardMarkup(
        keyboard=buttons,
        resize_keyboard=True,
        input_field_placeholder="🎯 Выберите действие или задайте вопрос..."
    )

    welcome_text = """
✨ <b>Добро пожаловать в MWS Content Analyzer!</b> ✨

🤖 <i>Ваш умный помощник для анализа контента</i>

🎯 <b>Доступные функции:</b>
• 📊 <b>Топ постов</b> - самые популярные публикации
• 🔮 <b>Прогноз</b> - AI-анализ эффективности  
• 📥 <b>Экспорт данных</b> - выгрузка в CSV формате

💬 <b>Просто напишите вопрос в чат:</b>
<i>"Какие посты получили больше лайков?"
"Какой контент самый популярный?"
"Покажи статистику за неделю"</i>

👇 <b>Выберите действие ниже</b>
    """

    await message.answer(
        welcome_text,
        reply_markup=keyboard,
        parse_mode="HTML"
    )


@dp.message(F.text == "📥 Экспорт данных в CSV")
async def export(message: types.Message):
    reply_markup = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text='🔙 Вернуться назад')]
        ],
        resize_keyboard=True
    )
    await message.answer("🔄 Готовлю файл... ", reply_markup=reply_markup)
    try:
        data = get_mws_data()
        if not data:
            await message.answer("❌ Нет данных для экспорта ")
            return

        output = io.StringIO()
        writer = csv.writer(output)
        headers = ["Название", "Дата", "Просмотры", "Лайки", "Источник", "Тональность"]
        writer.writerow(headers)
        for record in data:
            fields = record.get('fields', {})
            writer.writerow([
                fields.get('Название', '')[:100],  # Обрезаем длинные названия
                fields.get('Дата', ''),
                fields.get('Просмотры', 0),
                fields.get('Лайки', 0),
                fields.get('Источник', ''),
                fields.get('Тональность', '')
            ])
        csv_data = output.getvalue().encode('utf-8')
        await message.answer_document(
            types.BufferedInputFile(csv_data, filename=f"content_export_{datetime.now().strftime('%Y%m%d')}.csv"),
            caption="✅ Ваш CSV файл готов!"
        )
    except Exception as e:
        await message.answer(f"❌ Ошибка при экспорте: {str(e)}")


@dp.message(F.text == "🔙 Вернуться назад")
async def back_to_main(message: types.Message):
    # Вернуться в главное меню
    buttons = [
        [KeyboardButton(text="📊 Топ постов"), KeyboardButton(text="🔮 Прогноз")],
        [KeyboardButton(text="📥 Экспорт данных в CSV")]
    ]
    keyboard = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)
    await message.answer("Главное меню:", reply_markup=keyboard)


@dp.message(F.text)
async def handle_bot_question(message: types.Message):
    await message.bot.send_chat_action(chat_id=message.chat.id, action="typing")
    answer = get_smart_answer(message.text)
    await message.answer(answer)


# --- STARTUP ---
@app.post("/chat", response_model=ChatResponse)
async def chat_api(request: ChatRequest):
    return ChatResponse(answer=get_smart_answer(request.question))


@app.on_event("startup")
async def on_startup():
    asyncio.create_task(update_data_logic())
    asyncio.create_task(dp.start_polling(bot))
    logger.info("🚀 SYSTEM ONLINE: API + BOT + SCRAPERS")


if __name__ == '__main__':
    uvicorn.run(app, host="0.0.0.0", port=8000)