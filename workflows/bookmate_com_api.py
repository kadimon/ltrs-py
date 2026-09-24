import asyncio
import re
from datetime import datetime
from typing import Literal
from urllib.parse import urlparse

import dateparser
import httpx
from furl import furl
from hatchet_sdk import Context
from usp.fetch_parse import SitemapFetcher

import settings
from db import DbSamizdatPrisma
from interfaces import InputLivelibBook, Output, WorkerLabels
from utils import save_cover_httpx
from workflow_base import ApiMixin, BaseLivelibWorkflow

BOOK_PATH_RE = re.compile(r'^/(books|audiobooks|comicbooks)/[^/]+/?$')

WEB_URL = 'https://books.yandex.ru'
API_URL = 'https://api.bookmate.yandex.net/api/v5'
GRAPHQL_URL = 'https://api-gateway.bookmate.yandex.net/graphql'

# Только то, что описывает клиент: без Auth-Token / Authorization / IMEI /
# Device-Idfa.
API_HEADERS = {
    'Allow-Ppd-Content': 'true',
    'App-Language': 'ru',
    'App-Locale': 'ru',
    'App-Name': 'YandexBooks',
    'App-Platform': 'android',
    'App-User-Agent': 'Xiaomi/Redmi_Note_8_Pro Android/16 Bookmate/6.83',
    'App-Version': '6.83',
    'Bookmate-Version': '20200305',
    'Device-Os': 'Android',
    'Device-Os-Version': '16',
    'Subscription-Country': 'ru',
    'User-Agent': 'okhttp/4.12.0',
}

# раздел сайта -> ключ объекта в ответе `/api/v5/<раздел>/<uuid>`
CONTENT_KEYS = {
    'books': 'book',
    'audiobooks': 'audiobook',
    'comicbooks': 'comicbook',
}

# связанные версии книги -> раздел сайта
LINKED_KEYS = {
    'linked_book_uuids': 'books',
    'linked_audiobook_uuids': 'audiobooks',
    'linked_comicbook_uuids': 'comicbooks',
}

# Цена отдельной покупки живёт только в GraphQL: раздел -> (операция, корень)
PURCHASE_OFFER_OPERATIONS = {
    'books': ('GetTextBookPurchaseOffer', 'textBook'),
    'audiobooks': ('GetAudioBookPurchaseOffer', 'audioBook'),
    'comicbooks': ('GetComicBookPurchaseOffer', 'comicBook'),
}
PURCHASE_OFFER_QUERY = (
    'query {operation}($uuid: ID!) {{ {root}(uuid: $uuid) '
    '{{ book {{ __typename ...purchaseOfferFragment }} }} }}  '
    'fragment purchaseOfferFragment on Book {{ purchaseOffer {{ currency price }} }}'
)


def clean_url(url: str) -> str:
    """URL без query и якоря — дедупликация в `crawl` считает хэш от строки."""
    return furl(url).remove(args=True, fragment=True).url


def format_price(value: float | int | str) -> str:
    """`449.0` -> `449`, `449.5` -> `449.5` — без экспоненты и хвостовых нулей."""
    value = float(value)
    if value.is_integer():
        return str(int(value))
    return f'{value:.2f}'.rstrip('0').rstrip('.')


def parse_book_url(url: str) -> tuple[str, str] | None:
    """`https://books.yandex.ru/audiobooks/VIitWf9R` -> `('audiobooks', 'VIitWf9R')`."""
    path = urlparse(url).path
    if not BOOK_PATH_RE.match(path):
        return None
    kind, uuid = path.strip('/').split('/')
    return kind, uuid


def web_url(section: str, uuid: str) -> str:
    return f'{WEB_URL}/{section}/{uuid}'


def topic_url(topic: dict) -> str:
    """`{'slug': 'proza-ru', 'uuid': 'PNZcAxtW'}` -> `.../topic/all/proza-ru-PNZcAxtW`."""
    return f'{WEB_URL}/topic/all/{topic["slug"]}-{topic["uuid"]}'


def authors_of(data: dict) -> list[dict]:
    """У текстовой книги `authors` — строка, а список лежит в `authors_objects`;
    у аудио и комиксов `authors` — сразу список объектов."""
    if isinstance(data.get('authors_objects'), list):
        return data['authors_objects']
    if isinstance(data.get('authors'), list):
        return data['authors']
    return []


def people_names(items: list[dict] | None) -> str | None:
    """Список персон API -> строка через запятую."""
    names = [x['name'].strip() for x in items or [] if x.get('name')]
    return ', '.join(names) or None


def people_links(items: list[dict] | None, section: str = 'authors') -> list[dict]:
    """Персоны API -> `[{'name': ..., 'url': ...}]` — формат authors_data."""
    links = {}
    for item in items or []:
        if item.get('name') and item.get('uuid'):
            links[web_url(section, item['uuid'])] = item['name'].strip()
    return [{'name': name, 'url': url} for url, name in links.items()]


def release_year(value) -> str | None:
    """Год из `publication_date` (строка или unix-время) / `original_year`."""
    if value in (None, ''):
        return None
    if isinstance(value, (int, float)) and value > 9999:
        return str(datetime.fromtimestamp(value).year)
    if match := re.search(r'\d{4}', str(value)):
        return match.group(0)
    return None


class BookmateApiItem(ApiMixin, BaseLivelibWorkflow):
    name = 'livelib-bookmate-api-item'
    event = 'livelib:bookmate-api-item'
    site = 'bookmate.com'

    labels = WorkerLabels(ip='ru')

    input = InputLivelibBook
    output = Output

    concurrency = 1
    # карточка + эмоции/цена параллельно + обложка — с запасом над http_timeout
    execution_timeout_sec = 60

    # Заголовки httpx-клиента, который создаёт ApiMixin.session()
    headers = API_HEADERS

    @staticmethod
    async def fetch_emotions(client: httpx.AsyncClient, kind: str, uuid: str) -> list[dict]:
        """Эмоции-«награды». Нет ответа — не повод ронять всю карточку."""
        try:
            resp = await client.get(f'{API_URL}/{kind}/{uuid}/emotion_ratings')
            if resp.status_code != 200:
                return []
            return resp.json().get('emotion_ratings') or []
        except (httpx.HTTPError, ValueError):
            return []

    @staticmethod
    async def fetch_price(client: httpx.AsyncClient, kind: str, uuid: str) -> str | None:
        """Цена отдельной покупки из GraphQL. `purchaseOffer: null` — купить нельзя."""
        if kind not in PURCHASE_OFFER_OPERATIONS:
            return None

        operation, root = PURCHASE_OFFER_OPERATIONS[kind]
        try:
            resp = await client.post(
                GRAPHQL_URL,
                # склеиваются с заголовками клиента
                headers={
                    'Accept': 'application/json',
                    'X-APOLLO-OPERATION-NAME': operation,
                },
                json={
                    'operationName': operation,
                    'variables': {'uuid': uuid},
                    'query': PURCHASE_OFFER_QUERY.format(operation=operation, root=root),
                },
            )
            if resp.status_code != 200:
                return None
            payload = resp.json()
        except (httpx.HTTPError, ValueError):
            return None

        book = (((payload.get('data') or {}).get(root) or {}).get('book') or {})
        offer = book.get('purchaseOffer')
        if not offer or offer.get('price') is None:
            return None

        return format_price(offer['price'])

    @classmethod
    async def task(
        cls,
        input: InputLivelibBook,
        client: httpx.AsyncClient,
        ctx: Context | None = None,
    ) -> Output:
        url = clean_url(input.url)

        if not (parsed := parse_book_url(url)):
            return Output(result='error', data={'status': None, 'error': 'invalid_url_or_404'})
        kind, uuid = parsed

        resp = await client.get(f'{API_URL}/{kind}/{uuid}')

        # Проверка статуса
        if resp.status_code == 404:
            async with DbSamizdatPrisma() as db:
                await db.mark_book_deleted(url, cls.site)
            return Output(result='error', data={'status': resp.status_code, 'error': 'invalid_url_or_404'})
        resp.raise_for_status()

        data = resp.json().get(CONTENT_KEYS[kind]) or {}

        emotions, price = await asyncio.gather(
            cls.fetch_emotions(client, kind, uuid),
            cls.fetch_price(client, kind, uuid),
        )

        async with DbSamizdatPrisma() as db:
            book = {'url': url, 'source': cls.site}
            metrics = {'bookUrl': url}

            # Title
            if title := (data.get('title') or '').strip():
                book['title'] = title

            if not await db.check_book_exist(url):
                await db.create_book(book)

            # Authors
            authors = authors_of(data)
            if author := people_names(authors):
                book['author'] = author
                book['authors_data'] = people_links(authors, 'authors')

            # Annotation
            if annotation := (data.get('annotation') or '').strip():
                book['annotation'] = annotation

            # Cover
            if not await db.check_book_have_cover(url):
                img_src = (data.get('cover') or {}).get('large')
                if img_src and 'empty_cover' not in img_src:
                    if img_name := await save_cover_httpx(client, img_src):
                        book['coverImage'] = img_name

            # Category
            topics = [t for t in data.get('topics') or [] if t.get('title')]
            if topics:
                book['category'] = [t['title'].strip() for t in topics]
                book['categories_data'] = [
                    {'name': t['title'].strip(), 'url': topic_url(t)}
                    for t in topics if t.get('slug') and t.get('uuid')
                ]

            # Series
            series = [s for s in data.get('series_list') or [] if s.get('title')]
            if series:
                book['series'] = [s['title'].strip() for s in series]
                book['series_data'] = [
                    {'name': s['title'].strip(), 'url': web_url('series', s['uuid'])}
                    for s in series if s.get('uuid')
                ]

            # Release Date — только год
            year = release_year(data.get('publication_date')) or release_year(data.get('original_year'))
            if year:
                book['date_release'] = dateparser.parse(year, date_formats=['%Y'])

            # Owner
            if owner := (data.get('owner_catalog_title') or '').strip():
                book['owner'] = owner

            # Publisher
            if publisher := people_names(data.get('publishers')):
                book['publisher'] = publisher

            # Translator
            if translator := people_names(data.get('translators')):
                book['translate'] = translator

            # Artist
            if artist := people_names(data.get('illustrators')):
                book['artist'] = artist

            # Voice (у аудиокниг)
            if voice := people_names(data.get('narrators')):
                book['voice'] = voice

            # Age Rating: API отдаёт `16`, храним `16+`
            if (age := data.get('age_restriction')) not in (None, ''):
                book['age_rating'] = f'{age}+'

            # Audio
            if kind != 'audiobooks' and (audio_uuids := data.get('linked_audiobook_uuids')):
                book['url_audio'] = web_url('audiobooks', audio_uuids[0])

            # --- Metrics ---

            # Read Process («Читают» / «Слушают»)
            read_process = data.get('readers_count')
            if read_process is None:
                read_process = data.get('listeners_count')
            if read_process is not None:
                metrics['read_process'] = str(read_process)

            # Comments (Впечатления)
            if data.get('impressions_count') is not None:
                metrics['comments'] = str(data['impressions_count'])

            # Quotes (Цитаты)
            if data.get('quotes_count') is not None:
                metrics['quotes'] = str(data['quotes_count'])

            # Added to library (На полке)
            if data.get('bookshelves_count') is not None:
                metrics['added_to_lib'] = str(data['bookshelves_count'])

            # Price — цена отдельной покупки из purchaseOffer
            if price:
                metrics['price'] = price

            # Subscription.
            # TODO: в карточке /api/v5 нет признака подписки: `access_restrictions`
            # с `level: bookmate` бывает и у книг вне подписки (vNm1HD4t).
            # Пока считаем: есть цена отдельной покупки — вне подписки.
            metrics['in_subscribe'] = not metrics.get('price')

            # Pages count (`paper_pages` у книг, `pages_count` у комиксов)
            pages_count = data.get('paper_pages') or data.get('pages_count')
            if pages_count:
                metrics['pages_count'] = str(pages_count)

            # Duration (у аудиокниг, в секундах)
            if data.get('duration'):
                metrics['duration'] = int(data['duration'])

            # Awards
            awards = {}
            for rating in emotions:
                label = ((rating.get('emotion') or {}).get('label') or '').strip()
                if label and rating.get('count') is not None:
                    awards[label] = str(rating['count'])
            if awards:
                metrics['awards'] = awards

            await db.update_book(book)
            await db.create_metrics(metrics)

        # --- Crawl book formats ---
        for key, section in LINKED_KEYS.items():
            if section == kind:
                continue
            for linked_uuid in data.get(key) or []:
                await cls.crawl(web_url(section, linked_uuid), input.task_id)

        return Output(result='done', data={'book': book, 'metrics': metrics})


class BookmateApiListing(BookmateApiItem):
    name = 'livelib-bookmate-api-listing'
    event = 'livelib:bookmate-api-listing'

    item_wf = BookmateApiItem

    @classmethod
    async def run(cls, user_check: Literal['y', 'n'] | None = None) -> None:
        if settings.DEBUG:
            return

        for sitemap_url in [
            'https://books.yandex.ru/s3-assets/sitemap/ru/books/sitemap-books.xml',
            'https://books.yandex.ru/s3-assets/sitemap/ru/audiobooks/sitemap-audiobooks.xml',
            'https://books.yandex.ru/s3-assets/sitemap/ru/comics/sitemap-comics.xml',
            'https://books.yandex.ru/s3-assets/sitemap/ru/audio/sitemap-audio-1.xml',
        ]:
            sitemap = SitemapFetcher(url=sitemap_url, recursion_level=0).sitemap()
            cls.start_urls.extend([p.url for p in sitemap.all_pages()])

        print('books in sitemap:', len(cls.start_urls))

        await super().run()

if __name__ == '__main__':
    BookmateApiListing.run_sync()
    # Для отладки
    BookmateApiItem.debug_sync('https://books.yandex.ru/books/DNsh3Cxr')
    BookmateApiItem.debug_sync('https://books.yandex.ru/audiobooks/vNm1HD4t')
    BookmateApiItem.debug_sync('https://books.yandex.ru/audiobooks/BcSbSsAC')
