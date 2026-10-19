import asyncio
import html
import re
from datetime import datetime, timedelta
from typing import Literal
from urllib.parse import quote

import dateparser
import httpx
from usp.fetch_parse import SitemapFetcher

import settings
from db import DbSamizdatPrisma
from interfaces import InputLivelibBook, Output
from utils import save_cover_httpx
from workflow_base import ApiMixin, BaseLivelibWorkflow

WEB_URL = 'https://litnet.com/ru'
API_URL = 'https://superapi.litnet.com'
SITEMAP_URL = 'https://publicfs.litnet.com/sitemap/sitemap.xml'

# Обычные заголовки браузера, который ходит в superapi со страниц litnet.com
API_HEADERS = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'Origin': 'https://litnet.com',
    'Referer': 'https://litnet.com/',
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
        '(KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36'
    ),
}


class LitnetApiItem(ApiMixin, BaseLivelibWorkflow):
    name = 'livelib-litnet-api-item'
    event = 'livelib:litnet-api-item'
    site = 'litnet.com'

    input = InputLivelibBook
    output = Output

    concurrency = 25
    # карточка + рейтинги + награды параллельно, затем обложка — с запасом над http_timeout
    execution_timeout_sec = 60

    # Заголовки httpx-клиента, который создаёт ApiMixin.session()
    headers = API_HEADERS

    @classmethod
    async def run(cls, user_check: Literal['y', 'n'] | None = None) -> None:
        if settings.DEBUG:
            return

        if not user_check:
            user_check = input(f'Ты уверен что хочешь запустить {cls.site}? Y/N:')
        if user_check.lower() == 'y':
            # Из индекса берём только подкарты с книгами: sitemap_books_N.xml
            sitemap_index = httpx.get(SITEMAP_URL).text
            sitemap_books_urls = []
            for sitemap_url in re.findall(r'<loc>([^<]+/sitemap_books_\d+\.xml)</loc>', sitemap_index):
                sitemap = SitemapFetcher(url=sitemap_url, recursion_level=0).sitemap()
                sitemap_books_urls.extend([p.url for p in sitemap.all_pages()])

            print('books in sitemap:', len(sitemap_books_urls))

            async with DbSamizdatPrisma() as db:
                # done_books_urls = []
                done_books_urls = [
                    b.url for b in
                    await db.con.book.find_many(
                        where={
                            'source': cls.site,
                            'updated': {
                                'gte': datetime.now().astimezone() - timedelta(days=7),
                            },
                        }
                    )
                ]
                print('books done before:', len(done_books_urls))

                need_book_urls = await db.get_all_books_urls(cls.site)
            print('books need from db:', len(need_book_urls))

            cls.start_urls = list(set(need_book_urls + sitemap_books_urls)- set(done_books_urls))

            print('books total:', len(cls.start_urls))

            await super().run(user_check)
        elif user_check.lower() == 'n':
            return
        else:
            print('wrong value')

    @classmethod
    async def task(cls, input: InputLivelibBook, client: httpx.AsyncClient) -> Output:
        # `https://litnet.com/ru/book/klassnyi-muzhik-b599704` -> `599704`
        if not (match := re.search(r'/book/[^/?#]+-b(\d+)', input.url)):
            return Output(result='error', data={'status': None, 'error': 'invalid_url_or_404'})
        book_id = match.group(1)

        book_resp, ratings_resp, rewards_resp = await asyncio.gather(
            client.get(f'{API_URL}/v2/book', params={'bookId': book_id}),
            client.get(f'{API_URL}/v2/book/ratings', params={'bookId': book_id}),
            client.get(f'{API_URL}/v1/rewards/get-for-book', params={'book_id': book_id}),
        )

        # Проверка статуса
        if book_resp.status_code == 404:
            async with DbSamizdatPrisma() as db:
                await db.mark_book_deleted(input.url, cls.site)
            return Output(result='error', data={'status': book_resp.status_code, 'error': 'invalid_url_or_404'})
        book_resp.raise_for_status()

        data = book_resp.json()
        flags = data.get('flags') or {}
        stats = data.get('stats') or {}
        prices = data.get('prices') or {}

        # URL по актуальному alias — как раньше page.url после редиректа
        url = f'{WEB_URL}/book/{data["alias"]}'

        async with DbSamizdatPrisma() as db:
            book = {'url': url, 'source': cls.site}
            metrics = {'bookUrl': url}

            # Title
            if title := (data.get('title') or '').strip():
                book['title'] = title

            if not await db.check_book_exist(url):
                await db.create_book(book)

            # Authors. co_author в примерах всегда null — считаем, что формат как у author
            authors = [a for a in (data.get('author'), data.get('co_author')) if a and a.get('name')]
            if authors:
                book['author'] = ', '.join(a['name'].strip() for a in authors)
                book['authors_data'] = [
                    {'name': a['name'].strip(), 'url': f'{WEB_URL}/{a["alias"]}'}
                    for a in authors if a.get('alias')
                ]

            # Annotation: в API это HTML, храним текст
            if annotation := data.get('annotation'):
                annotation = re.sub(r'<br\s*/?>', '\n', annotation)
                book['annotation'] = html.unescape(re.sub(r'<[^>]+>', '', annotation)).strip()

            # Cover
            if not await db.check_book_have_cover(url):
                if img_src := data.get('image_original') or data.get('image'):
                    if img_name := await save_cover_httpx(client, img_src):
                        book['coverImage'] = img_name

            # Category
            genres = data.get('genres') or []
            book['category'] = [g['name'] for g in genres]
            book['categories_data'] = [
                {'name': g['name'].strip(), 'url': f'{WEB_URL}/top/{g["alias"]}'}
                for g in genres
            ]

            # Series
            if cycle := data.get('cycle'):
                book['series'] = [cycle['title']]

            # Tags: `жестокий дракон` -> `.../tag/%D0%B6...%20%D0%B4...-t22404880`
            tags = data.get('tags') or []
            book['tags'] = [t['name'] for t in tags]
            book['tags_data'] = [
                {'name': t['name'].strip(), 'url': f'{WEB_URL}/tag/{quote(t["name"])}-t{t["id"]}'}
                for t in tags
            ]

            # Age Rating: API отдаёт int, clear_item его не трогает
            book['age_rating'] = data.get('adult_label')

            # Release Date
            if published_at := data.get('published_at'):
                book['date_release'] = dateparser.parse(published_at, date_formats=['%Y-%m-%d'])

            # Final Date
            if finished_at := data.get('finished_at'):
                book['date_final'] = dateparser.parse(finished_at, date_formats=['%Y-%m-%d'])

            # Audio
            if flags.get('is_audio'):
                book['url_audio'] = url

            # --- Metrics ---

            metrics['rating'] = stats.get('rating')
            metrics['views'] = stats.get('count_views')
            metrics['likes'] = stats.get('count_likes')
            metrics['added_to_lib'] = stats.get('count_in_libraries')
            metrics['comments'] = stats.get('count_comments')
            metrics['pages_count'] = data.get('pages')
            metrics['characters_count'] = data.get('total_chr_length')

            # Status Writing
            if flags.get('is_finished'):
                metrics['status_writing'] = 'FINISH'
            else:
                metrics['status_writing'] = 'PROCESS'
                # Как раньше «В процессе: <дата>» — только у незаконченных
                if updated_at := data.get('updated_at'):
                    metrics['content_update_date'] = dateparser.parse(updated_at, date_formats=['%Y-%m-%d'])

            # Site Ratings
            if ratings_resp.status_code == 200:
                metrics['site_ratings'] = {r['name']: r['position'] for r in ratings_resp.json()}

            # Awards
            if rewards_resp.status_code == 200:
                rewards = (rewards_resp.json().get('data') or {}).get('rewards') or []
                metrics['awards'] = {r['reward']['name']: r['count'] for r in rewards}

            # Prices. Текущая цена — со скидкой, если она есть; 0 у бесплатных clear_item отбросит
            metrics['price'] = prices.get('price_with_discount') or prices.get('price')
            if prices.get('discount'):
                metrics['price_old'] = prices.get('price')
                metrics['price_discount'] = prices.get('price_with_discount')

            # TODO: в обоих примерах 0, даже у книги с аудио (599704)
            metrics['price_audio'] = prices.get('price_audio_with_discount') or prices.get('price_audio')

            # TODO: проверить на реальной книге. Аренда («N дней доступа») — раньше по ней ставили in_subscribe
            if prices.get('price_for_rent'):
                metrics['in_subscribe'] = True

            await db.update_book(book)
            await db.create_metrics(metrics)

        return Output(result='done', data={'book': book, 'metrics': metrics})


if __name__ == '__main__':
    LitnetApiItem.run_sync()
    # Для отладки
    LitnetApiItem.debug_sync('https://litnet.com/ru/book/oderzhimost-komissara-drakonov-b619326')
    LitnetApiItem.debug_sync('https://litnet.com/ru/book/klassnyi-muzhik-b599704')
