import asyncio
import html
import re
from datetime import datetime, timedelta
from typing import ClassVar, Literal
from urllib.parse import quote

import dateparser
import httpx
from hatchet_sdk import Context
from usp.fetch_parse import SitemapFetcher

import settings
from db import DbSamizdatPrisma
from interfaces import InputLivelibBook, Output
from utils import save_cover_httpx
from workflow_base import ApiMixin, BaseLivelibWorkflow

WEB_URL = 'https://author.today'
API_URL = 'https://api.author.today/v1'
SITEMAP_URL = 'https://author.today/sitemap.xml'

# `https://author.today/work/667430`, `https://author.today/audiobook/669139` -> id книги
WORK_URL_RE = re.compile(r'/(?:work|audiobook)/(\d+)')

# Заголовки клиента — как у браузера: статистику и просмотры берём с самого сайта
HEADERS = {
    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
        '(KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36'
    ),
}

# Гостевой токен — только к запросам в api.author.today
API_HEADERS = {
    'Accept': 'application/json',
    'Authorization': 'Bearer guest',
}


class AuthorTodayApiItem(ApiMixin, BaseLivelibWorkflow):
    name = 'livelib-author-today-api-item'
    event = 'livelib:author-today-api-item'
    site = 'author.today'

    input = InputLivelibBook
    output = Output

    concurrency = 15
    # карточка, затем статистика и страница параллельно, затем обложка — с запасом над http_timeout
    execution_timeout_sec = 90

    # Заголовки httpx-клиента, который создаёт ApiMixin.session()
    headers = HEADERS

    # id жанра -> жанр из /work/genres. В карточке книги только id жанров,
    # справочник грузим один раз на процесс воркера
    genres: ClassVar[dict[int, dict]] = {}

    @classmethod
    async def run(cls, user_check: Literal['y', 'n'] | None = None) -> None:
        if settings.DEBUG:
            return

        if not user_check:
            user_check = input(f'Ты уверен что хочешь запустить {cls.site}? Y/N:')
        if user_check.lower() == 'y':
            # Из индекса берём только подкарты с книгами: sitemap/works_N.xml
            sitemap_index = httpx.get(SITEMAP_URL).text
            sitemap_books_urls = []
            for sitemap_url in re.findall(r'<loc>([^<]+/sitemap/works_\d+\.xml)</loc>', sitemap_index):
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

            # Сводим по id: аудиокниги в БД лежат как /audiobook/{id}, а в sitemap
            # всё как /work/{id}. БД идёт последней — её URL и остаётся
            urls_by_id = {}
            for url in sitemap_books_urls + need_book_urls:
                if match := WORK_URL_RE.search(url):
                    urls_by_id[match.group(1)] = url

            done_ids = {match.group(1) for url in done_books_urls if (match := WORK_URL_RE.search(url))}

            cls.start_urls = [url for work_id, url in urls_by_id.items() if work_id not in done_ids]

            print('books total:', len(cls.start_urls))

            await super().run(user_check)
        elif user_check.lower() == 'n':
            return
        else:
            print('wrong value')

    @classmethod
    async def task(
        cls,
        input: InputLivelibBook,
        client: httpx.AsyncClient,
        ctx: Context | None = None,
    ) -> Output:
        # `https://author.today/work/667430` -> `667430`
        if not (match := WORK_URL_RE.search(input.url)):
            return Output(result='error', data={'status': None, 'error': 'invalid_url_or_404'})
        work_id = match.group(1)

        # retry_count у первой попытки 0, у последней — cls.retries.
        # В debug ctx нет — ведём себя как на последней попытке
        last_attempt = not ctx or ctx.retry_count >= cls.retries

        # Справочник жанров — один раз на процесс воркера
        if not cls.genres:
            genres_resp = await client.get(f'{API_URL}/work/genres', headers=API_HEADERS)
            genres_resp.raise_for_status()
            cls.genres = {g['id']: g for g in genres_resp.json()}

        details_resp = await client.get(f'{API_URL}/work/{work_id}/details', headers=API_HEADERS)

        # Проверка статуса: книга удалена, в черновике или скрыта модератором
        if details_resp.status_code == 404:
            async with DbSamizdatPrisma() as db:
                await db.mark_book_deleted(input.url, cls.site)
            return Output(result='error', data={'status': details_resp.status_code, 'error': 'not_found'})

        # «Доступ ограничен» — книга только для друзей автора.
        # Прочие 403 (VersionIsUnsupported, InternalServerError) уходят в ретрай
        if details_resp.status_code == 403 and details_resp.json().get('code') in (
            'AuthorizationRequired',
            'AccessOnlyForFriends',
        ):
            async with DbSamizdatPrisma() as db:
                await db.mark_book_deleted(input.url, cls.site)
            return Output(result='error', data={'status': details_resp.status_code, 'error': 'access_limited'})

        details_resp.raise_for_status()

        data = details_resp.json()
        is_audio = data.get('format') == 'Audiobook'

        # URL как раньше page.url после редиректа: аудиокниги живут на /audiobook/{id}
        url = f'{WEB_URL}/{"audiobook" if is_audio else "work"}/{data["id"]}'

        stats_resp, page_resp = await asyncio.gather(
            # Счётчики вкладки «Статистика» — в API их нет, только на сайте
            client.get(f'{WEB_URL}/work/work-stats', params={'workId': work_id}),
            # Просмотры — только в HTML страницы, сразу по каноническому URL
            client.get(url),
            return_exceptions=True,
        )
        if isinstance(stats_resp, BaseException):
            raise stats_resp

        # Страница нужна только ради просмотров: пока есть ретраи — повторяем,
        # на последней попытке сохраняем книгу без них.
        # Падаем до записи в БД, иначе каждый ретрай добавит лишнюю строку в Metrics
        views_match = None
        if isinstance(page_resp, httpx.Response) and page_resp.status_code == 200:
            views_match = re.search(r'Просмотры\s*·\s*([\d\s]+)', html.unescape(page_resp.text))
        if not views_match and not last_attempt:
            if isinstance(page_resp, BaseException):
                raise page_resp
            raise RuntimeError(f'Нет просмотров на {url}: HTTP {page_resp.status_code}')

        async with DbSamizdatPrisma() as db:
            book = {'url': url, 'source': cls.site}
            metrics = {'bookUrl': url}

            # Title
            if title := (data.get('title') or '').strip():
                book['title'] = title

            if not await db.check_book_exist(url):
                await db.create_book(book)

            # Authors. Соавторы — только подтвердившие соавторство
            authors = [(data.get('authorFIO'), data.get('authorUserName'))]
            if data.get('coAuthorConfirmed'):
                authors.append((data.get('coAuthorFIO'), data.get('coAuthorUserName')))
            if data.get('secondCoAuthorConfirmed'):
                authors.append((data.get('secondCoAuthorFIO'), data.get('secondCoAuthorUserName')))
            authors = [(fio.strip(), username) for fio, username in authors if fio]
            if authors:
                book['author'] = ', '.join(fio for fio, _ in authors)
                # Логин в ссылках сайта в нижнем регистре: YuriyVinokurov -> /u/yuriyvinokurov/works.
                # Иначе разъедемся с Person старого скрапера и с get_priority_persons_urls
                book['authors_data'] = [
                    {'name': fio, 'url': f'{WEB_URL}/u/{username.lower()}/works'}
                    for fio, username in authors if username
                ]

            # Annotation: в API это HTML, храним текст. Примечания автора не добавляем
            if annotation := data.get('annotation'):
                annotation = re.sub(r'<br\s*/?>|</p>', '\n', annotation)
                book['annotation'] = html.unescape(re.sub(r'<[^>]+>', '', annotation)).strip()

            # Cover: без `?width=265&height=400&rmode=max` — оригинал, а не превью
            if not await db.check_book_have_cover(url):
                if img_src := data.get('coverUrl'):
                    if img_name := await save_cover_httpx(client, img_src.split('?')[0]):
                        book['coverImage'] = img_name

            # Category: в карточке только id жанров, названия — из справочника
            book_genres = [
                cls.genres[genre_id]
                for genre_id in (data.get('genreId'), data.get('firstSubGenreId'), data.get('secondSubGenreId'))
                if genre_id in cls.genres
            ]
            if book_genres:
                book['category'] = [g['title'] for g in book_genres]
                book['categories_data'] = [
                    {'name': g['title'], 'url': f'{WEB_URL}/work/genre/{g["code"]}'}
                    for g in book_genres
                ]

            # Series
            if series_title := (data.get('seriesTitle') or '').strip():
                book['series'] = [series_title]
                book['series_data'] = [{'name': series_title, 'url': f'{WEB_URL}/work/series/{data["seriesId"]}'}]

            # Tags: `мякотка` -> `.../work/tag/%D0%BC%D1%8F%D0%BA%D0%BE%D1%82%D0%BA%D0%B0`
            if tags := [t.strip() for t in data.get('tags') or [] if t.strip()]:
                book['tags'] = tags
                book['tags_data'] = [{'name': t, 'url': f'{WEB_URL}/work/tag/{quote(t)}'} for t in tags]

            # Audio: у аудиокниги — она сама и чтец, у текстовой — связанная аудиоверсия
            if is_audio:
                book['url_audio'] = url
                if reciter := (data.get('reciter') or '').strip():
                    book['voice'] = reciter
            elif (linked := data.get('linkedWork')) and linked.get('format') == 'Audiobook':
                # TODO: проверить на текстовой книге с аудиоверсией — в примерах только обратная связь
                book['url_audio'] = f'{WEB_URL}/audiobook/{linked["id"]}'

            # Translator (только у переводов)
            if translator := (data.get('translator') or '').strip():
                book['translate'] = translator

            # Age Rating: на сайте только флаг 18+
            if data.get('adultOnly'):
                book['age_rating'] = 18

            # Release Date: в API нет, берём самую раннюю публикацию главы.
            # У текстовых глав publishTime, у аудио — uploadTime
            chapters = data.get('chapters') or []
            chapter_times = [c.get('publishTime') or c.get('uploadTime') for c in chapters]
            chapter_times = [t for t in chapter_times if t]
            if chapter_times:
                # ISO-время в UTC сравнивается и как строка
                book['date_release'] = dateparser.parse(min(chapter_times))

            # Final Date
            if finish_time := data.get('finishTime'):
                book['date_final'] = dateparser.parse(finish_time)

            # --- Metrics ---

            metrics['likes'] = data.get('likeCount')
            metrics['comments'] = data.get('commentCount')
            metrics['chapters_count'] = len(chapters)

            # Size: у аудиокниги textLength — длительность в секундах (27719 = 7:41:59, как на странице)
            if is_audio:
                metrics['duration'] = data.get('textLength')
            else:
                metrics['characters_count'] = data.get('textLength')

            # Awards: только общее число, как раньше
            if data.get('rewardCount'):
                metrics['awards'] = {'award': data['rewardCount']}

            # Status Writing
            metrics['status_writing'] = 'FINISH' if data.get('isFinished') else 'PROCESS'

            # «Последнее обновление» на странице — изменение текста на 15 000 знаков и больше
            if updated_at := data.get('lastModificationTime'):
                metrics['content_update_date'] = dateparser.parse(updated_at)

            # Prices: price — полная цена, discount — процент скидки.
            # TODO: проверить на реальной книге со скидкой — в примерах discount: null
            if price := data.get('price'):
                if discount := data.get('discount'):
                    metrics['price_old'] = price
                    metrics['price_discount'] = round(price * (100 - discount) / 100, 2)
                    metrics['price'] = metrics['price_discount']
                else:
                    metrics['price'] = price

            # Library: счётчики вкладки «Статистика». Без браузера сайт может отдать
            # не JSON (логин, заглушка) — тогда пропускаем
            if stats_resp.status_code == 200 and 'json' in stats_resp.headers.get('content-type', ''):
                stats = stats_resp.json().get('data') or {}
                metrics['added_to_lib'] = stats.get('totalCount')
                metrics['read_process'] = stats.get('readingCount')
                metrics['read_later'] = stats.get('savedCount')
                metrics['read_finished'] = stats.get('finishedCount')
                metrics['unlike'] = stats.get('dislikedCount')

            # Views: `data-hint="Просмотры · 16&nbsp;327"` -> `16 327`, пробелы уберёт convert_metrics
            if views_match:
                metrics['views'] = views_match.group(1)

            await db.update_book(book)
            await db.create_metrics(metrics)

        # --- Crawl linked format (текст <-> аудио) ---
        if linked := data.get('linkedWork'):
            linked_section = 'audiobook' if linked.get('format') == 'Audiobook' else 'work'
            await cls.crawl(f'{WEB_URL}/{linked_section}/{linked["id"]}', input.task_id)

        return Output(result='done', data={'book': book, 'metrics': metrics})


if __name__ == '__main__':
    AuthorTodayApiItem.run_sync()
    # Для отладки
    AuthorTodayApiItem.debug_sync('https://author.today/work/667430')
    AuthorTodayApiItem.debug_sync('https://author.today/audiobook/669139')
