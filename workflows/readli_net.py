import re
from urllib.parse import urljoin

from playwright.async_api import Page
import dateparser
from furl import furl

from workflow_base import BaseLivelibWorkflow
from interfaces import InputLivelibBook, Output, WorkerLabels
from db import DbSamizdatPrisma
from utils import save_cover


class ReadliNetListing(BaseLivelibWorkflow):
    name = 'livelib-readli-net-listing'
    event = 'livelib:readli-net-listing'
    site='readli.net'
    input = InputLivelibBook
    output = Output

    concurrency=3
    execution_timeout_sec=300
    retries=10
    backoff_max_seconds=30
    backoff_factor=2

    start_urls = [
        'https://readli.net/cat/proza-i-stihi/fanfik/',
    ]

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        resp = await page.goto(
            input.url,
            wait_until='domcontentloaded',
        )
        if not (200 <= resp.status < 400):
            return Output(
                result='error',
                data={'status': resp.status},
            )


        data = {
            'new-items-links': 0,
            'new-page-links': 0,
        }

        pages_locator = page.locator('.pagination :not(.disabled) a')
        for page_locator in await pages_locator.all():
            if await cls.crawl(
                urljoin(page.url, await page_locator.get_attribute('href')),
                input.task_id,
            ):
                data['new-page-links'] += 1

        items_links = await page.query_selector_all('.book__title a.book__link')
        for i in items_links:
            item_href = await i.get_attribute('href')
            item_url = urljoin(page.url, item_href)
            if await ReadliNetItem.crawl(item_url, input.task_id):
                data['new-items-links'] += 1

        if not items_links:
            raise Exception('ERROR: No Items')

        return Output(
            result='done',
            data=data,
        )

class ReadliNetItem(BaseLivelibWorkflow):
    name = 'livelib-readli-net-item'
    event = 'livelib:readli-net-item'
    site='readli.net'
    input = InputLivelibBook
    output = Output

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        resp = await page.goto(
            input.url,
            wait_until='domcontentloaded',
            timeout=20_000,
        )
        if not (200 <= resp.status < 400):
            return Output(
                result='error',
                data={'status': resp.status},
            )

        await page.wait_for_selector('h1')

        async with DbSamizdatPrisma() as db:
            book = {
                'url': page.url,
                'source': cls.site,
            };

            metrics = {
                'bookUrl': page.url,
            };

            if not await db.check_book_exist(page.url):
                book['title'] = await page.text_content('h1')
                await db.create_book(book)

            authors_locator = page.locator('.main-info > a')
            if await authors_locator.count() > 0:
                book['author'] = await authors_locator.first.text_content()
                # Все авторы с текстом и ссылками
                book['authors_data'] = []
                for a in await authors_locator.all():
                    href = await a.get_attribute('href')
                    text = await a.text_content()
                    absolute_url = urljoin(page.url, href)

                    book['authors_data'].append({
                        'name': text.strip(),
                        'url': absolute_url
                    })

            serie_locator = page.locator('.book-info > p').filter(
                has_text=re.compile(r'Серия:|Серии:')
            ).locator('a')
            if await serie_locator.count() > 0:
                book['series'] = [await s.text_content() for s in await serie_locator.all()]

            genres_locator = page.locator('.book-info > p').filter(
                has_text=re.compile(r'Жанр:|Жанры:')
            ).locator('a')
            if await genres_locator.count() > 0:
                book['tags'] = [await g.text_content() for g in await genres_locator.all()]

            date_release_locator = page.locator('.book-sidebar .book-chars__item').filter(
                has_text=re.compile(r'Размещено ')
            )
            if await date_release_locator.count() > 0:
                date_release_str = await date_release_locator.text_content()
                date_release_str = re.search(r'\d\d\.\d\d\.\d\d\d\d', date_release_str)[0]
                book['date_release'] = dateparser.parse(date_release_str, languages=['ru'])

            if annotation := await page.inner_text('.seo__content'):
                book['annotation'] = annotation

            if not await db.check_book_have_cover(page.url):
                if img_src := await page.get_attribute('.book-image img', 'src', timeout=2_000):
                    if img_name := await save_cover(page, img_src, timeout=10_000):
                        book['coverImage'] = img_name

            views_locator = page.locator('.book-sidebar .rating-numbers__item_icon-1')
            if await views_locator.count() > 0:
                metrics['views'] = await views_locator.text_content()

            added_to_lib_locator = page.locator('.book-sidebar .rating-numbers__item_icon-2')
            if await added_to_lib_locator.count() > 0:
                metrics['added_to_lib'] = await added_to_lib_locator.text_content()

            comments_locator = page.locator('.book-sidebar .rating-numbers__item_icon-3')
            if await comments_locator.count() > 0:
                metrics['comments'] = await comments_locator.text_content()

            likes_locator = page.locator('.book-sidebar .button-like__count')
            if await likes_locator.count() > 0:
                metrics['likes'] = await likes_locator.first.text_content()

            rating_locator = page.locator('.book-sidebar .rating-info__count')
            if await rating_locator.count() > 0:
                metrics['rating'] = await rating_locator.text_content()

            pages_count_locator = page.locator('.book-sidebar .button-pages__cols .button-pages__right')
            if await pages_count_locator.count() > 0:
                pages_count_str = await pages_count_locator.text_content()
                metrics['pages_count'] = re.search(r'\d+', pages_count_str)[0]

            await db.update_book(book)
            await db.create_metrics(metrics)

            return Output(
                result='done',
                data={
                    'book': book,
                    'metrics': metrics,
                },
            )

if __name__ == '__main__':
    ReadliNetListing.run_sync()

    ReadliNetListing.debug_sync(ReadliNetListing.start_urls[0])
    ReadliNetListing.debug_sync('https://readli.net/cat/proza-i-stihi/fanfik/page/2/')
    ReadliNetItem.debug_sync('https://readli.net/skazaniya-o-prepodobnom-demone-tom-2/')
