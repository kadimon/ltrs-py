import re
from urllib.parse import urljoin

from playwright.async_api import Page
import dateparser

from workflow_base import BaseLivelibWorkflow
from interfaces import InputLivelibBook, Output, WorkerLabels
from db import DbSamizdatPrisma
from utils import save_cover


class AcomicsRuItem(BaseLivelibWorkflow):
    name = 'livelib-acomics-ru-item'
    event = 'livelib:acomics-ru-item'
    site='acomics.ru'
    input = InputLivelibBook
    output = Output

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        resp = await page.goto(
            input.url + '/about',
            wait_until='domcontentloaded',
            timeout=20_000,
        )
        if not (200 <= resp.status < 400):
            return Output(
                result='error',
                data={'status': resp.status},
            )

        await page.wait_for_selector('.common-content')

        age_confirm_locator = page.locator('button[name="ageRestrict"]:not([value="no"])')
        if await age_confirm_locator.count() > 0:
            await age_confirm_locator.click()

        async with DbSamizdatPrisma() as db:
            if resp.status == 404:
                await db.mark_book_deleted(input.url, cls.site)
                return Output(result='error', data={'status': resp.status})

            book = {
                'url': input.url,
                'source': cls.site,
            }

            metrics = {
                'bookUrl': input.url,
            }

            book['title'] = await page.text_content('h1')
            if not await db.check_book_exist(input.url):
                book['title'] = await page.text_content('h1')
                await db.create_book(book)

            authors_locator = page.locator('.serial-about-authors a')
            if await authors_locator.count() > 0:
                book['author'] = ', '.join([await a.text_content() for a in await authors_locator.all()])
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

            genres_locator = page.locator('.serial-about-badges a')
            if await genres_locator.count() > 0:
                book['category'] = [await g.text_content() for g in await genres_locator.all()]

            if annotation := await page.inner_text('.serial-about-text'):
                book['annotation'] = annotation


            await db.update_book(book)
            # await db.create_metrics(metrics)

            return Output(
                result='done',
                data={
                    'book': book,
                    'metrics': metrics,
                },
            )


TOTAL_ITEMS = 2950

class AcomicsRuListing(BaseLivelibWorkflow):
    name = 'livelib-acomics-ru-listing'
    event = 'livelib:acomics-ru-listing'
    site='acomics.ru'
    input = InputLivelibBook
    output = Output
    item_wf = AcomicsRuItem

    concurrency=3
    execution_timeout_sec=300
    backoff_max_seconds=30
    backoff_factor=2

    start_urls = [
        f'https://acomics.ru/comics?skip={offset}'
        for offset in range(0, TOTAL_ITEMS, 10)
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
            'updated-items': 0,
            'new-nav-links': 0,
            'new-items-links': 0,
        }

        async with DbSamizdatPrisma() as db:
            for item in await page.locator('.serial-card').all():
                title_locator = item.locator('.title a:first-of-type')
                item_url = urljoin(page.url, await title_locator.get_attribute('href'))

                book = {
                    'url': item_url,
                    'source': cls.site,
                }

                metrics = {
                    'bookUrl': item_url,
                }

                book['title'] = await title_locator.text_content()
                if not await db.check_book_exist(item_url):
                    book['title'] = await title_locator.text_content()
                    await db.create_book(book)

                annotation_locator = item.locator('.about')
                if await annotation_locator.count() > 0:
                    book['annotation'] = await annotation_locator.first.text_content()

                age_rating_locator = item.locator('.age-rating a')
                if await age_rating_locator.count() > 0:
                     book['age_rating_str'] = await age_rating_locator.text_content()

                if not await db.check_book_have_cover(item_url):
                    if img_src := await item.locator(
                        '.cover img'
                    ).get_attribute('src', timeout=2_000):
                        if img_name := await save_cover(page, img_src, timeout=10_000):
                            book['coverImage'] = img_name

                content_update_date_locator = item.locator('.date-time-formatted')
                if await content_update_date_locator.count() > 0:
                    content_update_date_str = await content_update_date_locator.text_content()
                    metrics['content_update_date'] = dateparser.parse(content_update_date_str)

                added_to_lib_locator = item.locator('.subscr-count')
                if await added_to_lib_locator.count() > 0:
                    metrics['added_to_lib'] = await added_to_lib_locator.first.text_content()

                chapters_count_locator = item.locator('.issue-count')
                if await chapters_count_locator.count() > 0:
                    metrics['chapters_count'] = re.search(r'\d+', await chapters_count_locator.text_content())[0]

                await db.update_book(book)
                await db.create_metrics(metrics)
                data['updated-items'] += 1

                if await AcomicsRuItem.crawl(item_url, input.task_id):
                    data['new-items-links'] += 1

        return Output(
            result='done',
            data=data,
        )

if __name__ == '__main__':
    AcomicsRuListing.run_sync()

    AcomicsRuListing.debug_sync(AcomicsRuListing.start_urls[0])
    AcomicsRuItem.debug_sync('https://acomics.ru/~city-stories')
