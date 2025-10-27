import re
from urllib.parse import urljoin

from playwright.async_api import Page
from datetime import datetime

from workflow_base import BaseLivelibWorkflow
from interfaces import InputLivelibBook, Output, WorkerLabels
from db import DbSamizdatPrisma
from utils import save_cover


class MangabuffRuItem(BaseLivelibWorkflow):
    name = 'livelib-mangabuff-ru-item'
    event = 'livelib:mangabuff-ru-item'
    site='mangabuff.ru'
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

            titles_other_locator = page.locator('.manga__name-alt span')
            if await titles_other_locator.count() > 0:
                book['titles_other'] = [(await t.text_content()).strip() for t in await titles_other_locator.all()]

            if artwork_type := await page.text_content('.manga__middle-link:nth-child(1)'):
                book['artwork_type'] = artwork_type

            tags_locator = page.locator('.tags__item')
            if await tags_locator.count() > 0:
                book['tags'] = [await g.text_content() for g in await tags_locator.all()]

            age_rating_regex = r'^(\d+)\+$'
            age_rating_locator = page.locator('.tags__item').filter(
                has_text=re.compile(age_rating_regex)
            )
            if await age_rating_locator.count() > 0:
                age_rating_match = re.search(age_rating_regex, await age_rating_locator.text_content())
                book['age_rating'] = age_rating_match.group(1)

            annotation_locator = page.locator('.manga__description')
            if await annotation_locator.count() > 0:
                book['annotation'] = await annotation_locator.text_content()

            date_release_locator = page.locator('.manga__middle-link:nth-child(2)')
            if await date_release_locator.count() > 0:
                book['date_release'] = datetime.strptime(await date_release_locator.text_content(), "%Y")

            if not await db.check_book_have_cover(page.url):
                if img_src := await page.get_attribute('.manga__img img', 'src', timeout=2_000):
                    if img_name := await save_cover(page, img_src, timeout=10_000):
                        book['coverImage'] = img_name

            comments_locator = page.locator('.secondary-title').filter(
                has_text=re.compile(r'Комментарии')
            ).locator('secondary-text')
            if await comments_locator.count() > 0:
                metrics['comments'] = await comments_locator.text_content()

            rating_locator = page.locator('.manga__rating')
            if await rating_locator.count() > 0:
                metrics['rating'] = await rating_locator.text_content()

            views_locator = page.locator('.manga__views')
            if await views_locator.count() > 0:
                metrics['views'] = await views_locator.text_content()

            chapters_patern = r'Главы\s+\((\d+)\)'
            chapters_count_locator = page.locator('.tabs__item[data-page="chapters"]').filter(
                has_text=re.compile(chapters_patern)
            )
            if await chapters_count_locator.count() > 0:
                chapters_regex = re.search(chapters_patern, await chapters_count_locator.text_content())
                metrics['chapters_count'] = chapters_regex.group(1)

            writing_statuses_match = {
                'Продолжается': 'PROCESS',
                'Заморожен': 'PAUSE',
                'Завершен': 'FINISH',
                'Заброшен': 'STOP',
            }
            writing_status_loacator = page.locator('a.manga__middle-link[href*="status_id"]')
            if await writing_status_loacator.count() > 0:
                translation_status_str = await writing_status_loacator.text_content()
                if translation_status := writing_statuses_match.get(translation_status_str.strip()):
                    metrics['status_writing'] = translation_status

            await db.update_book(book)
            await db.create_metrics(metrics)

            return Output(
                result='done',
                data={
                    'book': book,
                    'metrics': metrics,
                },
            )

class MangabuffRuListing(BaseLivelibWorkflow):
    name = 'livelib-mangabuff-ru-listing'
    event = 'livelib:mangabuff-ru-listing'
    site='mangabuff.ru'
    input = InputLivelibBook
    output = Output
    item_wf = MangabuffRuItem

    concurrency=3
    execution_timeout_sec=300
    backoff_max_seconds=30
    backoff_factor=2

    start_urls = [
        'https://mangabuff.ru/manga',
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

        pages_locator = page.locator('.pagination a')
        for page_locator in await pages_locator.all():
            if await cls.crawl(
                urljoin(page.url, await page_locator.get_attribute('href')),
                input.task_id,
            ):
                data['new-page-links'] += 1

        items_links = await page.query_selector_all('a.cards__item')
        for i in items_links:
            item_href = await i.get_attribute('href')
            item_url = urljoin(page.url, item_href)
            if await MangabuffRuItem.crawl(item_url, input.task_id):
                data['new-items-links'] += 1

        if not items_links:
            raise Exception('ERROR: No Items')

        return Output(
            result='done',
            data=data,
        )

if __name__ == '__main__':
    MangabuffRuListing.run_sync()

    MangabuffRuListing.debug_sync(MangabuffRuListing.start_urls[0])
    MangabuffRuItem.debug_sync('https://mangabuff.ru/manga/prirozhdennyi-naemnik')
