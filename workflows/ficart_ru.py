import re
from urllib.parse import urljoin

from playwright.async_api import Page
import dateparser
from furl import furl

from workflow_base import BaseLivelibWorkflow
from interfaces import InputLivelibBook, Output, WorkerLabels
from db import DbSamizdatPrisma
from utils import save_cover


class FicartRuListing(BaseLivelibWorkflow):
    name = 'livelib-ficart-ru-listing'
    event = 'livelib:ficart-ru-listing'
    site='ficart.ru'
    input = InputLivelibBook
    output = Output

    concurrency=3
    execution_timeout_sec=300

    start_urls = [
        'https://ficart.ru/index.php',
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

        pages_locator = page.locator('.navigation a')
        for page_locator in await pages_locator.all():
            if await cls.crawl(
                urljoin(page.url, await page_locator.get_attribute('href')),
                input.task_id,
            ):
                data['new-page-links'] += 1

        items_links = await page.query_selector_all('.argmore a')
        for i in items_links:
            item_href = await i.get_attribute('href')
            item_url = urljoin(page.url, item_href)
            if await FicartRuItem.crawl(item_url, input.task_id):
                data['new-items-links'] += 1

        if not items_links:
            raise Exception('ERROR: No Items')

        return Output(
            result='done',
            data=data,
        )

class FicartRuItem(BaseLivelibWorkflow):
    name = 'livelib-ficart-ru-item'
    event = 'livelib:ficart-ru-item'
    site='ficart.ru'
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

        await page.wait_for_selector('.maincont')

        async with DbSamizdatPrisma() as db:
            book = {
                'url': page.url,
                'source': cls.site,
            };

            metrics = {
                'bookUrl': page.url,
            };

            eval_text_follow = 'el => el.nextSibling?.textContent?.trim()'

            if not await db.check_book_exist(page.url):
                book['title'] = await page.locator('//b[contains(text(), "Название:")]').evaluate(
                    eval_text_follow
                )
                await db.create_book(book)

            author_locator = page.locator('//b[contains(text(), "Автор:")]')
            if await author_locator.count() > 0:
                book['author'] = await author_locator.evaluate(eval_text_follow)

            genres_locator = page.locator('//b[contains(text(), "Жанр:")]')
            if await genres_locator.count() > 0:
                book['category'] = [g.strip() for g in (await genres_locator.evaluate(eval_text_follow)).split(', ')]

            age_rating_locator = page.locator('//b[contains(text(), "Рейтинг:")]')
            if await age_rating_locator.count() > 0:
                book['age_rating_str'] = await age_rating_locator.evaluate(eval_text_follow)

            annotation_locator =  page.locator('//b[contains(text(), "Саммари:")]')
            if await annotation_locator.count() > 0:
                book['annotation'] = await annotation_locator.evaluate(eval_text_follow)

            date_release_locator = page.locator('.baseinfo a:nth-of-type(2)')
            if await date_release_locator.count() > 0:
                release_date_str = await date_release_locator.text_content()
                book['date_release'] = dateparser.parse(release_date_str)

            views_locator = page.locator('.argviews')
            if await views_locator.count() > 0:
                metrics['views'] = await views_locator.text_content()

            comments_locator = page.locator('.argcoms')
            if await comments_locator.count() > 0:
                metrics['comments'] = await comments_locator.text_content()

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
    FicartRuListing.run_sync()

    FicartRuListing.debug_sync(FicartRuListing.start_urls[0])
    FicartRuItem.debug_sync('https://ficart.ru/fanfic/drama/23-fanfik-bring-me-a-life-pg-15.html')
