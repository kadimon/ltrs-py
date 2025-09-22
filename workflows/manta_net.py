import re
from urllib.parse import urljoin

from playwright.async_api import Page
import dateparser
from furl import furl

from workflow_base import BaseLivelibWorkflow
from interfaces import InputLivelibBook, Output, WorkerLabels
from db import DbSamizdatPrisma
from utils import save_cover


class MantaNetListing(BaseLivelibWorkflow):
    name = 'livelib-manta-net-listing'
    event = 'livelib:manta-net-listing'
    site='manta.net'
    input = InputLivelibBook
    output = Output

    concurrency=3
    execution_timeout_sec=3600

    start_urls = [
        'https://manta.net/en',
    ]

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        resp = await page.goto(
            input.url,
            wait_until='networkidle',
        )
        if not (200 <= resp.status < 400):
            return Output(
                result='error',
                data={'status': resp.status},
            )

        items_links_loacator = page.locator('a[href*="/series/"]')
        # скролим вниз чтобы прогрузился контент
        while True:
            initial_count = await items_links_loacator.count()
            await page.mouse.wheel(0, 1000)
            await page.wait_for_timeout(1500)
            await items_links_loacator.last.scroll_into_view_if_needed()
            new_count = await items_links_loacator.count()
            if new_count == initial_count:
                break

        data = {
            'new-items-links': 0,
            'new-page-links': 0,
        }

        for i in await items_links_loacator.all():
            item_href = await i.get_attribute('href')
            item_url = urljoin(page.url, item_href)
            if await MantaNetItem.crawl(item_url, input.task_id):
                data['new-items-links'] += 1

        if await items_links_loacator.count() == 0:
            raise Exception('ERROR: No Items')

        return Output(
            result='done',
            data=data,
        )

class MantaNetItem(BaseLivelibWorkflow):
    name = 'livelib-manta-net-item'
    event = 'livelib:manta-net-item'
    site='manta.net'
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

        await page.wait_for_selector('[data-test="BlockText1-title"]')

        async with DbSamizdatPrisma() as db:
            book = {
                'url': page.url,
                'source': cls.site,
            };

            metrics = {
                'bookUrl': page.url,
            };

            if not await db.check_book_exist(page.url):
                book['title'] = await page.text_content('[data-test="BlockText1-title"]')
                await db.create_book(book)

            genres_locator = page.locator('[data-test="BlockText1-tags"]')
            if await genres_locator.count() > 0:
                book['category'] =[g.strip() for g in (await genres_locator.text_content()).split(' · ')]

            tags_locator = page.locator('[data-test="ClickableTagItem-link"]')
            if await tags_locator.count() > 0:
                book['tags'] =[(await t.text_content()).strip() for t in await tags_locator.all()]

            if annotation := await page.text_content('[data-test="BlockText1-description"]'):
                book['annotation'] = annotation

            if not await db.check_book_have_cover(page.url):
                if img_src := await page.get_attribute('img[alt="series-main"]', 'src', timeout=2_000):
                    if img_name := await save_cover(page, img_src, timeout=10_000):
                        book['coverImage'] = img_name

            chapters_count_locator = page.locator('[data-test="EpisodeListHeader-count"]')
            if await chapters_count_locator.count() > 0:
                metrics['chapters_count'] = re.search(r'\d+', await chapters_count_locator.text_content())[0]

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
    MantaNetListing.run_sync()

    MantaNetListing.debug_sync(MantaNetListing.start_urls[0])
    MantaNetItem.debug_sync('https://manta.net/en/series/solo-leveling?seriesId=2729')
