import re
from urllib.parse import urljoin

from playwright.async_api import Page
import dateparser
from furl import furl

from workflow_base import BaseLivelibWorkflow
from interfaces import InputLivelibBook, Output, WorkerLabels
from db import DbSamizdatPrisma
from utils import save_cover


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
        await page.wait_for_timeout(2_000)

        async with DbSamizdatPrisma() as db:
            book = {
                'url': page.url,
                'source': cls.site,
            }

            metrics = {
                'bookUrl': page.url,
            }

            if not await db.check_book_exist(page.url):
                book['title'] = await page.text_content('[data-test="BlockText1-title"]')
                await db.create_book(book)

            authors_locator = page.locator('div[data-test="BlockText1-creator"]').filter(
                has_text=re.compile(r'Writer')
            ).locator('div')
            if await authors_locator.count() > 0:
                book['author'] = await authors_locator.last.text_content()

            translator_locator = page.locator('div[data-test="BlockText1-creator"]').filter(
                has_text=re.compile(r'Localization')
            ).locator('div')
            if await translator_locator.count() > 0:
                book['translate'] = await translator_locator.last.text_content()

            artist_locator = page.locator('div[data-test="BlockText1-creator"]').filter(
                has_text=re.compile(r'Illustration')
            ).locator('div')
            if await artist_locator.count() > 0:
                book['artist'] = await artist_locator.last.text_content()

            genres_locator = page.locator('[data-test="BlockText1-tags"]')
            if await genres_locator.count() > 0:
                book['category'] =[g.strip() for g in (await genres_locator.text_content()).split(' · ')]

            tags_locator = page.locator('[data-test="ClickableTagItem-link"]')
            if await tags_locator.count() > 0:
                book['tags'] =[(await t.text_content()).strip() for t in await tags_locator.all()]

            age_rating_locator = page.locator('img[alt^="ages"][alt$="badge"]')
            if await age_rating_locator.count() > 0:
                book['age_rating'] = re.search(r'\d+', await age_rating_locator.get_attribute('alt'))[0]

            date_release_locator = page.locator('div[data-test="EpisodeItem"] > div > div:nth-of-type(2)').filter(
                has_text=re.compile(r'\w{2,5} \d{1,2}, \d{4}')
            )
            if await date_release_locator.count() > 0:
                 book['date_release'] = dateparser.parse(await date_release_locator.first.text_content())

            if annotation := await page.text_content('[data-test="BlockText1-descriptionLong"] > span'):
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


class MantaNetListing(BaseLivelibWorkflow):
    name = 'livelib-manta-net-listing'
    event = 'livelib:manta-net-listing'
    site = 'manta.net'
    input = InputLivelibBook
    output = Output
    item_wf = MantaNetItem

    concurrency=3
    execution_timeout_sec=3600

    start_urls = [
        'https://manta.net/en',
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


if __name__ == '__main__':
    MantaNetListing.run_sync()

    # MantaNetListing.debug_sync(MantaNetListing.start_urls[0])
    MantaNetItem.debug_sync('https://manta.net/en/series/in-the-hall-of-the-mountain-king?seriesId=3756')
