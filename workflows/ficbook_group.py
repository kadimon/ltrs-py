import re
from urllib.parse import urljoin

from playwright.async_api import Page
import dateparser
from furl import furl

from workflow_base import BaseLivelibWorkflow
from interfaces import InputLivelibBook, Output, WorkerLabels
from db import DbSamizdatPrisma
from utils import save_cover


class FicbookGroupItem(BaseLivelibWorkflow):
    name = 'livelib-ficbook-group-item'
    event = 'livelib:ficbook-group-item'
    site='ficbook.group'
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
            if resp.status == 404:
                await db.mark_book_deleted(input.url, cls.site)
                return Output(result='error', data={'status': resp.status})

            book = {
                'url': page.url,
                'source': cls.site,
            }

            metrics = {
                'bookUrl': page.url,
            }

            book['title'] = await page.text_content('h1')
            if not await db.check_book_exist(page.url):
                book['title'] = await page.text_content('h1')
                await db.create_book(book)

            authors_locator = page.locator('.author-item ').filter(
                has=page.locator('//*[text()="автор" or text()="соавтор"]')
            ).locator('.author-item__name')
            if await authors_locator.count() > 0:
                book['author'] = ', '.join([await a.text_content() for a in await authors_locator.all()])

            tags_locator = page.locator('.tag-field__item')
            if await tags_locator.count() > 0:
                book['tags'] = [await g.text_content() for g in await tags_locator.all()]

            chapters_count = await page.locator('.part-item').count()
            if chapters_count > 0:
                metrics['chapters_count'] = chapters_count

            age_rating_locator = page.locator('.card-item__badges .card-item__badge:nth-of-type(3)')
            if await age_rating_locator.count() > 0:
                 book['age_rating_str'] = await age_rating_locator.text_content()

            dates_locator = page.locator('.part-item__info span, .part-header__date')
            if await dates_locator.count() > 0:
                book['date_release'] = dateparser.parse(await dates_locator.first.text_content())
                metrics['content_update_date'] = dateparser.parse(await dates_locator.last.text_content())

            annotation_locator = page.locator('.card-description__format')
            if await annotation_locator.count() > 0:
                book['annotation'] = await annotation_locator.first.text_content()

            likes_locator = page.locator('.article-count__likes')
            if await likes_locator.count() > 0:
                metrics['likes'] = await likes_locator.first.text_content()
                metrics['comments'] = await likes_locator.last.text_content()

            if not await db.check_book_have_cover(page.url):
                img_locator = page.locator('.article-top img.article-top__image:not([src$="nofanfic.jpg"])')
                if await img_locator.count() > 0:
                    img_src = await img_locator.get_attribute('src')
                    if img_name := await save_cover(page, img_src, timeout=10_000):
                        book['coverImage'] = img_name

            writing_statuses_match = {
                'В процессе': 'PROCESS',
                'Завершён': 'FINISH',
                'Заморожен': 'STOP',
            }
            writing_status_loacator = page.locator('.card-item__badges .card-item__badge:nth-of-type(2)')
            if await writing_status_loacator.count() > 0:
                writing_status_str = await writing_status_loacator.text_content()
                if translation_status := writing_statuses_match.get(writing_status_str.strip()):
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


class FicbookGroupListing(BaseLivelibWorkflow):
    name = 'livelib-ficbook-group-listing'
    event = 'livelib:ficbook-group-listing'
    site = 'ficbook.group'
    input = InputLivelibBook
    output = Output
    item_wf = FicbookGroupItem

    concurrency=3
    execution_timeout_sec=1800

    start_urls = [
        'https://ficbook.group/fanfiction',
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
            'new-nav-links': 0,
        }

        async def extract_links():
            pages_locator = page.locator('.pagination:first-of-type a')
            for page_locator in await pages_locator.all():
                if await cls.crawl(
                    urljoin(page.url, await page_locator.get_attribute('href')),
                    input.task_id,
                ):
                    data['new-nav-links'] += 1

            items_links = await page.query_selector_all('.card-item__name a')
            for i in items_links:
                item_href = await i.get_attribute('href')
                item_url = urljoin(page.url, item_href)
                if await FicbookGroupItem.crawl(item_url, input.task_id):
                    data['new-items-links'] += 1

        await extract_links()

        if page.url in cls.start_urls:
            checkboxes_locator = page.locator('.filter-field__checkbox')
            for chbox in await checkboxes_locator.all():
                prew_checked_locator = page.locator('.filter-field__checkbox:has([checked])')
                if await prew_checked_locator.count() > 0:
                    await prew_checked_locator.click()

                filter_groups_locator = page.locator('.filter-group:not(.active)')
                while await filter_groups_locator.count() > 0:
                    await filter_groups_locator.first.click()

                await chbox.click()
                await page.click('.filter-button')
                await page.wait_for_load_state('networkidle')
                await extract_links()

        return Output(
            result='done',
            data=data,
        )


if __name__ == '__main__':
    FicbookGroupListing.run_sync()

    # FicbookGroupListing.debug_sync(FicbookGroupListing.start_urls[0])
    FicbookGroupItem.debug_sync('https://ficbook.group/readfic/128789')
    # FicbookGroupItem.debug_sync('https://ficbook.group/readfic/127801')
