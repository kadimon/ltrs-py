import re
from urllib.parse import urljoin

from playwright.async_api import Page
from datetime import datetime

from workflow_base import BaseLivelibWorkflow
from interfaces import InputLivelibBook, Output, WorkerLabels
from db import DbSamizdatPrisma
from utils import save_cover


class Create8RuListing(BaseLivelibWorkflow):
    name = 'livelib-create8-ru-listing'
    event = 'livelib:create8-ru-listing'
    site='create8.ru'
    input = InputLivelibBook
    output = Output

    concurrency=3
    execution_timeout_sec=300
    backoff_max_seconds=30
    backoff_factor=2

    start_urls = [
        'https://www.create8.ru/comics/',
        'https://www.create8.ru/fanfics/',
        'https://www.create8.ru/novels/',
    ]

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        resp = await page.goto(
            input.url,
            wait_until='domcontentloaded',
            referer='https://create8.ru',
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

        for row in await page.locator('.undefined').all():
            await row.scroll_into_view_if_needed()
            await page.wait_for_timeout(500)

        age_confirm_locator = page.locator('.swiper-slide [class^="confirm-age_blur"]')
        if await age_confirm_locator.count() > 0:
            await age_confirm_locator.first.click()
            await page.click('.react-responsive-modal-modal button[class*="primary"]')
            await page.wait_for_timeout(10_000)

        for i in await page.locator('.swiper-slide a[class*="card_card"]').all():
            item_href = await i.get_attribute('href')
            item_url = urljoin(page.url, item_href)
            if await Create8RuItem.crawl(item_url, input.task_id):
                data['new-items-links'] += 1

        show_more_locator = page.locator('[class^="header_wrapper"] a')
        for page_locator in await show_more_locator.all():
            if await cls.crawl(
                urljoin(page.url, await page_locator.get_attribute('href')),
                input.task_id,
            ):
                data['new-nav-links'] += 1

        return Output(
            result='done',
            data=data,
        )

class Create8RuItem(BaseLivelibWorkflow):
    name = 'livelib-create8-ru-item'
    event = 'livelib:create8-ru-item'
    site='create8.ru'
    input = InputLivelibBook
    output = Output

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        resp = await page.goto(
            input.url,
            wait_until='domcontentloaded',
            referer='https://create8.ru',
        )
        if not (200 <= resp.status < 400):
            return Output(
                result='error',
                data={'status': resp.status},
            )

        await page.wait_for_selector('h1')

        age_confirm_locator = page.locator('.react-responsive-modal-modal button[class*="primary"]')
        if await age_confirm_locator.count() > 0:
            await age_confirm_locator.click()

        async with DbSamizdatPrisma() as db:
            book = {
                'url': page.url,
                'source': cls.site,
            };

            metrics = {
                'bookUrl': page.url,
            };

            if not await db.check_book_exist(page.url):
                book['title'] = await page.locator('h1').text_content()
                await db.create_book(book)

            authors_locator = page.locator('[class*="view_main__header"] a[class*="author-block_avatar__link"]')
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

            artwork_type_locator = page.locator('a[class*="breadcrumbs_breadcrumbs__item"]:nth-of-type(2)')
            if await artwork_type_locator.count() > 0:
                book['artwork_type'] = await artwork_type_locator.first.inner_text()

            genres_locator = page.locator('a[class*="tag_tag"][href*="/genres/"]')
            if await genres_locator.count() > 0:
                book['category'] = [await g.text_content() for g in await genres_locator.all()]

            button_show_all_tags_locator = page.locator('div[class^="tags-list_button"] button')
            if await button_show_all_tags_locator.count() > 0:
                await button_show_all_tags_locator.click()
            tags_locator = page.locator('a[class*="tag_tag"][href*="/tags/"]')
            if await tags_locator.count() > 0:
                book['tags'] = [await t.text_content() for t in await tags_locator.all()]

            annotation_locator = page.locator('p[class^="main_description__info"]')
            if await annotation_locator.count() > 0:
                book['annotation'] = await annotation_locator.first.inner_text()

            age_rating_locator = page.locator('div[class^="view_main__image"] div[class^="card-badges"] img')
            if await age_rating_locator.count() > 0:
                 book['age_rating'] = re.search(r'\d+', await age_rating_locator.get_attribute('src'))[0]

            views_locator = page.locator('[class^="main_description__item"]> div > [class^="statistic_counter"]:nth-of-type(1)')
            if await views_locator.count() > 0:
                metrics['views'] = await views_locator.text_content()

            comments_locator = page.locator('[class^="main_description__item"]> div > [class^="statistic_counter"]:nth-of-type(2)')
            if await comments_locator.count() > 0:
                metrics['comments'] = await comments_locator.text_content()

            likes_locator = page.locator('[class^="work-actions-block_actions"] > button:nth-of-type(2)')
            if await likes_locator.count() > 0:
                metrics['likes'] = await likes_locator.text_content()

            chapters_patern = r'Эпизоды\s+\((\d+)\)'
            chapters_count_locator = page.locator('h2[class^="episodes-view_title"]').filter(
                has_text=re.compile(chapters_patern)
            )
            if await chapters_count_locator.count() > 0:
                chapters_regex = re.search(chapters_patern, await chapters_count_locator.text_content())
                metrics['chapters_count'] = chapters_regex.group(1)

            if not await db.check_book_have_cover(page.url):
                await page.click('div[class^="view_main__image"]')
                if img_src := await page.locator(
                    'div[class^="lightbox_lightbox"] img'
                ).first.get_attribute('src', timeout=2_000):
                    if img_name := await save_cover(page, img_src, timeout=10_000):
                        book['coverImage'] = img_name

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
    Create8RuListing.run_sync()

    Create8RuListing.debug_sync(Create8RuListing.start_urls[0])
    Create8RuItem.debug_sync('https://www.create8.ru/comics/kliuch-7161/?feedId=3')
    Create8RuItem.debug_sync('https://www.create8.ru/novels/sleeping-star-4113/?feedId=1')
