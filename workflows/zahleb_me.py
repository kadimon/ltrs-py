import re
from urllib.parse import urljoin

from playwright.async_api import Page
import dateparser
from furl import furl

from workflow_base import BaseLivelibWorkflow
from interfaces import InputLivelibBook, Output, WorkerLabels
from db import DbSamizdatPrisma
from utils import save_cover, sitemap


class ZahlebMeItem(BaseLivelibWorkflow):
    name = 'livelib-zahleb-me-item'
    event = 'livelib:zahleb-me-item'
    site='zahleb.me'
    input = InputLivelibBook
    output = Output

    start_urls = [url for url in sitemap('https://zahleb.me') if '/story/' in url]

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

        await page.wait_for_selector('h2.ant-typography')

        age_checkbox_locator = page.locator('.ant-modal-body .ant-checkbox')
        if await age_checkbox_locator.count() > 0:
            await age_checkbox_locator.click()
            await page.click('.ant-modal-body .ant-btn')

        async with DbSamizdatPrisma() as db:
            book = {
                'url': page.url,
                'source': cls.site,
            };

            metrics = {
                'bookUrl': page.url,
            };

            if not await db.check_book_exist(page.url):
                book['title'] = await page.text_content('h2.ant-typography')
                await db.create_book(book)

            authors_locator = page.locator('a[class^="StoryInfoAuthor_author_name"]')
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

            genres_locator = page.locator('[class^="StoryInfo_container"] a[href*="/category/"]')
            if await genres_locator.count() > 0:
                book['category'] = [await g.text_content() for g in await genres_locator.all()]

            tags_locator = page.locator('[class^="StoryInfo_container"] a[href*="/tag/"]')
            if await tags_locator.count() > 0:
                book['tags'] = [await t.text_content() for t in await tags_locator.all()]

            dates_locator = page.locator('.ant-list-item-meta-description .ant-space-item:nth-of-type(2)')
            if await dates_locator.count() > 0:
                date_release_str = await dates_locator.first.text_content()
                book['date_release'] = dateparser.parse(date_release_str, languages=['ru'])

                date_updated_str = await dates_locator.last.text_content()
                metrics['content_update_date'] = dateparser.parse(date_updated_str, languages=['ru'])

            if annotation := await page.inner_text('[class*="StoryInfo_description"]'):
                book['annotation'] = annotation

            if not await db.check_book_have_cover(page.url):
                if img_src := await page.get_attribute('img[class^="StoryInfoCoverImage_storyCoverImageMain"]', 'src', timeout=2_000):
                    if img_name := await save_cover(page, img_src, timeout=10_000):
                        book['coverImage'] = img_name

            views_locator = page.locator('[class^="StoryCounter_storyCounter"]').filter(
                has=page.locator('use[*|href="#icon-view"]')
            ).locator('.ant-typography')
            if await views_locator.count() > 0:
                metrics['views'] = await views_locator.text_content()

            comments_locator = page.locator('[class^="StoryCounter_storyCounter"]').filter(
                has=page.locator('use[*|href="#icon-comment"]')
            ).locator('.ant-typography')
            if await comments_locator.count() > 0:
                metrics['comments'] = await comments_locator.text_content()

            likes_locator = page.locator('[class^="StoryCounter_storyCounter"]').filter(
                has=page.locator('use[*|href="#antd-heart-outlined"]')
            ).locator('.ant-typography')
            if await likes_locator.count() > 0:
                metrics['likes'] = await likes_locator.first.text_content()

            chapters_count_locator = page.locator('[class^="StoryCounter_storyCounter"]').filter(
                has=page.locator('use[*|href="#icon-list"]')
            ).locator('.ant-typography')
            if await chapters_count_locator.count() > 0:
                capters_count_str = await chapters_count_locator.text_content()
                metrics['chapters_count'] = re.search(r'\d+', capters_count_str)[0]

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
    ZahlebMeItem.run_sync()

    ZahlebMeItem.debug_sync('https://zahleb.me/story/vashe-serdtse-vzlomano-qZJBA7XcMs')
