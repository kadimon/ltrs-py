import re
from urllib.parse import urljoin

import dateparser
from playwright.async_api import Page

from db import DbSamizdatPrisma
from interfaces import InputLivelibBook, Output
from utils import save_cover
from workflow_base import BaseLivelibWorkflow


class ProdamanItem(BaseLivelibWorkflow):
    name = 'livelib-prodaman-item'
    event = 'livelib:prodaman-item'
    site = 'prodaman.ru'

    input = InputLivelibBook
    output = Output

    concurrency = 25

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        resp = await page.goto(input.url, wait_until='domcontentloaded')

        if resp.status == 404 or '/books/' not in page.url:
            async with DbSamizdatPrisma() as db:
                await db.mark_book_deleted(page.url, cls.site)
            return Output(result='error', data={'status': resp.status, 'error': 'invalid_url_or_404'})

        await page.wait_for_selector('div.ui-footer')

        async with DbSamizdatPrisma() as db:
            book = {'url': page.url, 'source': cls.site}
            metrics = {'bookUrl': page.url}

            # Title
            # JS: $('div[itemtype="http://schema.org/Product"] h1').text()
            title_locator = page.locator('div[itemtype="http://schema.org/Product"] h1')
            book['title'] = (await title_locator.text_content()).strip() if await title_locator.count() > 0 else ''

            if not await db.check_book_exist(page.url):
                await db.create_book(book)

            # Authors
            # JS: $('div[itemtype="..."] a[data-widget-feisovet-author]')
            authors_locator = page.locator('div[itemtype="http://schema.org/Product"] a[data-widget-feisovet-author]')
            if await authors_locator.count() > 0:
                book['author'] = ', '.join([
                    (await a.text_content()).strip()
                    for a in await authors_locator.all()
                ])
                book['authors_data'] = []
                for a in await authors_locator.all():
                    href = await a.get_attribute('href')
                    text = await a.text_content()
                    book['authors_data'].append({
                        'name': text.strip(),
                        'url': urljoin(page.url, href),
                    })

            # Annotation
            # JS: $('div[itemtype="..."] div.blog-text').text()
            annotation_locator = page.locator('div[itemtype="http://schema.org/Product"] div.blog-text')
            if await annotation_locator.count() > 0:
                book['annotation'] = (await annotation_locator.first.inner_text()).strip()

            # Cover
            # JS: $('div[itemtype="..."] img[itemprop="image"]').attr('src')
            if not await db.check_book_have_cover(page.url):
                cover_locator = page.locator('div[itemtype="http://schema.org/Product"] img[itemprop="image"]')
                if await cover_locator.count() > 0:
                    if img_src := await cover_locator.get_attribute('src'):
                        full_img_src = urljoin(page.url, img_src)
                        if img_name := await save_cover(page, full_img_src):
                            book['coverImage'] = img_name

            # Category
            # JS: $('div[itemtype="..."] p.blog-info:contains("Категории:") a')
            category_locator = page.locator('div[itemtype="http://schema.org/Product"] p.blog-info').filter(
                has_text=re.compile(r'Категории:')
            ).locator('a')
            if await category_locator.count() > 0:
                book['category'] = [(await x.text_content()).strip() for x in await category_locator.all()]

            # Series
            # JS: $('div[itemtype="..."] p.blog-info:contains("Из цикла:") a')
            series_locator = page.locator('div[itemtype="http://schema.org/Product"] p.blog-info').filter(
                has_text=re.compile(r'Из цикла:')
            ).locator('a')
            if await series_locator.count() > 0:
                book['series'] = [(await x.text_content()).strip() for x in await series_locator.all()]

            # Tags
            # JS: $('div[itemtype="..."] p.blog-info:contains("Хэштег:") a')
            tags_locator = page.locator('div[itemtype="http://schema.org/Product"] p.blog-info').filter(
                has_text=re.compile(r'Хэштег:')
            ).locator('a')
            if await tags_locator.count() > 0:
                book['tags'] = [
                    (await x.text_content()).replace('#', '').strip()
                    for x in await tags_locator.all()
                ]

            # Release Date & Content Update Date
            # JS: $('div[itemtype="..."] div:contains("Дата размещения:") strong').text().match(/\d{2}\.\d{2}.\d{4}/)
            # HTML: <div class=ui-block-a>Дата размещения: <strong>02.01.2026, 13:48</strong></div>
            # filter(has_text) поднимается до ближайшего div, содержащего нужный текст — берём inner_text()
            # самого div, а не спускаемся в strong (там нашлись бы и strong рейтинга и т.п.)
            dates_block = page.locator('div[itemtype="http://schema.org/Product"] div.ui-block-a').filter(
                has_text=re.compile(r'Дата размещения:')
            )
            if await dates_block.count() > 0:
                release_text = await dates_block.inner_text()
                if release_match := re.search(r'\d{2}\.\d{2}\.\d{4}', release_text):
                    book['date_release'] = dateparser.parse(release_match.group(0), date_formats=['%d.%m.%Y'])

            update_block = page.locator('div[itemtype="http://schema.org/Product"] div.ui-block-b').filter(
                has_text=re.compile(r'Дата обновления:')
            )
            if await update_block.count() > 0:
                update_text = await update_block.inner_text()
                if update_match := re.search(r'\d{2}\.\d{2}\.\d{4}', update_text):
                    metrics['content_update_date'] = dateparser.parse(update_match.group(0), date_formats=['%d.%m.%Y'])

            # Rating
            # JS: $('div[itemtype="..."] p.rating-title strong').text().match(/\d,\d{2}/)
            rating_locator = page.locator('div[itemtype="http://schema.org/Product"] p.rating-title strong').first
            if await rating_locator.count() > 0:
                if rating_match := re.search(r'\d,\d{2}', await rating_locator.text_content()):
                    metrics['rating'] = rating_match.group(0)

            # Shared blog-info text block for views/comments/added_to_lib/awards/pages/chars
            # JS: $('div[itemtype="..."] p.blog-info').text()  (called multiple times)
            blog_info_locator = page.locator('div[itemtype="http://schema.org/Product"] p.blog-info')
            blog_info_text = ''
            if await blog_info_locator.count() > 0:
                parts = [await el.text_content() for el in await blog_info_locator.all()]
                blog_info_text = ' '.join(parts)

            # Views
            # JS: .match(/(\d+)\s+просмотр/)
            if views_match := re.search(r'(\d+)\s+просмотр', blog_info_text):
                metrics['views'] = views_match.group(1)

            # Added to lib
            # JS: .match(/(\d+)\s+в\s+избранном/)
            if adds_match := re.search(r'(\d+)\s+в\s+избранном', blog_info_text):
                metrics['added_to_lib'] = adds_match.group(1)

            # Comments
            # JS: .match(/(\d+)\s+комментариев/)
            if comments_match := re.search(r'(\d+)\s+комментариев', blog_info_text):
                metrics['comments'] = comments_match.group(1)

            # Pages count
            # JS: .match(/(\d+)\s+стр/)
            if pages_match := re.search(r'(\d+)\s+стр', blog_info_text):
                metrics['pages_count'] = pages_match.group(1)

            # Characters count
            # JS: .match(/(\d+)\s+знаков/)
            if chars_match := re.search(r'(\d+)\s+знаков', blog_info_text):
                metrics['characters_count'] = chars_match.group(1)

            # Awards
            # JS: .match(/(\d+)\s+наград/) — сохраняет как { award: awards } если !== "0"
            if awards_match := re.search(r'(\d+)\s+наград', blog_info_text):
                awards = awards_match.group(1)
                if awards != '0':
                    metrics['awards'] = {'award': awards}

            # Status Writing
            # JS: span.inprocess-text / span.full-text / span.notfull-text
            if await page.locator('div[itemtype="http://schema.org/Product"] span.inprocess-text').count() > 0:
                metrics['status_writing'] = 'PROCESS'
            elif await page.locator('div[itemtype="http://schema.org/Product"] span.full-text').count() > 0:
                metrics['status_writing'] = 'FINISH'
            elif await page.locator('div[itemtype="http://schema.org/Product"] span.notfull-text').count() > 0:
                metrics['status_writing'] = 'STOP'

            # Price
            # JS: $('div[itemtype="..."] span[class$=-text] strong').text().match(/(\d+)\s+руб/)
            price_locator = page.locator('div[itemtype="http://schema.org/Product"] span[class$=-text] strong')
            if await price_locator.count() > 0:
                price_text = await price_locator.text_content()
                if price_match := re.search(r'(\d+)\s+руб', price_text):
                    metrics['price'] = price_match.group(1)

            # In Subscribe
            # JS: $('div[itemtype="..."] span[class$=-text]').text().includes("подписк")
            status_text_locator = page.locator('div[itemtype="http://schema.org/Product"] span[class$=-text]')
            if await status_text_locator.count() > 0:
                status_text = await status_text_locator.text_content()
                if 'подписк' in status_text:
                    metrics['in_subscribe'] = True

            await db.update_book(book)
            await db.create_metrics(metrics)

            return Output(result='done', data={'book': book, 'metrics': metrics})


class ProdamanListing(BaseLivelibWorkflow):
    name = 'livelib-prodaman-listing'
    event = 'livelib:prodaman-listing'
    site = 'prodaman.ru'

    input = InputLivelibBook
    output = Output
    item_wf = ProdamanItem

    concurrency = 4
    execution_timeout_sec = 3_600
    backoff_max_seconds = 30
    backoff_factor = 2

    start_urls = ['https://prodaman.ru/books/']

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        stats = {'new-page-links': 0, 'new-items-links': 0}

        resp = await page.goto(input.url, wait_until='domcontentloaded')
        await page.wait_for_selector('div.ui-footer')

        # Pagination
        # JS: selector: "div.pageList a"
        page_links = await page.locator('div.pageList a').all()
        for link in page_links:
            href = await link.get_attribute('href')
            if href:
                page_url = urljoin(page.url, href)
                if await cls.crawl(page_url, input.task_id):
                    stats['new-page-links'] += 1

        # Book links
        # JS: selector: "p.blog-title a", label: "book"
        book_links = await page.locator('p.blog-title a').all()
        for link in book_links:
            href = await link.get_attribute('href')
            if href:
                book_url = urljoin(page.url, href)
                if await ProdamanItem.crawl(book_url, input.task_id):
                    stats['new-items-links'] += 1

        return Output(result='done', data=stats)


if __name__ == '__main__':
    ProdamanListing.run_sync()
    # ProdamanListing.debug_sync(ProdamanListing.start_urls[0])
    # ProdamanItem.debug_sync('https://prodaman.ru/arinasemeonova/books/Zabud-mo-imya')
