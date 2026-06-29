import re
from urllib.parse import urljoin

from furl import furl
from playwright.async_api import Page

from db import DbSamizdatPrisma
from interfaces import InputLivelibBook, Output
from utils import save_cover
from workflow_base import BaseLivelibWorkflow


class BookriverItem(BaseLivelibWorkflow):
    name = 'livelib-bookriver-item'
    event = 'livelib:bookriver-item'
    site = 'bookriver.ru'

    input = InputLivelibBook
    output = Output

    concurrency = 25

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        resp = await page.goto(input.url, wait_until='domcontentloaded')

        if resp.status == 404 or '/book/' not in page.url:
            async with DbSamizdatPrisma() as db:
                await db.mark_book_deleted(page.url, cls.site)
            return Output(result='error', data={'status': resp.status, 'error': 'invalid_url_or_404'})

        await page.wait_for_selector('footer[class*="SCFooter"]')

        async with DbSamizdatPrisma() as db:
            book = {'url': page.url, 'source': cls.site}
            metrics = {'bookUrl': page.url}

            # Title
            # JS: div[class*="SCBookContent"] h1
            title_locator = page.locator('div[class*="SCBookContent"] h1')
            if await title_locator.count() > 0:
                book['title'] = (await title_locator.text_content()).strip()

            if not await db.check_book_exist(page.url):
                await db.create_book(book)

            # Authors
            # JS: div[class*="SCBookContent"] a[class*="SCCoAuthorsLink"]
            authors_locator = page.locator('div[class*="SCBookContent"] a[class*="SCCoAuthorsLink"]')
            if await authors_locator.count() > 0:
                book['author'] = ', '.join([
                    (await a.text_content()).strip()
                    for a in await authors_locator.all()
                ])

                book['authors_data'] = []
                for a in await authors_locator.all():
                    href = await a.get_attribute('href')
                    book['authors_data'].append({
                        'name': (await a.text_content()).strip(),
                        'url': urljoin(page.url, href),
                    })

            # Annotation
            # JS: div[class*="SCBookContent"] span[itemprop="description"]
            annotation_locator = page.locator('div[class*="SCBookContent"] span[itemprop="description"]')
            if await annotation_locator.count() > 0:
                book['annotation'] = (await annotation_locator.first.text_content()).strip()

            # Cover
            # JS: div[class*="SCBookContent"] img[itemprop="contentUrl"]
            if not await db.check_book_have_cover(page.url):
                cover_locator = page.locator('div[class*="SCBookContent"] img[itemprop="contentUrl"]')
                if await cover_locator.count() > 0:
                    if cover_url := await cover_locator.get_attribute('src'):
                        if img_name := await save_cover(page, cover_url):
                            book['coverImage'] = img_name

            # Category (genres)
            # JS: div[class*="SCBookContent"] span[itemprop="genre"]
            category_locator = page.locator('div[class*="SCBookContent"] span[itemprop="genre"]')
            if await category_locator.count() > 0:
                book['category'] = [
                    (await x.text_content()).strip()
                    for x in await category_locator.all()
                ]

            # Series
            # JS: div[class*="SCBookContent"] a[class*="SCCycleName"]
            # NOTE: SCCycleName не найден в HTML — серия отсутствует у большинства книг на странице.
            # Это поле присутствует в JS, но не присутствует в реальном HTML.
            series_locator = page.locator('div[class*="SCBookContent"] a[class*="SCCycleName"]')
            if await series_locator.count() > 0:
                book['series'] = [
                    (await x.text_content()).replace('серии', '').strip()
                    for x in await series_locator.all()
                ]

            # Tags
            # JS: div[class*="SCBookContent"] a[class*="BookTag"] span
            # ACTUAL HTML: span[itemprop="keywords"] (внутри a[class*="SCLink"])
            tags_locator = page.locator('div[class*="SCBookContent"] span[itemprop="keywords"]')
            if await tags_locator.count() > 0:
                book['tags'] = [
                    (await x.text_content()).strip()
                    for x in await tags_locator.all()
                ]

            # Artwork type
            # JS: span[class*="SCCycleText"] — только текстовые ноды (не дочерние элементы)
            # ACTUAL HTML: <span class="BookCycle__SCCycleText-...">Роман</span> — прямой текст
            artwork_locator = page.locator('span[class*="SCCycleText"]')
            if await artwork_locator.count() > 0:
                artwork_text = (await artwork_locator.text_content()).strip()
                if artwork_text:
                    book['artwork_type'] = artwork_text.split()[0]

            # Age rating
            # JS: div[class*="SCBookMainGroup"] div[class^="AgeRating"]
            # ACTUAL HTML: div[class*="AgeRating__SCAgeRating"] внутри SCBookContent
            age_locator = page.locator('div[class*="SCBookContent"] div[class*="AgeRating__SCAgeRating"]')
            if await age_locator.count() > 0:
                age_match = re.search(r'\d{1,2}', await age_locator.text_content())
                if age_match and age_match.group(0) != '0':
                    book['age_rating'] = age_match.group(0)

            # Views
            # JS: li:has(i[class*="EyeIcon"]) span
            # ACTUAL HTML: li > i[class*="EyeIcon"] + span[class*="SCValue"]
            views_locator = page.locator('div[class*="SCBookContent"] li:has(i[class*="EyeIcon"]) span[class*="SCValue"]')
            if await views_locator.count() > 0:
                views_match = re.search(r'[\d.KM]+', await views_locator.text_content())
                if views_match:
                    metrics['views'] = views_match.group(0)

            # Added to library
            # JS: li:has(i[class*="LibraryIcon"]) span
            adds_locator = page.locator('div[class*="SCBookContent"] li:has(i[class*="LibraryIcon"]) span[class*="SCValue"]')
            if await adds_locator.count() > 0:
                adds_match = re.search(r'[\d.KM]+', await adds_locator.text_content())
                if adds_match:
                    metrics['added_to_lib'] = adds_match.group(0)

            # Comments
            # JS: li:has(i[class*="CommentsIcon"]) span
            comments_locator = page.locator('div[class*="SCBookContent"] li:has(i[class*="CommentsIcon"]) span[class*="SCValue"]')
            if await comments_locator.count() > 0:
                comments_match = re.search(r'[\d.KM]+', await comments_locator.text_content())
                if comments_match:
                    metrics['comments'] = comments_match.group(0)

            # Characters count
            # JS: div[class*="SCBookContent"] div[class*="SCPages"]
            # ACTUAL HTML: div[class*="SCPages"] содержит текст типа "219.4K зн."
            chars_locator = page.locator('div[class*="SCBookContent"] div[class*="SCPages"]')
            if await chars_locator.count() > 0:
                chars_match = re.search(r'[\d.KM]+', await chars_locator.first.text_content())
                if chars_match:
                    metrics['characters_count'] = chars_match.group(0)

            # Status writing
            # JS: div[data-type="writing"] / div[data-type="complete"]
            if await page.locator('div[class*="SCBookContent"] div[data-type="writing"]').count() > 0:
                metrics['status_writing'] = 'PROCESS'
            elif await page.locator('div[class*="SCBookContent"] div[data-type="complete"]').count() > 0:
                metrics['status_writing'] = 'FINISH'

            # Price
            # JS: button:contains("К оплате") span[itemprop="price"]
            # ACTUAL HTML: button > span[class*="SCMainRow"] > span[itemprop="price"]  (внутри кнопки с текстом "К оплате")
            price_locator = page.locator(
                'div[class*="SCBookContent"] button:has-text("К оплате") span[itemprop="price"]'
            )
            if await price_locator.count() > 0:
                price_match = re.search(r'[\d.]+', await price_locator.text_content())
                if price_match:
                    metrics['price'] = price_match.group(0)

            # Price audio
            # JS: button:contains("аудио") span[itemprop="price"]
            # NOTE: В сохранённом HTML аудио-кнопки нет. Поле сохраняем по JS-логике.
            price_audio_locator = page.locator(
                'div[class*="SCBookContent"] button:has-text("аудио") span[itemprop="price"]'
            )
            if await price_audio_locator.count() > 0:
                price_audio_match = re.search(r'[\d.]+', await price_audio_locator.text_content())
                if price_audio_match:
                    metrics['price_audio'] = price_audio_match.group(0)

            # In subscribe (абонемент)
            # JS: div[class*="AvailableByAbonnementBookStatus"]
            if await page.locator('div[class*="SCBookContent"] div[class*="AvailableByAbonnementBookStatus"]').count() > 0:
                metrics['in_subscribe'] = True

            # Audio URL
            # JS: div[class*="SCListenBookButton"]
            # NOTE: В сохранённом HTML SCListenBookButton отсутствует (книга без аудио).
            if await page.locator('div[class*="SCBookContent"] div[class*="SCListenBookButton"]').count() > 0:
                book['url_audio'] = page.url

            await db.update_book(book)
            await db.create_metrics(metrics)

            return Output(result='done', data={'book': book, 'metrics': metrics})


class BookriverListing(BaseLivelibWorkflow):
    name = 'livelib-bookriver-listing'
    event = 'livelib:bookriver-listing'
    site = 'bookriver.ru'

    input = InputLivelibBook
    output = Output
    item_wf = BookriverItem

    concurrency = 4
    execution_timeout_sec = 3_600
    backoff_max_seconds = 30
    backoff_factor = 2

    start_urls = [
        'https://bookriver.ru/genre?page=1&perPage=96&sortingType=bestseller',
    ]

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        stats = {'new-page-links': 0, 'new-items-links': 0}

        resp = await page.goto(input.url, wait_until='domcontentloaded')
        await page.wait_for_selector('footer[class*="SCFooter"]')

        # Pagination
        # JS: ul.ant-pagination a — извлекает числа страниц и строит URL
        # ACTUAL HTML: li[class*="ant-pagination-item"][title=N] — числа страниц берём из title
        url_data = furl(input.url)
        pagination_items = await page.locator('ul.ant-pagination li[class*="ant-pagination-item"]').all()
        for item in pagination_items:
            title = await item.get_attribute('title')
            if title and re.match(r'^\d+$', title):
                url_data.args['page'] = title
                if await cls.crawl(url_data.url, input.task_id):
                    stats['new-page-links'] += 1

        # Book links
        # JS: a[class*="SCBookTitle"] — в реальном HTML этот класс отсутствует
        # ACTUAL HTML: a[class*="SCName"] — ссылка на книгу в карточке листинга
        book_links = await page.locator('a[class*="SCName"]').all()
        for link in book_links:
            href = await link.get_attribute('href')
            if href:
                book_url = urljoin(page.url, href)
                if await BookriverItem.crawl(book_url, input.task_id):
                    stats['new-items-links'] += 1

        return Output(result='done', data=stats)


if __name__ == '__main__':
    BookriverListing.run_sync()
    # BookriverListing.debug_sync(BookriverListing.start_urls[0])
    # BookriverItem.debug_sync('https://bookriver.ru/book/tatyana-solodkova-yablochnyi-sneg-2')
