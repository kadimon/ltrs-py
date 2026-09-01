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

        # OK: footer[class*="SCFooter"] -> footer.Footer__SCFooter-sc-1g18099-0
        await page.wait_for_selector('footer[class*="SCFooter"]')

        async with DbSamizdatPrisma() as db:
            book = {'url': page.url, 'source': cls.site}
            metrics = {'bookUrl': page.url}

            # Title
            # OK: div.styled__SCBookContent-sc-1jqa73l-4 h1.BookMainInfo__SCName-sc-q7n8jn-2
            title_locator = page.locator('div[class*="SCBookContent"] h1')
            if await title_locator.count() > 0:
                book['title'] = (await title_locator.text_content()).strip()

            if not await db.check_book_exist(page.url):
                await db.create_book(book)

            # Authors
            # OK: a.BookMainInfo__SCCoAuthorsLink-sc-q7n8jn-0 (href абсолютный)
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
            # OK: span.styled__SCBookInfoText-sc-1jqa73l-10[itemprop=description]
            # NOTE: таких span может быть 2 — аннотация и «Примечание автора».
            # Аннотация всегда идёт первой в DOM, поэтому .first обязателен.
            annotation_locator = page.locator('div[class*="SCBookContent"] span[itemprop="description"]')
            if await annotation_locator.count() > 0:
                book['annotation'] = (await annotation_locator.first.text_content()).strip()

            # Cover
            # OK: img.BookPageCover__SCImage-sc-g4svsn-3[itemprop=contentUrl]
            # NOTE: в сохранённых снапшотах src="data:," (картинки вырезаны при сохранении),
            # на живой странице src — абсолютный URL storage.bookriver.ru.
            if not await db.check_book_have_cover(page.url):
                cover_locator = page.locator('div[class*="SCBookContent"] img[itemprop="contentUrl"]')
                if await cover_locator.count() > 0:
                    if cover_url := await cover_locator.get_attribute('src'):
                        full_cover_url = urljoin(page.url, cover_url)
                        if img_name := await save_cover(page, full_cover_url):
                            book['coverImage'] = img_name

            # Category (genres)
            # OK: a.BookSubInfoLinks__SCGenresLink-sc-1ar8m5u-3 > span[itemprop=genre]
            category_locator = page.locator('div[class*="SCBookContent"] span[itemprop="genre"]')
            if await category_locator.count() > 0:
                book['category'] = [
                    (await x.text_content()).strip()
                    for x in await category_locator.all()
                ]

            # Series
            # OK: a.BookCycle__SCCycleName-sc-1nm4kn6-0 — «серии Яблочный снег» -> «Яблочный снег»
            # (в прошлой ревизии класс считался отсутствующим — сейчас он есть на обеих страницах)
            series_locator = page.locator('div[class*="SCBookContent"] a[class*="SCCycleName"]')
            if await series_locator.count() > 0:
                book['series'] = [
                    (await x.text_content()).replace('серии', '').strip()
                    for x in await series_locator.all()
                ]

            # Tags
            # OK: a.BookTags__SCLink-sc-x3jf8m-1 > span[itemprop=keywords]
            tags_locator = page.locator('div[class*="SCBookContent"] span[itemprop="keywords"]')
            if await tags_locator.count() > 0:
                book['tags'] = [
                    (await x.text_content()).strip()
                    for x in await tags_locator.all()
                ]

            # Artwork type
            # OK: span.BookCycle__SCCycleText-sc-1nm4kn6-1
            # text_content() -> «Роман из серии Яблочный снег #2», split()[0] -> «Роман»
            # NOTE: обе страницы — книги в цикле. Поведение для книги вне цикла не проверено.
            artwork_locator = page.locator('span[class*="SCCycleText"]')
            if await artwork_locator.count() > 0:
                artwork_text = (await artwork_locator.text_content()).strip()
                if artwork_text:
                    book['artwork_type'] = artwork_text.split()[0]

            # Age rating
            # OK: div.AgeRating__SCAgeRating-sc-s301qb-0 внутри BookCardBadgers на обложке книги.
            # Скоуп SCBookContent критичен: такие же бейджи есть в блоке рекомендаций и в футере.
            age_locator = page.locator('div[class*="SCBookContent"] div[class*="AgeRating__SCAgeRating"]')
            if await age_locator.count() > 0:
                age_match = re.search(r'\d{1,2}', await age_locator.text_content())
                if age_match and age_match.group(0) != '0':
                    book['age_rating'] = age_match.group(0)

            # Views
            # OK: li > i.bookriver-icon-EyeIcon + span.BookPublicStatistic__SCValue-sc-1akhwql-4
            views_locator = page.locator('div[class*="SCBookContent"] li:has(i[class*="EyeIcon"]) span[class*="SCValue"]')
            if await views_locator.count() > 0:
                views_match = re.search(r'[\d.KM]+', await views_locator.text_content())
                if views_match:
                    metrics['views'] = views_match.group(0)

            # Added to library
            # OK: li > i.bookriver-icon-LibraryIcon + span[class*="SCValue"]
            adds_locator = page.locator('div[class*="SCBookContent"] li:has(i[class*="LibraryIcon"]) span[class*="SCValue"]')
            if await adds_locator.count() > 0:
                adds_match = re.search(r'[\d.KM]+', await adds_locator.text_content())
                if adds_match:
                    metrics['added_to_lib'] = adds_match.group(0)

            # Comments
            # OK: li > i.bookriver-icon-CommentsIcon + span[class*="SCValue"]
            # NOTE: <li> рендерится только когда комментарии есть (на «Яблочном снеге» его нет).
            comments_locator = page.locator('div[class*="SCBookContent"] li:has(i[class*="CommentsIcon"]) span[class*="SCValue"]')
            if await comments_locator.count() > 0:
                comments_match = re.search(r'[\d.KM]+', await comments_locator.text_content())
                if comments_match:
                    metrics['comments'] = comments_match.group(0)

            # Characters count
            # OK: div.BookStatusInfo__SCPages-sc-gzwrwm-1 — «627.2K зн.»
            # NOTE: [class*="SCPages"] попадает и в SCPagesWrap, и во вложенный SCPages;
            # .first — это обёртка, текст у неё тот же.
            chars_locator = page.locator('div[class*="SCBookContent"] div[class*="SCPages"]')
            if await chars_locator.count() > 0:
                chars_match = re.search(r'[\d.KM]+', await chars_locator.first.text_content())
                if chars_match:
                    metrics['characters_count'] = chars_match.group(0)

            # Status writing
            # OK: div.BookStatus__SCStatus-sc-1zhlas-0[data-type=complete] — «Полностью»
            # data-type=writing («в процессе») подтверждён на странице листинга, тот же компонент.
            if await page.locator('div[class*="SCBookContent"] div[data-type="writing"]').count() > 0:
                metrics['status_writing'] = 'PROCESS'
            elif await page.locator('div[class*="SCBookContent"] div[data-type="complete"]').count() > 0:
                metrics['status_writing'] = 'FINISH'

            # Price
            # OK: button.BookButtonWithPrice__SCBuyBookButton-sc-7stq40-0 («К оплате»)
            #     > span.SCMainRow > span[itemprop=price]
            price_locator = page.locator(
                'div[class*="SCBookContent"] button:has-text("К оплате") span[itemprop="price"]'
            )
            if await price_locator.count() > 0:
                price_match = re.search(r'[\d.]+', await price_locator.text_content())
                if price_match:
                    metrics['price'] = price_match.group(0)

            # Price audio
            # NOT VERIFIED: аудио-кнопки нет ни на одной из предоставленных страниц.
            # Селектор оставлен как есть, по JS-логике.
            price_audio_locator = page.locator(
                'div[class*="SCBookContent"] button:has-text("аудио") span[itemprop="price"]'
            )
            if await price_audio_locator.count() > 0:
                price_audio_match = re.search(r'[\d.]+', await price_audio_locator.text_content())
                if price_audio_match:
                    metrics['price_audio'] = price_audio_match.group(0)

            # In subscribe (абонемент)
            # OK: div.AvailableByAbonnementBookStatus__SCStatus-sc-ayek3-0 — «Доступна по абонементу»
            if await page.locator('div[class*="SCBookContent"] div[class*="AvailableByAbonnementBookStatus"]').count() > 0:
                metrics['in_subscribe'] = True

            # Audio URL
            # NOT VERIFIED: SCListenBookButton отсутствует на обеих страницах (книги без аудио).
            # Селектор оставлен как есть, по JS-логике.
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

        await page.goto(input.url, wait_until='domcontentloaded')
        await page.wait_for_selector('footer[class*="SCFooter"]')

        # Pagination (только с первой страницы листинга, без проверки на дубли)
        # OK: ul.ant-pagination > li.ant-pagination-item[title=N], активная — li.ant-pagination-item-active.
        # antd всегда рендерит последнюю страницу отдельным item'ом (title=243), поэтому
        # max(title) = число страниц — аналог data-last у desu.
        # Ссылок в <a> нет (SPA), URL собираем через furl.
        pagination_locator = page.locator('ul.ant-pagination').first
        if await pagination_locator.count() > 0:
            active_locator = pagination_locator.locator('li[class*="ant-pagination-item-active"]').first
            active_page = None
            if await active_locator.count() > 0:
                active_page = await active_locator.get_attribute('title')

            if active_page == '1':
                page_numbers = []
                items_locator = pagination_locator.locator('li[class*="ant-pagination-item"]')
                for item in await items_locator.all():
                    title = await item.get_attribute('title')
                    if title and re.match(r'^\d+$', title):
                        page_numbers.append(int(title))

                if page_numbers:
                    url_data = furl(input.url)
                    page_urls = []
                    for n in range(2, max(page_numbers) + 1):
                        url_data.args['page'] = str(n)
                        page_urls.append(url_data.url)

                    if page_urls:
                        crawled = await cls.crawl_bulk(page_urls, input.task_id, dont_dedupe=True)
                        stats['new-page-links'] = len(crawled)

        # Books (bulk после проверки на дубли)
        # OK: a.BookListCard__SCName-sc-1vn2gl5-2 — 96 ссылок на perPage=96, все ведут на /book/.
        # (a[class*="SCBookTitle"] из JS в реальном HTML отсутствует.)
        book_urls = []
        book_links_locator = page.locator('a[class*="SCName"]')
        for link in await book_links_locator.all():
            href = await link.get_attribute('href')
            if href:
                book_urls.append(urljoin(page.url, href))

        if book_urls:
            crawled = await BookriverItem.crawl_bulk(book_urls, input.task_id)
            stats['new-items-links'] = len(crawled)

        return Output(result='done', data=stats)


if __name__ == '__main__':
    BookriverListing.run_sync()
    # BookriverListing.debug_sync(BookriverListing.start_urls[0])
    # BookriverItem.debug_sync('https://bookriver.ru/book/tatyana-solodkova-yablochnyi-sneg-2')
