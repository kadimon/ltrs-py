import re
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import dateparser
from playwright.async_api import Page

from db import DbSamizdatPrisma
from interfaces import InputLivelibBook, Output
from utils import save_cover
from workflow_base import BaseLivelibWorkflow


def build_page_url(url: str, number: int) -> str:
    """Подставляет ?page=N в URL листинга, сохраняя остальные query-параметры."""
    parsed = urlparse(url)
    query = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if k != 'page']
    query.append(('page', str(number)))
    return urlunparse(parsed._replace(query=urlencode(query)))


class LitgorodItem(BaseLivelibWorkflow):
    name = 'livelib-litgorod-item'
    event = 'livelib:litgorod-item'
    site = 'litgorod.ru'

    input = InputLivelibBook
    output = Output

    concurrency = 10

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        resp = await page.goto(input.url, wait_until='domcontentloaded')

        # Проверка URL и статуса (JS: if (response.status() == 404 || !page.url().includes("/books/")))
        if resp.status == 404 or '/books/' not in page.url:
            async with DbSamizdatPrisma() as db:
                await db.mark_book_deleted(page.url, cls.site)
            return Output(result='error', data={'status': resp.status, 'error': 'invalid_url_or_404'})

        await page.wait_for_selector("footer div.b-footer")

        async with DbSamizdatPrisma() as db:
            book = {'url': page.url, 'source': cls.site}
            metrics = {'bookUrl': page.url}

            # Title
            title_locator = page.locator("div.b-book_item__content h1")
            book['title'] = await title_locator.text_content() if await title_locator.count() > 0 else ""

            if not await db.check_book_exist(page.url):
                await db.create_book(book)

            # --- Сбор основной информации ---

            # Authors
            authors_locator = page.locator("div.b-book_item__content h2.h3 a")
            if await authors_locator.count() > 0:
                book['author'] = ', '.join([await a.text_content() for a in await authors_locator.all()]).strip()

                book['authors_data'] = []
                for a in await authors_locator.all():
                    href = await a.get_attribute('href')
                    text = await a.text_content()
                    absolute_url = urljoin(page.url, href)

                    book['authors_data'].append({
                        'name': text.strip(),
                        'url': absolute_url
                    })

            # Annotation
            annotation_locator = page.locator('div.b-tab p[itemprop="description"]')
            if await annotation_locator.count() > 0:
                book['annotation'] = (await annotation_locator.text_content()).strip()

            # Cover
            if not await db.check_book_have_cover(page.url):
                cover_locator = page.locator("div.b-book_item img._cover")
                if await cover_locator.count() > 0:
                    if img_src := await cover_locator.get_attribute('src'):
                        full_img_src = urljoin(page.url, img_src)
                        if img_name := await save_cover(page, full_img_src):
                            book['coverImage'] = img_name

            # Series
            series_locator = page.locator("div.b-book_item__content div.b-book_item__cycle a")
            if await series_locator.count() > 0:
                book['series'] = [(await x.text_content()).strip() for x in await series_locator.all()]

            # Tags
            tags_locator = page.locator("div.b-book_item__content div.b-book_item__includes a")
            if await tags_locator.count() > 0:
                book['tags'] = [(await x.text_content()).replace("#", "").strip() for x in await tags_locator.all()]

            # Age Rating
            age_rating_locator = page.locator("div.b-book_adult_old")
            if await age_rating_locator.count() > 0:
                if age_match := re.search(r'\d{1,2}', await age_rating_locator.text_content()):
                    book['age_rating'] = age_match.group(0)

            # Release Date
            release_date_locator = page.locator("div.b-book_item__content div._date")
            if await release_date_locator.count() > 0:
                if release_match := re.search(r'\d{2}\.\d{2}.\d{4}', await release_date_locator.first.text_content()):
                    book['date_release'] = dateparser.parse(release_match.group(0), date_formats=['%d.%m.%Y'])

            # --- Metrics ---

            # Views
            views_locator = page.locator(
                'div.b-book_item__content div.b-book_counters__item:has(i[class*="counters_eye"])'
            )
            if await views_locator.count() > 0:
                if views_match := re.search(r'\d+', await views_locator.get_attribute('data-tooltip')):
                    metrics['views'] = views_match.group(0)

            # Likes
            likes_locator = page.locator("div.b-book_item__content div.b-heart")
            if await likes_locator.count() > 0:
                metrics['likes'] = await likes_locator.first.text_content()

            # Added to lib
            adds_locator = page.locator(
                'div.b-book_item__content div.b-book_counters__item:has(i[class*="counters_book"])'
            )
            if await adds_locator.count() > 0:
                if adds_match := re.search(r'\d+', await adds_locator.get_attribute('data-tooltip')):
                    metrics['added_to_lib'] = adds_match.group(0)

            # Comments
            comments_locator = page.locator(
                'div.b-book_item__content div.b-book_counters__item:has(i[class*="counters_commentary"])'
            )
            if await comments_locator.count() > 0:
                if comments_match := re.search(r'\d+', await comments_locator.get_attribute('data-tooltip')):
                    metrics['comments'] = comments_match.group(0)

            # Characters count
            characters_count_locator = page.locator(
                'div.b-book_item__content div.b-book_counters__item:has(i[class*="counters_a"])'
            )
            if await characters_count_locator.count() > 0:
                if chars_match := re.search(r'\d+', await characters_count_locator.get_attribute('data-tooltip')):
                    metrics['characters_count'] = chars_match.group(0)

            # Site Ratings
            # Список вложенный: у родительских <li> (жанр верхнего уровня) нет прямого span._cnt,
            # позиция в рейтинге есть только у дочерних <li> (поджанр).
            ratings_locator = page.locator("div.b-book_item__content div.b-book_rating li")
            if await ratings_locator.count() > 0:
                metrics['site_ratings'] = {}
                for li in await ratings_locator.all():
                    rating_locator = li.locator("> span._cnt")
                    category_locator = li.locator("> span._text a")
                    if await rating_locator.count() == 0 or await category_locator.count() == 0:
                        continue

                    rating_text = await rating_locator.first.text_content()
                    category_text = await category_locator.first.text_content()

                    if rating_match := re.search(r'\d+', rating_text or ''):
                        if category_text:
                            metrics['site_ratings'][category_text.strip()] = rating_match.group(0)

            # Status Writing
            if await page.locator("div.b-book_item__content div.b-book_item__status-1").count() > 0:
                metrics['status_writing'] = "PROCESS"
            elif await page.locator("div.b-book_item__content div.b-book_item__status-2").count() > 0:
                metrics['status_writing'] = "FINISH"

            # Price
            price_text_locator = page.locator("div.b-book_item__container div.buy-button span._text")
            if await price_text_locator.count() > 0:
                price_el = price_text_locator.first
                direct_text = await price_el.evaluate(
                    "el => Array.from(el.childNodes).filter(n => n.nodeType === 3).map(n => n.textContent).join('')"
                )

                if price_match := re.search(r'\d+', direct_text or ''):
                    metrics['price'] = price_match.group(0)

                    price_old_locator = page.locator(
                        "div.b-book_item__container div.buy-button span.text-danger"
                    )
                    if await price_old_locator.count() > 0:
                        if price_old_match := re.search(r'[\d\,]+', await price_old_locator.text_content()):
                            metrics['price_old'] = price_old_match.group(0)
                            metrics['price_discount'] = price_match.group(0)

            # In subscribe
            buy_button_locator = page.locator("div.b-book_item__container div.buy-button")
            if await buy_button_locator.count() > 0:
                if "Подписка" in (await buy_button_locator.text_content() or ''):
                    metrics['in_subscribe'] = True

            await db.update_book(book)
            await db.create_metrics(metrics)

            return Output(result='done', data={'book': book, 'metrics': metrics})


class LitgorodListing(BaseLivelibWorkflow):
    name = 'livelib-litgorod-listing'
    event = 'livelib:litgorod-listing'
    site = 'litgorod.ru'

    input = InputLivelibBook
    output = Output
    item_wf = LitgorodItem

    concurrency = 4
    execution_timeout_sec = 3_600
    backoff_max_seconds = 30
    backoff_factor = 2

    start_urls = ["https://litgorod.ru/genres"]

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        stats = {'new-page-links': 0, 'new-items-links': 0}

        await page.goto(input.url, wait_until='domcontentloaded')

        await page.wait_for_selector("footer div.b-footer")

        # Genres (bulk, без проверки на дубли — страница жанров одна на запуск)
        genre_urls = []
        genre_links_locator = page.locator("div.genres-map a")
        for link in await genre_links_locator.all():
            href = await link.get_attribute('href')
            if href:
                genre_url = urljoin(page.url, href)
                if 'genre_id=' in genre_url:
                    genre_urls.append(genre_url)

        if genre_urls:
            crawled = await cls.crawl_bulk(genre_urls, input.task_id, dont_dedupe=True)
            stats['new-page-links'] += len(crawled)

        # Pagination (только с первой страницы листинга, без проверки на дубли)
        pagination_locator = page.locator("div.b-paging__numbers").first
        if await pagination_locator.count() > 0:
            query = dict(parse_qsl(urlparse(page.url).query, keep_blank_values=True))
            if query.get('page', '1') == '1':
                last_page = 0
                for num_link in await pagination_locator.locator("a.b-paging__num").all():
                    if num_match := re.search(r'\d+', (await num_link.text_content()) or ''):
                        last_page = max(last_page, int(num_match.group(0)))

                if last_page > 1:
                    page_urls = [build_page_url(page.url, n) for n in range(2, last_page + 1)]
                    crawled = await cls.crawl_bulk(page_urls, input.task_id, dont_dedupe=True)
                    stats['new-page-links'] += len(crawled)

        # Books (bulk после проверки на дубли)
        book_urls = []
        book_links_locator = page.locator("div.b-book_item div.h2 > a")
        for link in await book_links_locator.all():
            href = await link.get_attribute('href')
            if href:
                book_url = urljoin(page.url, href)
                if '/books/view/' in book_url:
                    book_urls.append(book_url)

        if book_urls:
            crawled = await LitgorodItem.crawl_bulk(book_urls, input.task_id)
            stats['new-items-links'] = len(crawled)

        return Output(result='done', data=stats)


if __name__ == '__main__':
    LitgorodListing.run_sync()
    # Для отладки
    LitgorodListing.debug_sync(LitgorodListing.start_urls[0])
    LitgorodListing.debug_sync('https://litgorod.ru/books/search?genre_id=3')
    LitgorodItem.debug_sync('https://litgorod.ru/books/view/64875')
