import re
from urllib.parse import quote, urljoin

from playwright.async_api import Page

from db import DbSamizdatPrisma
from interfaces import InputLivelibBook, Output
from utils import save_cover
from workflow_base import BaseLivelibWorkflow


class FeisovietItem(BaseLivelibWorkflow):
    name = 'samizdat-feisovet-item'
    event = 'samizdat:feisovet-item'
    site = 'feisovet.ru'

    input = InputLivelibBook
    output = Output

    concurrency = 25

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        resp = await page.goto(input.url, wait_until='domcontentloaded')

        print(page.url)

        # JS: response.status() == 404 || !page.url().includes("/%D0%BC%D0%B0%D0%B3%D0%B0%D0%B7%D0%B8%D0%BD/")
        if resp.status == 404 or '/%D0%BC%D0%B0%D0%B3%D0%B0%D0%B7%D0%B8%D0%BD/' not in page.url:
            async with DbSamizdatPrisma() as db:
                await db.mark_book_deleted(page.url, cls.site)
            return Output(result='error', data={'status': resp.status, 'error': 'invalid_url_or_404'})

        await page.wait_for_selector('div#footer')

        # JS: $('div.alert-danger, div.alert-warning').length > 0
        if await page.locator('div.alert-danger, div.alert-warning').count() > 0:
            async with DbSamizdatPrisma() as db:
                await db.mark_book_deleted(page.url, cls.site)
            return Output(result='error', data={'error': 'alert_danger_or_warning'})

        async with DbSamizdatPrisma() as db:
            book = {'url': page.url, 'source': cls.site}
            metrics = {'bookUrl': page.url}

            if not await db.check_book_exist(page.url):
                # JS: div[itemtype=...] > div.blog-preview > p, last text node, replace('»','').trim()
                # Берём весь текст <p>, вычитаем тексты всех <a> — остаётся только последний текстовый узел
                title_p = page.locator('div[itemtype="http://schema.org/Product"] > div.blog-preview > p')
                if await title_p.count() > 0:
                    full_text = await title_p.text_content()
                    for link in await title_p.locator('a').all():
                        full_text = full_text.replace(await link.text_content(), '', 1)
                    title = full_text.replace('»', '').strip()
                    if title:
                        book['title'] = title

                await db.create_book(book)

            # Author
            # JS: div[itemtype=...] > div.blog-preview > p > a:nth-of-type(3)
            author_links = page.locator(
                'div[itemtype="http://schema.org/Product"] > div.blog-preview > p > a'
            )
            if await author_links.count() >= 3:
                author_a = author_links.nth(2)
                author_name = (await author_a.text_content()).strip()
                author_href = await author_a.get_attribute('href')
                book['author'] = author_name
                book['authors_data'] = [
                    {
                        'name': author_name,
                        'url': urljoin(page.url, author_href),
                    }
                ]

            # Annotation
            # JS: div[itemtype=...] div.blog-text html -> html-to-text
            annotation_locator = page.locator(
                'div[itemtype="http://schema.org/Product"] div.blog-text'
            )
            if await annotation_locator.count() > 0:
                book['annotation'] = (await annotation_locator.inner_text()).strip()

            # Cover
            if not await db.check_book_have_cover(page.url):
                cover_locator = page.locator(
                    'div[itemtype="http://schema.org/Product"] img[itemprop="image"]'
                )
                if await cover_locator.count() > 0:
                    if cover_src := await cover_locator.get_attribute('src'):
                        full_cover_url = urljoin(page.url, cover_src)
                        if cover_name := await save_cover(page, full_cover_url):
                            book['coverImage'] = cover_name

            # Category
            # JS: p.blog-info:contains("Категории:") a > strong
            category_locator = page.locator(
                'div[itemtype="http://schema.org/Product"] p.blog-info'
            ).filter(has_text=re.compile(r'Категории:')).locator('a > strong')
            if await category_locator.count() > 0:
                book['category'] = [
                    (await el.text_content()).strip()
                    for el in await category_locator.all()
                ]

            # Tags
            # JS: p.blog-info:contains("Тэги:") a > strong
            tags_locator = page.locator(
                'div[itemtype="http://schema.org/Product"] p.blog-info'
            ).filter(has_text=re.compile(r'Тэги:')).locator('a > strong')
            if await tags_locator.count() > 0:
                book['tags'] = [
                    (await el.text_content()).strip()
                    for el in await tags_locator.all()
                ]

            # Artwork type
            # JS: tr:contains("Размер книги") > td:nth-of-type(2), split(/\.\s+/)[0]
            rows = page.locator('div[itemtype="http://schema.org/Product"] tr')
            for row in await rows.all():
                tds = row.locator('td')
                if await tds.count() >= 2:
                    if 'Размер книги' in (await tds.nth(0).text_content()):
                        second_td = (await tds.nth(1).text_content()).strip()
                        if artwork_type := re.split(r'\.\s+', second_td)[0]:
                            book['artwork_type'] = artwork_type
                        break

            # Metrics: Views
            # JS: p.blog-info text match /(\d+)\s+просмотров/
            blog_info_locator = page.locator(
                'div[itemtype="http://schema.org/Product"] p.blog-info'
            )
            blog_info_text = ' '.join([
                await el.text_content()
                for el in await blog_info_locator.all()
            ])
            if views_match := re.search(r'(\d+)\s+просмотров', blog_info_text):
                metrics['views'] = views_match.group(1)

            # Metrics: Comments
            # JS: a[href="#comments"] text match /\d+/
            comments_locator = page.locator(
                'div[itemtype="http://schema.org/Product"] a[href="#comments"]'
            )
            if await comments_locator.count() > 0:
                if comments_match := re.search(r'\d+', await comments_locator.text_content()):
                    metrics['comments'] = comments_match.group(0)

            # Metrics: Pages count
            # JS: td text match /([\d\,]+)\s+алк/
            tds_locator = page.locator('div[itemtype="http://schema.org/Product"] td')
            tds_text = ' '.join([await td.text_content() for td in await tds_locator.all()])
            if pages_match := re.search(r'([\d\,]+)\s+алк', tds_text):
                metrics['pages_count'] = pages_match.group(1)

            # Metrics: Price
            # JS: span[itemprop="price"] content attr
            price_locator = page.locator(
                'div[itemtype="http://schema.org/Product"] span[itemprop="price"]'
            )
            if await price_locator.count() > 0:
                if price := await price_locator.get_attribute('content'):
                    metrics['price'] = price

                    # Metrics: Price old + price discount
                    # JS: span.discount-price text match /[\d\,]+/
                    price_old_locator = page.locator(
                        'div[itemtype="http://schema.org/Product"] span.discount-price'
                    )
                    if await price_old_locator.count() > 0:
                        if price_old_match := re.search(r'[\d\,]+', await price_old_locator.text_content()):
                            metrics['price_old'] = price_old_match.group(0)
                            metrics['price_discount'] = price

            # Metrics: in_subscribe
            # JS: a.btn-success text includes 'подписк'
            subscribe_btn = page.locator(
                'div[itemtype="http://schema.org/Product"] a.btn-success'
            )
            if await subscribe_btn.count() > 0:
                if 'подписк' in (await subscribe_btn.text_content()).lower():
                    metrics['in_subscribe'] = True

            await db.update_book(book)
            await db.create_metrics(metrics)

            return Output(result='done', data={'book': book, 'metrics': metrics})


class FeisovietListing(BaseLivelibWorkflow):
    name = 'samizdat-feisovet-listing'
    event = 'samizdat:feisovet-listing'
    site = 'feisovet.ru'

    input = InputLivelibBook
    output = Output
    item_wf = FeisovietItem

    concurrency = 4
    execution_timeout_sec = 3_600
    backoff_max_seconds = 30
    backoff_factor = 2

    start_urls = ['https://feisovet.ru/магазин/?sortby=16&page=1']

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        stats = {'new-page-links': 0, 'new-items-links': 0}

        await page.goto(input.url, wait_until='domcontentloaded')
        await page.wait_for_selector('div#footer')

        # Pagination
        # JS: enqueueLinks selector 'ul.pagination a'
        for link in await page.locator('ul.pagination').first.locator('a').all():
            if href := await link.get_attribute('href'):
                if await cls.crawl(urljoin(page.url, href), input.task_id):
                    stats['new-page-links'] += 1

        # Book links
        # JS: enqueueLinks selector 'p.book-inlist-title a', label: 'book'
        for link in await page.locator('p.book-inlist-title a').all():
            if href := await link.get_attribute('href'):
                if await FeisovietItem.crawl(urljoin(page.url, quote(href, safe=":/?&=")), input.task_id):
                    stats['new-items-links'] += 1

        return Output(result='done', data=stats)


if __name__ == '__main__':
    FeisovietListing.run_cron_sync()
    # FeisovietListing.debug_sync(FeisovietListing.start_urls[0])
    # FeisovietItem.debug_sync('https://feisovet.ru/магазин/Притяжение-любви-Без-границ-Нинель-Нуар')
