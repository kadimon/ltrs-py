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
            # HTML: <h1 data-widget-feisovet-book itemprop=name title="Забудь моё имя">Забудь моё имя</h1>
            title_locator = page.locator('div[itemtype="http://schema.org/Product"] h1').first
            book['title'] = (await title_locator.text_content()).strip() if await title_locator.count() > 0 else ''

            if not await db.check_book_exist(page.url):
                await db.create_book(book)

            # Authors
            # JS: $('div[itemtype="..."] a[data-widget-feisovet-author]')
            # HTML: <a class="ui-link" data-widget-feisovet-author href=https://prodaman.ru/arinasemeonova>Арина Семёнова</a>
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
            # HTML: <div class=blog-text itemprop=description>...</div>
            annotation_locator = page.locator('div[itemtype="http://schema.org/Product"] div.blog-text')
            if await annotation_locator.count() > 0:
                book['annotation'] = (await annotation_locator.first.inner_text()).strip()

            # Cover
            # JS: $('div[itemtype="..."] img[itemprop="image"]').attr('src')
            # HTML: <img itemprop=image class=sam-detailed-pic src=... alt="Забудь моё имя">
            if not await db.check_book_have_cover(page.url):
                cover_locator = page.locator('div[itemtype="http://schema.org/Product"] img[itemprop="image"]').first
                if await cover_locator.count() > 0:
                    if img_src := await cover_locator.get_attribute('src'):
                        full_img_src = urljoin(page.url, img_src)
                        if img_name := await save_cover(page, full_img_src):
                            book['coverImage'] = img_name

            # Category
            # JS: $('div[itemtype="..."] p.blog-info:contains("Категории:") a')
            # HTML: <p class=blog-info>Категории: <span><a class="ui-link" href=...><strong>Триллеры</strong></a>, ...</span></p>
            category_locator = page.locator('div[itemtype="http://schema.org/Product"] p.blog-info').filter(
                has_text=re.compile(r'Категории:')
            ).locator('a')
            if await category_locator.count() > 0:
                book['category'] = [(await x.text_content()).strip() for x in await category_locator.all()]

            # Series
            # JS: $('div[itemtype="..."] p.blog-info:contains("Из цикла:") a')
            # ВНИМАНИЕ: блока "Из цикла:" нет ни на одной из проверенных страниц — селектор оставлен как есть.
            series_locator = page.locator('div[itemtype="http://schema.org/Product"] p.blog-info').filter(
                has_text=re.compile(r'Из цикла:')
            ).locator('a')
            if await series_locator.count() > 0:
                book['series'] = [(await x.text_content()).strip() for x in await series_locator.all()]

            # Tags
            # JS: $('div[itemtype="..."] p.blog-info:contains("Хэштег:") a')
            # HTML: <p class=blog-info>Хэштег: <a class="ui-link" href=...>#СЛР,_криминал,_тайны</a></p>
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
            # HTML: <div class=ui-block-a>Дата размещения: <strong>11.05.2024, 01:30</strong></div>
            #       <div class=ui-block-b>Дата обновления: <strong>21.06.2026, 12:56</strong></div>
            # filter(has_text) поднимается до ближайшего div, содержащего нужный текст — берём inner_text()
            # самого div, а не спускаемся в strong (там нашлись бы и strong рейтинга и т.п.)
            dates_block = page.locator('div[itemtype="http://schema.org/Product"] div.ui-block-a').filter(
                has_text=re.compile(r'Дата размещения:')
            ).first
            if await dates_block.count() > 0:
                release_text = await dates_block.inner_text()
                if release_match := re.search(r'\d{2}\.\d{2}\.\d{4}', release_text):
                    book['date_release'] = dateparser.parse(release_match.group(0), date_formats=['%d.%m.%Y'])

            update_block = page.locator('div[itemtype="http://schema.org/Product"] div.ui-block-b').filter(
                has_text=re.compile(r'Дата обновления:')
            ).first
            if await update_block.count() > 0:
                update_text = await update_block.inner_text()
                if update_match := re.search(r'\d{2}\.\d{2}\.\d{4}', update_text):
                    metrics['content_update_date'] = dateparser.parse(update_match.group(0), date_formats=['%d.%m.%Y'])

            # Rating
            # JS: $('div[itemtype="..."] p.rating-title strong').text().match(/\d,\d{2}/)
            # УСТАРЕЛО: блока p.rating-title на странице книги больше нет (в DOM вообще нет элементов
            # с классом *rating*). Значение осталось только в микроразметке в <head>:
            # <meta itemprop=bestRating content=5><meta itemprop=ratingValue content=5,00><meta itemprop=ratingCount content=105>
            # Формат ("5,00") совпадает со старым, поэтому regex и тип значения не меняются.
            rating_text = ''
            rating_meta_locator = page.locator('head meta[itemprop="ratingValue"]').first
            if await rating_meta_locator.count() > 0:
                rating_text = await rating_meta_locator.get_attribute('content') or ''
            else:
                # фоллбэк на старую вёрстку, если блок вернут обратно
                rating_locator = page.locator('div[itemtype="http://schema.org/Product"] p.rating-title strong').first
                if await rating_locator.count() > 0:
                    rating_text = await rating_locator.text_content()
            if rating_match := re.search(r'\d,\d{2}', rating_text):
                metrics['rating'] = rating_match.group(0)

            # Shared blog-info text block for views/comments/added_to_lib/awards/pages/chars
            # JS: $('div[itemtype="..."] p.blog-info').text()  (called multiple times)
            # HTML: <p class=blog-info>7067 просмотров | 77 комментариев | 83 в избранном | 13 наград</p>
            #       <p class=blog-info>...199 руб...Размер: 5,18 алк / 207207 знаков / 14 стр</p>
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
            # HTML: <span class=full-text><strong>199 руб</strong><br>Онлайн-книга</span>
            # ВНИМАНИЕ: на проверенных страницах есть только full-text и notfull-text (listing).
            # inprocess-text не встретился — селектор оставлен как есть.
            # Классы с префиксом f- (f-full-text / f-inprocess-text / f-notfull-text) относятся
            # к блоку "для одобренных" и лежат ВНЕ контейнера Product, поэтому не конфликтуют.
            if await page.locator('div[itemtype="http://schema.org/Product"] span.inprocess-text').count() > 0:
                metrics['status_writing'] = 'PROCESS'
            elif await page.locator('div[itemtype="http://schema.org/Product"] span.full-text').count() > 0:
                metrics['status_writing'] = 'FINISH'
            elif await page.locator('div[itemtype="http://schema.org/Product"] span.notfull-text').count() > 0:
                metrics['status_writing'] = 'STOP'

            # Shared span[class$=-text] text block for price/in_subscribe
            # JS: $('div[itemtype="..."] span[class$=-text]').text() — jQuery склеивает все совпадения,
            # поэтому собираем так же (и заодно не ловим strict mode violation, если span больше одного)
            status_text_locator = page.locator('div[itemtype="http://schema.org/Product"] span[class$="-text"]')
            status_text = ''
            if await status_text_locator.count() > 0:
                status_text = ' '.join([await el.text_content() for el in await status_text_locator.all()])

            # Price
            # JS: $('div[itemtype="..."] span[class$=-text] strong').text().match(/(\d+)\s+руб/)
            price_locator = page.locator('div[itemtype="http://schema.org/Product"] span[class$="-text"] strong')
            if await price_locator.count() > 0:
                price_text = ' '.join([await el.text_content() for el in await price_locator.all()])
                if price_match := re.search(r'(\d+)\s+руб', price_text):
                    metrics['price'] = price_match.group(1)

            # In Subscribe
            # JS: $('div[itemtype="..."] span[class$=-text]').text().includes("подписк")
            # ВНИМАНИЕ: книг по подписке на проверенных страницах нет — условие оставлено как есть.
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

        await page.goto(input.url, wait_until='domcontentloaded')
        await page.wait_for_selector('div.ui-footer')

        # Pagination (только с первой страницы листинга, без проверки на дубли)
        # JS: selector: "div.pageList a"
        # HTML: <div class=pageList><p>
        #         <a class="link ui-link" href="...?sortby=-1&page=5">◄</a>
        #         <a class="page end ui-link" href="...?sortby=-1&page=1">1</a>
        #         <a class="page ui-link" href="...?sortby=-1&page=2">2</a> ...
        #         <span class=current>6</span> ... <span class=break>...</span>
        #         <a class="page end ui-link" href="...?sortby=-1&page=1023">1023</a>
        #         <a class="link ui-link" href="...?sortby=-1&page=7">►</a></p></div>
        # data-атрибутов, как у desu (data-page / data-baseurl / data-last), здесь нет:
        # текущую страницу берём из span.current (фоллбэк — query-параметр page),
        # последнюю — как максимальный page= среди ссылок пагинации, её же href используем
        # как шаблон (в нём сохраняются остальные параметры, например sortby).
        pagination_locator = page.locator('div.pageList').first
        if await pagination_locator.count() > 0:
            current_page = ''
            current_locator = pagination_locator.locator('span.current')
            if await current_locator.count() > 0:
                current_page = (await current_locator.first.text_content()).strip()
            if not current_page:
                current_match = re.search(r'[?&]page=(\d+)', page.url)
                current_page = current_match.group(1) if current_match else '1'

            if current_page == '1':
                last_page = 1
                last_page_url = None
                for link in await pagination_locator.locator('a[href]').all():
                    href = await link.get_attribute('href')
                    if not href:
                        continue
                    if page_match := re.search(r'[?&]page=(\d+)', href):
                        if (page_num := int(page_match.group(1))) > last_page:
                            last_page = page_num
                            last_page_url = urljoin(page.url, href)

                if last_page_url and last_page > 1:
                    page_urls = [
                        re.sub(r'([?&]page=)\d+', lambda m, n=n: m.group(1) + str(n), last_page_url)
                        for n in range(2, last_page + 1)
                    ]
                    crawled = await cls.crawl_bulk(page_urls, input.task_id, dont_dedupe=True)
                    stats['new-page-links'] = len(crawled)

        # Books (bulk после проверки на дубли)
        # JS: selector: "p.blog-title a", label: "book"
        # HTML: <p class=blog-title><a class="ui-link" href=https://prodaman.ru/.../books/...>Название</a></p>
        book_urls = []
        book_links_locator = page.locator('p.blog-title a')
        for link in await book_links_locator.all():
            href = await link.get_attribute('href')
            if href:
                book_urls.append(urljoin(page.url, href))

        if book_urls:
            crawled = await ProdamanItem.crawl_bulk(book_urls, input.task_id)
            stats['new-items-links'] = len(crawled)

        return Output(result='done', data=stats)


if __name__ == '__main__':
    ProdamanListing.run_sync()
    # ProdamanListing.debug_sync(ProdamanListing.start_urls[0])
    ProdamanItem.debug_sync('https://prodaman.ru/arinasemeonova/books/Zabud-mo-imya')
