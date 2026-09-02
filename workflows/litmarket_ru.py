import re
from datetime import datetime
from urllib.parse import urljoin

import dateparser
from furl import furl
from playwright.async_api import Page
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from db import DbSamizdatPrisma
from interfaces import InputLivelibBook, Output
from utils import save_cover
from workflow_base import BaseLivelibWorkflow


class LitmarketItem(BaseLivelibWorkflow):
    name = 'livelib-litmarket-item'
    event = 'livelib:litmarket-item'
    site = 'litmarket.ru'

    input = InputLivelibBook
    output = Output

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        resp = await page.goto(input.url, wait_until='domcontentloaded')

        # Проверка на 404, 500, ошибки на странице или неверный URL
        deleted_profile_locator = page.locator("div.card h5.profileDeletedText")

        if resp.status in (404, 500) \
        or '/books/' not in page.url \
        or await page.locator('.card-content').filter(
            has_text=re.compile(r'Доступ закрыт')
        ).count() > 0:
            async with DbSamizdatPrisma() as db:
                await db.mark_book_deleted(page.url, cls.site)
            return Output(result='error', data={'status': resp.status, 'error': 'invalid_url_or_status'})

        await page.wait_for_selector("footer.footer")
        # Карточка книги отрисовывается сервером, но подстраховываемся:
        # footer в DOM ещё не гарантирует, что блок книги отрендерен
        await page.wait_for_selector("div.card-info h1")

        if await deleted_profile_locator.count() > 0:
             async with DbSamizdatPrisma() as db:
                await db.mark_book_deleted(page.url, cls.site)
             return Output(result='error', data={'status': resp.status, 'error': 'profile_deleted'})

        async with DbSamizdatPrisma() as db:
            book = {'url': page.url, 'source': cls.site}
            metrics = {'bookUrl': page.url}

            book['title'] = await page.locator("div.card-info h1").first.text_content()
            if not await db.check_book_exist(page.url):
                await db.create_book(book)

            # --- Сбор основной информации ---
            authors_locator = page.locator("div.card-info div.card-author a")
            if await authors_locator.count() > 0:
                book['author'] = ', '.join([(await a.text_content()).strip() for a in await authors_locator.all()])
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

            annotation_locator = page.locator("div.card-info div.card-description")
            if await annotation_locator.count() > 0:
                book['annotation'] = await annotation_locator.first.inner_text()

            if not await db.check_book_have_cover(page.url):
                cover_locator = page.locator('div.card-info img[itemprop="contentUrl"]').first
                if await cover_locator.count() > 0:
                    img_src = await cover_locator.get_attribute('src')
                    # Обложка ленивая: до подмены в src лежит плейсхолдер "data:,"
                    if not img_src or img_src.startswith('data:'):
                        try:
                            await page.wait_for_function(
                                """() => {
                                    const img = document.querySelector('div.card-info img[itemprop="contentUrl"]');
                                    return img && img.src && !img.src.startsWith('data:');
                                }""",
                                timeout=10_000,
                            )
                            img_src = await cover_locator.get_attribute('src')
                        except PlaywrightTimeoutError:
                            img_src = await cover_locator.get_attribute('data-src') or img_src

                    if img_src and not img_src.startswith('data:'):
                        full_img_src = urljoin(page.url, img_src)
                        if img_name := await save_cover(page, full_img_src):
                            book['coverImage'] = img_name

            # Жанры: itemprop="genre" переехал на div.card-top-positions,
            # название жанра лежит в вложенном span[itemprop="name"]
            genres_locators = await page.locator(
                'div.card-info div.card-genres div.card-top-positions a span[itemprop="name"]'
            ).all()
            if genres_locators:
                book['category'] = list(set([
                    (await g.text_content()).strip()
                    for g in genres_locators
                    if (await g.text_content()).strip()
                ]))

            series_locators = await page.locator("div.card-info div.card-cycle a").all()
            if series_locators:
                series_list = []
                for s in series_locators:
                    text = await s.text_content()
                    clean_text = re.sub(r'\s?#\d+.*$', '', text.strip())
                    # мобильный дубль блока (a.sf-hidden) приходит пустым
                    if clean_text:
                        series_list.append(clean_text)
                if series_list:
                    book['series'] = list(set(series_list))

            tags_locators = await page.locator("div.card-info ul.tags a").all()
            if tags_locators:
                tags_list = []
                for t in tags_locators:
                    text = await t.text_content()
                    clean_text = text.replace("#", "").strip()
                    tags_list.append(clean_text)
                book['tags'] = tags_list

            age_rating_regex = r'(\d{1,2})\+'
            age_rating_locator = page.locator('div.age-limit').filter(
                has_text=re.compile(r'Возрастное ограничение:')
            ).locator('span.data-value').filter(
                has_text=re.compile(age_rating_regex)
            )
            if await age_rating_locator.count() > 0:
                age_text = await age_rating_locator.first.text_content()
                if age_match := re.search(age_rating_regex, age_text):
                    book['age_rating'] = int(age_match.group(1))

            # Дата создания: у "Создана:" есть itemprop="dateCreated".
            # Раньше фильтр вешался на весь div.card-info и .first ловил дату
            # из блока "В работе: N часов назад"
            release_date_locator = page.locator('div.card-info span.btn-price__date[itemprop="dateCreated"]')
            if await release_date_locator.count() > 0:
                book['date_release'] = dateparser.parse(await release_date_locator.first.text_content())

            # Дата завершения: фильтруем сам div.btn-price, а не весь card-info
            final_date_locator = page.locator('div.card-info div.btn-price').filter(
                has_text=re.compile(r'Закончена:')
            ).locator("span.btn-price__date")
            if await final_date_locator.count() > 0:
                book['date_final'] = dateparser.parse(await final_date_locator.first.text_content())

            # --- Статус написания ---
            btn_price_locator = page.locator("div.card-info div.btn-price")
            # status_full_count = await page.locator("div.book-view-box span.book-status-full").count()

            if await btn_price_locator.filter(has_text=re.compile(r'В работе')).count() > 0:
                metrics['status_writing'] = "PROCESS"
            elif await btn_price_locator.filter(has_text=re.compile(r'Закончена')).count() > 0:
                metrics['status_writing'] = "FINISH"

            # --- Метрики (Views, Likes, etc) ---
            # Используем Playwright фильтры для иконок там, где нет текста

            views_locator = page.locator("div.card-statistics div").filter(has=page.locator("i.lmfont-views")).locator("span")
            if await views_locator.count() > 0:
                views_text = await views_locator.first.text_content()
                if views_match := re.search(r'[\d\.\,kmKM]+', views_text):
                    metrics['views'] = views_match.group(0) # JS код сохраняет как строку (views[0])

            likes_locator = page.locator("div.card-info span.rating-total").first
            if await likes_locator.count() > 0:
                likes_text = await likes_locator.text_content()
                if likes_match := re.search(r'[\d\.\,kmKM]+', likes_text):
                     metrics['likes'] = likes_match.group(0)

            adds_locator = page.locator("div.card-info span.libraries-count")
            if await adds_locator.count() > 0:
                adds_text = await adds_locator.first.text_content()
                if adds_match := re.search(r'[\d\.\,kmKM]+', adds_text):
                    metrics['added_to_lib'] = adds_match.group(0)

            comments_locator = page.locator("div.card-statistics span.comments-count")
            if await comments_locator.count() > 0:
                comments_text = await comments_locator.first.text_content()
                if comments_match := re.search(r'[\d\.\,kmKM]+', comments_text):
                    metrics['comments'] = comments_match.group(0)

            pages_locator = page.locator("div.card-statistics div").filter(has=page.locator("i.lmfont-pages")).locator("span")
            if await pages_locator.count() > 0:
                pages_text = await pages_locator.first.text_content()
                if pages_match := re.search(r'\d+', pages_text):
                    metrics['pages_count'] = int(pages_match.group(0))

            # --- Рейтинги сайта ---
            ratings_locators = await page.locator("div.card-info div.card-top-positions").all()
            if ratings_locators:
                metrics['site_ratings'] = {}
                for r in ratings_locators:
                    # мобильные дубли блока (div.card-top-positions.sf-hidden) пустые
                    if await r.locator("span.number").count() == 0:
                        continue
                    num_text = await r.locator("span.number").first.text_content()
                    cat_text = await r.locator('span[itemprop="name"]').first.text_content()

                    rating_match = re.search(r'\d+', num_text)
                    if rating_match and cat_text:
                        metrics['site_ratings'][cat_text.strip()] = rating_match.group(0)

            # --- Донаты ---
            donats_locator = page.locator("div.card-info span.donate-count")
            if await donats_locator.count() > 0:
                donats_text = await donats_locator.first.text_content()
                if donats_match := re.search(r'\d+', donats_text):
                    if donats_match.group(0) != "0":
                         metrics['awards'] = {'donats': donats_match.group(0)}

            # --- Цены ---
            price_btn_locator = page.locator("div.card-info div.btn-success.price-btn > a")
            if await price_btn_locator.count() > 0:
                price_btn_text = await price_btn_locator.first.text_content()

                if price_match := re.search(r'[\d\.]+', price_btn_text):
                    metrics['price'] = float(price_match.group(0))

                if "Подписка" in price_btn_text:
                    metrics['in_subscribe'] = True

            price_audio_locator = page.locator("div.card-info div.btn-info.price-btn > a")
            if await price_audio_locator.count() > 0:
                price_audio_text = await price_audio_locator.first.text_content()
                if price_audio_match := re.search(r'[\d\.]+', price_audio_text):
                    metrics['price_audio'] = float(price_audio_match.group(0))

            price_old_locator = page.locator("div.card-info div.btn-success.price-btn > a > span.strike")
            if await price_old_locator.count() > 0:
                price_old_text = await price_old_locator.first.text_content()
                if price_old_match := re.search(r'[\d\.]+', price_old_text):
                    metrics['price_old'] = float(price_old_match.group(0))

            price_disc_locator = page.locator("div.card-info div.btn-success.price-btn > a > span.discount-price")
            if await price_disc_locator.count() > 0:
                price_disc_text = await price_disc_locator.first.text_content()
                if price_disc_match := re.search(r'[\d\.]+', price_disc_text):
                    metrics['price_discount'] = float(price_disc_match.group(0))

            await db.update_book(book)
            await db.create_metrics(metrics)

            return Output(result='done', data={'book': book, 'metrics': metrics})

class LitmarketListing(BaseLivelibWorkflow):
    name = 'livelib-litmarket-listing'
    event = 'livelib:litmarket-listing'
    site = 'litmarket.ru'

    input = InputLivelibBook
    output = Output
    item_wf = LitmarketItem

    concurrency=4
    execution_timeout_sec=3600
    backoff_max_seconds=30
    backoff_factor=2

    start_urls = ["https://litmarket.ru/books"]

    cron_urls = ['https://litmarket.ru/books?access=free&sorting=rating&periods=month']

    title_selector = ".books-array article h4 a, div.card-title a, .slideshow .card-name a"

    # На сайте два разных пагинатора, но оба переключаются через ?page=N:
    #   /books      — серверный, <a class="page-link" href="...?page=N">N</a>
    #   /<автор>-pN — ReactPaginate внутри #profileBooks-react: href нет,
    #                 номер лежит в тексте ссылки и в aria-label="Page N"
    # :not(.lmPaginate) отсекает пагинатор комментариев на карточке книги.
    pagination_selector = 'ul.pagination:not(.lmPaginate) a'

    @classmethod
    async def collect_book_urls(cls, page: Page) -> list[str]:
        book_urls = []
        for link in await page.locator(cls.title_selector).all():
            href = await link.get_attribute('href')
            if href:
                book_url = urljoin(page.url, href)
                if '/books/' in book_url:
                    book_urls.append(book_url)
        return book_urls

    @classmethod
    async def get_last_page(cls, page: Page) -> int:
        """Номер последней страницы — оба пагинатора показывают её в хвосте."""
        page_numbers = []

        for link in await page.locator(cls.pagination_selector).all():
            text = ((await link.text_content()) or '').strip()
            if text.isdigit():
                page_numbers.append(int(text))
                continue

            # ReactPaginate: у '...', '\u2039', '\u203a' цифр в тексте нет,
            # но у номерных ссылок номер продублирован в aria-label
            if aria_label := await link.get_attribute('aria-label'):
                if aria_match := re.search(r'Page\s+(\d+)', aria_label):
                    page_numbers.append(int(aria_match.group(1)))

        return max(page_numbers) if page_numbers else 1

    @classmethod
    async def wait_for_cards(cls, page: Page, timeout: int = 30_000) -> int:
        """Ждёт появления карточек книг и стабилизации их количества.

        Карточки догружаются лениво (скелетоны в основной сетке,
        React-блок #profileBooks-react на страницах авторов), поэтому
        наличие footer.footer в DOM ещё не значит, что список отрисован.
        Пагинатор авторов живёт в том же React-блоке — без этого ожидания
        его в DOM ещё нет.
        """
        try:
            await page.wait_for_selector(cls.title_selector, state='attached', timeout=timeout)
        except PlaywrightTimeoutError:
            return 0

        cards_locator = page.locator(cls.title_selector)
        prev_count, stable_rounds = -1, 0

        # ждём, пока количество карточек перестанет расти (2 одинаковых замера)
        for _ in range(20):
            count = await cards_locator.count()
            if count > 0 and count == prev_count:
                stable_rounds += 1
                if stable_rounds >= 2:
                    break
            else:
                stable_rounds = 0
            prev_count = count
            await page.wait_for_timeout(500)

        return max(prev_count, 0)

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        resp = await page.goto(input.url, wait_until='domcontentloaded')
        if not (200 <= resp.status < 400):
            return Output(result='error', data={'status': resp.status})

        await page.wait_for_selector("footer.footer")

        data = {'new-page-links': 0, 'new-items-links': 0}

        # Проверка, что карточки книг реально прогрузились
        cards_count = await cls.wait_for_cards(page)
        if not cards_count:
            print(f"WARNING: No book cards rendered on page {page.url}")
            return Output(result='error', data={**data, 'error': 'cards_not_loaded'})

        # Пагинация (только с первой страницы листинга, без проверки на дубли)
        # JS: globs: ["https://litmarket.ru/books?page=*"]
        url_data = furl(page.url)
        if str(url_data.args.get('page', '1')) == '1':
            last_page = await cls.get_last_page(page)

            page_urls = []
            for n in range(2, last_page + 1):
                next_url = furl(page.url)
                next_url.args['page'] = str(n)
                page_urls.append(next_url.url)

            if page_urls:
                crawled = await cls.crawl_bulk(page_urls, input.task_id, dont_dedupe=True)
                data['new-page-links'] = len(crawled)

        # Обработка ссылок на книги (bulk после проверки на дубли)
        # JS: globs: ["https://litmarket.ru/books/*"]
        book_urls = list(dict.fromkeys(await cls.collect_book_urls(page)))

        if book_urls:
            crawled = await LitmarketItem.crawl_bulk(book_urls, input.task_id)
            data['new-items-links'] = len(crawled)
        else:
            print(f"WARNING: No book links found on page {page.url}")

        return Output(result='done', data=data)

if __name__ == '__main__':
    LitmarketListing.run_sync()
    # LitmarketListing.run_cron_sync()
    # Пример ссылки для отладки
    # LitmarketListing.debug_sync('https://litmarket.ru/books')
    # for cron_url in LitmarketListing.cron_urls:
    #     LitmarketListing.debug_sync(cron_url)
    LitmarketListing.debug_sync('https://litmarket.ru/karina-demina-p154501?utm_source=lm&utm_medium=&utm_campaign=karina-demina-p154501')
    LitmarketListing.debug_sync('https://litmarket.ru/aleksandra-cherchen-p11719?utm_source=lm&utm_medium=&utm_campaign=aleksandra-cherchen-p11719')
    LitmarketItem.debug_sync('https://litmarket.ru/books/ne-vremya-dlya-drakonov')
    LitmarketItem.debug_sync('https://litmarket.ru/books/mrachnye-okovy')
