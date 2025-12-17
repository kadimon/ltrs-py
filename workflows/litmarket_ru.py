import re
from datetime import datetime
from urllib.parse import urljoin

import dateparser
from furl import furl
from playwright.async_api import Page

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

        if resp.status in (404, 500) or '/books/' not in page.url:
            async with DbSamizdatPrisma() as db:
                await db.mark_book_deleted(page.url, cls.site)
            return Output(result='error', data={'status': resp.status, 'error': 'invalid_url_or_status'})

        await page.wait_for_selector("footer.footer")

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
            authors_locator = page.locator("div.card-info div.card-author > a")
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
                book['annotation'] = await annotation_locator.inner_text()

            if not await db.check_book_have_cover(page.url):
                cover_locator = page.locator('div.card-info img[itemprop="thumbnailUrl"]')
                if await cover_locator.count() > 0:
                    if img_src := await cover_locator.get_attribute('src'):
                        full_img_src = urljoin(page.url, img_src)
                        if img_name := await save_cover(page, full_img_src):
                            book['coverImage'] = img_name

            genres_locators = await page.locator('.card-caption span[itemprop="genre"]').all()
            if genres_locators:
                book['category'] = list(set([
                    (await g.text_content()).strip()
                    for g in genres_locators
                ]))

            series_locators = await page.locator("div.card-info div.card-cycle a").all()
            if series_locators:
                series_list = []
                for s in series_locators:
                    text = await s.text_content()
                    clean_text = re.sub(r'\s?#\d+.*$', '', text.strip())
                    series_list.append(clean_text)
                book['series'] = list(set(series_list))

            tags_locators = await page.locator("div.card-info div.tags a").all()
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

            release_date_locator = page.locator('div.card-info').filter(
                has_text=re.compile(r'Создана:')
            ).locator("time.btn-price__date")
            if await release_date_locator.count() > 0:
                book['date_release'] = dateparser.parse(await release_date_locator.first.text_content())

            final_date_locator = page.locator('div.card-info').filter(
                has_text=re.compile(r'Закончена:')
            ).locator("span.btn-price__date")
            if await final_date_locator.count() > 0:
                book['date_final'] = dateparser.parse(await final_date_locator.first.text_content())

            # --- Статус написания ---
            btn_price_text = await page.locator("div.card-info div.btn-price").first.text_content() if await page.locator("div.card-info div.btn-price").count() > 0 else ""
            # status_full_count = await page.locator("div.book-view-box span.book-status-full").count()

            if "В работе" in btn_price_text:
                metrics['status_writing'] = "PROCESS"
            elif "Закончена" in btn_price_text:
                metrics['status_writing'] = "FINISH"

            # --- Метрики (Views, Likes, etc) ---
            # Используем Playwright фильтры для иконок там, где нет текста

            views_locator = page.locator("div.card-statistics div").filter(has=page.locator("i.lmfont-views")).locator("span")
            if await views_locator.count() > 0:
                views_text = await views_locator.text_content()
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
                comments_text = await comments_locator.text_content()
                if comments_match := re.search(r'[\d\.\,kmKM]+', comments_text):
                    metrics['comments'] = comments_match.group(0)

            pages_locator = page.locator("div.card-statistics div").filter(has=page.locator("i.lmfont-pages")).locator("span")
            if await pages_locator.count() > 0:
                pages_text = await pages_locator.text_content()
                if pages_match := re.search(r'\d+', pages_text):
                    metrics['pages_count'] = int(pages_match.group(0))

            # --- Рейтинги сайта ---
            ratings_locators = await page.locator("div.card-info div.card-top-positions").all()
            if ratings_locators:
                metrics['site_ratings'] = {}
                for r in ratings_locators:
                    if await r.locator("span.number").count() == 0:
                        continue
                    num_text = await r.locator("span.number").text_content()
                    cat_text = await r.locator('span[itemprop="genre"]').text_content()

                    rating_match = re.search(r'\d+', num_text)
                    if rating_match and cat_text:
                        metrics['site_ratings'][cat_text] = rating_match.group(0)

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
                price_btn_text = await price_btn_locator.text_content()

                if price_match := re.search(r'[\d\.]+', price_btn_text):
                    metrics['price'] = float(price_match.group(0))

                if "Подписка" in price_btn_text:
                    metrics['in_subscribe'] = True

            price_audio_locator = page.locator("div.card-info div.btn-info.price-btn > a")
            if await price_audio_locator.count() > 0:
                price_audio_text = await price_audio_locator.text_content()
                if price_audio_match := re.search(r'[\d\.]+', price_audio_text):
                    metrics['price_audio'] = float(price_audio_match.group(0))

            price_old_locator = page.locator("div.card-info div.btn-success.price-btn > a > span.strike")
            if await price_old_locator.count() > 0:
                price_old_text = await price_old_locator.text_content()
                if price_old_match := re.search(r'[\d\.]+', price_old_text):
                    metrics['price_old'] = float(price_old_match.group(0))

            price_disc_locator = page.locator("div.card-info div.btn-success.price-btn > a > span.discount-price")
            if await price_disc_locator.count() > 0:
                price_disc_text = await price_disc_locator.text_content()
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

    concurrency=3
    execution_timeout_sec=300
    backoff_max_seconds=30
    backoff_factor=2

    start_urls = ["https://litmarket.ru/books"]

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        resp = await page.goto(input.url, wait_until='domcontentloaded')
        if not (200 <= resp.status < 400):
            return Output(result='error', data={'status': resp.status})

        title_selector = ".books-array article h4 a , div.card-title a, .slideshow .card-name a"
        await page.wait_for_selector(title_selector)

        data = {'new-page-links': 0, 'new-items-links': 0}

        # Обработка пагинации
        # JS: globs: ["https://litmarket.ru/books?page=*"]
        pagination_links = await page.locator("ul.pagination a").all()
        url_data = furl(page.url)
        for link in pagination_links:
            if href := await link.get_attribute('href'):
                page_url = urljoin(page.url, await link.get_attribute('href'))
                # Простая проверка на соответствие паттерну пагинации
                if 'page=' in page_url:
                    if await cls.crawl(page_url, input.task_id):
                        data['new-page-links'] += 1
            else:
                page_num_locator = link.filter(
                    has_text=re.compile(r'\d+')
                )
                if await page_num_locator.count() > 0:
                    page_num = (await page_num_locator.text_content()).strip()
                    if page_num == '1':
                        continue
                    url_data.args['page'] = page_num
                    if await cls.crawl(url_data.url, input.task_id):
                        data['new-page-links'] += 1


        # Обработка ссылок на книги
        # JS: globs: ["https://litmarket.ru/books/*"]
        book_links = await page.locator(title_selector).all()
        for link in book_links:
            href = await link.get_attribute('href')
            if href:
                book_url = urljoin(page.url, href)
                if '/books/' in book_url:
                    if await LitmarketItem.crawl(book_url, input.task_id):
                        data['new-items-links'] += 1

        if not book_links:
            print(f"WARNING: No book links found on page {page.url}")

        return Output(result='done', data=data)

if __name__ == '__main__':
    # LitmarketListing.run_sync()
    import asyncio
    # asyncio.run(LitmarketListing.run_cron())
    # Пример ссылки для отладки
    # LitmarketListing.debug_sync('https://litmarket.ru/books')
    # LitmarketListing.debug_sync('https://litmarket.ru/karina-demina-p154501?utm_source=lm&utm_medium=&utm_campaign=karina-demina-p154501')
    LitmarketListing.debug_sync('https://litmarket.ru/aleksandra-cherchen-p11719?utm_source=lm&utm_medium=&utm_campaign=aleksandra-cherchen-p11719')
    # LitmarketItem.debug_sync('https://litmarket.ru/books/ne-vremya-dlya-drakonov')
