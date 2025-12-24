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


class AuthorTodayItem(BaseLivelibWorkflow):
    name = 'author-today-item'
    event = 'author-today:item'
    site = 'author.today'

    input = InputLivelibBook
    output = Output

    concurrency = 15

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        resp = await page.goto(input.url, wait_until='domcontentloaded')

        # Проверка на 404, 500
        if resp.status == 404:
            async with DbSamizdatPrisma() as db:
                await db.mark_book_deleted(page.url, cls.site)
            return Output(result='error', data={'status': resp.status, 'error': 'not_found'})

        # Проверка URL (work или audiobook)
        if not any(sub in page.url for sub in ["/work/", "/audiobook/"]):
             async with DbSamizdatPrisma() as db:
                await db.mark_book_deleted(page.url, cls.site)
             return Output(result='error', data={'status': resp.status, 'error': 'invalid_url_structure'})

        await page.wait_for_selector("footer.footer")

        # Проверка "Доступ ограничен"
        access_limited_locator = page.locator('h1').filter(has_text=re.compile(r"Доступ ограничен"))
        if await access_limited_locator.count() > 0:
            async with DbSamizdatPrisma() as db:
                await db.mark_book_deleted(page.url, cls.site)
            return Output(result='error', data={'status': resp.status, 'error': 'access_limited'})

        async with DbSamizdatPrisma() as db:
            book = {'url': page.url, 'source': cls.site}
            metrics = {'bookUrl': page.url}

            # --- Создание книги ---
            book['title'] = await page.text_content('div[itemtype="http://schema.org/Book"] h1')

            if not await db.check_book_exist(page.url):
                await db.create_book(book)

            # --- Авторы ---
            authors_locator = page.locator('div.book-panel span[itemprop="author"] > a')
            if await authors_locator.count() > 0:
                book['author'] = ', '.join([(await a.text_content()).strip() for a in await authors_locator.all()])

                book['authors_data'] = []
                for a in await authors_locator.all():
                    href = await a.get_attribute('href')
                    text = await a.text_content()
                    absolute_url = urljoin(page.url, href)

                    book['authors_data'].append({
                        'name': text.strip(),
                        'url': absolute_url
                    })

            # --- Аннотация ---
            annotation_locator = page.locator('div[itemtype="http://schema.org/Book"] div.rich-content')
            if await annotation_locator.count() > 0:
                book['annotation'] = '\n'.join([await a.inner_text() for a in await annotation_locator.all()])

            # --- Обложка ---
            if not await db.check_book_have_cover(page.url):
                cover_locator = page.locator('div[itemtype="http://schema.org/Book"] img.cover-image')
                if await cover_locator.count() > 0:
                    if img_src := await cover_locator.get_attribute('src'):
                        # В JS просто передавался url, предполагаем что save_cover обработает
                        # или нужно собрать полный url если он относительный
                        full_img_src = urljoin(page.url, img_src)
                        if img_name := await save_cover(page, full_img_src):
                            book['coverImage'] = img_name

            # --- Категории (Жанры) ---
            genres_locators = await page.locator('div[itemtype="http://schema.org/Book"] div.book-genres a').all()
            if genres_locators:
                book['category'] = list(set([
                    (await g.text_content()).strip()
                    for g in genres_locators
                ]))

            # --- Серии ---
            series_locators = await page.locator('div.book-panel a[href*="/series/"]').all()
            if series_locators:
                book['series'] = list(set([
                    (await s.text_content()).strip()
                    for s in series_locators
                ]))

            # --- Теги ---
            tags_locators = await page.locator('div[itemtype="http://schema.org/Book"] span.tags a').all()
            if tags_locators:
                book['tags'] = list(set([
                    (await t.text_content()).strip()
                    for t in tags_locators
                ]))

            # --- Метрики: Дата обновления ---
            update_date_locator = page.locator('div.book-panel span[data-format="calendar-short"]')
            if await update_date_locator.count() > 0:
                data_time = await update_date_locator.get_attribute('data-time')
                if data_time:
                    metrics['content_update_date'] = dateparser.parse(data_time)

            # --- Метрики: Просмотры ---
            views_locator = page.locator('div[itemtype="http://schema.org/Book"] div.book-stats span').filter(
                has=page.locator("i.icon-eye")
            )
            if await views_locator.count() > 0:
                data_hint = await views_locator.get_attribute("data-hint")
                if data_hint:
                    if views_match := re.search(r'\d[\d\s]*', data_hint):
                         metrics['views'] = views_match.group(0)

            # --- Метрики: Лайки ---
            likes_locator = page.locator('div[itemtype="http://schema.org/Book"] span.like-count')
            if await likes_locator.count() > 0:
                likes_text = await likes_locator.text_content()
                if likes_match := re.search(r'\d[\d\s]*', likes_text):
                    metrics['likes'] = likes_match.group(0)

            # --- Метрики: Комментарии ---
            comments_locator = page.locator("div.book-page span#commentTotalCount")
            if await comments_locator.count() > 0:
                comments_text = await comments_locator.text_content()
                if comments_match := re.search(r'\d[\d\s]*', comments_text):
                    metrics['comments'] = comments_match.group(0)

            # --- Метрики: Количество знаков ---
            chars_locator = page.locator('div.book-panel span[data-hint="Размер, кол-во знаков с пробелами"]')
            if await chars_locator.count() > 0:
                chars_text = await chars_locator.text_content()
                if chars_match := re.search(r'\d[\d\s]*', chars_text):
                    metrics['characters_count'] = chars_match.group(0)

            # --- Метрики: Награды ---
            awards_locator = page.locator('div[itemtype="http://schema.org/Book"] button.btn-reward')
            if await awards_locator.count() > 0:
                awards_text = await awards_locator.text_content()
                if awards_match := re.search(r'\d[\d\s]*', awards_text):
                    awards_val = awards_match.group(0).strip()
                    if awards_val != "0":
                         metrics['awards'] = {'award': awards_val}

            # --- Статус написания ---
            if await page.locator('div[itemtype="http://schema.org/Book"] span.label-primary').count() > 0:
                metrics['status_writing'] = "PROCESS"
            elif await page.locator('div[itemtype="http://schema.org/Book"] span.label-success').count() > 0:
                metrics['status_writing'] = "FINISH"

            # --- Цены ---
            price_locator = page.locator('div.book-panel span[data-bind="html: priceText"]')
            if await price_locator.count() > 0:
                price_text = await price_locator.text_content()
                if price_match := re.search(r'[\d\,]+', price_text):
                    if price_match.group(0) != "0":
                        metrics['price'] = float(price_match.group(0).replace(',', '.'))

                        price_old_locator = page.locator('div.book-panel span[data-bind="text: oldPriceText"]')
                        if await price_old_locator.count() > 0:
                            price_old_text = await price_old_locator.text_content()
                            if price_old_match := re.search(r'[\d\,]+', price_old_text):
                                metrics['price_old'] = float(price_old_match.group(0).replace(',', '.'))
                                metrics['price_discount'] = metrics['price']

            # --- Аудио URL ---
            audio_locator = page.locator('div[itemtype="http://schema.org/Book"] a').filter(has=page.locator("i.icon-2-headphones"))
            if await audio_locator.count() > 0:
                audio_href = await audio_locator.get_attribute("href")
                if audio_href:
                    book['url_audio'] = "https://author.today" + audio_href

            # --- Таб Статистики (клик и ожидание) ---
            tab_stats = page.locator("a#href-tab-stats")
            if await tab_stats.count() > 0:
                await tab_stats.click()
                # Ожидаем появления элемента внутри таба статистики, чтобы убедиться в загрузке
                try:
                    await page.wait_for_selector('div.book-details-row', timeout=3000)
                except:
                    pass # Если не появилось, пробуем парсить как есть

                # --- Дата выхода ---
                release_row = page.locator('div.book-details-row').filter(
                    has_text=re.compile(r"Впервые опубликовано")
                ).locator('div > span')

                if await release_row.count() > 0:
                    release_date_attr = await release_row.get_attribute("data-time")
                    if release_date_attr:
                        book['date_release'] = dateparser.parse(release_date_attr)

                # --- Статистика библиотеки (Added to lib, Read process, etc) ---

                # Helper function for similar rows
                async def get_stat_value(text_pattern):
                    row = page.locator('div.book-details-row').filter(
                        has_text=re.compile(text_pattern)
                    ).locator('> div').last
                    if await row.count() > 0:
                        return await row.text_content()
                    return None

                if val := await get_stat_value(r"Добавили в библиотеку"):
                    metrics['added_to_lib'] = val

                if val := await get_stat_value(r"Читаю / слушаю"):
                    metrics['read_process'] = val

                if val := await get_stat_value(r"Отложено на потом"):
                    metrics['read_later'] = val

                if val := await get_stat_value(r"Прочитано"):
                    metrics['read_finished'] = val

                if val := await get_stat_value(r"Не интересно"):
                    metrics['unlike'] = val

                if val := await get_stat_value(r"Скачали"):
                    metrics['downloaded'] = val

            await db.update_book(book)
            await db.create_metrics(metrics)

            return Output(result='done', data={'book': book, 'metrics': metrics})


class AuthorTodayListing(BaseLivelibWorkflow):
    name = 'author-today-listing'
    event = 'author-today:listing'
    site = 'author.today'

    input = InputLivelibBook
    output = Output
    item_wf = AuthorTodayItem

    concurrency = 3
    execution_timeout_sec = 300
    backoff_max_seconds = 30
    backoff_factor = 2

    start_urls = ["https://author.today/work/genres"]

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        resp = await page.goto(input.url, wait_until='domcontentloaded')
        if not (200 <= resp.status < 400):
            return Output(result='error', data={'status': resp.status})

        await page.wait_for_selector("footer.footer")

        data = {'new-page-links': 0, 'new-items-links': 0}

        # --- Жанры ---
        # selector: "div.genre-title a"
        # globs: ["https://author.today/work/genre/*"]
        genre_links = await page.locator("div.genre-title a").all()
        for link in genre_links:
            href = await link.get_attribute('href')
            if href:
                genre_url = urljoin(page.url, href)
                if '/work/genre/' in genre_url:
                    if await cls.crawl(genre_url, input.task_id):
                        data['new-page-links'] += 1

        # --- Пагинация ---
        # selector: "ul.pagination a"
        # globs: ["https://author.today/work/genre/*&page=*"]
        pagination_links = await page.locator("ul.pagination a").all()
        for link in pagination_links:
            href = await link.get_attribute('href')
            if href:
                page_url = urljoin(page.url, href)
                if 'page=' in page_url:
                     if await cls.crawl(page_url, input.task_id):
                        data['new-page-links'] += 1

        # --- Книги ---
        # selector: "div.book-title a"
        book_links = await page.locator('[id*="search-results"] div.book-title > a').all()
        for link in book_links:
            href = await link.get_attribute('href')
            if href:
                book_url = urljoin(page.url, href)
                # JS проверяет response.status() и url().includes("/work/" or "/audiobook/") в handler('book')
                # Здесь мы просто собираем ссылки. Логика роутера JS: router.addHandler("book", ...)
                if await AuthorTodayItem.crawl(book_url, input.task_id):
                    data['new-items-links'] += 1

        return Output(result='done', data=data)


if __name__ == '__main__':
    AuthorTodayListing.run_sync()
    # import asyncio
    # asyncio.run(AuthorTodayListing.run_cron())
    # Пример ссылки для отладки
    # AuthorTodayListing.debug_sync(AuthorTodayListing.start_urls[0])
    # AuthorTodayListing.debug_sync('https://author.today/u/igor_koltsov/works')
    AuthorTodayItem.debug_sync('https://author.today/work/519196')
