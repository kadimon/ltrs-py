import re
from datetime import datetime
from urllib.parse import urljoin

from playwright.async_api import Page

from db import DbSamizdatPrisma
from interfaces import InputLivelibBook, Output
from utils import save_cover
from workflow_base import BaseLivelibWorkflow


class StrokiMtsItem(BaseLivelibWorkflow):
    name = 'livelib-stroki-mts-item'
    event = 'livelib:stroki-mts-item'
    site = 'stroki.mts.ru'

    input = InputLivelibBook
    output = Output

    concurrency = 25

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        resp = await page.goto(input.url, wait_until='domcontentloaded')
        await page.wait_for_timeout(3000)

        if resp.status == 404 or page.url == "https://stroki.mts.ru/not-found":
            async with DbSamizdatPrisma() as db:
                await db.mark_book_deleted(input.url, cls.site)
            return Output(result='error', data={'status': resp.status, 'error': 'invalid_url_or_404'})

        await page.wait_for_selector("detail-page h1")

        adult_button = page.locator("adult-content-modal stroki-button.stroki-btn-primary")
        if await adult_button.count() > 0:
            await adult_button.first.click()


        async with DbSamizdatPrisma() as db:
            book = {'url': page.url, 'source': cls.site}
            metrics = {'bookUrl': page.url}

            # Title
            title_locator = page.locator("detail-page h1")
            book['title'] = await title_locator.text_content() if await title_locator.count() > 0 else ""

            if not await db.check_book_exist(page.url):
                await db.create_book(book)

            # Original Title
            title_original_locator = page.locator("book-body-description-widget-item").filter(
                has=page.locator("div.title").filter(has_text=re.compile(r"Название на языке оригинала"))
            ).locator("div.content")
            if await title_original_locator.count() > 0:
                book['title_original'] = await title_original_locator.inner_text()

            # Authors
            authors_locator = page.locator("authors-links a.author-link")
            if await authors_locator.count() > 0:
                author_elements = await authors_locator.all()
                book['author'] = ', '.join([await a.text_content() for a in author_elements]).strip()

                book['authors_data'] = []
                for a in author_elements:
                    href = await a.get_attribute('href')
                    text = await a.text_content()
                    book['authors_data'].append({
                        'name': text.strip(),
                        'url': urljoin(page.url, href)
                    })

            # Annotation
            annotation_locator = page.locator("p.multi-card-description-content__text")
            if await annotation_locator.count() > 0:
                texts = [await p.inner_text() for p in await annotation_locator.all()]
                annotation = "\n".join([t.strip() for t in texts if t.strip()])
                if annotation:
                    book['annotation'] = annotation

            # Cover
            if not await db.check_book_have_cover(page.url):
                cover_locator = page.locator(".left-block cover img")
                if await cover_locator.count() > 0:
                    if img_src := await cover_locator.get_attribute('src'):
                        full_img_src = urljoin(page.url, img_src)
                        if img_name := await save_cover(page, full_img_src):
                            book['coverImage'] = img_name

            # Category
            category_locator = page.locator("div.genre-wrapper badge-pill")
            if await category_locator.count() > 0:
                book['category'] = [(await x.text_content()).strip() for x in await category_locator.all()]

            # Release Date
            release_date_locator = page.locator("div.info-block__item").filter(
                has=page.locator("p").filter(has_text=re.compile(r"Год создания"))
            ).locator("p.info-block__item-value")
            if await release_date_locator.count() > 0:
                if release_match := re.search(r'\d{4}', await release_date_locator.text_content()):
                    book['date_release'] = datetime.strptime(release_match.group(0), "%Y")

            # Publisher
            publisher_locator = page.locator("book-body-description-widget-item").filter(
                has=page.locator("div.title").filter(has_text=re.compile(r"издатель"))
            ).locator("div.content")
            if await publisher_locator.count() > 0:
                book['publisher'] = await publisher_locator.inner_text()

            # Translator
            translator_locator = page.locator("book-body-description-widget-item").filter(
                has=page.locator("div.title").filter(has_text=re.compile(r"переводчик"))
            ).locator("div.content")
            if await translator_locator.count() > 0:
                book['translate'] = await translator_locator.inner_text()

            # Voice
            voice_locator = page.locator("book-body-description-widget-item").filter(
                has=page.locator("div.title").filter(has_text=re.compile(r"чтецы"))
            ).locator("div.content")
            if await voice_locator.count() > 0:
                book['voice'] = await voice_locator.inner_text()

            # Age Rating
            age_rating_locator = page.locator("book-body-description-widget-item").filter(
                has=page.locator("div.title").filter(has_text=re.compile(r"Возраст"))
            ).locator("div.content")
            if await age_rating_locator.count() > 0:
                book['age_rating'] = await age_rating_locator.inner_text()

            # Language
            lang_locator = page.locator("book-body-description-widget-item").filter(
                has=page.locator("div.title").filter(has_text=re.compile(r"Язык"))
            ).locator("div.content")
            if await lang_locator.count() > 0:
                book['language'] = await lang_locator.inner_text()

            # Duration
            duration_locator = page.locator("div.info-block__item").filter(
                has=page.locator("p").filter(has_text=re.compile(r"Длительность"))
            ).locator("p.info-block__item-value")
            if await duration_locator.count() > 0:
                duration_text = await duration_locator.text_content()
                hours = 0
                minutes = 0

                if hours_regex := re.search(r'(\d{1,4})\s+ч', duration_text):
                    hours = int(hours_regex.group(1))
                if minutes_regex := re.search(r'(\d{1,2})\s+мин', duration_text):
                    minutes = int(minutes_regex.group(1))

                if hours > 0 or minutes > 0:
                    metrics['duration'] = hours * 3600 + minutes * 60

            # Rating
            rating_locator = page.locator("div.info-block__item").filter(
                has=page.locator("p").filter(has_text=re.compile(r"оценок"))
            ).locator("p.info-block__item-value")
            if await rating_locator.count() > 0:
                if rating_match := re.search(r'[\d\.]+', await rating_locator.text_content()):
                    if rating_match.group(0) != "0":
                        metrics['rating'] = rating_match.group(0)

            # Votes
            votes_locator = page.locator("div.info-block__item p").filter(has_text=re.compile(r"оценок"))
            if await votes_locator.count() > 0:
                if votes_match := re.search(r'\d+', await votes_locator.text_content()):
                    if votes_match.group(0) != "0":
                        metrics['votes'] = votes_match.group(0)

            # Price
            # price_locator = page.locator("client-offer span.price-wrapper")
            # if await price_locator.count() > 0:
            #     if price_match := re.search(r'[\d\.]+', await price_locator.first.text_content()):
            #         metrics['price'] = price_match.group(0)

            # # Old Price
            # old_price_locator = page.locator("client-offer span.price-wrapper .old-price")
            # if await old_price_locator.count() > 0:
            #     if old_price_match := re.search(r'[\d\.]+', await old_price_locator.first.text_content()):
            #         metrics['price_old'] = old_price_match.group(0)

            # In Subscribe
            in_subscribe_locator = page.locator("access-badges span").filter(has_text=re.compile(r"По подписке"))
            if await in_subscribe_locator.count() > 0:
                metrics['in_subscribe'] = True

            # Бесплатно
            in_free_locator = page.locator("access-badges span").filter(has_text=re.compile(r"Бесплатно"))
            if await in_free_locator.count() > 0:
                metrics['price'] = '0'

            # Только платно
            in_paid_locator = page.locator("access-badges span").filter(has_text=re.compile(r"Платная книга"))
            if await in_paid_locator.count() > 0:
                price_locator = page.locator('.priority-offer .price-wrapper')
                if price_match := re.search(r'[\d\.]+', await price_locator.first.text_content()):
                    metrics['price'] = price_match.group(0)

            # Показать полную цену
            show_price_button_locator =page.locator('.stroki-btn-secondary-inverted p').filter(
                has_text=re.compile(r"Все варианты приобретения")
            )
            if await show_price_button_locator.count() > 0:
                await show_price_button_locator.click()
                await page.wait_for_timeout(500)

                price_locator = page.locator("subscription-card").filter(
                    has_text=re.compile(r"Останется у вас навсегда")
                ).locator('.price-wrapper')
                if await price_locator.count() > 0:
                    if price_match := re.search(r'[\d\.]+', await price_locator.first.text_content()):
                        metrics['price'] = price_match.group(0)

                await page.keyboard.press('Escape')

            # Audio Button
            button_audio_locator = page.locator(".slider-wrapper .slider-item:not(.active) p").filter(
                 has_text=re.compile(r"Аудиокнига")
            )
            if await button_audio_locator.count() > 0:
                await button_audio_locator.first.click()
                book['url_audio'] = page.url

            await db.update_book(book)
            print("book", book)

            await db.create_metrics(metrics)
            print("metrics", metrics)

            return Output(result='done', data={'book': book, 'metrics': metrics})


class StrokiMtsListing(BaseLivelibWorkflow):
    name = 'livelib-stroki-mts-listing'
    event = 'livelib:stroki-mts-listing'
    site = 'stroki.mts.ru'

    input = InputLivelibBook
    output = Output
    item_wf = StrokiMtsItem

    concurrency = 3
    execution_timeout_sec = 36_000
    backoff_max_seconds = 30
    backoff_factor = 2

    start_urls = ["https://stroki.mts.ru/genres"]

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        stats = {'new-page-links': 0, 'new-items-links': 0}

        await page.goto(input.url, wait_until='domcontentloaded')
        await page.wait_for_selector("page-title h1")
        await page.wait_for_timeout(2000)

        book_urls_done = []
        while True:
            book_urls_new = []
            # Сбор книг во время скролла списка
            book_links =  page.locator("a.content-name")
            for link in await book_links.all():
                if href := await link.get_attribute('href'):
                    book_url = urljoin(page.url, href)
                    if '/book/' in book_url or '/audiobook/' in book_url or '/comics/' in book_url:
                        if book_url not in book_urls_done:
                            book_urls_new.append(book_url)
                            if await StrokiMtsItem.crawl(book_url, input.task_id):
                                stats['new-items-links'] += 1
            book_urls_new = list(set(book_urls_new))
            if book_urls_new:
                book_urls_done.extend(book_urls_new)
                # Скролим к последней книге
                await book_links.last.hover()
                await page.wait_for_timeout(500)
            else:
                break

            # try:
            #     more_button = page.locator("div.more stroki-button")
            #     if await more_button.count() > 0 and await more_button.is_visible():
            #         await more_button.click(timeout=5000)
            #         await page.wait_for_timeout(2000)
            #     else:
            #         break
            # except Exception:
            #     break

        # Сбор жанров
        genre_links = await page.locator("genre-tree a").all()
        for link in genre_links:
            if href := await link.get_attribute('href'):
                genre_url = urljoin(page.url, href)
                print(genre_url)
                if await cls.crawl(genre_url, input.task_id):
                    stats['new-page-links'] += 1

        return Output(result='done', data=stats)


if __name__ == '__main__':
    StrokiMtsListing.run_sync()
    # import asyncio
    # asyncio.run(StrokiMtsListing.run_cron())
    # Для отладки
    # StrokiMtsListing.debug_sync(StrokiMtsListing.start_urls[0])
    # StrokiMtsListing.debug_sync('https://stroki.mts.ru/genres/young-adult-1206')
    # StrokiMtsItem.debug_sync('https://stroki.mts.ru/book/chetvertoye-krylo-240562')
    # StrokiMtsItem.debug_sync('https://stroki.mts.ru/audiobook/chetvertoye-krylo-240563')
    # StrokiMtsItem.debug_sync('https://stroki.mts.ru/book/zeleniy-svet-30182')
