import re
from datetime import datetime
from random import randint
from urllib.parse import urljoin

import dateparser
from furl import furl
from playwright.async_api import Page, expect

from db import DbSamizdatPrisma
from interfaces import InputLivelibBook, Output
from utils import save_cover
from workflow_base import BaseLivelibWorkflow


class BookmateItem(BaseLivelibWorkflow):
    name = 'livelib-bookmate-item'
    event = 'livelib:bookmate-item'
    site = 'bookmate.com'

    input = InputLivelibBook
    output = Output

    concurrency = 25

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        resp = await page.goto(input.url, wait_until='domcontentloaded')

        # Проверка URL и статуса
        error_locator = page.locator('h1[class*="ContentErrorPageTitle"]')
        if resp.status == 404 or await error_locator.count() > 0:
            async with DbSamizdatPrisma() as db:
                await db.mark_book_deleted(page.url, cls.site)
            return Output(result='error', data={'status': resp.status, 'error': 'invalid_url_or_404'})

        await page.wait_for_selector("div.main-content h1")

        async with DbSamizdatPrisma() as db:
            book = {'url': page.url, 'source': cls.site}
            metrics = {'bookUrl': page.url}

            # Title
            title_locator = page.locator('h1 span[data-test-id="CONTENT_TITLE_MAIN"]')
            if await title_locator.count() > 0:
                book['title'] = (await title_locator.first.text_content()).strip()

            if not await db.check_book_exist(page.url):
                await db.create_book(book)

            # Authors
            authors_locator = page.locator('h1 a[data-test-id="CONTENT_AUTHOR_AUTHOR_NAME"]')
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
            annotation_locator = page.locator('div[class*="ExpandableText"] > span')
            if await annotation_locator.count() > 0:
                book['annotation'] = (await annotation_locator.first.text_content()).strip()

            # Cover
            if not await db.check_book_have_cover(page.url):
                cover_locator = page.locator('div[class*="ContentLeftColumn_wrapper"] div[data-test-id="COVER"] img')
                if await cover_locator.count() > 0:
                    img_src = await cover_locator.first.get_attribute('src')
                    if img_src and "empty_cover" not in img_src:
                        full_img_src = urljoin(page.url, img_src)
                        if img_name := await save_cover(page, full_img_src):
                            book['coverImage'] = img_name

            # Category
            category_locator = page.locator('div[data-test-id="CONTENT_TOPICS"] a')
            if await category_locator.count() > 0:
                book['category'] = [(await x.text_content()).strip() for x in await category_locator.all()]

            # Series
            serie_locator = page.locator('div[class*="ContentInfo_container"] div[data-test-id="CONTENT_INFO_ITEM"]').filter(
                has=page.locator('span[class*="label"]').filter(has_text=re.compile(r"Серия|Серии"))
            ).locator('span[class*="value"]')
            if await serie_locator.count() > 0:
                book['series'] = [(await x.text_content()).strip() for x in await serie_locator.all()]

            # Release Date
            release_date_loc = page.locator('div[class*="ContentInfo_container"] div[data-test-id="CONTENT_INFO_ITEM"]').filter(
                has=page.locator('span[class*="label"]').filter(has_text=re.compile(r"Дата публикации|Год выхода"))
            ).locator('span[class*="value"]')
            if await release_date_loc.count() > 0:
                if rd_match := re.search(r'\d{4}', await release_date_loc.first.text_content()):
                    book['date_release'] = dateparser.parse(rd_match.group(0), date_formats=['%Y'])

            # Owner
            owner_loc = page.locator('div[class*="ContentInfo_container"] div[data-test-id="CONTENT_INFO_ITEM"]').filter(
                has=page.locator('span[class*="label"]').filter(has_text=re.compile(r"Правообладатель"))
            ).locator('span[class*="value"]')
            if await owner_loc.count() > 0:
                book['owner'] = (await owner_loc.first.text_content()).strip()

            # Publisher
            publisher_loc = page.locator('div[class*="ContentInfo_container"] div[data-test-id="CONTENT_INFO_ITEM"]').filter(
                has=page.locator('span[class*="label"]').filter(has_text=re.compile(r"Издатель"))
            ).locator('span[class*="value"]')
            if await publisher_loc.count() > 0:
                book['publisher'] = (await publisher_loc.first.text_content()).strip()

            # Translator
            translator_loc = page.locator('div[class*="ContentInfo_container"] div[data-test-id="CONTENT_INFO_ITEM"]').filter(
                has=page.locator('span[class*="label"]').filter(has_text=re.compile(r"Переводчик"))
            ).locator('span[class*="value"]')
            if await translator_loc.count() > 0:
                book['translate'] = (await translator_loc.first.text_content()).strip()

            # Voice
            voice_loc = page.locator('div[class*="ContentInfo_container"] div[data-test-id="CONTENT_INFO_ITEM"]').filter(
                has=page.locator('span[class*="label"]').filter(has_text=re.compile(r"Рассказчик"))
            ).locator('span[class*="value"]')
            if await voice_loc.count() > 0:
                book['voice'] = (await voice_loc.first.text_content()).strip()

            # Age Rating
            age_rating_loc = page.locator('div[class*="ContentInfo_container"] div[data-test-id="CONTENT_INFO_ITEM"]').filter(
                has=page.locator('span[class*="label"]').filter(has_text=re.compile(r"Возрастные ограничения"))
            ).locator('span[class*="value"]')
            if await age_rating_loc.count() > 0:
                book['age_rating'] = (await age_rating_loc.first.text_content()).strip()

            # Audio Button
            button_audio_locator = page.locator('a[data-test-id="CONTENT_SYNC_TAB_AUDIO"]:not([data-tab-active="true"])')
            if await button_audio_locator.count() > 0:
                if url_audio := await button_audio_locator.get_attribute('href'):
                    book['url_audio'] = urljoin(page.url, url_audio)

            # --- Metrics ---

            # Read Process
            read_process_loc = page.locator('span[data-test-id="CONTENT_TAB_READERS_COUNTER"], span[data-test-id="CONTENT_TAB_LISTENERS_COUNTER"]')
            if await read_process_loc.count() > 0:
                metrics['read_process'] = (await read_process_loc.first.text_content()).strip()

            # Comments
            comments_loc = page.locator('span[data-test-id="CONTENT_TAB_IMPRESSIONS_COUNTER"]')
            if await comments_loc.count() > 0:
                metrics['comments'] = (await comments_loc.first.text_content()).strip()

            # Pages count
            pages_count_loc = page.locator('div[class*="ContentInfo_container"] div[data-test-id="CONTENT_INFO_ITEM"]').filter(
                has=page.locator('span[class*="label"]').filter(has_text=re.compile(r"страниц"))
            ).locator('span[class*="value"]')
            if await pages_count_loc.count() > 0:
                if pc_match := re.search(r'\d+', await pages_count_loc.first.text_content()):
                    metrics['pages_count'] = pc_match.group(0)

            # Duration
            duration_loc = page.locator('div[class*="ContentInfo_container"] div[data-test-id="CONTENT_INFO_ITEM"]').filter(
                has=page.locator('span[class*="label"]').filter(has_text=re.compile(r"Длительность"))
            ).locator('span[class*="value"]')
            if await duration_loc.count() > 0:
                dur_text = await duration_loc.first.text_content()
                hours, minutes = 0, 0
                if h_match := re.search(r'(\d{1,4})\s+ч', dur_text):
                    hours = int(h_match.group(1))
                if m_match := re.search(r'(\d{1,2})\s+мин', dur_text):
                    minutes = int(m_match.group(1))
                metrics['duration'] = hours * 3600 + minutes * 60

            # Awards
            awards_rows = page.locator('div[class*="Emotion_emotion"]').filter(
                has=page.locator('span[class*="EmotionIcon_unicode"]')
            ).filter(
                has=page.locator('span[class*="Emotion_count"]')
            )
            if await awards_rows.count() > 0:
                metrics['awards'] = {}
                for row in await awards_rows.all():
                    award_loc = row.locator('span[class*="EmotionIcon_unicode"]')
                    value_loc = row.locator('span[class*="Emotion_count"]')
                    if await award_loc.count() > 0 and await value_loc.count() > 0:
                        award = (await award_loc.first.get_attribute('title')).strip()
                        if val_match := re.search(r'\d+', await value_loc.first.text_content()):
                            metrics['awards'][award] = val_match.group(0)

            await db.update_book(book)
            await db.create_metrics(metrics)

            # --- Crawl book formats ---
            format_tab_locator = page.locator('a[data-test-id^="CONTENT_SYNC_TAB"]:not([data-tab-active="true"])')
            for tab_locator in await format_tab_locator.all():
                if url_tab := await tab_locator.get_attribute('href'):
                    await cls.crawl(
                        urljoin(page.url, url_tab),
                        input.task_id
                    )

            return Output(result='done', data={'book': book, 'metrics': metrics})


class BookmateListing(BaseLivelibWorkflow):
    name = 'livelib-bookmate-listing'
    event = 'livelib:bookmate-listing'
    site = 'bookmate.com'

    input = InputLivelibBook
    output = Output
    item_wf = BookmateItem

    concurrency = 4
    execution_timeout_sec = 7_200
    backoff_max_seconds = 30
    backoff_factor = 2

    start_urls = [
        "https://books.yandex.ru/books",
        "https://books.yandex.ru/audiobooks",
        "https://books.yandex.ru/comicbooks",
        "https://books.yandex.ru/library/t-detyam-ru",
    ]

    cron_urls = []

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        stats = {'new-page-links': 0, 'new-items-links': 0}

        resp = await page.goto(
            input.url,
            wait_until='domcontentloaded',
        )

        await page.wait_for_selector("div.main-content h1")

        book_locator = page.locator('div[class*="ContentPreview_info"]> a[title], div[class*="SnippetTitle_container"] > a')

        # Скролл до конца страницы пока количество элементов меняется
        previous_count = 0
        while True:
            current_count = await book_locator.count()
            if current_count == previous_count:
                # Ждем пару секунд для надежности подгрузки новых данных
                await page.wait_for_timeout(10000)
                current_count = await book_locator.count()
                if current_count == previous_count:
                    break  # Элементы перестали добавляться, выходим из цикла
            elif current_count == 0:
                raise ValueError(f'Нет книг на странице (до этого было {previous_count})')

            previous_count = current_count
            await page.keyboard.press("End")
            # await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(200)
            await page.wait_for_load_state('networkidle') # Задержка перед следующей проверкой
            await page.wait_for_timeout(1000)

        # Обработка пагинации/списков
        listing_selectors = 'a[class*="BubbleLink_link"], a[class*="LinkTitle_link"], a.tab, a.pagination-page'
        links = await page.locator(listing_selectors).all()
        for link in links:
            href = await link.get_attribute('href')
            if href:
                page_url = urljoin(page.url, href)
                if await cls.crawl(page_url, input.task_id):
                    stats['new-page-links'] += 1

        # Обработка книг
        for link in await book_locator.all():
            href = await link.get_attribute('href')
            if href:
                book_url = urljoin(page.url, href)
                if any(x in book_url for x in ["/books/", "/audiobooks/", "/comicbooks/"]):
                    if await BookmateItem.crawl(book_url, input.task_id):
                        stats['new-items-links'] += 1

        return Output(result='done', data=stats)


if __name__ == '__main__':
    BookmateListing.run_sync()
    # BookmateListing.run_cron_sync()
    # Для отладки
    # BookmateListing.debug_sync(BookmateListing.start_urls[0])
    # BookmateListing.debug_sync('https://books.yandex.ru/section/all/uyutnye-detektivy-qGulE45y')
    # BookmateListing.debug_sync('https://books.yandex.ru/section/all/samorazvitie-tq0QW7Lz')
    # BookmateListing.debug_sync('https://books.yandex.ru/section/audiobook/sovremennaya-russkaya-proza-XHwMYsO6')
    # BookmateItem.debug_sync('https://books.yandex.ru/books/k5ZjBit1')
    # BookmateItem.debug_sync('https://books.yandex.ru/audiobooks/VIitWf9R')
