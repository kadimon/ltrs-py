import re
from datetime import datetime
from urllib.parse import urljoin

from playwright.async_api import Page

from db import DbSamizdatPrisma
from interfaces import InputLivelibBook, Output
from utils import save_cover
from workflow_base import BaseLivelibWorkflow


class ComXLifeItem(BaseLivelibWorkflow):
    name = 'livelib-com-x-life-item'
    event = 'livelib:com-x-life-item'
    site = 'com-x.life'

    input = InputLivelibBook
    output = Output

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        resp = await page.goto(input.url, wait_until='domcontentloaded')

        # Проверка на 404, ошибки на странице или неверный URL, как в JS
        error_title_locator = page.locator("div.message-info__title")
        is_invalid_url = not re.search(r'com-x\.life/\d+-', page.url)
        if not (200 <= resp.status < 400) or await error_title_locator.count() > 0 or is_invalid_url:
            async with DbSamizdatPrisma() as db:
                await db.mark_book_deleted(page.url, cls.site)
            return Output(result='error', data={'status': resp.status, 'error_page': await error_title_locator.is_visible()})

        await page.wait_for_selector("ul.footer__menu")

        async with DbSamizdatPrisma() as db:
            book = {'url': page.url, 'source': cls.site}
            metrics = {'bookUrl': page.url}

            book['title'] = await page.text_content('div#dle-content h1')
            if not await db.check_book_exist(page.url):
                await db.create_book(book)

            # --- Сбор основной информации ---
            title_original_locator = page.locator("div#dle-content h2.page__title-original")
            if await title_original_locator.count() > 0:
                book['title_original'] = (await title_original_locator.text_content()).strip()

            author_locator = page.locator('div#dle-content ul.page__list li').filter(has_text=re.compile(r'^Автор:'))
            if await author_locator.count() > 0:
                author_text = await author_locator.text_content()
                book['author'] = author_text.split(':')[-1].strip()

            artist_locator = page.locator('div#dle-content ul.page__list li').filter(has_text=re.compile(r'^Художник:'))
            if await artist_locator.count() > 0:
                artist_text = await artist_locator.text_content()
                book['artist'] = artist_text.split(':')[-1].strip()

            publisher_locator = page.locator('div#dle-content ul.page__list li').filter(has_text=re.compile(r'^Издатель:'))
            if await publisher_locator.count() > 0:
                publisher_text = await publisher_locator.text_content()
                book['publisher'] = publisher_text.split(':')[-1].strip()

            # --- Сбор дополнительной информации ---
            translator_locator = page.locator("div#dle-content div.page__translators")
            if await translator_locator.count() > 0:
                translator_text = await translator_locator.text_content()
                book['translate'] = translator_text.replace("Переводчики: ", "").strip()

            annotation_locator = page.locator("div#dle-content div.page__text")
            if await annotation_locator.count() > 0:
                book['annotation'] = await annotation_locator.inner_text()

            if not await db.check_book_have_cover(page.url):
                cover_locator = page.locator("div#dle-content div.page__poster > img")
                if await cover_locator.count() > 0:
                    if img_src := await cover_locator.get_attribute('src'):
                        full_img_src = urljoin(page.url, img_src)
                        if img_name := await save_cover(page, full_img_src):
                            book['coverImage'] = img_name

            genres_locators = await page.locator("div#dle-content div.page__tags a").all()
            if genres_locators:
                book['category'] = [await tag.text_content() for tag in genres_locators]

            year_locator = page.locator('div#dle-content ul.page__list li').filter(has_text=re.compile(r'^Год:')).locator('a')
            if await year_locator.count() > 0:
                year_text = await year_locator.text_content()
                if year_match := re.search(r'\d{4}', year_text):
                    book['date_release'] = datetime(int(year_match.group(0)), 1, 1)

            artwork_type_locator = page.locator("main div.speedbar a").last
            if await artwork_type_locator.count() > 0:
                book['artwork_type'] = await artwork_type_locator.text_content()

            age_rating_locator = page.locator('div#dle-content ul.page__list li').filter(has_text=re.compile(r'^Возрастное ограничение:'))
            if await age_rating_locator.count() > 0:
                age_rating_text = await age_rating_locator.text_content()
                if age_match := re.search(r'\d+', age_rating_text):
                    book['age_rating'] = int(age_match.group(0))

            # --- Сбор метрик ---
            rating_locator = page.locator("div#dle-content div.page__activity-votes")
            if await rating_locator.count() > 0:
                rating_text = await rating_locator.text_content()
                if rating_match := re.search(r'[\d.]+', rating_text):
                    if rating_match.group(0) != "0":
                        metrics['rating'] = float(rating_match.group(0))

            votes_locator = page.locator("div#dle-content div.page__activity-votes span span")
            if await votes_locator.count() > 0:
                votes_text = await votes_locator.text_content()
                if votes_match := re.search(r'\d+', votes_text):
                     if votes_match.group(0) != "0":
                        metrics['votes'] = int(votes_match.group(0))

            adds_regex = r'В списках у (\d+) человек'
            adds_locator = page.locator("div#dle-content div.page__activity-title").filter(
                has_text=re.compile(adds_regex)
            )
            if await adds_locator.count() > 0:
                adds_text = await adds_locator.text_content()
                if adds_match := re.search(adds_regex, adds_text):
                    if adds_match.group(1) != "0":
                        metrics['added_to_lib'] = int(adds_match.group(1))

            comments_locator = page.locator("div#dle-content div.page__comments-title")
            if await comments_locator.count() > 0:
                comments_text = await comments_locator.text_content()
                if comments_match := re.search(r'Отзывы после прочтения \((\d+)\):', comments_text):
                    if comments_match.group(1) != "0":
                        metrics['comments'] = int(comments_match.group(1))

            pages_regex = r'Главы \((\d+)\)'
            pages_locator = page.locator("div#dle-content li.tabs__select-item").filter(
                has_text=re.compile(pages_regex)
            )
            if await pages_locator.count() > 0:
                pages_text = await pages_locator.text_content()
                if pages_match := re.search(pages_regex, pages_text):
                     if pages_match.group(1) != "0":
                        metrics['pages_count'] = int(pages_match.group(1))

            status_locator = page.locator('div#dle-content ul.page__list li').filter(has_text=re.compile(r'^Статус:'))
            if await status_locator.count() > 0:
                status_text = await status_locator.text_content()
                status = status_text.split(':')[-1].strip()
                if status == "Продолжается":
                    metrics['status_writing'] = "PROCESS"
                elif status == "Завершён":
                    metrics['status_writing'] = "FINISH"
                elif status in ["Заморожен", "Приостановлен"]:
                    metrics['status_writing'] = "STOP"

            await db.update_book(book)
            await db.create_metrics(metrics)

            return Output(result='done', data={'book': book, 'metrics': metrics})

class ComXLifeListing(BaseLivelibWorkflow):
    name = 'livelib-com-x-life-listing'
    event = 'livelib:com-x-life-listing'
    site = 'com-x.life'

    input = InputLivelibBook
    output = Output
    item_wf = ComXLifeItem

    concurrency=3
    execution_timeout_sec=300
    backoff_max_seconds=30
    backoff_factor=2

    start_urls = ["https://com-x.life/comix-read/"]

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        resp = await page.goto(input.url, wait_until='domcontentloaded')
        if not (200 <= resp.status < 400):
            return Output(result='error', data={'status': resp.status})

        await page.wait_for_selector("ul.footer__menu")

        data = {'new_page_links': 0, 'new_items_links': 0}

        # Обработка пагинации
        pagination_links = await page.locator("div.pagination__pages a").all()
        for link in pagination_links:
            href = await link.get_attribute('href')
            if href:
                page_url = urljoin(page.url, href)
                if await cls.crawl(page_url, input.task_id):
                    data['new_page_links'] += 1

        # Обработка ссылок на комиксы
        book_links = await page.locator("h3.readed__title a").all()
        for link in book_links:
            href = await link.get_attribute('href')
            if href:
                book_url = urljoin(page.url, href)
                if await ComXLifeItem.crawl(book_url, input.task_id):
                    data['new_items_links'] += 1

        if not book_links:
            print(f"WARNING: No book links found on page {page.url}")

        return Output(result='done', data=data)


if __name__ == '__main__':
    ComXLifeListing.run_sync()
    ComXLifeItem.debug_sync('https://com-x.life/12756-genialnyj-mag-pozhirajuschij-lekarstva.html')
