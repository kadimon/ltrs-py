import re
from urllib.parse import urljoin

import dateparser
from playwright.async_api import Page

from db import DbSamizdatPrisma
from interfaces import InputLivelibBook, Output
from utils import save_cover
from workflow_base import BaseLivelibWorkflow


class DesuItem(BaseLivelibWorkflow):
    name = 'desu-store-item'
    event = 'desu:store-item'
    site = 'desu.store'

    input = InputLivelibBook
    output = Output

    concurrency = 25

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        resp = await page.goto(input.url, wait_until='domcontentloaded')

        if resp.status in (404, 451):
            async with DbSamizdatPrisma() as db:
                await db.mark_book_deleted(page.url, cls.site)
            return Output(result='error', data={'status': resp.status})

        await page.wait_for_selector('div.footerLegal')

        async with DbSamizdatPrisma() as db:
            book = {'url': page.url, 'source': cls.site}
            metrics = {'bookUrl': page.url}

            # --- Основная информация ---

            # Title
            title_locator = page.locator('h1 span.rus-name')
            if await title_locator.count() > 0:
                book['title'] = await title_locator.text_content()

            if not await db.check_book_exist(page.url):
                await db.create_book(book)

            # Title Original
            title_original_locator = page.locator('h1 span.name')
            if await title_original_locator.count() > 0:
                book['title_original'] = await title_original_locator.text_content()

            # Authors
            authors_locator = page.locator(
                'div.b-db_entry div.line-container:has(div.key:text-is("Автор")) div.value ul > li > a'
            )
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

            # Artists
            artists_locator = page.locator(
                'div.b-db_entry div.line-container:has(div.key:text-is("Художник")) div.value ul > li > a'
            )
            if await artists_locator.count() > 0:
                book['artist'] = ', '.join([
                    (await a.text_content()).strip()
                    for a in await artists_locator.all()
                ])
                book['artists_data'] = []
                for a in await artists_locator.all():
                    href = await a.get_attribute('href')
                    book['artists_data'].append({
                        'name': (await a.text_content()).strip(),
                        'url': urljoin(page.url, href),
                    })

            # Translators
            translators_locator = page.locator(
                'div.b-db_entry div.line-container:has(div.key:text-is("Переводчик")) div.value ul.translators li a'
            )
            if await translators_locator.count() > 0:
                book['translate'] = ', '.join([
                    (await a.text_content()).strip()
                    for a in await translators_locator.all()
                ])
                book['translators_data'] = []
                for a in await translators_locator.all():
                    href = await a.get_attribute('href')
                    book['translators_data'].append({
                        'name': (await a.text_content()).strip(),
                        'url': urljoin(page.url, href),
                    })

            # Annotation
            annotation_locator = page.locator('div[itemprop="description"]')
            if await annotation_locator.count() > 0:
                book['annotation'] = await annotation_locator.text_content()

            # Cover
            if not await db.check_book_have_cover(page.url):
                cover_locator = page.locator('div.c-poster img').first
                if await cover_locator.count() > 0:
                    if cover_url := await cover_locator.get_attribute('src'):
                        full_cover_url = urljoin(page.url, cover_url)
                        if cover_name := await save_cover(page, full_cover_url):
                            book['coverImage'] = cover_name

            # Tags & Categories
            tags_and_categories_locator = page.locator('div.b-db_entry a[itemprop="genre"]')
            if await tags_and_categories_locator.count() > 0:
                book['category'] = []
                book['tags'] = []
                for t in await tags_and_categories_locator.all():
                    value = (await t.text_content()).strip()
                    if value.startswith('#'):
                        book['tags'].append(re.sub(r'^#\s+', '', value))
                    else:
                        book['category'].append(value)

            # Release Year
            release_date_locator = page.locator(
                'div.b-db_entry div.line-container:has(div.key:text-is("Статус:")) div.value'
            )
            if await release_date_locator.count() > 0:
                book['date_release'] = dateparser.parse(await release_date_locator.first.text_content())

            # Artwork Type
            artwork_type_locator = page.locator(
                'div.b-db_entry div.line-container:has(div.key:text-is("Тип:")) div.value'
            )
            if await artwork_type_locator.count() > 0:
                if artwork_type := (await artwork_type_locator.text_content()).strip():
                    book['artwork_type'] = artwork_type

            # Age Rating
            if await page.locator('div.c-poster.age_18_plus').count() > 0:
                book['age_rating'] = '18'

            # --- Метрики ---

            # Rating
            rating_locator = page.locator('div.b-db_entry div.score-value')
            if await rating_locator.count() > 0:
                if rating_match := re.search(r'[\d.]+', await rating_locator.text_content()):
                    if rating_match.group(0) != '0':
                        metrics['rating'] = rating_match.group(0)

            # Votes
            votes_locator = page.locator(
                'div.secondaryContent:has(h3:text-is("Оценки пользователей")) div.bar[title]'
            )
            if await votes_locator.count() > 0:
                metrics['votes'] = 0
                for v in await votes_locator.all():
                    if v_title := await v.get_attribute('title'):
                        if v_match := re.search(r'\d+', v_title):
                            metrics['votes'] += int(v_match.group(0))

            # Views
            views_locator = page.locator(
                'div.b-db_entry div.line-container:has(div.key:text-is("Просмотров:")) div.value div.value'
            )
            if await views_locator.count() > 0:
                if views_match := re.search(r'\d+', await views_locator.text_content()):
                    metrics['views'] = views_match.group(0)

            # Added to lib
            adds_locator = page.locator('h3.textWithCount span.count')
            if await adds_locator.count() > 0:
                adds_text = (await adds_locator.text_content()).strip()
                if adds_text and adds_text != '0':
                    metrics['added_to_lib'] = adds_text

            # Comments
            comments_locator = page.locator('div.comments-loader')
            if await comments_locator.count() > 0:
                comments = await comments_locator.get_attribute('data-count')
                if comments and comments != '0':
                    metrics['comments'] = comments

            # Chapters Count
            chapters_locator = page.locator('a.read-ch-online')
            if await chapters_locator.count() > 0:
                if chapters_match := re.search(r'Глава\s+(\d+)', await chapters_locator.text_content()):
                    if chapters_match.group(1) != '0':
                        metrics['chapters_count'] = chapters_match.group(1)

            # Status Writing
            if await page.locator(
                'div.b-db_entry div.line-container:has(div.key:text-is("Статус:")) div.value span.released'
            ).count() > 0:
                metrics['status_writing'] = 'FINISH'
            elif await page.locator(
                'div.b-db_entry div.line-container:has(div.key:text-is("Статус:")) div.value span.ongoing'
            ).count() > 0:
                metrics['status_writing'] = 'PROCESS'

            # Status Translate
            if await page.locator(
                'div.b-db_entry div.line-container:has(div.key:text-is("Перевод:")) div.value span.completed'
            ).count() > 0:
                metrics['status_translate'] = 'FINISH'
            elif await page.locator(
                'div.b-db_entry div.line-container:has(div.key:text-is("Перевод:")) div.value span.continued'
            ).count() > 0:
                metrics['status_translate'] = 'PROCESS'

            # Read Process
            read_process_locator = page.locator(
                'div.secondaryContent div.line:has(div.x_label:text-is("Читаю")) div.bar'
            )
            if await read_process_locator.count() > 0:
                if read_process := await read_process_locator.get_attribute('title'):
                    metrics['read_process'] = read_process

            # Read Stopped
            read_stoped_locator = page.locator(
                'div.secondaryContent div.line:has(div.x_label:text-is("Брошено")) div.bar'
            )
            if await read_stoped_locator.count() > 0:
                if read_stoped := await read_stoped_locator.get_attribute('title'):
                    metrics['read_stoped'] = read_stoped

            # Read On Pause
            read_on_pause_locator = page.locator(
                'div.secondaryContent div.line:has(div.x_label:text-is("Отложено")) div.bar'
            )
            if await read_on_pause_locator.count() > 0:
                if read_on_pause := await read_on_pause_locator.get_attribute('title'):
                    metrics['read_on_pause'] = read_on_pause

            # Read Later
            read_later_locator = page.locator(
                'div.secondaryContent div.line:has(div.x_label:text-is("Запланировано")) div.bar'
            )
            if await read_later_locator.count() > 0:
                if read_later := await read_later_locator.get_attribute('title'):
                    metrics['read_later'] = read_later

            # Read Finished
            read_finished_locator = page.locator(
                'div.secondaryContent div.line:has(div.x_label:text-is("Прочитано")) div.bar'
            )
            if await read_finished_locator.count() > 0:
                if read_finished := await read_finished_locator.get_attribute('title'):
                    metrics['read_finished'] = read_finished

            # Likes
            likes_locator = page.locator(
                'div.secondaryContent div.line:has(div.x_label:text-is("Любимое")) div.bar'
            )
            if await likes_locator.count() > 0:
                if likes := await likes_locator.get_attribute('title'):
                    metrics['likes'] = likes

            await db.update_book(book)
            await db.create_metrics(metrics)

            return Output(result='done', data={'book': book, 'metrics': metrics})


class DesuListing(BaseLivelibWorkflow):
    name = 'desu-store-listing'
    event = 'desu:store-listing'
    site = 'desu.store'

    input = InputLivelibBook
    output = Output
    item_wf = DesuItem

    concurrency = 4
    execution_timeout_sec = 3_600
    backoff_max_seconds = 30
    backoff_factor = 2

    start_urls = ['https://desu.uno/manga/']

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        stats = {'new-page-links': 0, 'new-items-links': 0}

        await page.goto(input.url, wait_until='domcontentloaded')
        await page.wait_for_selector('div.footerLegal')

        # Pagination
        pagination_locator = page.locator('div.PageNav a')
        for link in await pagination_locator.all():
            href = await link.get_attribute('href')
            if href:
                if await cls.crawl(urljoin(page.url, href), input.task_id):
                    stats['new-page-links'] += 1

        # Books
        book_links_locator = page.locator('ol.memberList h3 a')
        for link in await book_links_locator.all():
            href = await link.get_attribute('href')
            if href:
                if await DesuItem.crawl(urljoin(page.url, href), input.task_id):
                    stats['new-items-links'] += 1

        return Output(result='done', data=stats)


if __name__ == '__main__':
    DesuListing.run_sync()
    DesuListing.debug_sync(DesuListing.start_urls[0])
    DesuItem.debug_sync('https://desu.uno/manga/the-reversal-of-my-life-as-a-mob-character.6563/')
