import re
from urllib.parse import urljoin

from playwright.async_api import Page
from datetime import datetime

from workflow_base import BaseLivelibWorkflow
from interfaces import InputLivelibBook, Output, WorkerLabels
from db import DbSamizdatPrisma
from utils import save_cover

class UsagiOneItem(BaseLivelibWorkflow):
    name = 'livelib-usagi-one-item'
    event = 'livelib:usagi-one-item'
    site='usagi.one'
    input = InputLivelibBook
    output = Output

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        resp = await page.goto(
            input.url,
            wait_until='domcontentloaded',
            referer='https://web.usagi.one',
        )
        if not (200 <= resp.status < 400):
            return Output(
                result='error',
                data={'status': resp.status},
            )

        await page.wait_for_selector('h1')

        async with DbSamizdatPrisma() as db:
            book = {
                'url': page.url,
                'source': cls.site,
            }

            metrics = {
                'bookUrl': page.url,
            }

            book['title'] = await page.locator('h1 > .name').text_content()
            if not await db.check_book_exist(page.url):
                book['title'] = await page.locator('h1 > .name').text_content()
                await db.create_book(book)

            titles_other_locator = page.locator('.another-names')
            if await titles_other_locator.count() > 0:
                book['titles_other'] = [t.strip() for t in (await titles_other_locator.text_content()).split(' / ')]

            authors_locator = page.locator('.elementList').filter(
                has_text=re.compile('Сценарист:|Сценаристы:')
            ).locator('a')
            if await authors_locator.count() > 0:
                book['author'] = ', '.join([await a.text_content() for a in await authors_locator.all()])
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

            translators_locator = page.locator('.elementList').filter(
                has_text=re.compile('Переводчик:|Переводчики:')
            ).locator('a')
            if await translators_locator.count() > 0:
                book['translators_data'] = []
                for a in await translators_locator.all():
                    href = await a.get_attribute('href')
                    text = await a.text_content()
                    absolute_url = urljoin(page.url, href)

                    book['translators_data'].append({
                        'name': text.strip(),
                        'url': absolute_url
                    })

            artists_locator = page.locator('.elementList').filter(
                has_text=re.compile('Художник:|Художники:')
            ).locator('a')
            if await artists_locator.count() > 0:
                book['artists_data'] = []
                for a in await artists_locator.all():
                    href = await a.get_attribute('href')
                    text = await a.text_content()
                    absolute_url = urljoin(page.url, href)

                    book['artists_data'].append({
                        'name': text.strip(),
                        'url': absolute_url
                    })

            publisher_locator = page.locator('.elementList').filter(
                has_text=re.compile('Издательство:|Издательства:')
            ).locator('> span:nth-child(2)')
            if await publisher_locator.count() > 0:
                book['publisher'] = await publisher_locator.first.text_content()

            artwork_type_locator = page.locator('.elementList').filter(
                has_text=re.compile('Категория:|Категории:')
            ).locator('a')
            if await artwork_type_locator.count() > 0:
                book['artwork_type'] = await artwork_type_locator.first.inner_text()

            genres_locator = page.locator('.elementList').filter(
                has_text=re.compile('Жанр:|Жанры:')
            ).locator('a')
            if await genres_locator.count() > 0:
                book['category'] = [await g.text_content() for g in await genres_locator.all()]

            tags_locator = page.locator('.elementList').filter(
                has_text=re.compile('Тег:|Теги:')
            ).locator('a')
            if await tags_locator.count() > 0:
                book['tags'] = [await t.text_content() for t in await tags_locator.all()]

            annotation_locator = page.locator('#tab-description .manga-description')
            if await annotation_locator.count() > 0:
                book['annotation'] = await annotation_locator.first.inner_text()

            date_release_locator = page.locator('.elementList').filter(
                has_text=re.compile('Год выпуска:')
            ).locator('a')
            if await date_release_locator.count() > 0:
                book['date_release'] = datetime.strptime(await date_release_locator.text_content(), "%Y")

            age_rating_locator = page.locator('.elementList').filter(
                has_text=re.compile('Возрастная рекомендация:')
            ).locator('a')
            if await age_rating_locator.count() > 0:
                 book['age_rating_str'] = await age_rating_locator.inner_text()

            if not await db.check_book_have_cover(page.url):
                if img_src := await page.locator(
                    '.fotorama__stage__frame:first-of-type img'
                ).first.get_attribute('src', timeout=2_000):
                    if img_name := await save_cover(page, img_src, timeout=10_000):
                        book['coverImage'] = img_name


            rating_locator = page.locator('.rating-block')
            if await rating_locator.count() > 0:
                metrics['rating'] = await rating_locator.first.get_attribute('data-score')

            chapters_patern = r'Читать\s+\d+\s+-\s+(\d+)'
            chapters_count_locator = page.locator('a.read-last-chapter').filter(
                has_text=re.compile(chapters_patern)
            )
            if await chapters_count_locator.count() > 0:
                chapters_regex = re.search(chapters_patern, await chapters_count_locator.text_content())
                metrics['chapters_count'] = chapters_regex.group(1)

            writing_statuses_match = {
                'выпуск запланирован': 'ANNOUNCE',
                'выпуск продолжается': 'PROCESS',
                'выпуск приостановлен': 'PAUSE',
                'выпуск завершён': 'FINISH',
                'выпуск отменён': 'STOP',
                'выпуск не окончен': 'PROCESS',
            }
            writing_status_loacator = page.locator('.subject-meta > p:nth-child(2) .badge:nth-of-type(1)')
            if await writing_status_loacator.count() > 0:
                writing_status_str = await writing_status_loacator.text_content()
                if translation_status := writing_statuses_match.get(writing_status_str.strip()):
                    metrics['status_writing'] = translation_status

            translation_statuses_match = {
                'перевод начат': 'ANNOUNCE',
                'переводится': 'PROCESS',
                'перевод приостановлен': 'PAUSE',
                'переведено': 'FINISH',
            }
            translation_status_loacator = page.locator('.subject-meta > p:nth-child(2) .badge:nth-of-type(2)')
            if await translation_status_loacator.count() > 0:
                translation_status_str = await translation_status_loacator.text_content()
                if translation_status := translation_statuses_match.get(translation_status_str.strip().lower()):
                    metrics['status_translate'] = translation_status

            await db.update_book(book)
            await db.create_metrics(metrics)

            return Output(
                result='done',
                data={
                    'book': book,
                    'metrics': metrics,
                },
            )


class UsagiOneListing(BaseLivelibWorkflow):
    name = 'livelib-usagi-one-listing'
    event = 'livelib:usagi-one-listing'
    site='usagi.one'
    input = InputLivelibBook
    output = Output
    item_wf = UsagiOneItem

    concurrency=3
    execution_timeout_sec=300
    backoff_max_seconds=30
    backoff_factor=2

    start_urls = [
        'https://web.usagi.one/list/genres/sort_year',
    ]

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        resp = await page.goto(
            input.url,
            wait_until='domcontentloaded',
            referer='https://web.usagi.one',
        )

        if not (200 <= resp.status < 400):
            return Output(
                result='error',
                data={'status': resp.status},
            )

        data = {
            'new-items-links': 0,
            'new-nav-links': 0,
        }

        pages_locator = page.locator('a.element-link[href*="/genre/"], .pagination:first-of-type a')
        for page_locator in await pages_locator.all():
            if await cls.crawl(
                urljoin(page.url, await page_locator.get_attribute('href')),
                input.task_id,
            ):
                data['new-nav-links'] += 1

        items_links = await page.query_selector_all('.tile h3 > a')
        for i in items_links:
            item_href = await i.get_attribute('href')
            item_url = urljoin(page.url, item_href)
            if await UsagiOneItem.crawl(item_url, input.task_id):
                data['new-items-links'] += 1

        return Output(
            result='done',
            data=data,
        )

if __name__ == '__main__':
    UsagiOneListing.run_sync()

    # UsagiOneListing.debug_sync(UsagiOneListing.start_urls[0])
    UsagiOneItem.debug_sync('https://web.usagi.one/the_world_s_best_engineer')
