import re
from urllib.parse import urljoin

from playwright.async_api import Page
from datetime import datetime

from workflow_base import BaseLivelibWorkflow
from interfaces import InputLivelibBook, Output, WorkerLabels
from db import DbSamizdatPrisma
from utils import save_cover


class FanficsMeItem(BaseLivelibWorkflow):
    name = 'livelib-fanfics-me-item'
    event = 'livelib:fanfics-me-item'
    site='fanfics.me'
    input = InputLivelibBook
    output = Output

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        resp = await page.goto(
            input.url,
            wait_until='domcontentloaded',
            timeout=20_000,
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
            };

            metrics = {
                'bookUrl': page.url,
            };

            if not await db.check_book_exist(page.url):
                book['title'] = re.sub(r'\([^()]+?\)$', '', await page.text_content('h1'))
                await db.create_book(book)

            authors_locator = page.locator('.FicHead tr').filter(
                has=page.locator('tr').filter(
                    has_text=re.compile(r'Автор:|Авторы:')
                )
            ).locator('a[href*="/translations?str="]')
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

            translators_locator = page.locator('.FicHead .tr').filter(
                has_text=re.compile(r'Переводчик:|Переводчики:')
            ).locator('a[href*="/user"]')
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

            genres_locator = page.locator('.FicHead .tr').filter(
                has_text=re.compile(r'Жанр:')
            ).locator('.content')
            if await genres_locator.count() > 0:
                book['category'] = (await genres_locator.first.inner_text()).split(', ')

            tags_locator = page.locator('.FicHead .tr').filter(
                has_text=re.compile(r'События:|Фандом:|События:')
            ).locator('a:not([data-action])')
            if await tags_locator.count() > 0:
                book['tags'] = [await t.text_content() for t in await tags_locator.all()]

            serie_locator = page.locator('.FicHead .tr').filter(
                has_text=re.compile(r'Серия:')
            ).locator('a')
            if await serie_locator.count() > 0:
                book['series'] = [await serie_locator.text_content()]

            age_rating_locator = page.locator('.FicHead .tr').filter(
                has_text=re.compile(r'Рейтинг:')
            ).locator('.content')
            if await age_rating_locator.count() > 0:
                 book['age_rating_str'] = await age_rating_locator.inner_text()

            annotation_locator = page.locator('[id^="summary"]')
            if await annotation_locator.count() > 0:
                book['annotation'] = await annotation_locator.inner_text()

            date_locator = page.locator('.DateUpdate')
            if await date_locator.count() > 0:
                date_regex = re.search(
                    r'(\d{2}\.\d{2}\.\d{4}) - (\d{2}\.\d{2}\.\d{4})',
                    await date_locator.inner_text()
                )
                book['date_release'] = datetime.strptime(date_regex.group(1), '%d.%m.%Y')
                metrics['content_update_date'] = datetime.strptime(date_regex.group(2), '%d.%m.%Y')

            comments_locator = page.locator('.Comments')
            if await comments_locator.count() > 0:
                metrics['comments'] = await comments_locator.text_content()

            likes_locator = page.locator('.RecommendsCount')
            if await likes_locator.count() > 0:
                metrics['likes'] = await likes_locator.first.text_content()

            added_to_lib_locator = page.locator('.ReadersCount')
            if await added_to_lib_locator.count() > 0:
                metrics['added_to_lib'] = await added_to_lib_locator.first.text_content()

            views_locator = page.locator('.Views')
            if await views_locator.count() > 0:
                views = await views_locator.text_content()
                metrics['views'] = re.search(r'^[\d]+[KkMk]?', views.strip())[0]

            characters_patern = r'\| ([\d\s]+) знаков'
            characters_count_locator = page.locator('.FicHead .tr').filter(
                has_text=re.compile(r'Размер:')
            ).filter(
                has_text=re.compile(characters_patern)
            ).locator('.content')
            if await characters_count_locator.count() > 0:
                chapters_regex = re.search(characters_patern, await characters_count_locator.text_content())
                metrics['characters_count'] = chapters_regex.group(1)

            writing_statuses_match = {
                'Закончен+В процессе': 'PROCESS',
                'В процессе': 'PROCESS',
                'Заморожен': 'PAUSE',
                'Закончен': 'FINISH',
            }
            writing_status_loacator = page.locator('.FicHead .tr').filter(
                has_text=re.compile(r'Статус:')
            ).locator('.content')
            if await writing_status_loacator.count() > 0:
                writing_status_str = await writing_status_loacator.inner_text()
                if translation_status := writing_statuses_match.get(writing_status_str.strip()):
                    metrics['status_writing'] = translation_status

            await db.update_book(book)
            await db.create_metrics(metrics)

            return Output(
                result='done',
                data={
                    'book': book,
                    'metrics': metrics,
                },
            )


class FanficsMeListing(BaseLivelibWorkflow):
    name = 'livelib-fanfics-me-listing'
    event = 'livelib:fanfics-me-listing'
    site = 'fanfics.me'
    input = InputLivelibBook
    output = Output

    item_wf = FanficsMeItem

    concurrency=3
    execution_timeout_sec=300
    backoff_max_seconds=30
    backoff_factor=2

    start_urls = [
        'https://fanfics.me/find',
    ]

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        resp = await page.goto(
            input.url,
            wait_until='domcontentloaded',
        )
        if not (200 <= resp.status < 400):
            return Output(
                result='error',
                data={'status': resp.status},
            )

        data = {
            'new-items-links': 0,
            'new-page-links': 0,
        }

        pages_locator = page.locator('.paginator a')
        for page_locator in await pages_locator.all():
            if await cls.crawl(
                urljoin(page.url, await page_locator.get_attribute('href')),
                input.task_id,
            ):
                data['new-page-links'] += 1

        items_links = await page.query_selector_all('.FicTable_Title h4 a')
        for i in items_links:
            item_href = await i.get_attribute('href')
            item_url = urljoin(page.url, item_href)
            if await FanficsMeItem.crawl(item_url, input.task_id):
                data['new-items-links'] += 1

        if not items_links:
            raise Exception('ERROR: No Items')

        return Output(
            result='done',
            data=data,
        )


if __name__ == '__main__':
    FanficsMeListing.run_sync()

    # FanficsMeListing.debug_sync(FanficsMeListing.start_urls[0])
    FanficsMeItem.debug_sync('https://fanfics.me/fic231649')
    FanficsMeItem.debug_sync('https://fanfics.me/fic233531')
