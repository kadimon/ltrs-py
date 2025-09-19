import re
from urllib.parse import urljoin

from playwright.async_api import Page
import dateparser
from furl import furl

from workflow_base import BaseLivelibWorkflow
from interfaces import InputLivelibBook, Output, WorkerLabels
from db import DbSamizdatPrisma
from utils import save_cover


class IfreedomSuListing(BaseLivelibWorkflow):
    name = 'livelib-ifreedom-su-listing'
    event = 'livelib:ifreedom-su-listing'
    site='ifreedom.su'
    input = InputLivelibBook
    output = Output

    concurrency=3
    execution_timeout_sec=300

    start_urls = [
        'https://ifreedom.su/vse-knigi/',
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

        pages_locator = page.locator('.pagi-block a')
        for page_locator in await pages_locator.all():
            if await cls.crawl(
                urljoin(page.url, await page_locator.get_attribute('href')),
                input.task_id,
            ):
                data['new-page-links'] += 1

        items_links = await page.query_selector_all('.title-home a')
        for i in items_links:
            item_href = await i.get_attribute('href')
            item_url = urljoin(page.url, item_href)
            if await IfreedomSuItem.crawl(item_url, input.task_id):
                data['new-items-links'] += 1

        if not items_links:
            raise Exception('ERROR: No Items')

        return Output(
            result='done',
            data=data,
        )

class IfreedomSuItem(BaseLivelibWorkflow):
    name = 'livelib-ifreedom-su-item'
    event = 'livelib:ifreedom-su-item'
    site='ifreedom.su'
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
                book['title'] = await page.text_content('h1')
                await db.create_book(book)

            authors_str_locator = page.locator('.data-ranobe').filter(
                has_text=re.compile('Автор')
            ).locator('.data-value')
            if await authors_str_locator.count() > 0:
                book['author'] = await authors_str_locator.text_content()

            authors_locator = page.locator('.data-ranobe').filter(
                has_text=re.compile('Автор')
            ).locator('.data-value a')
            if await authors_locator.count() > 0:
                book['authors_data'] = []
                for a in await authors_locator.all():
                    href = await a.get_attribute('href')
                    text = await a.text_content()
                    absolute_url = urljoin(page.url, href)

                    book['authors_data'].append({
                        'name': text.strip(),
                        'url': absolute_url
                    })

            translators_locator = page.locator('.data-ranobe').filter(
                has_text=re.compile('Переводчик')
            ).locator('.data-value a')
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

            genres_locator = page.locator('.data-ranobe').filter(
                has_text=re.compile(r'Жанр')
            ).locator('.data-value a')
            if await genres_locator.count() > 0:
                book['tags'] = [await g.text_content() for g in await genres_locator.all()]

            lang_locator = page.locator('.data-ranobe').filter(
                has_text=re.compile(r'Язык')
            ).locator('.data-value')
            if await lang_locator.count() > 0:
                book['language'] = await lang_locator.first.text_content()

            show_annotation_button_locator = page.locator('.descr-ranobe .open-desc')
            if await show_annotation_button_locator.count() > 0:
                await show_annotation_button_locator.click()
            if annotation := await page.inner_text('.descr-ranobe'):
                book['annotation'] = annotation

            if not await db.check_book_have_cover(page.url):
                if img_src := await page.get_attribute('.img-ranobe img', 'src', timeout=2_000):
                    if img_name := await save_cover(page, img_src, timeout=10_000):
                        book['coverImage'] = img_name

            views_locator = page.locator('.data-ranobe').filter(
                has_text=re.compile('Просмотры')
            ).locator('.data-value')
            if await views_locator.count() > 0:
                metrics['views'] = await views_locator.text_content()


            comments_locator = page.locator('.wpd-thread-info')
            if await comments_locator.count() > 0:
                metrics['comments'] = await comments_locator.get_attribute('data-comments-count')

            likes_locator = page.locator('.dashicons-thumbs-up + .count')
            if await likes_locator.count() > 0:
                metrics['likes'] = await likes_locator.first.text_content()

            dislikes_locator = page.locator('.dashicons-thumbs-down + .count')
            if await dislikes_locator.count() > 0:
                metrics['unlike'] = await dislikes_locator.first.text_content()

            rating_patern = r'([\d\.]+)\s+\/\s(\d+)\s+голосов'
            rating_locator = page.locator('.block-start-total .total-star').filter(
                has_text=re.compile(rating_patern)
            )
            if await rating_locator.count() > 0:
                rating_regex = re.search(rating_patern, await rating_locator.first.text_content())
                metrics['rating'] = rating_regex.group(1)
                metrics['votes'] = rating_regex.group(2)

            chapters_count_locator = page.locator('.data-ranobe').filter(
                has_text=re.compile('Количество записей')
            ).locator('.data-value').filter(
                has_text=re.compile(r'\d+')
            )
            if await chapters_count_locator.count() > 0:
                metrics['chapters_count'] = re.search(r'\d+', await chapters_count_locator.text_content())[0]

            translation_statuses_match = {
                'Перевод активен': 'PROCESS',
                'Перевод приостановлен': 'PAUSE',
                'Произведение завершено': 'FINISH',
            }
            translation_status_loacator = page.locator('.data-ranobe').filter(
                has_text=re.compile('Статус книги')
            ).locator('.data-value')
            if await translation_status_loacator.count() > 0:
                translation_status_str = await translation_status_loacator.text_content()
                if translation_status := translation_statuses_match.get(translation_status_str.strip()):
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

if __name__ == '__main__':
    IfreedomSuListing.run_sync()

    IfreedomSuListing.debug_sync(IfreedomSuListing.start_urls[0])
    IfreedomSuItem.debug_sync('https://ifreedom.su/ranobe/ya-stala-preziraemoj-vnuchkoj-klana-murim/')
