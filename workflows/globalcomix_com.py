import re
from urllib.parse import urljoin
from datetime import datetime

from playwright.async_api import Page
from furl import furl
import dateparser

from workflow_base import BaseLivelibWorkflow
from interfaces import InputLivelibBook, Output
from db import DbSamizdatPrisma
from utils import save_cover

class GlobalcomixComItem(BaseLivelibWorkflow):
    name = 'livelib-globalcomix-com-item'
    event = 'livelib:globalcomix-com-item'
    site = 'globalcomix.com'

    input = InputLivelibBook
    output = Output

    execution_timeout_sec = 240

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        resp = await page.goto(input.url, wait_until='domcontentloaded')

        async with DbSamizdatPrisma() as db:
            if resp.status == 404:
                await db.mark_book_deleted(page.url, cls.site)
                return Output(result='error', data={'status': resp.status})

            await page.wait_for_selector('h1.title')


            serie = {
                'series': [await page.text_content('h1.title')],
                'artwork_type': await page.locator('.is-breadcrumb a').first.text_content(),
                'category': [await page.locator('.is-breadcrumb a').last.text_content()],
                'language': await page.text_content('.sidebar-lang'),
                'tags': [await t.text_content() for t in await page.locator('.sidebar-intro .label a').all()],
            }

            authors_locator = page.locator('.sidebar-credit > ul').first.locator('li').filter(
                has_text=re.compile(r'Writer')
            ).locator('a')
            if await authors_locator.count() > 0:
                serie['author'] = ', '.join([await a.text_content() for a in await authors_locator.all()])
                # Все авторы с текстом и ссылками
                serie['authors_data'] = []
                for a in await authors_locator.all():
                    href = await a.get_attribute('href')
                    text = await a.text_content()
                    absolute_url = urljoin(page.url, href)

                    serie['authors_data'].append({
                        'name': text.strip(),
                        'url': absolute_url
                    })

            artists_locator = page.locator('.sidebar-credit > ul').first.locator('li').filter(
                has_text=re.compile(r'Artist|Colorist|Letterer|Penciler|Inker|Cover Artist')
            ).locator('a')
            if await artists_locator.count() > 0:
                serie['artist'] = ', '.join([await a.text_content() for a in await artists_locator.all()])
                # Все авторы с текстом и ссылками
                serie['artists_data'] = []
                for a in await artists_locator.all():
                    href = await a.get_attribute('href')
                    text = await a.text_content()
                    absolute_url = urljoin(page.url, href)

                    serie['artists_data'].append({
                        'name': text.strip(),
                        'url': absolute_url
                    })

            publisher_locator = page.locator('.sidebar-credit a.is-block')
            if await publisher_locator.count() > 0:
                name = (await publisher_locator.first.text_content()).strip()
                url = await publisher_locator.first.get_attribute('href')
                serie['publisher'] = name
                serie['publishers_data'] = [
                    {
                        'name': name,
                        'url': urljoin(page.url, url)}
                ]

            age_rating_locator = page.locator('.sidebar-title').filter(
                has_text=re.compile(r'Audience')
            ).locator('+ .label').filter(
                has_text=re.compile(r'\d+')
            )
            if await age_rating_locator.count() > 0:
                age_rating_match = re.search(r'\d+', await age_rating_locator.first.text_content())
                serie['age_rating'] = age_rating_match.group(0)

            items = []
            for item in await page.locator('.release').all():
                title_locator = item.locator('.release-name > a')
                book_url = urljoin(page.url, await title_locator.get_attribute('href'))
                book = {
                    'url': book_url,
                    'source': cls.site,
                }

                metrics = {
                    'bookUrl': book_url,
                }

                if not await db.check_book_exist(book_url):
                    book['title'] = await title_locator.text_content()
                    await db.create_book(book)

                annotation_locator = item.locator('.sheet-content-description')
                if await annotation_locator.count() > 0:
                    book['annotation'] = await annotation_locator.inner_text()

                price_regex = r'\$([\d\.]+)'
                price_locator = item.locator('a.is-gold').filter(
                    has_text=re.compile(price_regex)
                )
                if await price_locator.count() > 0:
                    price_match = re.search(price_regex, await price_locator.text_content())
                    metrics['price'] = price_match.group(1)

                views_regex = r'[\d\.\,]+(M|K)?'
                views_locator = item.locator('.release-info').filter(
                    has_text=re.compile(views_regex)
                )
                if await views_locator.count() > 0:
                    views_regex_match = re.search(views_regex, await views_locator.text_content())
                    metrics['views'] = views_regex_match.group(0)

                comments_regex = r'[\d\.\,]+(M|K)?'
                comments_locator = item.locator('.comment-button').filter(
                    has_text=re.compile(comments_regex)
                )
                if await comments_locator.count() > 0:
                    comments_match = re.search(comments_regex, await comments_locator.text_content())
                    metrics['comments'] = comments_match.group(0)

                pages_count_regex = r'\d+'
                pages_count_locator = item.locator('.release-unit_pagenumber').filter(
                    has_text=re.compile(pages_count_regex)
                )
                if await pages_count_locator.count() > 0:
                    pages_count_match = re.search(pages_count_regex, await pages_count_locator.text_content())
                    metrics['pages_count'] = pages_count_match.group(0)

                date_release_regex = r'•(.+)'
                date_release_locator = item.locator('.release-info').filter(
                     has_text=re.compile(date_release_regex)
                )
                if await date_release_locator.count() > 0:
                    date_release_match = re.search(date_release_regex, await date_release_locator.text_content())
                    book['date_release'] = dateparser.parse(date_release_match.group(1))

                if not await db.check_book_have_cover(book_url):
                    cover_locator = item.locator('img')
                    if await cover_locator.count() > 0:
                        if img_src := await cover_locator.first.get_attribute('src'):
                            full_img_src = urljoin(page.url, img_src)
                            if img_name := await save_cover(page, full_img_src):
                                book['coverImage'] = img_name

                await db.update_book(book | serie)
                await db.create_metrics(metrics)
                items.append(book | metrics)

            return Output(result='done', data={'serie': serie, 'items': items})

# Класс для листинга, как в примере, но start_urls изменены на API
# Логика task здесь не нужна, так как первоначальные ссылки получаются через API
class GlobalcomixComListing(BaseLivelibWorkflow):
    name = 'livelib-globalcomix-com-listing'
    event = 'livelib:globalcomix-com-listing'
    site = 'globalcomix.com'

    input = InputLivelibBook
    output = Output
    # item_wf = GlobalcomixComItem

    concurrency=3
    execution_timeout_sec=3600
    backoff_max_seconds=30
    backoff_factor=2

    start_urls = [
        'https://api.globalcomix.com/v1/comics?p=1',
        'https://api.globalcomix.com/v1/comics?sd=7&p=1',
        'https://api.globalcomix.com/v1/comics?sd=90&p=1',
        'https://api.globalcomix.com/v1/comics?sd=365&p=1',
        'https://api.globalcomix.com/v1/comics?comic_type_id=1&p=1',
        'https://api.globalcomix.com/v1/comics?comic_type_id=2&p=1',
        'https://api.globalcomix.com/v1/comics?comic_type_id=3&p=1',
        'https://api.globalcomix.com/v1/comics?comic_type_id=4&p=1',
    ]

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        # Поскольку start_urls ведут на API, мы используем Playwright для запроса и получения JSON
        resp = await page.context.request.get(
            input.url,
            headers={
                'x-gc-client': 'gck_b4d492261ec541eda44ce41de79da424'
            }
        )
        if not (200 <= resp.status < 400):
            return Output(result='error', data={'status': resp.status})

        data = await resp.json()

        stats = {'new-page-links': 0, 'new-items-links': 0}

        for item in data['payload']['results']:
            if await GlobalcomixComItem.crawl(item['url'], input.task_id):
                stats['new-items-links'] += 1

        url_data = furl(input.url)
        if url_data.args['p'] == '1':
            total_pages = int(data['payload']['pagination']['total_pages'])
            for page_num in range(2, total_pages + 1):
                url_data.args['p'] = page_num
                if await cls.crawl(url_data.url, input.task_id):
                    stats['new-page-links'] += 1

        return Output(result='done', data=stats)

if __name__ == '__main__':
    GlobalcomixComListing.run_sync()
    # GlobalcomixComListing.debug_sync(GlobalcomixComListing.start_urls[0])
    GlobalcomixComItem.debug_sync('https://globalcomix.com/c/the-book-of-bronwyn-ronin')
    # GlobalcomixComItem.debug_sync('https://globalcomix.com/c/pirates-of-the-hard-nox')
