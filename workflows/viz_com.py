import re
from urllib.parse import urljoin

from hatchet_sdk.clients.rest.models.worker import WorkerLabel
from playwright.async_api import Page
import dateparser

from workflow_base import BaseLivelibWorkflow
from interfaces import InputLivelibBook, Output, WorkerLabels
from db import DbSamizdatPrisma
from utils import save_cover


class VizComItem(BaseLivelibWorkflow):
    name = 'livelib-viz-com-item'
    event = 'livelib:viz-com-item'
    site='viz.com'
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
            }

            metrics = {
                'bookUrl': page.url,
            }

            if not await db.check_book_exist(page.url):
                book['title'] = await page.text_content('#purchase_links_block h2')
                await db.create_book(book)

            # replacement_list = [
            #     # --- 1. Самые специфичные и составные роли ---
            #     "Original Character and Hero Design by ",
            #     "With the participation of ",
            #     "Script and Art by ",
            #     "Story and Art by ",
            #     "Planning Story by ",
            #     "Main Character Design by ",
            #     "Original Illustrations by ",
            #     "Script Cooperation by ",
            #     "Based on the video game by ",
            #     "Based on the novel by ",
            #     "and Script by ",
            #     "and Art by ",
            #     # --- 2. Стандартные одиночные роли ---
            #     "Art by ",
            #     "Adapted by ",
            #     "Character Designs by ",
            #     "Character Design by ",
            #     "Concept by ",
            #     "Created by ",
            #     "Contributor ",
            #     "Earlier Script by ",
            #     "Original Concept by ",
            #     "Original Script by ",
            #     "Original Story by ",
            #     "Novel by ",
            #     "Planning by ",
            #     "Script by ",
            #     "Story by ",
            #     "Storyboards by ",
            #     "Supervised by ",
            #     "Supervision by ",
            #     "Translated by ",
            #     "Written by ",
            #     # --- 3. Соединительные слова и ошибки данных ---
            #     # (Их тоже заменяем на запятую)
            #     " amp ",
            #     " and ",
            #     " Original ",  # (Обнаружено в источнике 8: "Art by Junji Ito Original Hirokatsu Kihara")
            #     "p p",  # (Обнаружено в нескольких источниках, например 3, 4, 26)
            # ]
            # for b in await con.book.find_many(
            #     where={"source": "viz.com", "author": {"contains": " by "}}
            # ):
            #     author = b.author
            #     for r in replacement_list:
            #         author = author.replace(r, ", ")

            #     author = re.sub(r"\s+,", ",", author)
            #     author = re.sub(r",{2,}", ",", author)
            #     author = re.sub("(^,)|(,$)", "", author)
            #     author = author.strip()

            author_locator = page.locator('.mar-b-md:has(>strong)').filter(
                has_text=re.compile(r'Story by|Story and Art by')
            )
            if await author_locator.count() > 0:
                author = await author_locator.inner_html()
                book['author'] = author.rsplit('</strong>', 1)[-1]

            artist_locator = page.locator('.mar-b-md:has(>strong)').filter(
                has_text=re.compile(r'Art by|Story and Art by')
            )
            if await artist_locator.count() > 0:
                artist= await artist_locator.inner_html()
                book['artist'] = artist.rsplit('</strong>', 1)[-1]

            artwork_type_locator = page.locator('.mar-b-md:has(>strong)').filter(
                has_text=re.compile('Category')
            )
            if await artwork_type_locator.count() > 0:
                artwork_type = await artwork_type_locator.inner_text()
                book['artwork_type'] = artwork_type.replace('Category', '')

            serie_locator = page.locator('.mar-b-md:has(>strong)').filter(
                has_text=re.compile('Series')
            ).locator('a')
            if await serie_locator.count() > 0:
                book['series'] = [(await s.inner_text()).strip() for s in await serie_locator.all()]

            genres_locator = page.locator('#purchase_links_block .float-l a.hover-red')
            if await genres_locator.count() > 0:
                book['tags'] = [(await g.inner_text()).strip() for g in await genres_locator.all()]

            isbn_locator = page.locator('.mar-b-md:has(>strong)').filter(
                has_text=re.compile('Age Rating')
            )
            if await isbn_locator.count() > 0:
                isbn = await isbn_locator.inner_text()
                book['age_rating_str'] = isbn.replace('Age Rating', '')

            isbn_locator = page.locator('.mar-b-md:has(>strong)').filter(
                has_text=re.compile('ISBN')
            )
            if await isbn_locator.count() > 0:
                isbn = await isbn_locator.inner_text()
                for i in ('ISBN-13', 'ISBN-11', '-'):
                    isbn = isbn.replace(i, '')
                book['isbn'] = isbn

            annotation_locator = page.locator('hr + div')
            if await annotation_locator.count() > 0:
                book['annotation'] = await annotation_locator.inner_text()

            date_release_locator = page.locator('.mar-b-md:has(>strong)').filter(
                has_text=re.compile('Release')
            )
            if await date_release_locator.count() > 0:
                date_release = await date_release_locator.inner_text()
                book['date_release'] = dateparser.parse(date_release.replace('Release', ''))

            if not await db.check_book_have_cover(page.url):
                if img_src := await page.get_attribute('.product-image img', 'src', timeout=2_000):
                    if img_name := await save_cover(page, img_src, timeout=10_000):
                        book['coverImage'] = img_name

            pages_patern = r'(\d+)\s+pages'
            pages_count_locator = page.locator('.mar-b-md:has(>strong)').filter(
                has_text=re.compile(pages_patern)
            )
            if await pages_count_locator.count() > 0:
                if pages_regex := re.search(pages_patern, await pages_count_locator.inner_text()):
                    metrics['pages_count'] = pages_regex.group(1)

            await db.update_book(book)
            await db.create_metrics(metrics)

            return Output(
                result='done',
                data={
                    'book': book,
                    'metrics': metrics,
                },
            )


class VizComListing(BaseLivelibWorkflow):
    name = 'livelib-viz-com-listing'
    event = 'livelib:viz-com-listing'
    site = 'viz.com'
    input = InputLivelibBook
    output = Output
    item_wf = VizComItem

    concurrency=3
    execution_timeout_sec=300

    start_urls = [
        'https://www.viz.com/manga-books',
        'https://www.viz.com/manga-books/genres',
        'https://www.viz.com/manga-books/series',
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
            'new-nav-links': 0,
        }

        nav_link_locator = page.locator('.section_genres a, .section_see_all a, a[href$="all"], .p-cs-tile a ')
        for gen_locator in await nav_link_locator.all():
            if await cls.crawl(
                urljoin(page.url, await gen_locator.get_attribute('href')),
                input.task_id,
            ):
                data['new-nav-links'] += 1

        items_links = await page.query_selector_all('article a[role="presentation"]')
        for i in items_links:
            item_href = await i.get_attribute('href')
            item_url = urljoin(page.url, item_href)
            if await VizComItem.crawl(item_url, input.task_id):
                data['new-items-links'] += 1

        return Output(
            result='done',
            data=data,
        )

if __name__ == '__main__':
    VizComListing.run_sync()

    # VizComListing.debug_sync(VizComListing.start_urls[0])
    VizComItem.debug_sync('https://www.viz.com/manga-books/manga/kill-blue-volume-4/product/8627')
    VizComItem.debug_sync('https://www.viz.com/manga-books/manga/naruto-chibi-sasukes-sharingan-legend-volume-3/product/5496')
