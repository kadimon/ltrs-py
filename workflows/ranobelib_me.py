import re
from urllib.parse import urljoin

import dateparser
from furl import furl
from playwright.async_api import Page

from db import DbSamizdatPrisma
from interfaces import InputLivelibBook, Output
from utils import save_cover
from workflow_base import BaseLivelibWorkflow


class RanobelibItem(BaseLivelibWorkflow):
    name = 'livelib-ranobelib-item'
    event = 'livelib:ranobelib-item'
    site = 'ranobelib.me'

    input = InputLivelibBook
    output = Output

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        resp = await page.goto(input.url, wait_until='domcontentloaded')

        # JS: await page.waitForSelector("h1", { state: "attached" })
        await page.wait_for_selector("h1", state="attached")

        # JS: if (response.status() == 404 || page.url() == "https://ranobelib.me/404")
        if resp.status == 404 or page.url == "https://ranobelib.me/404":
            async with DbSamizdatPrisma() as db:
                await db.mark_book_deleted(input.url, cls.site)
            return Output(result='error', data={'status': resp.status, 'error': 'invalid_url_or_404'})

        # JS: await page.waitForSelector("a.site-logo")
        await page.wait_for_selector("a.site-logo")


        async with DbSamizdatPrisma() as db:
            book = {'url': page.url, 'source': cls.site}
            metrics = {'bookUrl': page.url}

            # --- Вспомогательная функция для полей "Автор / Переводчик / Художник / Издатель" ---
            async def collect_people(locator):
                names, data = [], []
                for el in await locator.all():
                    text = (await el.text_content() or "").strip()
                    href = await el.get_attribute('href')
                    names.append(text)
                    data.append({'name': text, 'url': urljoin(page.url, href) if href else page.url})
                return names, data

            # Title (в JS устанавливается всегда, до проверки существования)
            # JS: book["title"] = $("h1 > span").text();
            title_locator = page.locator("h1 > span")
            if await title_locator.count() > 0:
                book['title'] = (await title_locator.first.text_content() or "").strip()

            if not await db.check_book_exist(page.url):
                await db.create_book(book)

            # Title original
            # JS: $("h1 ~ h2").text()
            title_orig_locator = page.locator("h1 ~ h2")
            if await title_orig_locator.count() > 0:
                if title_original := (await title_orig_locator.first.text_content() or "").strip():
                    book['title_original'] = title_original

            # Titles other (Альтернативные названия)
            # JS: div[data-info-variant]:has(div:contains("Альтернативные названия")) a
            titles_other_locator = page.locator("div[data-info-variant]").filter(
                has=page.locator("div").filter(has_text=re.compile("Альтернативные названия"))
            ).locator("a")
            if await titles_other_locator.count() > 0:
                book['titles_other'] = [
                    (await x.text_content() or "").strip() for x in await titles_other_locator.all()
                ]

            # Authors
            # JS: div[data-info-variant]:has(div:contains("Автор")) a
            authors_locator = page.locator("div[data-info-variant]").filter(
                has=page.locator("div").filter(has_text=re.compile("Автор"))
            ).locator("a")
            if await authors_locator.count() > 0:
                names, data = await collect_people(authors_locator)
                book['author'] = ', '.join(names)
                book['authors_data'] = data

            # Translators
            # JS: a:has(div.team-item__name)
            translators_locator = page.locator("a").filter(has=page.locator("div.team-item__name"))
            if await translators_locator.count() > 0:
                names, data = await collect_people(translators_locator)
                book['translate'] = ', '.join(names)
                book['translators_data'] = data

            # Artists
            # JS: div[data-info-variant]:has(div:contains("Художник")) a
            artists_locator = page.locator("div[data-info-variant]").filter(
                has=page.locator("div").filter(has_text=re.compile("Художник"))
            ).locator("a")
            if await artists_locator.count() > 0:
                names, data = await collect_people(artists_locator)
                book['artist'] = ', '.join(names)
                book['artists_data'] = data

            # Publishers
            # JS: div[data-info-variant]:has(div:contains("Издател")) a
            publishers_locator = page.locator("div[data-info-variant]").filter(
                has=page.locator("div").filter(has_text=re.compile("Издател"))
            ).locator("a")
            if await publishers_locator.count() > 0:
                names, data = await collect_people(publishers_locator)
                book['publisher'] = ', '.join(names)
                book['publishers_data'] = data

            # Annotation
            # JS: div.section-body div.text-collapse > div
            annotation_locator = page.locator("div.section-body div.text-collapse > div")
            if await annotation_locator.count() > 0:
                if annotation := (await annotation_locator.first.text_content() or "").strip():
                    book['annotation'] = annotation

            # Cover
            # JS: div.fade > div > div.cover > div.cover__wrap > img
            if not await db.check_book_have_cover(page.url):
                cover_locator = page.locator("div.fade > div > div.cover > div.cover__wrap > img")
                if await cover_locator.count() > 0:
                    if cover_url := await cover_locator.first.get_attribute('src'):
                        full_cover_url = urljoin(page.url, cover_url)
                        if cover_name := await save_cover(page, full_cover_url):
                            book['coverImage'] = cover_name

            # Category
            # JS: div.section-body a[data-type="genre"] > span
            category_locator = page.locator('div.section-body a[data-type="genre"] > span')
            if await category_locator.count() > 0:
                book['category'] = [
                    (await x.text_content() or "").strip() for x in await category_locator.all()
                ]

            # Tags
            # JS: div.section-body a[data-type="tag"] > span
            tags_locator = page.locator('div.section-body a[data-type="tag"] > span')
            if await tags_locator.count() > 0:
                book['tags'] = [
                    (await x.text_content() or "").strip() for x in await tags_locator.all()
                ]

            # Release year
            # JS: a[data-info-variant]:has(div:contains("Выпуск")) span -> /\d{4}/
            release_year_locator = page.locator("a[data-info-variant]").filter(
                has=page.locator("div").filter(has_text=re.compile("Выпуск"))
            ).locator("span")
            if await release_year_locator.count() > 0:
                if release_match := re.search(r'\d{4}', await release_year_locator.first.text_content()):
                    book['date_release'] = dateparser.parse(release_match.group(0), date_formats=['%Y'])

            # Artwork type
            # JS: a[data-info-variant]:has(div:contains("Тип")) span -> .last()
            artwork_type_locator = page.locator("a[data-info-variant]").filter(
                has=page.locator("div").filter(has_text=re.compile("Тип"))
            ).locator("span")
            if await artwork_type_locator.count() > 0:
                if artwork_type := (await artwork_type_locator.last.text_content() or "").strip():
                    book['artwork_type'] = artwork_type

            # Age rating
            # JS: a[data-type="restriction"] span -> /\d{1,2}/
            age_rating_locator = page.locator('a[data-type="restriction"] span')
            if await age_rating_locator.count() > 0:
                if age_match := re.search(r'\d{1,2}', await age_rating_locator.first.text_content()):
                    book['age_rating'] = age_match.group(0)

            # --- Metrics ---

            # Rating
            # JS: span.rating-info__value (first) -> /[\d.]+/ , если != "0"
            rating_locator = page.locator("span.rating-info__value")
            if await rating_locator.count() > 0:
                if rating_match := re.search(r'[\d.]+', await rating_locator.first.text_content()):
                    if rating_match.group(0) != "0":
                        metrics['rating'] = rating_match.group(0)

            # Votes
            # JS: span.rating-info__votes (first) , если != "0"
            votes_locator = page.locator("span.rating-info__votes")
            if await votes_locator.count() > 0:
                votes = (await votes_locator.first.text_content() or "").strip()
                if votes and votes != "0":
                    metrics['votes'] = votes

            # Added to lib
            # JS: div[data-stats="bookmarks"] div.section-title -> /\d+/ , если != "0"
            adds_locator = page.locator('div[data-stats="bookmarks"] div.section-title')
            if await adds_locator.count() > 0:
                if adds_match := re.search(r'\d+', await adds_locator.first.text_content()):
                    if adds_match.group(0) != "0":
                        metrics['added_to_lib'] = adds_match.group(0)

            # Chapters count
            # JS: div[data-info-variant]:has(div:contains("Глав")) span -> /\d+/
            chapters_locator = page.locator("div[data-info-variant]").filter(
                has=page.locator("div").filter(has_text=re.compile("Глав"))
            ).locator("span")
            if await chapters_locator.count() > 0:
                if chapters_match := re.search(r'\d+', await chapters_locator.first.text_content()):
                    metrics['chapters_count'] = chapters_match.group(0)

            # Writing status
            # JS: a[data-info-variant]:has(div:contains("Статус")) span
            writing_status_map = {
                "Завершён": "FINISH",
                "Онгоинг": "PROCESS",
                "Приостановлен": "PAUSE",
                "Выпуск прекращён": "STOP",
                "Анонс": "ANNOUNCE",
            }
            writing_status_locator = page.locator("a[data-info-variant]").filter(
                has=page.locator("div").filter(has_text=re.compile("Статус"))
            ).locator("span")
            if await writing_status_locator.count() > 0:
                writing_status = (await writing_status_locator.first.text_content() or "").strip()
                if writing_status in writing_status_map:
                    metrics['status_writing'] = writing_status_map[writing_status]

            # Translate status
            # JS: a[data-info-variant]:has(div:contains("Перевод")) span
            translate_status_map = {
                "Завершён": "FINISH",
                "Продолжается": "PROCESS",
                "Заморожен": "PAUSE",
                "Заброшен": "STOP",
            }
            translate_status_locator = page.locator("a[data-info-variant]").filter(
                has=page.locator("div").filter(has_text=re.compile("Перевод"))
            ).locator("span")
            if await translate_status_locator.count() > 0:
                translate_status = (await translate_status_locator.first.text_content() or "").strip()
                if translate_status in translate_status_map:
                    metrics['status_translate'] = translate_status_map[translate_status]

            # Read process (Читаю)
            # JS: div[data-stats="bookmarks"] > div > div:contains("Читаю") div:last() -> /^\d+$/
            read_process_locator = page.locator('div[data-stats="bookmarks"] > div > div').filter(
                has_text=re.compile("Читаю")
            ).locator("div").last
            if await read_process_locator.count() > 0:
                if read_process := re.match(r'^\d+$', (await read_process_locator.text_content() or "").strip()):
                    metrics['read_process'] = read_process.group(0)

            # Read stoped (Брошено)
            read_stoped_locator = page.locator('div[data-stats="bookmarks"] > div > div').filter(
                has_text=re.compile("Брошено")
            ).locator("div").last
            if await read_stoped_locator.count() > 0:
                if read_stoped := re.match(r'^\d+$', (await read_stoped_locator.text_content() or "").strip()):
                    metrics['read_stoped'] = read_stoped.group(0)

            # Read later (В планах)
            read_later_locator = page.locator('div[data-stats="bookmarks"] > div > div').filter(
                has_text=re.compile("В планах")
            ).locator("div").last
            if await read_later_locator.count() > 0:
                if read_later := re.match(r'^\d+$', (await read_later_locator.text_content() or "").strip()):
                    metrics['read_later'] = read_later.group(0)

            # Read finished (Прочитано)
            read_finished_locator = page.locator('div[data-stats="bookmarks"] > div > div').filter(
                has_text=re.compile("Прочитано")
            ).locator("div").last
            if await read_finished_locator.count() > 0:
                if read_finished := re.match(r'^\d+$', (await read_finished_locator.text_content() or "").strip()):
                    metrics['read_finished'] = read_finished.group(0)

            # Likes (Любимые)
            likes_locator = page.locator('div[data-stats="bookmarks"] > div > div').filter(
                has_text=re.compile("Любимые")
            ).locator("div").last
            if await likes_locator.count() > 0:
                if likes := re.match(r'^\d+$', (await likes_locator.text_content() or "").strip()):
                    metrics['likes'] = likes.group(0)

            await db.update_book(book)
            await db.create_metrics(metrics)

            return Output(result='done', data={'book': book, 'metrics': metrics})


class RanobelibListing(BaseLivelibWorkflow):
    name = 'livelib-ranobelib-listing'
    event = 'livelib:ranobelib-listing'
    site = 'ranobelib.me'

    input = InputLivelibBook
    output = Output
    item_wf = RanobelibItem

    concurrency = 1
    execution_timeout_sec = 600
    backoff_max_seconds = 30
    backoff_factor = 2

    start_urls = ["https://api2.mangalib.me/api/manga?site_id[]=3&page=1"]

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        stats = {'new-page-links': 0, 'new-items-links': 0}

        # JS: const data = await response.json();
        resp = await page.request.get(input.url)
        data = await resp.json()

        # Пагинация
        # JS: nextPageUrl = data["links"]["next"]; pageNum = nextPageUrl.match(/page=(\d+)/)[1]
        next_page_url = data.get("links", {}).get("next")
        if next_page_url:
            if page_match := re.search(r'page=(\d+)', next_page_url):
                page_num = page_match.group(1)
                next_url = f"https://api2.mangalib.me/api/manga?site_id[]=3&page={page_num}"
                if await cls.crawl(next_url, input.task_id):
                    stats['new-page-links'] += 1

        # Книги
        # JS: data["data"].forEach(i => "https://ranobelib.me/ru/book/" + i["slug_url"])
        for i in data.get("data", []):
            book_url = "https://ranobelib.me/ru/book/" + i["slug_url"]
            if await RanobelibItem.crawl(book_url, input.task_id):
                stats['new-items-links'] += 1

        return Output(result='done', data=stats)


if __name__ == '__main__':
    RanobelibListing.run_sync()
    # Для отладки
    RanobelibListing.debug_sync(RanobelibListing.start_urls[0])
    RanobelibItem.debug_sync('https://ranobelib.me/ru/book/230329--kanjo-no-nai-shojo')
