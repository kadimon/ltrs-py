import re
from urllib.parse import urljoin
from datetime import datetime

from playwright.async_api import Page
from furl import furl

from workflow_base import BaseLivelibWorkflow
from interfaces import InputLivelibBook, Output
from db import DbSamizdatPrisma
from utils import save_cover

class RemangaOrgItem(BaseLivelibWorkflow):
    name = 'livelib-remanga-org-item'
    event = 'livelib:remanga-org-item'
    site = 'remanga.org'

    input = InputLivelibBook
    output = Output

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        resp = await page.goto(input.url, wait_until='domcontentloaded')

        async with DbSamizdatPrisma() as db:
            error_title_locator = page.locator('img[src="https://remanga.org/media/public/errors/500.webp"]')
            if resp.status == 404 or await error_title_locator.count() > 0:
                await db.mark_book_deleted(page.url, cls.site)
                return Output(result='error', data={'status': resp.status})

            await page.wait_for_selector('footer')

            # Обработка диалога 18+
            confirm_dialog_locator = page.locator('div[data-sentry-component="Card"]')
            if await confirm_dialog_locator.count() > 0 and await confirm_dialog_locator.is_visible():
                await confirm_dialog_locator.locator('button[role="checkbox"]').check()
                await confirm_dialog_locator.locator('button').filter(
                    has_text=re.compile(r'Мне есть 18')
                ).click()
                await page.wait_for_timeout(1_000)
            book = {'url': page.url, 'source': cls.site}
            metrics = {'bookUrl': page.url}

            if not await db.check_book_exist(page.url):
                book['title'] = await page.text_content('p.cs-layout-title-text')
                await db.create_book(book)

            titles_other_locator = page.locator('p[data-testid="title-alt-name-item"]')
            if await titles_other_locator.count() > 0:
                 titles_other = await titles_other_locator.all_text_contents()
                 book['titles_other'] = [t.strip() for t in titles_other]

            # Создатели (Авторы, художники и т.д.)
            creators_locator = page.locator('div.cs-layout-stats-line-item').filter(
                has_text=re.compile(r'Создател')
            ).locator('a')
            if await creators_locator.count() > 0:
                authors = []
                authors_data = []
                for author_link in await creators_locator.all():
                    name = await author_link.text_content()
                    url = await author_link.get_attribute('href')
                    authors.append(name.strip())
                    authors_data.append({'name': name.strip(), 'url': urljoin(page.url, url)})
                if authors:
                    book['author'] = ", ".join(authors)
                if authors_data:
                    book['authors_data'] = authors_data

            publishers_locator = page.locator('div.cs-layout-stats-line-item').filter(
                has_text=re.compile(r'Паблишер')
            ).locator('a')
            if await publishers_locator.count() > 0:
                publishers = []
                publishers_data = []
                for pub_link in await publishers_locator.all():
                    name = await pub_link.text_content()
                    url = await pub_link.get_attribute('href')
                    publishers.append(name.strip())
                    publishers_data.append({'name': name.strip(), 'url': urljoin(page.url, url)})
                if publishers:
                    book['publisher'] = ", ".join(publishers)
                if publishers_data:
                    book['publishers_data'] = publishers_data

            annotation_locator = page.locator('div[data-sentry-component="Description"]')
            if await annotation_locator.count() > 0:
                book['annotation'] = await annotation_locator.inner_text()

            if not await db.check_book_have_cover(page.url):
                cover_locator = page.locator('div[data-sentry-component="TitleCoverBlock"] img')
                if await cover_locator.count() > 0:
                    if img_src := await cover_locator.first.get_attribute('src'):
                        full_img_src = urljoin(page.url, img_src)
                        if img_name := await save_cover(page, full_img_src):
                            book['coverImage'] = img_name

            tags_blok_locator = page.locator('div[data-sentry-component="EntityLayoutStatsLineItemContent"]').filter(
                has=page.locator(' > a[href*="/manga/"]')
            )
            if await tags_blok_locator.count() > 0:
                # Нажать кнопку "еще" (больше) для тегов
                more_tags_button_locator = tags_blok_locator.locator('button')
                if await more_tags_button_locator.count() > 0:
                    await more_tags_button_locator.click()
                    await page.wait_for_timeout(500)

                book['category'] = [
                    await c.text_content()
                    for c in await tags_blok_locator.locator('a[href*="/manga/genres/"]').all()
                ]

                book['tags'] = [
                    await t.text_content()
                    for t in await tags_blok_locator.locator('a[href*="/manga/categories/"]').all()
                ]

            year_regex = r'\d{4}'
            year_locator = page.locator('[data-sentry-component="EntityLayoutSubtitle"] a:nth-of-type(2)').filter(
                has_text=re.compile(year_regex)
            )
            if await year_locator.count() > 0:
                year_match = re.search(year_regex, await year_locator.text_content())
                book['date_release'] = datetime(int(year_match.group(0)), 1, 1)

            artwork_type_locator = page.locator('[data-sentry-component="EntityLayoutSubtitle"] a:nth-of-type(1)')
            if await artwork_type_locator.count() > 0:
                book['artwork_type'] = await artwork_type_locator.text_content()

            age_rating_regex = r'\d{1,2}'
            age_rating_locator = page.locator('[data-sentry-component="EntityLayoutStatsLineItem"]').filter(
                has_text=re.compile(r'Возрастное ограничение')
            ).locator('a').filter(
                has_text=re.compile(age_rating_regex)
            )
            if await age_rating_locator.count() > 0:
                if age_match := re.search(age_rating_regex, await age_rating_locator.text_content()):
                    book['age_rating'] = int(age_match.group(0))

            # --- Сбор метрик ---
            # Рейтинг и голоса
            rating_locator = page.locator('div[data-sentry-component="Rating"] p')
            if await rating_locator.count() > 0:
                metrics['rating'] = await rating_locator.first.text_content()

            votes_regex = r'(\d+)\s+голосов'
            votes_locator = page.locator('div[data-sentry-component="Rating"] p').filter(
                has_text=re.compile(votes_regex)
            )
            if await votes_locator.count() > 0:
                votes_match = re.search(votes_regex, await votes_locator.text_content())
                metrics['votes'] = votes_match.group(1)

            badge_metrics_regex = r'[\d\.\,]+(M|K)?'
            badge_metrics_locator = page.locator('div[data-slot="badge"] p').filter(
                has_text=re.compile(badge_metrics_regex)
            )

            views_locator = badge_metrics_locator.filter(
                has_text=re.compile(r'Просмотров:')
            )
            if await views_locator.count() > 0:
                views_match = re.search(badge_metrics_regex, await views_locator.text_content())
                metrics["views"] = views_match.group(0)

            adds_to_lib_locator = badge_metrics_locator.filter(
                has_text=re.compile(r'Закладок:')
            )
            if await adds_to_lib_locator.count() > 0:
                adds_to_lib_match = re.search(badge_metrics_regex, await adds_to_lib_locator.text_content())
                metrics["added_to_lib"] = adds_to_lib_match.group(0)

            likes_locator = badge_metrics_locator.filter(
                has_text=re.compile(r'Лайков:')
            )
            if await likes_locator.count() > 0:
                likes_match = re.search(badge_metrics_regex, await likes_locator.text_content())
                metrics["likes"] = likes_match.group(0)

            tab_metrics_locator = page.locator('button[role="tab"]')

            chapters_count_locator = tab_metrics_locator.filter(
                has_text=re.compile(r'Главы')
            ).filter(
                has_text=re.compile(r'\d+')
            )
            if await chapters_count_locator.count() > 0:
                chapters_match = re.search(r'\d+', await chapters_count_locator.text_content())
                metrics["chapters_count"] = chapters_match.group(0)

            comments_count_locator = tab_metrics_locator.filter(
                has_text=re.compile(r'Обсуждения')
            ).filter(
                has_text=re.compile(r'\d+')
            )
            if await comments_count_locator.count() > 0:
                comments_match = re.search(r'\d+', await comments_count_locator.text_content())
                metrics["comments"] = comments_match.group(0)

            writing_status_locator = page.locator('div.cs-layout-stats-line-item').filter(
                has_text=re.compile(r'Выпуск')
            ).locator('a')
            if await writing_status_locator.count() > 0:
                status = (await writing_status_locator.text_content() or "").strip()
                if status == "Закончен":
                    metrics["status_writing"] = "FINISH"
                elif status == "Продолжается":
                    metrics["status_writing"] = "PROCESS"
                elif status == "Заморожен":
                    metrics["status_writing"] = "PAUSE"
                elif status == "Лицензировано":
                    metrics["status_writing"] = "LICENSE"
                elif status == "Анонс":
                    metrics["status_writing"] = "ANNOUNCE"

            translate_status_locator = page.locator('div.cs-layout-stats-line-item').filter(
                has_text=re.compile(r'Статус перевода')
            ).locator('a')
            if await translate_status_locator.count() > 0:
                status = (await translate_status_locator.text_content() or "").strip()
                if status == "Закончен":
                    metrics["status_translate"] = "FINISH"
                elif status == "Продолжается":
                    metrics["status_translate"] = "PROCESS"
                elif status == "Заморожен":
                    metrics["status_translate"] = "PAUSE"
                elif status == "Нет переводчика":
                    metrics["status_translate"] = "STOP"

            await db.update_book(book)
            await db.create_metrics(metrics)

            return Output(result='done', data={'book': book, 'metrics': metrics})

# Класс для листинга, как в примере, но start_urls изменены на API
# Логика task здесь не нужна, так как первоначальные ссылки получаются через API
class RemangaOrgListing(BaseLivelibWorkflow):
    name = 'livelib-remanga-org-listing'
    event = 'livelib:remanga-org-listing'
    site = 'remanga.org'

    input = InputLivelibBook
    output = Output
    item_wf = RemangaOrgItem

    concurrency=3
    execution_timeout_sec=1800
    backoff_max_seconds=30
    backoff_factor=2

    start_urls = [
        "https://api.remanga.org/api/search/catalog/?content=manga&count=30&ordering=-rating&page=1",
        "https://api.remanga.org/api/search/catalog/?content=manga&count=30&ordering=rating&page=1",
        "https://api.remanga.org/api/search/catalog/?content=manga&count=30&ordering=-id&page=1",
        "https://api.remanga.org/api/search/catalog/?content=manga&count=30&ordering=id&page=1",
        "https://api.remanga.org/api/search/catalog/?content=manga&count=30&ordering=-chapter_date&page=1",
        "https://api.remanga.org/api/search/catalog/?content=manga&count=30&ordering=chapter_date&page=1",
        "https://api.remanga.org/api/search/catalog/?content=manga&count=30&ordering=-votes&page=1",
        "https://api.remanga.org/api/search/catalog/?content=manga&count=30&ordering=votes&page=1",
        "https://api.remanga.org/api/search/catalog/?content=manga&count=30&ordering=-views&page=1",
        "https://api.remanga.org/api/search/catalog/?content=manga&count=30&ordering=views&page=1",
        "https://api.remanga.org/api/search/catalog/?content=manga&count=30&ordering=-count_chapters&page=1",
        "https://api.remanga.org/api/search/catalog/?content=manga&count=30&ordering=count_chapters&page=1",
    ]

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        # Поскольку start_urls ведут на API, мы используем Playwright для запроса и получения JSON
        resp = await page.goto(input.url)
        if not (200 <= resp.status < 400):
            return Output(result='error', data={'status': resp.status})

        data = await resp.json()

        stats = {'new-page-links': 0, 'new-items-links': 0}

        # Обработка пагинации как в JS файле
        url_data = furl(page.url)
        if url_data.args['page'] == '1':
            for page_num in range(2, 1001): # Страницы со 2 по 1000
                url_data.args['page'] = page_num
                if await cls.crawl(url_data.url, input.task_id):
                    stats['new-page-links'] += 1

            for item in data['content']:
                book_url = f"https://remanga.org/manga/{item['dir']}/main"
                # Ставим в очередь задачу для RemangaOrgItem
                if await cls.item_wf.crawl(book_url, input.task_id):
                    stats['new-items-links'] += 1

        return Output(result='done', data=stats)

if __name__ == '__main__':
    RemangaOrgListing.run_sync()
    RemangaOrgListing.debug_sync(RemangaOrgListing.start_urls[0])
    RemangaOrgItem.debug_sync('https://remanga.org/manga/enten/main')
