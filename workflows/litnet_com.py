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


class LitnetItem(BaseLivelibWorkflow):
    name = 'livelib-litnet-item'
    event = 'livelib:litnet-item'
    site = 'litnet.com'

    input = InputLivelibBook
    output = Output

    concurrency = 25

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        resp = await page.goto(input.url, wait_until='domcontentloaded')

        # Проверка URL и статуса (JS: if (response.status() == 404 || !page.url().includes("/book/")))
        if resp.status == 404 or '/book/' not in page.url:
            async with DbSamizdatPrisma() as db:
                await db.mark_book_deleted(page.url, cls.site)
            return Output(result='error', data={'status': resp.status, 'error': 'invalid_url_or_404'})

        await page.wait_for_selector(".main_footer-inform")

        async with DbSamizdatPrisma() as db:
            book = {'url': page.url, 'source': cls.site}
            metrics = {'bookUrl': page.url}

            # Title
            title_locator = page.locator("div.book-view-box h1")
            book['title'] = await title_locator.text_content() if await title_locator.count() > 0 else ""

            if not await db.check_book_exist(page.url):
                await db.create_book(book)

            # --- Сбор основной информации ---

            # Authors
            authors_locator = page.locator("div.book-view-box h2.p > a.author")
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
            annotation_locator = page.locator("div.book-view div#annotation")
            if await annotation_locator.count() > 0:
                # В JS фильтруются текстовые ноды, в Python Playwright берем inner_text, обычно это эквивалентно
                book['annotation'] = (await annotation_locator.inner_text()).strip()

            # Cover
            if not await db.check_book_have_cover(page.url):
                cover_locator = page.locator("div.book-view-box div.book-view-cover > img")
                if await cover_locator.count() > 0:
                    if img_src := await cover_locator.get_attribute('src'):
                        full_img_src = urljoin(page.url, img_src)
                        if img_name := await save_cover(page, full_img_src):
                            book['coverImage'] = img_name

            # Category
            # JS: div.book-view-box p:has(span:contains("Текущий рейтинг:")) a
            category_locator = page.locator("div.book-view-box p").filter(
                has=page.locator("span").filter(has_text=re.compile(r"Текущий рейтинг:"))
            ).locator("a")

            if await category_locator.count() > 0:
                book['category'] = [await x.text_content() for x in await category_locator.all()]

            # Series
            # JS: div.book-view-box p:has(span:contains("Цикл:")) a
            series_locator = page.locator("div.book-view-box p").filter(
                has=page.locator("span").filter(has_text=re.compile(r"Цикл:"))
            ).locator("a")

            if await series_locator.count() > 0:
                book['series'] = [await x.text_content() for x in await series_locator.all()]

            # Tags
            # JS: div.book-view-box p:has(span:contains("В тексте есть:")) a
            tags_locator = page.locator("div.book-view-box p").filter(
                has=page.locator("span").filter(has_text=re.compile(r"В тексте есть:"))
            ).locator("a")

            if await tags_locator.count() > 0:
                book['tags'] = [await x.text_content() for x in await tags_locator.all()]

            # Age Rating
            # JS: div.book-view-box p:has(span:contains("Ограничение:")) span
            age_rating_locator = page.locator("div.book-view-box p").filter(
                has_text=re.compile(r"Ограничение:")
            ).locator("span").filter(
                has_text=re.compile(r'\d{1,2}')
            )
            if await age_rating_locator.count() > 0:
                age_match = re.search(r'\d{1,2}', await age_rating_locator.text_content())
                book['age_rating'] = age_match.group(0)

            # Dates (Release & Update)
            # JS: div.book-view-box p:has(span:contains("Публикация:")) > span
            dates_p = page.locator("div.book-view-box p").filter(
                has=page.locator("span").filter(has_text=re.compile(r"Публикация:"))
            )
            if await dates_p.count() > 0:
                write_dates_text = await dates_p.locator("> span").last.text_content()

                # Release Date
                if release_match := re.search(r'\d{2}\.\d{2}.\d{4}', write_dates_text):
                    book['date_release'] = dateparser.parse(release_match.group(0), date_formats=['%d.%m.%Y'])

                # Content Update Date
                if final_match := re.search(r'— (\d{2}\.\d{2}.\d{4})', write_dates_text):
                    book['date_final'] = dateparser.parse(final_match.group(1), date_formats=['%d.%m.%Y'])

            dete_update_regex = r'В процессе:\s+(.+)'
            date_update_locator = page.locator('.book-view-status').filter(
                has_text=re.compile(dete_update_regex)
            )
            if await date_update_locator.count() > 0:
                if update_match := re.search(dete_update_regex, await date_update_locator.text_content()):
                    metrics['content_update_date'] = dateparser.parse(update_match.group(1), date_formats=['%d.%m.%Y'])


            # --- Metrics ---

            # Rating
            rating_locator = page.locator("div.book-view-box span.book-rating-info-value span")
            if await rating_locator.count() > 0:
                if rating_match := re.search(r'\d+', await rating_locator.text_content()):
                    metrics['rating'] = rating_match.group(0)

            # Views
            views_locator = page.locator("div.book-view-box span.count-views")
            if await views_locator.count() > 0:
                if views_match := re.search(r'\d+', await views_locator.text_content()):
                    metrics['views'] = views_match.group(0)

            # Likes
            likes_locator = page.locator("a.rate-btn-like > span")
            if await likes_locator.count() > 0:
                metrics['likes'] = await likes_locator.text_content()

            # Added to lib
            adds_locator = page.locator("div.book-view-box span.count-favourites")
            if await adds_locator.count() > 0:
                if adds_match := re.search(r'\d+', await adds_locator.text_content()):
                    metrics['added_to_lib'] = adds_match.group(0)

            # Comments
            comments_locator = page.locator("h3.comments-head-title")
            if await comments_locator.count() > 0:
                if comments_match := re.search(r'\d+', await comments_locator.text_content()):
                    metrics['comments'] = comments_match.group(0)

            # Pages count
            # JS: div.book-view-box div.book-view-status span:contains("стр.")
            pages_locator = page.locator("div.book-view-box div.book-view-status span > span").filter(
                has_text=re.compile(r"стр\.")
            )
            if await pages_locator.count() > 0:
                if pages_match := re.search(r'\d+', await pages_locator.text_content()):
                    metrics['pages_count'] = pages_match.group(0)

            # Site Ratings
            # JS: div.book-view-box p:has(span:contains("Текущий рейтинг:"))
            # Здесь JS парсит текстовые ноды. В Python возьмем весь текст и распарсим regex-ом.
            rating_block = page.locator("div.book-view-box p").filter(
                has=page.locator("span").filter(has_text=re.compile(r"Текущий рейтинг:"))
            )
            if await rating_block.count() > 0:
                full_text = await rating_block.inner_text()
                # Пример текста: "Текущий рейтинг: #1 в Фэнтези #5 в Попаданцы"
                # Ищем паттерн #число в категория
                matches = re.findall(r'#(\d+)\s+в\s+([^\n\r#]+)', full_text)
                if matches:
                    metrics['site_ratings'] = {}
                    for rank, category in matches:
                        metrics['site_ratings'][category.strip()] = rank

            # Awards Logic
            awards_tab = page.locator("#js-show_rewards")
            if await awards_tab.count() > 0 and await awards_tab.is_visible():
                await awards_tab.scroll_into_view_if_needed()

                async with page.expect_response(lambda response: "/rewards-tab" in response.url and response.status == 200):
                    await awards_tab.click()

                show_more = page.locator("button#rewards-list-showcase-show-more")
                if await show_more.count() > 0 and await show_more.is_visible():
                    await show_more.click()

                await page.wait_for_timeout(1000)

                awards_items = await page.locator("ul#rewards-list-showcase > li").all()
                if awards_items:
                    metrics['awards'] = {}
                    for item in awards_items:
                        # Key is in p tag
                        k_loc = item.locator("p")
                        v_loc = item.locator("ul > li")

                        if await k_loc.count() > 0 and await v_loc.count() > 0:
                            k_text = (await k_loc.text_content()).strip()
                            v_text = await v_loc.first.text_content()

                            if v_match := re.search(r'[\d\.]+', v_text):
                                metrics['awards'][k_text] = v_match.group(0)

            # Status Writing
            if await page.locator("div.book-view-box span.book-status-process").count() > 0:
                metrics['status_writing'] = "PROCESS"
            elif await page.locator("div.book-view-box span.book-status-full").count() > 0:
                metrics['status_writing'] = "FINISH"

            # Prices
            price_btn_locator = page.locator("div.book-view-box span.ln_btn-get-text").filter(
                has_text=re.compile(r'\d+')
            )
            if await price_btn_locator.count() > 0:
                price_btn_text = await price_btn_locator.text_content()

                if price_match := re.search(r'[\d\.]+', price_btn_text):
                    metrics['price'] = float(price_match.group(0))
                if "Подписка" in price_btn_text:
                    metrics['in_subscribe'] = True

            # Price Old
            price_old_locator = page.locator("div.book-view-box span.get_prise_old")
            if await price_old_locator.count() > 0:
                if price_old_match := re.search(r'[\d\.]+', await price_old_locator.text_content()):
                    metrics['price_old'] = float(price_old_match.group(0))

            # Price Discount
            price_disc_locator = page.locator("div.book-view-box span.ln_btn_get-discount")
            if await price_disc_locator.count() > 0 and price_btn_text:
                 if disc_match := re.search(r'([\d\.]+)\s+(\₽|RUB)', price_btn_text):
                     metrics['price_discount'] = disc_match.group(1) # Сохраняем как строку, как в JS примере (match[1])

            # Audio
            if await page.locator("div.book-view-box span.tw-audio").count() > 0:
                book['url_audio'] = book['url']

                buy_button = page.locator("div.book-view-box a#js-buyModal")
                confirm_age = page.locator("div.book-view-box a#js-age-confirm")

                if await buy_button.count() > 0 and await buy_button.is_visible():
                    await buy_button.click()
                elif await confirm_age.count() > 0 and await confirm_age.is_visible():
                    await confirm_age.click()
                    age_input = page.locator("div.modal-content input#checkadulthoodform-userbirthdate")
                    if await age_input.count() > 0 and await age_input.is_visible():

                        await age_input.type(f'{randint(10,25)}.{randint(10,12)}.{randint(1975,2004)}')
                        await page.click('div.modal-content button[type="submit"]')

                        next_btn = page.locator("div.modal-content button#btnIfSubscriptionNext")
                        # JS просто кликает, добавим проверку видимости на всякий случай, как в JS
                        await next_btn.click()

                try:
                    async with page.expect_response(lambda response: "/popup-buy" in response.url and response.status == 200, timeout=5000):
                        pass # Wait for response triggered by previous clicks
                except Exception:
                    pass # Ignore timeout if already loaded

                await page.wait_for_timeout(2000)

                # Price Audio inside modal
                # JS: div.modal-content div.extra-cart-option:has(p:contains("Аудиоверсия книги")) div[data-value]
                price_audio_div = page.locator("div.modal-content div.extra-cart-option").filter(
                    has=page.locator("p").filter(has_text=re.compile(r"Аудиоверсия книги"))
                ).locator("div[data-value]")

                if await price_audio_div.count() > 0:
                    metrics['price_audio'] = await price_audio_div.get_attribute("data-value")
                else:
                    # JS: throw new Error
                    print("Error: Нет отобразилась цена на аудиокнигу")
                    # В Python workflow лучше не кидать исключение, которое остановит воркер, если это не критично
                    # Но следуем JS логике - если критично, можно оставить print или raise

            await db.update_book(book)
            await db.create_metrics(metrics)

            return Output(result='done', data={'book': book, 'metrics': metrics})


class LitnetListing(BaseLivelibWorkflow):
    name = 'livelib-litnet-listing'
    event = 'livelib:litnet-listing'
    site = 'litnet.com'

    input = InputLivelibBook
    output = Output
    item_wf = LitnetItem

    concurrency=3
    execution_timeout_sec=300
    backoff_max_seconds=30
    backoff_factor=2

    start_urls = ["https://superapi.litnet.com/v2/genres/top?limit=20&offset=0&sort=rate&sortDirection=DESC"]

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        print(input.url)
        resp = await page.goto(input.url, wait_until='domcontentloaded')
        stats = {'new-page-links': 0, 'new-items-links': 0}

        if 'superapi.litnet.com' in input.url:
            data = await resp.json()
            url_data = furl(page.url)

            for item in data['items']:
                if await LitnetItem.crawl(f'https://litnet.com/ru/book/{item['alias']}', input.task_id):
                    stats['new-items-links'] += 1

            if url_data.args['offset'] == '0':
                total_items = int(data['total'])
                for offset in range(20, total_items + 20, 20):
                    url_data.args['offset'] = offset
                    if await cls.crawl(url_data.url, input.task_id):
                        stats['new-page-links'] += 1

        else:
            await page.wait_for_selector(".main_footer-inform")

            # Обработка пагинации
            # JS globs: ["https://litnet.com/ru/top/all?alias=all&page=*"]
            # Selector: "ul.pagination a"
            pagination_links = await page.locator("ul.pagination a").all()
            for link in pagination_links:
                href = await link.get_attribute('href')
                if href:
                    page_url = urljoin(page.url, href)
                    # Простая проверка на паттерн (наличие page=)
                    if 'page=' in page_url:
                        if await cls.crawl(page_url, input.task_id):
                            stats['new-page-links'] += 1

            # Обработка книг
            # JS selector: "h4.book-title a", label: "book"
            book_links = await page.locator("h4.book-title a").all()
            for link in book_links:
                href = await link.get_attribute('href')
                if href:
                    book_url = urljoin(page.url, href)
                    if '/book/' in book_url:
                        if await LitnetItem.crawl(book_url, input.task_id):
                            stats['new-items-links'] += 1

        return Output(result='done', data=stats)


if __name__ == '__main__':
    # LitnetListing.run_sync()
    import asyncio
    asyncio.run(LitnetListing.run_cron())
    # Для отладки
    # LitnetListing.debug_sync(LitnetListing.start_urls[0])
    # LitnetListing.debug_sync('https://litnet.com/ru/authors/%D0%90%D0%BD%D0%B4%D1%80%D0%B5%D0%B9%20%D0%91%D0%B5%D0%BB%D1%8F%D0%BD%D0%B8%D0%BD-t119205')
    LitnetListing.debug_sync('https://litnet.com/ru/anya-istomina-u11251559')
    LitnetItem.debug_sync('https://litnet.com/ru/book/my-nevozmozhny-b564772')
    LitnetItem.debug_sync('https://litnet.com/ru/book/posle-razvoda-desyat-let-spustya-b547174')
