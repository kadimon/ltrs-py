import re
from urllib.parse import urljoin, urlparse

import dateparser
from furl import furl
from hatchet_sdk import Context
from playwright.async_api import Page
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from db import DbSamizdatPrisma
from interfaces import InputLivelibBook, Output, WorkerLabels
from utils import save_cover
from workflow_base import BaseLivelibWorkflow

# Вёрстка переехала на CSS-модули: классы теперь вида
# `ContentInfo-module__<hash>__container`, хэш меняется от сборки к сборке.
# Поэтому всё, что можно, цепляем за data-test-id, а по классу ищем только
# по неизменной части имени (`[class*="ContentPreview"][class*="info"]`).

BOOK_PATH_RE = re.compile(r'^/(books|audiobooks|comicbooks)/[^/]+/?$')
LISTING_PATH_RE = re.compile(r'^/(section|showcase|topic|library|series)/')


def clean_url(page_url: str, href: str) -> str:
    """Абсолютный URL без query и якоря.

    У карточек в блоках витрины к ссылке приклеен `?utm_place=post_slider`,
    из-за чего одна и та же книга уезжала бы в очередь как отдельная задача:
    дедупликация в `crawl` считает хэш от строки URL.
    """
    return furl(urljoin(page_url, href)).remove(args=True, fragment=True).url


def parse_price(text: str | None) -> str | None:
    """`Купить книгу за 499 ₽` / `499 ₽` -> `499`.

    До базы цена доезжает через `str2float`, а он не переживает ни символ
    валюты, ни неразрывный пробел в разряде тысяч.
    """
    if not text:
        return None
    if match := re.search(r'\d[\d\s\u00a0.,]*', text):
        return re.sub(r'[^\d.]', '', match.group(0).replace(',', '.')).rstrip('.') or None
    return None


async def scroll_to_tail(
    page: Page,
    *,
    steps: int = 4,
    delta: int = 1_500,
    pause: int = 250,
) -> None:
    """Прокрутка в конец страницы без locator-действий (взято из search.py).

    Не используем hover / focus / scroll_into_view_if_needed:
      - hover проверяет, что элемент получает события мыши, и падает на
        фиксированных шапках и оверлеях;
      - focus не делает никаких проверок, но на div без tabindex не двигает
        фокус вообще, из-за чего он остаётся в поисковом поле и последующий
        End уходит в инпут, а не в документ.
    Mouse API работает на уровне ввода: элементы не резолвит, на перехвате
    событий не падает. Шагами, а не одним прыжком — подгрузка и ленивые
    картинки висят на IntersectionObserver, которому нужен кадр на срабатывание.
    """
    box = page.viewport_size or {'width': 1280, 'height': 720}
    # курсор по умолчанию в (0, 0), то есть над шапкой — колесо ушло бы в неё
    await page.mouse.move(box['width'] / 2, box['height'] / 2)

    for _ in range(steps):
        await page.mouse.wheel(0, delta)
        await page.wait_for_timeout(pause)

    # добиваем из JS: снимаем фокус с инпута и доводим до низа тот контейнер,
    # который реально прокручивается (у SPA это часто не документ)
    await page.evaluate('''() => {
        const active = document.activeElement;
        if (active && active !== document.body && typeof active.blur === 'function') {
            active.blur();
        }
        const doc = document.scrollingElement;
        if (doc && doc.scrollHeight > doc.clientHeight + 200) {
            doc.scrollTop = doc.scrollHeight;
            return;
        }
        for (const el of document.querySelectorAll('*')) {
            const s = getComputedStyle(el);
            if (/(auto|scroll)/.test(s.overflowY) && el.scrollHeight > el.clientHeight + 200) {
                el.scrollTop = el.scrollHeight;
                return;
            }
        }
    }''')


class BookmateItem(BaseLivelibWorkflow):
    name = 'livelib-bookmate-item'
    event = 'livelib:bookmate-item'
    site = 'bookmate.com'

    labels = WorkerLabels(ip='ru')

    input = InputLivelibBook
    output = Output

    concurrency = 25

    @staticmethod
    def info_value(page: Page, label: str):
        """Значение строки инфо-блока по её подписи.

        Контейнер берём по data-test-id: класс стал
        `ContentInfo-module__<hash>__container`, и старый `ContentInfo_container`
        не находил ничего — молча терялся весь блок целиком (серия, дата,
        правообладатель, издатель, переводчик, рассказчик, возраст, страницы,
        длительность).
        """
        return page.locator(
            'div[data-test-id="CONTENT_INFO"] div[data-test-id="CONTENT_INFO_ITEM"]'
        ).filter(
            has=page.locator('span[class*="label"]').filter(has_text=re.compile(label))
        ).locator('span[class*="value"]')

    @staticmethod
    async def collect_links(page: Page, locator) -> list[dict]:
        """Ссылки локатора в вид `[{'name': ..., 'url': ...}]` — как authors_data.

        Дедупликация по URL: у книги встречаются две разные подборки с
        одинаковым названием («Фэнтези»), и это разные сущности.
        """
        links = {}
        for link in await locator.all():
            href = await link.get_attribute('href')
            text = await link.text_content()
            if not href or not text or not text.strip():
                continue
            links[clean_url(page.url, href)] = text.strip()

        return [{'name': name, 'url': url} for url, name in links.items()]

    @staticmethod
    async def exact_count(page: Page, label: str) -> str | None:
        """Точный счётчик из заголовка секции.

        На вкладках числа округлены (`Впечатления 17.3K`), а в заголовке лежит
        точное значение (`Впечатления 17253`). Именно округление давало
        расхождение счётчиков между обходами.
        """
        counter = page.locator('[data-test-id="LINK_TITLE"]').filter(
            has_text=re.compile(label)
        ).locator('[data-test-id="LINK_TITLE_COUNT"]')

        if await counter.count() > 0:
            return (await counter.first.text_content()).strip()

        return None

    @staticmethod
    def event_price(ctx: Context | None) -> str | None:
        """Цена, снятая в листинге и уехавшая в метаданных события.

        `additional_metadata` отдаётся None, если метаданных не было, а при
        прогоне через `debug_sync` контекста нет вообще.
        """
        if ctx is None:
            return None

        return (ctx.additional_metadata or {}).get('price')

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page, ctx: Context | None = None) -> Output:
        resp = await page.goto(input.url, wait_until='domcontentloaded')

        await page.wait_for_selector('h1')

        # Проверка URL и статуса
        error_locator = page.locator('h1[class*="ContentErrorPageTitle"]')
        if resp.status == 404 or await error_locator.count() > 0:
            async with DbSamizdatPrisma() as db:
                await db.mark_book_deleted(page.url, cls.site)
            return Output(result='error', data={'status': resp.status, 'error': 'invalid_url_or_404'})

        await page.wait_for_selector("div.main-content h1")

        async with DbSamizdatPrisma() as db:
            book = {'url': page.url, 'source': cls.site}
            metrics = {'bookUrl': page.url}

            # Title
            title_locator = page.locator('h1 span[data-test-id="CONTENT_TITLE_MAIN"]')
            if await title_locator.count() > 0:
                book['title'] = (await title_locator.first.text_content()).strip()

            if not await db.check_book_exist(page.url):
                await db.create_book(book)

            # Authors
            authors_locator = page.locator('h1 a[data-test-id="CONTENT_AUTHOR_AUTHOR_NAME"]')
            if await authors_locator.count() > 0:
                book['author'] = ', '.join([await a.text_content() for a in await authors_locator.all()]).strip()
                book['authors_data'] = await cls.collect_links(page, authors_locator)

            # Annotation
            annotation_locator = page.locator('div[class*="ExpandableText"] > span')
            if await annotation_locator.count() > 0:
                book['annotation'] = (await annotation_locator.first.text_content()).strip()

            # Cover
            if not await db.check_book_have_cover(page.url):
                cover_locator = page.locator('[data-test-id="CONTENT_LEFT_COLUMN"] div[data-test-id="COVER"] img')
                if await cover_locator.count() > 0:
                    img_src = await cover_locator.first.get_attribute('src')
                    if img_src and "empty_cover" not in img_src:
                        full_img_src = urljoin(page.url, img_src)
                        if img_name := await save_cover(page, full_img_src):
                            book['coverImage'] = img_name

            # Category — те самые «теги» из ТЗ, чипсы под описанием
            category_locator = page.locator('a[data-test-id="CONTENT_TOPICS_SLUG"]')
            if await category_locator.count() > 0:
                book['category'] = [(await x.text_content()).strip() for x in await category_locator.all()]
                book['categories_data'] = await cls.collect_links(page, category_locator)

            # Series
            serie_locator = cls.info_value(page, r"Серия|Серии")
            if await serie_locator.count() > 0:
                book['series'] = [(await x.text_content()).strip() for x in await serie_locator.all()]
                book['series_data'] = await cls.collect_links(page, serie_locator.locator('a'))

            # Release Date
            release_date_loc = cls.info_value(page, r"Дата публикации|Год выхода")
            if await release_date_loc.count() > 0:
                if rd_match := re.search(r'\d{4}', await release_date_loc.first.text_content()):
                    book['date_release'] = dateparser.parse(rd_match.group(0), date_formats=['%Y'])

            # Owner
            owner_loc = cls.info_value(page, r"Правообладатель")
            if await owner_loc.count() > 0:
                book['owner'] = (await owner_loc.first.text_content()).strip()

            # Publisher
            publisher_loc = cls.info_value(page, r"Издатель")
            if await publisher_loc.count() > 0:
                book['publisher'] = (await publisher_loc.first.text_content()).strip()

            # Translator
            translator_loc = cls.info_value(page, r"Переводчик")
            if await translator_loc.count() > 0:
                book['translate'] = (await translator_loc.first.text_content()).strip()

            # Artist
            artist_loc = cls.info_value(page, r"Художник")
            if await artist_loc.count() > 0:
                book['artist'] = (await artist_loc.first.text_content()).strip()

            # Voice
            voice_loc = cls.info_value(page, r"Рассказчик")
            if await voice_loc.count() > 0:
                book['voice'] = (await voice_loc.first.text_content()).strip()

            # Age Rating
            age_rating_loc = cls.info_value(page, r"Возрастные ограничения")
            if await age_rating_loc.count() > 0:
                book['age_rating'] = (await age_rating_loc.first.text_content()).strip()

            # Audio Button
            button_audio_locator = page.locator('a[data-test-id="CONTENT_SYNC_TAB_AUDIO"]:not([data-tab-active="true"])')
            if await button_audio_locator.count() > 0:
                if url_audio := await button_audio_locator.get_attribute('href'):
                    book['url_audio'] = urljoin(page.url, url_audio)

            # --- Metrics ---

            # Read Process. Точного числа «Читают»/«Слушают» на странице нет
            # нигде — только округлённое на вкладке, `str2int` разложит `2K`.
            read_process_loc = page.locator(
                'span[data-test-id="CONTENT_TAB_READERS_COUNTER"], '
                'span[data-test-id="CONTENT_TAB_LISTENERS_COUNTER"]'
            )
            if await read_process_loc.count() > 0:
                metrics['read_process'] = (await read_process_loc.first.text_content()).strip()

            # Comments (Впечатления)
            if comments := await cls.exact_count(page, r"Впечатлени"):
                metrics['comments'] = comments
            else:
                comments_loc = page.locator('span[data-test-id="CONTENT_TAB_IMPRESSIONS_COUNTER"]')
                if await comments_loc.count() > 0:
                    metrics['comments'] = (await comments_loc.first.text_content()).strip()

            # Quotes (Цитаты)
            if quotes := await cls.exact_count(page, r"Цитат"):
                metrics['quotes'] = quotes
            else:
                quotes_loc = page.locator('span[data-test-id="CONTENT_TAB_QUOTES_COUNTER"]')
                if await quotes_loc.count() > 0:
                    metrics['quotes'] = (await quotes_loc.first.text_content()).strip()

            # Added to library (На полке)
            if added_to_lib := await cls.exact_count(page, r"На\s*полке"):
                metrics['added_to_lib'] = added_to_lib

            # Subscription. Скоупим внутрь блока кнопок: в шапке страницы
            # висит свой Плюс-баннер, он есть вообще везде.
            plus_button_loc = page.locator(
                '[data-test-id="CONTENT_INTERACTION_BUTTONS"] '
                '[data-test-id="CONTENT_INTERACTION_PLUS_BUTTON"]'
            )
            metrics['in_subscribe'] = await plus_button_loc.count() > 0

            # Price. `449 ₽` рядом с Плюс-кнопкой — это цена подписки, а не
            # книги, берём только кнопку покупки. У части книг её на карточке
            # нет вовсе — такая цена приезжает в метаданных события из листинга.
            buy_button_loc = page.locator('[data-test-id="CONTENT_INTERACTION_PPD_BUY_BUTTON"]')
            if await buy_button_loc.count() > 0:
                metrics['price'] = parse_price(await buy_button_loc.first.text_content())
            elif listing_price := cls.event_price(ctx):
                metrics['price'] = parse_price(listing_price)

            # Pages count
            pages_count_loc = cls.info_value(page, r"страниц")
            if await pages_count_loc.count() > 0:
                if pc_match := re.search(r'\d+', await pages_count_loc.first.text_content()):
                    metrics['pages_count'] = pc_match.group(0)

            # Duration
            duration_loc = cls.info_value(page, r"Длительность")
            if await duration_loc.count() > 0:
                dur_text = await duration_loc.first.text_content()
                hours, minutes = 0, 0
                if h_match := re.search(r'(\d{1,4})\s+ч', dur_text):
                    hours = int(h_match.group(1))
                if m_match := re.search(r'(\d{1,2})\s+мин', dur_text):
                    minutes = int(m_match.group(1))
                metrics['duration'] = hours * 3600 + minutes * 60

            # Awards. Скоуп по EMOTION_RATING обязателен: такие же иконки
            # эмоций стоят в каждом пользовательском отзыве ниже по странице.
            awards_rows = page.locator('[data-test-id="EMOTION_RATING"] div[class*="__emotion"]').filter(
                has=page.locator('span[data-test-id="EMOTION_RATING_EMOTION_ICON"]')
            ).filter(
                has=page.locator('span[class*="Emotion-module"][class*="count"]')
            )
            if await awards_rows.count() > 0:
                metrics['awards'] = {}
                for row in await awards_rows.all():
                    award_loc = row.locator('span[data-test-id="EMOTION_RATING_EMOTION_ICON"]')
                    value_loc = row.locator('span[class*="Emotion-module"][class*="count"]')
                    if await award_loc.count() > 0 and await value_loc.count() > 0:
                        award = (await award_loc.first.get_attribute('title')).strip()
                        if val_match := re.search(r'\d+', await value_loc.first.text_content()):
                            metrics['awards'][award] = val_match.group(0)

            await db.update_book(book)
            await db.create_metrics(metrics)

            # --- Crawl book formats ---
            format_tab_locator = page.locator('a[data-test-id^="CONTENT_SYNC_TAB"]:not([data-tab-active="true"])')
            for tab_locator in await format_tab_locator.all():
                if url_tab := await tab_locator.get_attribute('href'):
                    await cls.crawl(
                        clean_url(page.url, url_tab),
                        input.task_id
                    )

            return Output(result='done', data={'book': book, 'metrics': metrics})


class BookmateListing(BaseLivelibWorkflow):
    name = 'livelib-bookmate-listing'
    event = 'livelib:bookmate-listing'
    site = 'bookmate.com'

    labels = WorkerLabels(ip='ru')

    input = InputLivelibBook
    output = Output
    item_wf = BookmateItem

    concurrency = 4
    execution_timeout_sec = 7_200
    backoff_max_seconds = 30
    backoff_factor = 2

    # Сниппеты секций и карточки каруселей витрины — разная вёрстка, но обе
    # дают ссылку на книгу; цена есть только у сниппетов.
    cards_locator = '[data-test-id="SNIPPET"], [data-test-id="CONTENT_PREVIEW"]'
    cards_js = '''els => els.map(el => {
        const link = el.querySelector(
            '[data-test-id="SNIPPET_TITLE"] a, [class*="ContentPreview"][class*="info"] > a[title]'
        );
        const price = el.querySelector('[data-test-id="PPD_PRICE_BADGE"]');
        return {
            href: link ? link.href : null,
            price: price ? price.textContent.trim() : null,
        };
    })'''

    start_urls = [
        "https://books.yandex.ru/books",
        "https://books.yandex.ru/audiobooks",
        "https://books.yandex.ru/comicbooks",
        "https://books.yandex.ru/library/t-detyam-ru",
    ]

    cron_urls = [
        'https://books.yandex.ru/section/all/novinki-uQfUIsur',
        'https://books.yandex.ru/section/audiobook/novinki_2_0-ZecJScMc',
        'https://books.yandex.ru/section/all/mozhno-kupit-otdelno-piGPhH4m',
    ]

    @classmethod
    async def collect_cards(
        cls,
        page: Page,
        idle_rounds: int = 2,
        pending_rounds: int = 4,
        scroll_timeout: int = 10_000,
    ) -> dict[str, str | None]:
        """Скроллит выдачу и копит `url -> цена` до конца списка.

        Позиция карточки в DOM не идентификатор: список пересобирается, и
        вставка выше хвоста сдвигает уже обработанные карточки. Поэтому
        забираем список целиком через evaluate_all и отсеиваем виденное по
        URL, а конец определяем по отсутствию новых карточек, а не по
        счётчику — тот расходился с реальным DOM и рвал цикл на середине.
        """
        results_locator = page.locator(cls.cards_locator)

        books: dict[str, str | None] = {}
        idle = 0
        pending_waits = 0

        while True:
            blocks = await results_locator.evaluate_all(cls.cards_js)
            count_before = len(blocks)

            fresh = 0
            pending = 0
            for block in blocks:
                if not block['href']:
                    # карточка отрисована, но данных в ней ещё нет
                    pending += 1
                    continue

                url = clean_url(page.url, block['href'])
                if not BOOK_PATH_RE.match(urlparse(url).path):
                    continue

                price = parse_price(block['price'])
                if url in books:
                    # цену могла отдать не первая отрисовка карточки
                    if price and not books[url]:
                        books[url] = price
                    continue

                books[url] = price
                fresh += 1

            if fresh:
                idle = 0
                pending_waits = 0
            elif pending and pending_waits < pending_rounds:
                # НЕ конец выдачи: узлы есть, площадка их дозаполняет.
                # Ждём и перечитываем, не трогая скролл и не засчитывая idle.
                pending_waits += 1
                await page.wait_for_timeout(1_000)
                continue
            else:
                idle += 1
                if idle >= idle_rounds:
                    return books

            await scroll_to_tail(page)

            try:
                await results_locator.nth(count_before).wait_for(
                    state='attached', timeout=scroll_timeout,
                )
            except PlaywrightTimeoutError:
                # Список мог перерисоваться без роста count (виртуализация,
                # замена узлов). Не выходим сразу — дубликаты отсечёт `books`,
                # а выход даст idle_rounds.
                await page.wait_for_timeout(1_000)

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        stats = {'new-page-links': 0, 'new-items-links': 0, 'prices-found': 0}

        await page.goto(
            input.url,
            wait_until='domcontentloaded',
        )

        await page.wait_for_selector("div.main-content h1")

        books = await cls.collect_cards(page)
        stats['prices-found'] = sum(1 for price in books.values() if price)

        # Бабл-навигация витрины, ссылки «Показать все» у каруселей и
        # переключатели формата секции различаются только классами с
        # плавающим хэшем, поэтому фильтруем по пути.
        # Тем же проходом добираем книги, которые лежат мимо карточек:
        # в промо-слайдере витрины их около трети от страницы.
        hrefs = await page.locator('a[href]').evaluate_all('els => els.map(el => el.href)')

        for href in dict.fromkeys(hrefs):
            url = clean_url(page.url, href)
            path = urlparse(url).path

            if LISTING_PATH_RE.match(path):
                pass
                # if await cls.crawl(url, input.task_id):
                #     stats['new-page-links'] += 1
            elif BOOK_PATH_RE.match(path):
                books.setdefault(url, None)

        # Обработка книг. Цену отдаём в метаданных события: на самой карточке
        # её у многих книг нет, а в payload ей не место — задача на книгу
        # описывается только своим URL.
        for book_url, price in books.items():
            if await BookmateItem.crawl(
                book_url,
                input.task_id,
                metadata={'price': price},
            ):
                stats['new-items-links'] += 1

        return Output(result='done' if books else 'empty', data=stats)


if __name__ == '__main__':
    # BookmateListing.run_sync()
    BookmateListing.run_cron_sync()
    # Для отладки
    # BookmateListing.debug_sync(BookmateListing.start_urls[0])
    # BookmateListing.debug_sync('https://books.yandex.ru/section/all/mozhno-kupit-otdelno-piGPhH4m')
    # BookmateListing.debug_sync('https://books.yandex.ru/section/all/uyutnye-detektivy-qGulE45y')
    # BookmateListing.debug_sync('https://books.yandex.ru/section/audiobook/sovremennaya-russkaya-proza-XHwMYsO6')
    # BookmateItem.debug_sync('https://books.yandex.ru/books/k5ZjBit1')
    # BookmateItem.debug_sync('https://books.yandex.ru/books/FwogPVbZ')
    # BookmateItem.debug_sync('https://books.yandex.ru/audiobooks/VIitWf9R')
