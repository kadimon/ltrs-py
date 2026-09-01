import re
from urllib.parse import urljoin

import dateparser
from playwright.async_api import Page

from db import DbSamizdatPrisma
from interfaces import InputLivelibBook, Output
from utils import save_cover
from workflow_base import BaseLivelibWorkflow

# Один проход по DOM вместо ~35 locator-вызовов. Каждый locator — это отдельный
# round-trip в браузер с перепроверкой актуальности узла, а `:has()` +
# `:text-is()` Playwright резолвит своим движком поверх querySelectorAll, то есть
# на каждое поле шёл повторный обход всего b-db_entry. Здесь весь разбор
# страницы — одна сериализация результата.
ITEM_JS = r'''() => {
    const norm = el => el ? el.textContent.replace(/\s+/g, ' ').trim() : null;
    const raw = el => el ? el.textContent.trim() : null;
    const attr = (el, name) => el ? el.getAttribute(name) : null;

    const entry = document.querySelector('div.b-db_entry');

    // Замена для 'div.b-db_entry div.line-container:has(div.key:text-is("X")) div.value'.
    // Ключи собираем в Map один раз, дальше это lookup, а не обход DOM.
    // Двоеточие в ключе снимаем: разметка отдаёт "Тип:", старые селекторы
    // писались без него.
    const lines = new Map();
    for (const line of document.querySelectorAll('div.b-db_entry div.line-container')) {
        const key = (norm(line.querySelector('div.key')) ?? '').replace(/:$/, '');
        if (key && !lines.has(key)) {
            lines.set(key, line.querySelector('div.value'));
        }
    }
    // Список ключей, а не один: сайт переехал на множественное число
    // ("Авторы", "Переводчики"), старые варианты оставлены как запасные.
    const value = keys => {
        for (const key of keys) {
            const found = lines.get(key);
            if (found) return found;
        }
        return null;
    };
    const people = keys => {
        const el = value(keys);
        if (!el) return null;
        const links = [...el.querySelectorAll('ul > li > a')];
        return links.length ? links.map(a => ({name: norm(a), href: attr(a, 'href')})) : null;
    };

    // Замена для 'div.secondaryContent:has(h3:text-is("X"))'
    const sections = [...document.querySelectorAll('div.secondaryContent')];
    const section = heading => sections.find(el => norm(el.querySelector('h3')) === heading) ?? null;
    const votes = section('Оценки пользователей');

    // Замена для 'div.secondaryContent div.line:has(div.x_label:text-is("X")) div.bar':
    // все шесть подписей блока «В списках» забираем за один обход.
    const bars = {};
    for (const line of document.querySelectorAll('div.secondaryContent div.line')) {
        const label = norm(line.querySelector('div.x_label'));
        const title = attr(line.querySelector('div.bar[title]'), 'title');
        if (label && title !== null && !(label in bars)) {
            bars[label] = title;
        }
    }

    // На выдаче постер лениво подгружается скриптом, до этого в src лежит
    // заглушка 'data:,' — тогда берём тот же файл из og:image.
    let cover = attr(document.querySelector('div.c-poster img'), 'src');
    if (!cover || cover.startsWith('data:')) {
        cover = attr(document.querySelector('meta[property="og:image"]'), 'content');
    }

    const status = value(['Статус']);
    const translate = value(['Перевод']);

    return {
        // --- Основная информация ---
        title: norm(document.querySelector('h1 span.rus-name')),
        title_original: norm(document.querySelector('h1 span.name')),
        authors: people(['Авторы', 'Автор']),
        artists: people(['Художник', 'Художники']),
        translators: people(['Переводчики', 'Переводчик']),
        annotation: raw(document.querySelector('div[itemprop="description"]')),
        cover: cover,
        genres: entry ? [...entry.querySelectorAll('a[itemprop="genre"]')].map(norm) : [],
        date_release: norm(status),
        artwork_type: norm(value(['Тип'])),
        age_18_plus: document.querySelector('div.c-poster.age_18_plus') !== null,

        // --- Метрики ---
        rating: norm(entry ? entry.querySelector('div.score-value') : null),
        votes: votes ? [...votes.querySelectorAll('div.bar[title]')].map(el => attr(el, 'title')) : [],
        views: norm(value(['Просмотров'])),
        added_to_lib: norm(document.querySelector('h3.textWithCount span.count')),
        comments: attr(document.querySelector('div.desu-comments-shell[data-comment-count]'), 'data-comment-count'),
        chapters: norm(document.querySelector('a.read-ch-online')),
        status_writing: status && status.querySelector('span.released') ? 'FINISH'
            : status && status.querySelector('span.ongoing') ? 'PROCESS' : null,
        status_translate: translate && translate.querySelector('span.completed') ? 'FINISH'
            : translate && translate.querySelector('span.continued') ? 'PROCESS' : null,
        bars: bars,
    };
}'''

# Пагинация и карточки одним вызовом: PageNav отрисован дважды (над и под
# списком), поэтому ссылки приходят с дублями — их снимает dict.fromkeys.
LISTING_JS = r'''() => ({
    pages: [...document.querySelectorAll('div.PageNav a')]
        .map(a => a.getAttribute('href'))
        .filter(Boolean),
    items: [...document.querySelectorAll('ol.memberList h3 a')]
        .map(a => a.getAttribute('href'))
        .filter(Boolean),
})'''

# Подписи блока «В списках» -> поля метрик
READ_STATS = (
    ('read_process', 'Читаю'),
    ('read_stoped', 'Брошено'),
    ('read_on_pause', 'Отложено'),
    ('read_later', 'Запланировано'),
    ('read_finished', 'Прочитано'),
    ('likes', 'Любимое'),
)


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

        data = await page.evaluate(ITEM_JS)

        async with DbSamizdatPrisma() as db:
            book = {'url': page.url, 'source': cls.site}
            metrics = {'bookUrl': page.url}

            # --- Основная информация ---

            # Title
            if data['title']:
                book['title'] = data['title']

            if not await db.check_book_exist(page.url):
                await db.create_book(book)

            # Title Original
            if data['title_original']:
                book['title_original'] = data['title_original']

            # Authors
            if data['authors']:
                book['author'] = ', '.join([a['name'] for a in data['authors']])
                book['authors_data'] = [
                    {'name': a['name'], 'url': urljoin(page.url, a['href'])}
                    for a in data['authors']
                ]

            # Artists
            if data['artists']:
                book['artist'] = ', '.join([a['name'] for a in data['artists']])
                book['artists_data'] = [
                    {'name': a['name'], 'url': urljoin(page.url, a['href'])}
                    for a in data['artists']
                ]

            # Translators
            if data['translators']:
                book['translate'] = ', '.join([t['name'] for t in data['translators']])
                book['translators_data'] = [
                    {'name': t['name'], 'url': urljoin(page.url, t['href'])}
                    for t in data['translators']
                ]

            # Annotation
            if data['annotation']:
                book['annotation'] = data['annotation']

            # Cover
            if not await db.check_book_have_cover(page.url):
                if cover_url := data['cover']:
                    full_cover_url = urljoin(page.url, cover_url)
                    if cover_name := await save_cover(page, full_cover_url):
                        book['coverImage'] = cover_name

            # Tags & Categories
            if data['genres']:
                book['category'] = []
                book['tags'] = []
                for value in data['genres']:
                    if value.startswith('#'):
                        book['tags'].append(re.sub(r'^#\s+', '', value))
                    else:
                        book['category'].append(value)

            # Release Year
            # В значении лежит "выходит с 2021 г." — dateparser на такой строке
            # целиком отдаёт None, поэтому сначала вытаскиваем год. Дня и месяца
            # разметка не содержит, прибиваем к 1 января.
            if data['date_release']:
                if year_match := re.search(r'\d{4}', data['date_release']):
                    book['date_release'] = dateparser.parse(f'{year_match.group(0)}-01-01')

            # Artwork Type
            if artwork_type := data['artwork_type']:
                book['artwork_type'] = artwork_type

            # Age Rating
            if data['age_18_plus']:
                book['age_rating'] = '18'

            # --- Метрики ---

            # Rating
            if data['rating']:
                if rating_match := re.search(r'[\d.]+', data['rating']):
                    if rating_match.group(0) != '0':
                        metrics['rating'] = rating_match.group(0)

            # Votes
            if data['votes']:
                metrics['votes'] = 0
                for v_title in data['votes']:
                    if v_match := re.search(r'\d+', v_title):
                        metrics['votes'] += int(v_match.group(0))

            # Views
            if data['views']:
                if views_match := re.search(r'\d+', data['views']):
                    metrics['views'] = views_match.group(0)

            # Added to lib
            adds_text = data['added_to_lib']
            if adds_text and adds_text != '0':
                metrics['added_to_lib'] = adds_text

            # Comments
            comments = data['comments']
            if comments and comments != '0':
                metrics['comments'] = comments

            # Chapters Count
            if data['chapters']:
                if chapters_match := re.search(r'Глава\s+(\d+)', data['chapters']):
                    if chapters_match.group(1) != '0':
                        metrics['chapters_count'] = chapters_match.group(1)

            # Status Writing
            if data['status_writing']:
                metrics['status_writing'] = data['status_writing']

            # Status Translate
            if data['status_translate']:
                metrics['status_translate'] = data['status_translate']

            # Read Process / Stopped / On Pause / Later / Finished, Likes
            for field, label in READ_STATS:
                if bar := data['bars'].get(label):
                    metrics[field] = bar

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

        data = await page.evaluate(LISTING_JS)

        # Pagination
        for href in dict.fromkeys(data['pages']):
            if await cls.crawl(urljoin(page.url, href), input.task_id):
                stats['new-page-links'] += 1

        # Books
        for href in dict.fromkeys(data['items']):
            if await DesuItem.crawl(urljoin(page.url, href), input.task_id):
                stats['new-items-links'] += 1

        return Output(result='done', data=stats)


if __name__ == '__main__':
    DesuListing.run_sync()
    DesuListing.debug_sync(DesuListing.start_urls[0])
    # DesuItem.debug_sync('https://desu.uno/manga/the-reversal-of-my-life-as-a-mob-character.6563/')
