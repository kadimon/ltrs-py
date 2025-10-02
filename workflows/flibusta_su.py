from urllib.parse import urljoin

from playwright.async_api import Page

from workflow_base import BaseLitresPartnersWorkflow
from interfaces import InputLitresPartnersBook, Output, WorkerLabels
from db import save_book_mongo


class FlibustaSu(BaseLitresPartnersWorkflow):
    name = 'ltrs-flibusta-su'
    event = 'ltrs:flibusta-su'
    site='flibusta.su'
    url_patern = r'^https:\/\/flibusta\.su\/book\/\d+-[\w-]+\/$'
    input = InputLitresPartnersBook
    output = Output

    @classmethod
    async def task(cls, input: InputLitresPartnersBook, page: Page) -> Output:
        await page.goto(
            input.url,
            wait_until='domcontentloaded',
        )

        await page.wait_for_selector('h1')

        book = {
            'title': await page.text_content('h1'),
            'author': await page.text_content('a[itemprop="author"]'),
        }

        litres_reader_locator = page.locator('div[class="btn list litres"] a')
        if await litres_reader_locator.count() > 0:
            litres_reader_href = await litres_reader_locator.get_attribute('href')
            book['reader-litres'] = [urljoin(page.url, litres_reader_href)]

        await save_book_mongo(input, cls.site, book)

        return Output(
            result='done',
            data=book,
        )

if __name__ == '__main__':
    FlibustaSu.run_sync()

    FlibustaSu.debug_sync('https://flibusta.su/book/412375-zastav-mena-vlubitsa/')
