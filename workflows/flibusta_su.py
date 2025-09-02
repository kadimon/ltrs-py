from playwright.async_api import Page

from workflow_base import BaseLitresPartnersWorkflow
from interfaces import InputLitresPartnersBook, Output
from db import save_book
from utils import run_task


class FlibustaSu(BaseLitresPartnersWorkflow):
    name = 'ltrs-flibusta-su'
    event = 'ltrs:flibusta-su'
    input = InputLitresPartnersBook
    output = Output

    async def task(self, input: InputLitresPartnersBook, page: Page) -> Output:
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
            book['reader-litres'] = ['https://flibusta.su' + litres_reader_href if litres_reader_href.startswith('/') else litres_reader_href]

        await save_book(input, book)

        return Output(
            result='done',
            data=book,
        )

if __name__ == '__main__':
    run_task(
        FlibustaSu,
        InputLitresPartnersBook(
            url='https://flibusta.su/book/412375-zastav-mena-vlubitsa/',
            site='flibusta.su',
            book_id=0,
        )
    )
