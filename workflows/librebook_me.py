from playwright.async_api import Page

from workflow_base import BaseLitresPartnersWorkflow
from interfaces import InputLitresPartnersBook, Output
from db import save_book_mongo
from utils import run_task


class LibrebookMe(BaseLitresPartnersWorkflow):
    name = 'ltrs-librebook-me'
    event = 'ltrs:librebook-me'
    input = InputLitresPartnersBook
    output = Output

    async def task(self, input: InputLitresPartnersBook, page: Page) -> Output:
        await page.goto(
            input.url,
            wait_until='domcontentloaded',
        )

        await page.wait_for_selector('h1 > .name')

        # await page.wait_for_timeout(10_000)
        # await page.screenshot(path='screenshot.png', full_page=True)

        book = {
            'title': await page.text_content('h1 > .name'),
            'author': await page.text_content('.elem_author'),
        }

        if links_litres := await page.query_selector_all('a.sell-tile-info'):
            book['links-litres'] = [await l.get_attribute('href') for l in links_litres]

        await save_book_mongo(input, book)

        return Output(
            result='done',
            data=book,
        )

if __name__ == '__main__':
    run_task(
        LibrebookMe,
        InputLitresPartnersBook(
            url='https://1.librebook.me/strasti_mordasti__daria_saltykova',
            site='librebook.me',
            book_id=0,
        )
    )
