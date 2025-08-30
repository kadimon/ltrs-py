from playwright.async_api import Page

from workflow_base import BaseLitresPartnersWorkflow
from interfaces import InputLitresPartnersBook, Output
from db import save_book
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

        link_litres_locator = page.locator('a.sell-tile-info')
        if await link_litres_locator.count() > 0:
            book['links-litres'] = [await link_litres_locator.get_attribute('href')]

        await save_book(input, book)

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
