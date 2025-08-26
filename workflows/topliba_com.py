from playwright.async_api import Page

from workflow_base import BaseLitresPartnersWorkflow
from interfaces import InputLitresPartnersBook, Output
from db import save_book
from utils import run_task


class ToplibaCom(BaseLitresPartnersWorkflow):
    name = 'ltrs-topliba-com'
    event = 'ltrs:topliba-book'
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
            'author': await page.text_content('h2.book-author'),
        }

        if links_download := await page.query_selector_all('a.download-btn'):
            book['links-download'] = [await l.get_attribute('href') for l in links_download]

        reader_site_locator = page.locator('a.read-btn')
        if await reader_site_locator.count() > 0:
            book['reader-site'] = await reader_site_locator.get_attribute('href')

        try:
            reader_litres_locator = page.locator('div.litres_fragment_body iframe')
            await reader_litres_locator.wait_for(state='attached', timeout=10_000)
            book['reader-litres'] = await reader_litres_locator.get_attribute('src')
        except:
            pass

        await save_book(input, book)

        return Output(
            result='done',
            data=book,
        )

if __name__ == '__main__':
    run_task(
        ToplibaCom,
        InputLitresPartnersBook(
            url='https://topliba.com/books/535238',
            site='topliba.com',
            book_id=0,
        )
    )
