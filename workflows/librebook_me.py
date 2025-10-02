from urllib.parse import urljoin

from playwright.async_api import Page

from workflow_base import BaseLitresPartnersWorkflow
from interfaces import InputLitresPartnersBook, Output, WorkerLabels
from db import save_book_mongo


class LibrebookMe(BaseLitresPartnersWorkflow):
    name = 'ltrs-librebook-me'
    event = 'ltrs:librebook-me'
    site='librebook.me'
    url_patern=r'^https:\/\/1\.librebook\.me\/[\w_]+$'
    input = InputLitresPartnersBook
    output = Output

    @classmethod
    async def task(cls, input: InputLitresPartnersBook, page: Page) -> Output:
        await page.goto(
            input.url,
            wait_until='domcontentloaded',
        )

        await page.wait_for_selector('h1 > .name')

        book = {
            'title': await page.text_content('h1 > .name'),
            'author': await page.text_content('.elem_author'),
        }

        if links_litres := await page.query_selector_all('a.sell-tile-info'):
            book['links-litres'] = [await l.get_attribute('href') for l in links_litres]

        await save_book_mongo(input, cls.site, book)

        return Output(
            result='done',
            data=book,
        )

if __name__ == '__main__':
    LibrebookMe.run_sync()

    LibrebookMe.debug_sync('https://1.librebook.me/strasti_mordasti__daria_saltykova')
