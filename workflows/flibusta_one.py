from urllib.parse import urljoin

from playwright.async_api import Page

from workflow_base import BaseLitresPartnersWorkflow
from interfaces import InputLitresPartnersBook, Output, WorkerLabels
from db import save_book_mongo


class FlibustaOne(BaseLitresPartnersWorkflow):
    name = 'ltrs-flibusta-one'
    event = 'ltrs:flibusta-one'
    site='flibusta.one'
    url_patern = r'^https:\/\/flibusta\.one\/books\/\d+-[\w-]+\/$'
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
            'author': await page.text_content('.flist a[itemprop="author"]'),
        }

        if links_litres := await page.query_selector_all('.sect-format span'):
            book['links-litres'] = [await l.get_attribute('data-link') for l in links_litres]

        await save_book_mongo(input, cls.site, book)

        return Output(
            result='done',
            data=book,
        )

if __name__ == '__main__':
    FlibustaOne.run_sync()

    FlibustaOne.debug_sync('https://flibusta.one/books/78991-devyanosto-tretiy-god/')
