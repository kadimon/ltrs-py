from playwright.async_api import Page

from workflow_base import BaseLitresPartnersWorkflow
from interfaces import InputLitresPartnersBook, Output
from db import save_book
from utils import run_task


class YaknigaOrg(BaseLitresPartnersWorkflow):
    name = 'ltrs-yakniga-org'
    event = 'ltrs:yakniga-org'
    input = InputLitresPartnersBook
    output = Output

    async def task(self, input: InputLitresPartnersBook, page: Page) -> Output:
        resp = await page.goto(
            input.url,
            wait_until='domcontentloaded',
        )
        if not (200 <= resp.status < 400):
            return Output(
                result='error',
                data={'status': resp.status},
            )

        await page.wait_for_selector('.breadcrumb__list > li > .breadcrumb__text')

        # await page.wait_for_timeout(10_000)
        # await page.screenshot(path='screenshot.png', full_page=True)

        book = {
            'title': await page.text_content('.breadcrumb__list > li > .breadcrumb__text'),
            'author': await page.text_content('.book__author-link'),
        }

        if links_litres := await page.query_selector_all('a.litres__link'):
            book['links-litres'] = [await l.get_attribute('href') for l in links_litres]

        await save_book(input, book)

        return Output(
            result='done',
            data=book,
        )

if __name__ == '__main__':
    run_task(
        YaknigaOrg,
        InputLitresPartnersBook(
            url='https://yakniga.org/andrey-zorin/dohodyaga',
            site='yakniga.org',
            book_id=0,
        )
    )
