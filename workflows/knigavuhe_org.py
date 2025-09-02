from playwright.async_api import Page

from workflow_base import BaseLitresPartnersWorkflow
from interfaces import InputLitresPartnersBook, Output
from db import save_book
from utils import run_task


class KnigavuheOrg(BaseLitresPartnersWorkflow):
    name = 'ltrs-knigavuhe-org'
    event = 'ltrs:knigavuhe-org'
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

        book = {
            'title': await page.text_content('h1 span[itemprop="name"]'),
            'author': await page.text_content('h1 span[itemprop="author"] a'),
        }

        litres_buton_locator = page.locator('.book_buy_wrap a')
        if await litres_buton_locator.is_visible():
            book['links-litres'] = ['https://knigavuhe'+(await litres_buton_locator.get_attribute('href'))]

        await save_book(input, book)

        return Output(
            result='done',
            data=book,
        )

if __name__ == '__main__':
    run_task(
        KnigavuheOrg,
        InputLitresPartnersBook(
            url='https://knigavuhe.org/book/13-neschastijj-gerakla/',
            site='avidreaders.ru',
            book_id=0,
        )
    )
