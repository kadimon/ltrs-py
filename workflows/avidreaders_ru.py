from playwright.async_api import Page

from workflow_base import BaseLitresPartnersWorkflow
from interfaces import InputLitresPartnersBook, Output
from db import save_book
from utils import run_task


class AvidreadersRu(BaseLitresPartnersWorkflow):
    name = 'ltrs-avidreaders-ru'
    event = 'ltrs:avidreaders-ru'
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
            'author': await page.text_content('.author_wrapper div[itemprop="author"] a'),
        }

        await save_book(input, book)

        return Output(
            result='done',
            data=book,
        )

if __name__ == '__main__':
    run_task(
        AvidreadersRu,
        InputLitresPartnersBook(
            url='https://avidreaders.ru/book/taksi-do-lesa-berendeya.html',
            site='avidreaders.ru',
            book_id=0,
        )
    )
