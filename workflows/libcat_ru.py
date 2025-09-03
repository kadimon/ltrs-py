from playwright.async_api import Page

from workflow_base import BaseLitresPartnersWorkflow
from interfaces import InputLitresPartnersBook, Output
from db import save_book_mongo
from utils import run_task


class LibcatRu(BaseLitresPartnersWorkflow):
    name = 'ltrs-libcat-ru'
    event = 'ltrs:libcat-ru'
    input = InputLitresPartnersBook
    output = Output

    async def task(self, input: InputLitresPartnersBook, page: Page) -> Output:
        await page.goto(
            input.url,
            wait_until='domcontentloaded',
        )

        await page.wait_for_selector('h1')

        book = {
            'title': await page.text_content('div[itemprop="name"]'),
            'author': await page.text_content('a[itemprop="author"]'),
        }

        await page.locator('a.pagenav').last.click()
        await page.wait_for_load_state('domcontentloaded')

        litres_buton_locator = page.locator('.litresclick')
        if await litres_buton_locator.is_visible():
            book['links-litres'] = ['https://libcat.ru'+(await litres_buton_locator.get_attribute('data-href'))]

        await save_book_mongo(input, book)

        return Output(
            result='done',
            data=book,
        )

if __name__ == '__main__':
    run_task(
        LibcatRu,
        InputLitresPartnersBook(
            url='https://libcat.ru/knigi/detektivy-i-trillery/ironicheskij-detektiv/409520-darya-doncova-zmeinyj-gadzhet.html',
            site='libcat.ru',
            book_id=0,
        )
    )
