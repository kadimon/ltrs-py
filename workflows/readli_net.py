from playwright.async_api import Page

from workflow_base import BaseLitresPartnersWorkflow
from interfaces import InputLitresPartnersBook, Output
from db import save_book
from utils import run_task


class ReadliNet(BaseLitresPartnersWorkflow):
    name = 'ltrs-readli-net'
    event = 'ltrs:readli-net'
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
            'author': await page.text_content('h1~a[href^="/avtor/"]'),
        }

        if links_download := await page.query_selector_all('.download a.download__link[href^="/download.php"]'):
            book['links-download'] = ['https://readli.net'+(await l.get_attribute('href')) for l in links_download]

        if links_litres := await page.query_selector_all('.download a.download__link[href^="/getfile.php"]'):
            book['links-litres'] = ['https://readli.net'+(await l.get_attribute('href')) for l in links_litres]

        await save_book(input, book)

        return Output(
            result='done',
            data=book,
        )


if __name__ == '__main__':
    run_task(
        ReadliNet,
        InputLitresPartnersBook(
            url='https://readli.net/neprikayannyiy-2-3/',
            site='readli.net',
            book_id=0,
        )
    )
