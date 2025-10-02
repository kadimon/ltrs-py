from urllib.parse import urljoin

from playwright.async_api import Page

from workflow_base import BaseLitresPartnersWorkflow
from interfaces import InputLitresPartnersBook, Output, WorkerLabels
from db import save_book_mongo

class ReadliNet(BaseLitresPartnersWorkflow):
    name = 'ltrs-readli-net'
    event = 'ltrs:readli-net'
    site='readli.net'
    url_patern=r'^https://readli\.net/[\w-]+/$'
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
            'author': await page.text_content('h1~a[href^="/avtor/"]'),
        }

        if links_download := await page.query_selector_all('.download a.download__link[href^="/download.php"]'):
            book['links-download'] = [
                urljoin(page.url, await l.get_attribute('href'))
                for l in links_download
            ]

        if links_litres := await page.query_selector_all('.download a.download__link[href^="/getfile.php"]'):
            book['links-litres'] = [
                urljoin(page.url, await l.get_attribute('href'))
                for l in links_litres
            ]

        await save_book_mongo(input, cls.site, book)

        return Output(
            result='done',
            data=book,
        )


if __name__ == '__main__':
    ReadliNet.run_sync()

    ReadliNet.debug_sync('https://readli.net/neprikayannyiy-2-3/')
