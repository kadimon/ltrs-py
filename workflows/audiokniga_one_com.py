from urllib.parse import urljoin

from playwright.async_api import Page

from workflow_base import BaseLitresPartnersWorkflow
from interfaces import InputLitresPartnersBook, Output, WorkerLabels
from db import save_book_mongo


class AudioknigaOneCom(BaseLitresPartnersWorkflow):
    name = 'ltrs-audiokniga-one-com'
    event = 'ltrs:audiokniga-one.com'
    site='audiokniga-one.com'
    url_patern=r'^https:\/\/audiokniga-one\.com\/\d+-[\w-]+\.html$'
    input = InputLitresPartnersBook
    output = Output

    @classmethod
    async def task(cls, input: InputLitresPartnersBook, page: Page) -> Output:
        await page.goto(
            input.url,
            wait_until='domcontentloaded',
        )

        await page.wait_for_selector('h1')

        # await page.wait_for_timeout(10_000)
        # await page.screenshot(path='screenshot.png', full_page=True)

        book = {
            'title': (await page.text_content('h1')).rsplit(' - ', 1)[0],
            'author': await page.text_content('//div[@class="pmovie__genres" and contains(text(), "Автор:")]/a'),
        }

        if links_litres := await page.query_selector_all('//div[@class="pmovie__player tabs-block"]//a[contains(@href, "litres.ru")]'):
            book['links-litres'] = [await l.get_attribute('href') for l in links_litres]

        await save_book_mongo(input,cls.site, book)

        return Output(
            result='done',
            data=book,
        )

if __name__ == '__main__':
    AudioknigaOneCom.run_sync()

    AudioknigaOneCom.debug_sync('https://audiokniga-one.com/3645-legkiy-sposob-brosit-kurit-allen-karr.html')
