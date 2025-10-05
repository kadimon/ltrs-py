from playwright.async_api import Page

from workflow_base import BaseLitresPartnersWorkflow
from interfaces import InputLitresPartnersBook, Output, WorkerLabels
from db import save_book_mongo


class AvidreadersRu(BaseLitresPartnersWorkflow):
    name = 'ltrs-avidreaders-ru'
    event = 'ltrs:avidreaders-ru'
    site='avidreaders.ru'
    url_patern=r'^https:\/\/avidreaders\.ru\/book\/[\w-]+\.html$'
    labels=WorkerLabels(ip='rs')
    input = InputLitresPartnersBook
    output = Output

    execution_timeout_sec=60

    @classmethod
    async def task(cls, input: InputLitresPartnersBook, page: Page) -> Output:
        resp = await page.goto(
            input.url,
            wait_until='domcontentloaded',
        )

        if not (200 <= resp.status < 400):
            return Output(
                result='error',
                data={'status': resp.status},
            )

        await page.wait_for_selector('h1')

        book = {
            'title': await page.text_content('h1'),
            'author': await page.text_content('.author_wrapper div[itemprop="author"] *[itemprop="name"]'),
        }

        download_button_locator = page.locator('.format_download a')
        if await download_button_locator.count() > 0:
            await download_button_locator.first.click()
            download_waiter = page.wait_for_event('download', timeout=10_000)
            await page.click('.dnld-info')
            try:
                if download := await download_waiter:
                    if 'litres.ru' in download.url:
                        book['links-litres'] = [download.url]
                    await download.cancel()
            except:
                pass

        await save_book_mongo(input, cls.site, book)

        return Output(
            result='done',
            data=book,
        )

if __name__ == '__main__':
    # AvidreadersRu.run_sync()

    AvidreadersRu.debug_sync('https://avidreaders.ru/book/taksi-do-lesa-berendeya.html')
