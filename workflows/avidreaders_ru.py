import asyncio

from playwright.async_api import Page

from workflow_base import BaseLitresPartnersWorkflow
from interfaces import InputLitresPartnersBook, Output, WorkerLabels
from db import save_book_mongo
from utils import detect_new_tab_url


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
            new_tab_url_waiter = detect_new_tab_url(page)
            download_waiter = page.wait_for_event('download', timeout=10_000)

            await download_button_locator.first.click()

            new_tab_url = None
            download = None
            try:
                new_tab_url, download = await asyncio.gather(
                    new_tab_url_waiter,
                    download_waiter,
                    return_exceptions=True
                )
            except Exception:
                pass

            if isinstance(new_tab_url, str) and 'litres.ru' in new_tab_url:
                book['links-litres'] = [new_tab_url]
            elif hasattr(download, 'url') and 'litres.ru' in download.url:
                book['links-litres'] = [download.url]
                try:
                    await download.cancel()
                except Exception:
                    pass

        await save_book_mongo(input, cls.site, book)

        return Output(
            result='done',
            data=book,
        )

if __name__ == '__main__':
    # AvidreadersRu.run_sync()

    AvidreadersRu.debug_sync('https://avidreaders.ru/book/goryaschie-serdca-sbornik-stihotvoreniy.html')
