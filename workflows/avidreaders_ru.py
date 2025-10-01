from playwright.async_api import Page

from workflow_base import BaseLitresPartnersWorkflow
from interfaces import InputLitresPartnersBook, Output
from db import save_book_mongo


class AvidreadersRu(BaseLitresPartnersWorkflow):
    name = 'ltrs-avidreaders-ru'
    event = 'ltrs:avidreaders-ru'
    site='avidreaders.ru'
    url_patern=r'^https:\/\/avidreaders\.ru\/book\/[\w-]+\.html$'
    input = InputLitresPartnersBook
    output = Output

    url_patern = r'^https:\/\/avidreaders\.ru\/book\/[\w-]+\.html$'

    @classmethod
    async def task(cls, input: InputLitresPartnersBook, page: Page) -> Output:
        resp = await page.goto(
            input.url,
            wait_until='networkidle',
        )

        if not (200 <= resp.status < 400):
            return Output(
                result='error',
                data={'status': resp.status},
            )

        recaptcha_locator = page.frame_locator('[title="reCAPTCHA"]').locator('.rc-anchor-center-item')
        if await recaptcha_locator.count() > 0:
            await recaptcha_locator.click()

        await page.wait_for_selector('h1')

        book = {
            'title': await page.text_content('h1'),
            'author': await page.text_content('.author_wrapper div[itemprop="author"] *[itemprop="name"]'),
        }

        download_button_locator = page.locator('.format_download a')
        if await download_button_locator.count() > 0:
            await download_button_locator.first.click()
            if download := await page.wait_for_event('download', timeout=10_000):
                if 'litres.ru' in download.url:
                    book['links-litres'] = [download.url]
                await download.cancel()

        await save_book_mongo(input, cls.site, book)

        return Output(
            result='done',
            data=book,
        )

if __name__ == '__main__':
    # AvidreadersRu.run_sync()

    AvidreadersRu.debug_sync('https://avidreaders.ru/book/predel-pogruzheniya.html')
