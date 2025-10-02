from urllib.parse import urljoin

from playwright.async_api import Page

from workflow_base import BaseLitresPartnersWorkflow
from interfaces import InputLitresPartnersBook, Output, WorkerLabels
from db import save_book_mongo


class KnigavuheOrg(BaseLitresPartnersWorkflow):
    name = 'ltrs-knigavuhe-org'
    event = 'ltrs:knigavuhe-org'
    site = 'knigavuhe.org'
    url_patern = r'^https:\/\/knigavuhe\.org(\/[\w-]+)?\/book\/[\w-]+\/$'
    input = InputLitresPartnersBook
    output = Output

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

        book = {
            'title': await page.text_content('h1 span[itemprop="name"]'),
            'author': await page.text_content('h1 span[itemprop="author"] a'),
        }

        litres_buton_locator = page.locator('.book_buy_wrap a')
        if await litres_buton_locator.is_visible():
            book['links-litres'] = [urljoin(page.url, await litres_buton_locator.get_attribute('href'))]

        await save_book_mongo(input, cls.site, book)

        return Output(
            result='done',
            data=book,
        )

if __name__ == '__main__':
    # KnigavuheOrg.run_sync()

    KnigavuheOrg.debug_sync('https://knigavuhe.org/book/13-neschastijj-gerakla/')
