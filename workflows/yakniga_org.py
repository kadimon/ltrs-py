from urllib.parse import urljoin

from playwright.async_api import Page

from workflow_base import BaseLitresPartnersWorkflow
from interfaces import InputLitresPartnersBook, Output, WorkerLabels
from db import save_book_mongo


class YaknigaOrg(BaseLitresPartnersWorkflow):
    name = 'ltrs-yakniga-org'
    event = 'ltrs:yakniga-org'
    site = 'yakniga.org'
    url_patern = r'^https:\/\/yakniga\.org\/(?!reader\/|genres\/|series\/)[\w-]+\/[\w-]+$'
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

        await page.wait_for_selector('.breadcrumb__list > li > .breadcrumb__text')

        book = {
            'title': await page.text_content('.breadcrumb__list > li > .breadcrumb__text'),
            'author': await page.text_content('.book__author-link'),
        }

        if links_litres := await page.query_selector_all('a.litres__link'):
            book['links-litres'] = [await l.get_attribute('href') for l in links_litres]

        await save_book_mongo(input, cls.site, book)

        return Output(
            result='done',
            data=book,
        )

if __name__ == '__main__':
    YaknigaOrg.run_sync()

    YaknigaOrg.debug_sync('https://yakniga.org/andrey-zorin/dohodyaga')
