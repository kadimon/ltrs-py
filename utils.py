import asyncio
from pprint import pp
import logging
from urllib.parse import urljoin
from pathlib import Path
import hashlib

from pydantic import BaseModel
from playwright.async_api import async_playwright, Page
from hatchet_sdk import Hatchet, ClientConfig, PushEventOptions
from PIL import Image


from workflow_base import BaseWorkflow
from interfaces import InputEvent
import settings

root_logger = logging.getLogger('hatchet')
root_logger.setLevel(logging.WARNING)

hatchet = Hatchet(
    debug=False,
    config=ClientConfig(
        logger=root_logger,
    ),
)

async def run_task_async(wf: BaseWorkflow, input: BaseModel):
    async with async_playwright() as p:
        browser = await p.firefox.connect('ws://127.0.0.1:3000/')

        context = await browser.new_context(
            proxy={'server': settings.PROXY_URI},
            viewport={'width': 1920, 'height': 1080},
        )

        page = await context.new_page()

        instance = wf(
            name=wf.name,
            event=wf.event,
            input=wf.input,
            output=wf.output,
        )
        result = await instance.task(input, page)

        await context.close()
        await browser.close()

        pp(result.model_dump())

def run_task(wf: BaseWorkflow, input: BaseModel):
    asyncio.run(run_task_async(wf, input))

async def set_task(input: InputEvent):
    if settings.DEBUG:
        return

    await hatchet.event.aio_push(
        input.event,
        {
            'url': input.url,
            'site': input.site,
        },
        options=PushEventOptions(
            additional_metadata={
                'customer': input.customer,
                'site': input.site,
            }
        )
    )

async def save_cover(page: Page, cover_url: str, timeout: int = 10_000) -> str | None:
    page_url = page.url
    cover_url = urljoin(page_url, cover_url).split('?', 1)[0]

    headers = {
        'referer': page_url,
        'cookie': 'PHPSESSID=a1;',
    }

    try:
        img_resp = await page.request.get(cover_url, headers=headers, timeout=timeout)
        if not img_resp.ok:
            return None

        content_type = img_resp.headers.get('content-type', 'image/jpeg')
        extension = content_type.split('/')[-1].lower()

        cover_name = f'{hashlib.md5(cover_url.encode()).hexdigest()}.{extension}'
        cover_path = f'{settings.COVERS_DIR}/{cover_name}'

        Path(cover_path).write_bytes(await img_resp.body())

        Image.open(cover_path)

        return cover_name

    except Exception:
        return None
