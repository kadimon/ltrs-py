from hatchet_sdk import Hatchet, Context, EmptyModel
from playwright.async_api import async_playwright

import settings

hatchet = Hatchet(debug=True)


check_status = hatchet.workflow(
    name='check-status',
    on_events=['status:check'],
    input_validator=EmptyModel,
)

@check_status.task(
    execution_timeout='30s',
    schedule_timeout='10m',
    retries=5,
    backoff_max_seconds=3,
    backoff_factor=2.0,
)
async def get_ip(input: EmptyModel, ctx: Context):
    async with async_playwright() as p:
        context = await p.firefox.launch_persistent_context(
            './profileDir',
            proxy={'server': settings.PROXY_URI},
            headless=False,
            viewport={'width': 1920, 'height': 1080},
        )

        page = context.pages[0] if context.pages else await context.new_page()

        await page.goto(
            f'https://2ip.ru',
            wait_until='domcontentloaded',
        )

        ip_locator = page.locator('div.ip > span')
        await ip_locator.is_visible()
        ip = await ip_locator.text_content()

        await context.close()

        return {
            'ip': ip
        }
