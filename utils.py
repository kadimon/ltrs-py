import asyncio
from pprint import pp

from pydantic import BaseModel
from playwright.async_api import async_playwright


from workflow_base import BaseWorkflow
import settings

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
