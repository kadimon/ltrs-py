from dataclasses import dataclass
from typing import Type, TypeVar, ClassVar, Generic, Optional
import asyncio
from pprint import pp

from pydantic import BaseModel
from playwright.async_api import async_playwright, Page

import interfaces
import settings
# from utils import run_task, set_task

TInput = TypeVar('TInput', bound=BaseModel)
TOutput = TypeVar('TOutput', bound=BaseModel)

@dataclass
class BaseWorkflow(
    Generic[TInput, TOutput]
):
    name: str = 'default'
    event: str = 'default'
    site: str = 'default'
    input: Type[TInput] = BaseModel
    output: Type[TOutput] = BaseModel

    proxy_enable: bool = True
    labels: ClassVar[interfaces.WorkerLabels] = {}

    customer: str = 'default'

    @classmethod
    async def task(cls, input: TInput, page: Page) -> TOutput:
        return cls.output(
            result='debug',
            data=input.model_dump()
        )

    concurrency: int = 10
    execution_timeout_sec: int = 30
    schedule_timeout_hours: int = 120
    retries: int = 5
    backoff_max_seconds: int = 10
    backoff_factor: float = 1.5

    @classmethod
    async def debug(cls, url: str, dedupe_hours: int = 48) -> None:
        if settings.DEBUG:
            async with async_playwright() as p:
                browser = await p.firefox.connect(settings.DEBUG_PW_SERVER)

                context = await browser.new_context(
                    proxy={'server': settings.PROXY_URI} if cls.proxy_enable else None,
                    viewport={'width': 1920, 'height': 1080},
                )

                page = await context.new_page()

                input = cls.input(url=url)
                result = await cls.task(input, page)

                await context.close()
                await browser.close()

                pp(result.model_dump())

    @classmethod
    def debug_sync(cls, url: str) -> Optional[bool]:
        return asyncio.run(cls.debug(url))

@dataclass
class BaseLitresPartnersWorkflow(
    BaseWorkflow[
        interfaces.InputLitresPartnersBook,
        interfaces.Output,
    ]
):
    input: Type[interfaces.InputLitresPartnersBook]
    output: Type[interfaces.Output]

    customer = 'ltrs-partners'

    async def task(self, input: interfaces.InputLitresPartnersBook, page: Page) -> interfaces.Output:
        return interfaces.Output(
            result='debug',
            data=input.model_dump()
        )

@dataclass
class BaseLivelibWorkflow(
    BaseWorkflow[
        interfaces.InputLivelibBook,
        interfaces.Output,
    ]
):
    input: Type[interfaces.InputLivelibBook]
    output: Type[interfaces.Output]

    customer = 'livelib'

    @classmethod
    async def task(cls, input: interfaces.InputLivelibBook, page: Page) -> interfaces.Output:
        return interfaces.Output(
            result='debug',
            data=input.model_dump()
        )
