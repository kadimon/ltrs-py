from dataclasses import dataclass
from typing import Type, TypeVar, ClassVar, Generic, Optional
import asyncio
from pprint import pp
import hashlib
import logging
from datetime import datetime, timedelta, timezone

from pydantic import BaseModel
from playwright.async_api import async_playwright, Page
from hatchet_sdk import Hatchet, ClientConfig, PushEventOptions, V1TaskStatus

import interfaces
import settings
# from utils import run_task, set_task


root_logger = logging.getLogger('hatchet')
root_logger.setLevel(logging.WARNING)

hatchet = Hatchet(
    debug=False,
    config=ClientConfig(
        logger=root_logger,
    ),
)

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

    @classmethod
    async def crawl(cls, url: str, dedupe_hours: int = 48) -> bool:
        if settings.DEBUG:
            return False

        hash = hashlib.md5(f'{cls.event}{url}'.encode()).hexdigest()
        if await not_dupe(hash, dedupe_hours):
            await hatchet.event.aio_push(
                cls.event,
                {
                    'url': url,
                    'site': cls.site,
                },
                options=PushEventOptions(
                    additional_metadata={
                        'customer': cls.customer,
                        'site': cls.site,
                        'url': url,
                        'hash': hash,
                    }
                )
            )
            return True
        else:
            return False

    @classmethod
    def crawl_sync(cls, url: str, dedupe_hours: int = 48) -> bool:
        return asyncio.run(cls.crawl(url, dedupe_hours))


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


async def not_dupe(hash: str, hours: int) -> bool:
    runs_list = await hatchet.runs.aio_list_with_pagination(
        since=datetime.now(timezone.utc) - timedelta(hours=hours),
        additional_metadata={
            'hash': hash,
        },
        statuses=[
            V1TaskStatus.RUNNING,
            V1TaskStatus.QUEUED,
            V1TaskStatus.COMPLETED,
        ],
        limit=1,
        # only_tasks=True,
    )
    # for t in runs_list:
    #     print(t.additional_metadata)

    if runs_list:
        return False
    else:
        return True
