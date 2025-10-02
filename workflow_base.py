from dataclasses import dataclass
from typing import Type, TypeVar, ClassVar, Generic, Optional
import asyncio
from pprint import pp
import hashlib
import logging
from datetime import datetime, timedelta, timezone
import re
from itertools import batched

from pydantic import BaseModel
from patchright.async_api import async_playwright, Page
from hatchet_sdk import Hatchet, ClientConfig, PushEventOptions, V1TaskStatus
from hatchet_sdk.clients.events import BulkPushEventWithMetadata
from pymongo import AsyncMongoClient
import pandas as pd

import interfaces
import settings


root_logger = logging.getLogger('hatchet')
root_logger.setLevel(logging.WARNING)

hatchet = Hatchet(
    debug=False,
    config=ClientConfig(
        server_url='http://homeserver:8888',
        logger=root_logger,
    ),
)

TInput = TypeVar('TInput', bound=interfaces.InputBase)
TOutput = TypeVar('TOutput', bound=interfaces.InputBase)

@dataclass
class BaseWorkflow(
    Generic[TInput, TOutput]
):
    name: str = 'default'
    event: str = 'default'
    site: str = 'default'
    input: Type[TInput] = TInput
    output: Type[TOutput] = TOutput

    proxy_enable: bool = True
    labels: ClassVar[interfaces.WorkerLabels] = {}

    customer: str = 'default'

    start_urls: ClassVar[list[str]] = []

    concurrency: int = 10
    execution_timeout_sec: int = 30
    schedule_timeout_hours: int = 120
    retries: int = 5
    backoff_max_seconds: int = 10
    backoff_factor: float = 1.5

    @classmethod
    async def task(cls, input: TInput, page: Page) -> TOutput:
        return cls.output(
            result='debug',
            data=input.model_dump()
        )

    @classmethod
    async def run(cls) -> None:
        if settings.DEBUG:
            return

        while True:
            user_check = input(f'Ты уверен что хочешь запустить {cls.site}? Y/N:')
            if user_check.lower() == 'y':
                task_id = cls.site + settings.START_TIME


                for batch_urls in batched(cls.start_urls, 1000):
                    events = []
                    for url in batch_urls:
                        events.append(
                            BulkPushEventWithMetadata(
                                key=cls.event,
                                payload=cls.input(
                                    url=url,
                                    task_id=task_id
                                ).model_dump(),
                                additional_metadata={
                                    'customer': cls.customer,
                                    'site': cls.site,
                                    'url': url,
                                    'hash': cls._task_hash(task_id, url),
                                    'task_id': task_id,
                                }
                            )
                        )

                    await hatchet.event.aio_bulk_push(
                        events=events
                    )

                print(f'\ntask_id: {task_id}')
                return
            elif user_check.lower() == 'n':
                return

    @classmethod
    def run_sync(cls) -> None:
        asyncio.run(cls.run())

    @classmethod
    async def debug(cls, url: str, **kwargs) -> None:
        if settings.DEBUG:
            async with async_playwright() as p:
                browser = await p.firefox.connect(settings.DEBUG_PW_SERVER)

                context = await browser.new_context(
                    proxy={'server': settings.PROXY_URI} if cls.proxy_enable else None,
                    viewport={'width': 1920, 'height': 1080},
                )

                page = await context.new_page()
                input = cls.input(url=url, **kwargs)
                result = await cls.task(input, page)

                await context.close()
                await browser.close()

                pp(result.model_dump())

    @classmethod
    def debug_sync(cls, url: str, **kwargs) -> Optional[bool]:
        return asyncio.run(cls.debug(url, **kwargs))

    @classmethod
    async def crawl(
        cls,
        url: str,
        task_id: str,
        dedupe_hours: int = 480,
        **kwargs
    ) -> bool:
        if settings.DEBUG:
            return True

        hash = cls._task_hash(task_id, url)
        if await cls._not_dupe(hash, dedupe_hours):
            payload = {
                'url': url,
                'task_id': task_id,
            } | kwargs
            await hatchet.event.aio_push(
                cls.event,
                payload,
                options=PushEventOptions(
                    additional_metadata={
                        'customer': cls.customer,
                        'site': cls.site,
                        'url': url,
                        'hash': hash,
                        'task_id': task_id,
                    }
                )
            )
            return True
        else:
            return False

    @classmethod
    def crawl_sync(
        cls,
        url: str,
        task_id: str,
        dedupe_hours: int = 480,
        **kwargs
    ) -> bool:
        return asyncio.run(cls.crawl(url, task_id, dedupe_hours))

    @classmethod
    async def _not_dupe(cls, hash: str, hours: int) -> bool:
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

    @classmethod
    def _task_hash(cls, task_id: str, url: str):
        return task_id + hashlib.md5(f'{cls.event}{url}'.encode()).hexdigest()


@dataclass
class BaseLitresPartnersWorkflow(
    BaseWorkflow[
        interfaces.InputLitresPartnersBook,
        interfaces.Output,
    ]
):
    input: Type[interfaces.InputLitresPartnersBook]
    output: Type[interfaces.Output]

    customer: str = 'ltrs-partners'

    url_patern: str = r'.+'

    @classmethod
    async def task(cls, input: interfaces.InputLitresPartnersBook, page: Page) -> interfaces.Output:
        return interfaces.Output(
            result='debug',
            data=input.model_dump()
        )

    @classmethod
    async def run(cls) -> None:
        if settings.DEBUG:
            return

        while True:
            user_check = input(f'Ты уверен что хочешь запустить {cls.site}? Y/N:')
            if user_check.lower() == 'y':
                task_id = cls.site + settings.START_TIME

                client = AsyncMongoClient(settings.MONGO_URI)
                db = client['ltrs']
                col_yandex = db['yandex']
                col_books = db['books']

                tasks = []
                async for search_result in col_yandex.find({'source': cls.site}):
                    book_urls = [position['url'] for position in search_result['results']]
                    book_urls = [url for url in book_urls if re.search(cls.url_patern, url)]

                    for url in book_urls[:3]:
                        tasks.append({
                            'url': url,
                            'book_id': search_result['book_id'],
                        })
                print(tasks)
                for batch in batched(tasks, 1000):
                    events = []
                    for t in batch:
                        if not await col_books.find_one({'url': t['url']}):
                            events.append(
                                BulkPushEventWithMetadata(
                                    key=cls.event,
                                    payload=cls.input(
                                        url=t['url'],
                                        task_id=task_id,
                                        book_id=t['book_id']
                                    ).model_dump(),
                                    additional_metadata={
                                        'customer': cls.customer,
                                        'site': cls.site,
                                        'url': t['url'],
                                        'hash': cls._task_hash(task_id, t['url']),
                                        'task_id': task_id,
                                    }
                                )
                            )

                    await hatchet.event.aio_bulk_push(
                        events=events
                    )

                print(f'\ntask_id: {task_id}')
                return
            elif user_check.lower() == 'n':
                return


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

@dataclass
class BaseLtrsSeWorkflow(
    BaseWorkflow[
        interfaces.InputSeLtrs,
        interfaces.Output,
    ]
):
    input: Type[interfaces.InputSeLtrs]
    output: Type[interfaces.Output]

    customer = 'ltrs-partners'

    sources: ClassVar[list[str]] = []
    start_file = 'data_files/Топ-10.000.xlsx'


    execution_timeout_sec = 15
    schedule_timeout_hours = 240

    retries=5
    backoff_max_seconds=10
    backoff_factor=2.0

    @classmethod
    async def task(cls, input: interfaces.InputSeLtrs, page: Page) -> interfaces.Output:
        return interfaces.Output(
            result='debug',
            data=input.model_dump()
        )

    @classmethod
    async def run(cls) -> None:
        if settings.DEBUG:
            return

        while True:
            user_check = input(f'Ты уверен что хочешь запустить {cls.site}? Y/N:')
            task_id = input(f'Введи имя задачи:')
            if user_check.lower() == 'y':
                client = AsyncMongoClient(settings.MONGO_URI)
                col = client['ltrs']['yandex']

                df = pd.read_excel('data_files/Топ-10.000.xlsx')
                for row in df.to_dict(orient='records'):
                    for source in cls.sources:
                        query = f'{row['Название арта']} {row['Авторы']}'
                        if not await col.find_one({
                            'book_id': int(row['ID арта']),
                            'source': source,
                        }):
                            url = f'https://ya.ru/search/?text=site:{source}+{query}&lr=225'
                            await cls.crawl(
                                url,
                                task_id,
                                source=source,
                                query = query,
                                book_id=int(row['ID арта']),
                            )
                            print(f'url started: {url}')

                print(f'\ntask_id: {task_id}')
                return
            elif user_check.lower() == 'n':
                return
