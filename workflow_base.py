import asyncio
import hashlib
import inspect
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from itertools import batched
from pathlib import Path
from pprint import pp
from typing import (
    Any,
    AsyncIterator,
    ClassVar,
    Generic,
    Literal,
    Optional,
    Type,
    TypeVar,
)

import httpx
import pandas as pd
from browserforge.fingerprints import Screen
from camoufox.async_api import AsyncCamoufox
from hatchet_sdk import PushEventOptions, V1TaskStatus
from hatchet_sdk.clients.events import BulkPushEventWithMetadata
from playwright.async_api import Page
from pymongo import AsyncMongoClient

import interfaces
import settings
from db import DbSamizdatPrisma
from settings import hatchet

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

    # --- Транспорт ---

    @classmethod
    def _proxy_uri(cls) -> str | None:
        return settings.PROXY_URI if cls.proxy_enable else None

    @classmethod
    @asynccontextmanager
    async def session(cls, debug: bool = False) -> AsyncIterator[Page]:
        """Ресурс, который `task` получает вторым аргументом.

        По умолчанию — страница Camoufox. API-воркфлоу переопределяют это
        через `ApiMixin` и получают `httpx.AsyncClient`.
        """
        if debug:
            extra = {}
        else:
            addons_dir = Path(settings.BROWSER_ADDONS_DIR)
            addons = (
                [str(f.resolve()) for f in addons_dir.iterdir()]
                if addons_dir.is_dir() else []
            )
            extra = {
                'headless': 'virtual',
                'persistent_context': True,
                'user_data_dir': 'user_data',
                'addons': addons,
            }

        proxy = cls._proxy_uri()
        async with AsyncCamoufox(
            os='windows',
            humanize=True,
            screen=Screen(max_width=1920, max_height=1080),
            locale=['ru-RU', 'en-US'],
            proxy={'server': proxy} if proxy else None,
            **extra,
        ) as browser:
            yield await browser.new_page()

    @classmethod
    async def call_task(cls, input: TInput, session: Any, ctx=None) -> TOutput:
        """ctx отдаём только тем задачам, которые его просят: у большинства
        воркфлоу сигнатура `task(input, page)`. Кому нужно — дописывает
        `ctx: Context | None = None` и читает `ctx.additional_metadata`."""
        if 'ctx' in inspect.signature(cls.task).parameters:
            return await cls.task(input, session, ctx=ctx)
        return await cls.task(input, session)

    # --- Запуск ---

    @classmethod
    async def run(cls, user_check: Literal['y', 'n'] | None = None) -> None:
        if settings.DEBUG:
            return

        while True:
            if not user_check:
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
        if not settings.DEBUG:
            return

        async with cls.session(debug=True) as session:
            input = cls.input(url=url, **kwargs)
            result = await cls.call_task(input, session)

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
        dont_dedupe: bool = False,
        metadata: Optional[dict] = None,
        **kwargs
    ) -> bool:
        if settings.DEBUG:
            return True

        hash = cls._task_hash(task_id, url)
        if dont_dedupe or await cls._not_dupe(hash, dedupe_hours):
            payload = {
                'url': url,
                'task_id': task_id,
            } | kwargs
            await hatchet.event.aio_push(
                cls.event,
                payload,
                options=PushEventOptions(
                    additional_metadata=cls._metadata(task_id, url, hash, metadata)
                )
            )
            return True
        else:
            return False

    @classmethod
    async def crawl_bulk(
        cls,
        urls: list[str],
        task_id: str,
        dedupe_hours: int = 480,
        dont_dedupe: bool = False,
        chunk_size: int = 1_000,
        metadata: Optional[dict] = None,
        **kwargs
    ) -> list[str]:
        urls = list(dict.fromkeys(urls))

        if settings.DEBUG:
            return urls

        events = []
        crawled = []

        for url in urls:
            hash = cls._task_hash(task_id, url)
            if dont_dedupe or await cls._not_dupe(hash, dedupe_hours):
                events.append(
                    BulkPushEventWithMetadata(
                        key=cls.event,
                        payload={
                            'url': url,
                            'task_id': task_id,
                        } | kwargs,
                        additional_metadata=cls._metadata(task_id, url, hash, metadata)
                    )
                )
                crawled.append(url)

        for i in range(0, len(events), chunk_size):
            await hatchet.event.aio_bulk_push(events[i:i + chunk_size])

        return crawled

    @classmethod
    def crawl_sync(
        cls,
        url: str,
        task_id: str,
        dedupe_hours: int = 24,
        **kwargs
    ) -> bool:
        return asyncio.run(cls.crawl(url, task_id, dedupe_hours))

    @classmethod
    async def _not_dupe(cls, hash: str, hours: int) -> bool:
        runs_list = await hatchet.runs.aio_list_with_pagination(
            since=datetime.now().astimezone() - timedelta(hours=hours),
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
    def _metadata(
        cls,
        task_id: str,
        url: str,
        hash: str,
        extra: Optional[dict] = None,
    ) -> dict:
        """Метаданные события.

        `extra` — то, что знает только вызывающая сторона и чего не будет на
        целевой странице (цена из листинга). Hatchet хранит метаданные
        строками, поэтому приводим значения сами и выкидываем пустые.
        Служебные ключи идут последними: перетереть `hash` нельзя, по нему
        работает дедупликация в `_not_dupe`.
        """
        metadata = {
            str(k): str(v)
            for k, v in (extra or {}).items()
            if v is not None and v != ''
        }

        return metadata | {
            'customer': cls.customer,
            'site': cls.site,
            'url': url,
            'hash': hash,
            'task_id': task_id,
        }

    @classmethod
    def _task_hash(cls, task_id: str, url: str):
        return task_id + hashlib.md5(f'{cls.event}{url}'.encode()).hexdigest()


class ApiMixin:
    """Транспорт без браузера: `task` вторым аргументом получает
    `httpx.AsyncClient`, уже настроенный заголовками и прокси класса.

    Ставить в базах ПЕРВЫМ, чтобы его `session` победил браузерный:

        class X(ApiMixin, BaseLivelibWorkflow):
            headers = {...}

    Заголовки отдельного запроса (`client.get(..., headers=...)`) httpx
    склеивает с заголовками клиента.

    Все атрибуты — ClassVar, чтобы `@dataclass` у наследников не превращал
    их в поля.
    """

    headers: ClassVar[dict[str, str]] = {}
    http_timeout: ClassVar[float] = 15
    follow_redirects: ClassVar[bool] = True
    # http2, verify, cookies, limits и прочее для httpx.AsyncClient
    http_client_kwargs: ClassVar[dict[str, Any]] = {}

    @classmethod
    @asynccontextmanager
    async def session(cls, debug: bool = False) -> AsyncIterator[httpx.AsyncClient]:
        async with httpx.AsyncClient(
            headers=cls.headers,
            proxy=cls._proxy_uri(),
            timeout=cls.http_timeout,
            follow_redirects=cls.follow_redirects,
            **cls.http_client_kwargs,
        ) as client:
            yield client


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
    async def run(cls, user_check: Literal['y', 'n'] | None = None) -> None:
        if settings.DEBUG:
            return

        while True:
            if not user_check:
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

    item_wf: Optional[Type["BaseLivelibWorkflow"]] = None

    cron: Optional[str] = None
    cron_urls: Optional[list[str]] = None

    customer = 'livelib'

    @classmethod
    async def task(cls, input: interfaces.InputLivelibBook, page: Page) -> interfaces.Output:
        return interfaces.Output(
            result='debug',
            data=input.model_dump()
        )


    @classmethod
    async def run(cls, user_check: Literal['y', 'n'] | None = None) -> None:
        if settings.DEBUG:
            return

        if not user_check:
            user_check = input(f'Ты уверен что хочешь запустить {cls.site}? Y/N:')

        if cls.item_wf:
            async with DbSamizdatPrisma() as db:
                cls.item_wf.start_urls = await db.get_all_books_urls(cls.item_wf.site)

            cls.start_urls = [u for u in cls.start_urls if u not in cls.item_wf.start_urls]

            await cls.item_wf.run(user_check)

        await super().run(user_check)

    @classmethod
    async def run_cron(cls) -> None:
        async with DbSamizdatPrisma() as db:
            cls.start_urls = await db.get_priority_persons_urls(cls.site)
        if cron_urls := cls.cron_urls:
            cls.start_urls.extend(cron_urls)

        await super().run('y')

    @classmethod
    def run_cron_sync(cls) -> None:
        asyncio.run(cls.run_cron())


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
    async def run(cls, user_check: Literal['y', 'n'] | None = None) -> None:
        if settings.DEBUG:
            return

        while True:
            if not user_check:
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

                print(f'\ntask_id: {task_id}')
                return
            elif user_check.lower() == 'n':
                return
