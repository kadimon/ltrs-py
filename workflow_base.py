from dataclasses import dataclass
from typing import Type, TypeVar, Generic

from pydantic import BaseModel
from playwright.async_api import Page

import interfaces

TInput = TypeVar('TInput', bound=BaseModel)
TOutput = TypeVar('TOutput', bound=BaseModel)

@dataclass
class BaseWorkflow(
    Generic[TInput, TOutput]
):
    name: str
    event: str
    input: Type[TInput]
    output: Type[TOutput]

    async def task(self, input: TInput, page: Page) -> TOutput:
        return self.output(
            result='debug',
            data=input.model_dump()
        )

    concurrency: int = 10
    execution_timeout_sec: int = 30
    schedule_timeout_hours: int = 120
    retries: int = 5
    backoff_max_seconds: int = 10
    backoff_factor: float = 1.5


@dataclass
class BaseLitresPartnersWorkflow(
    BaseWorkflow[
        interfaces.InputLitresPartnersBook,
        interfaces.Output,
    ]
):
    input: Type[interfaces.InputLitresPartnersBook]
    output: Type[interfaces.Output]

    async def task(self, input: interfaces.InputLitresPartnersBook, page: Page) -> interfaces.Output:
        return interfaces.Output(
            result='debug',
            data=input.model_dump()
        )
