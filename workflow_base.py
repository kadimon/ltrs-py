from dataclasses import dataclass
from typing import Protocol, Type, TypeVar, Generic

from pydantic import BaseModel
from hatchet_sdk import Context

import interfaces

TInput = TypeVar('TInput', bound=BaseModel)
TOutput = TypeVar('TOutput', bound=BaseModel)

class BaseWorkflowProtocol(Protocol, Generic[TInput, TOutput]):
    event: str
    input: Type[TInput]
    output: Type[TOutput]

    async def task(self, input: interfaces.InputLitresPartnersBook, ctx: Context) -> interfaces.Output: ...

    # атрибуты по умолчанию
    concurrency: int
    execution_timeout_sec: int
    schedule_timeout_hours: int
    retries: int
    backoff_max_seconds: int
    backoff_factor: float

@dataclass
class BaseLitresPartnersWorkflow(
    BaseWorkflowProtocol[
        interfaces.InputLitresPartnersBook,
        interfaces.Output,
    ]
):
    name: str
    event: str
    input: Type[interfaces.InputLitresPartnersBook]
    output: Type[interfaces.Output]

    async def task(self, input: interfaces.InputLitresPartnersBook, ctx: Context) -> interfaces.Output:
        return interfaces.Output(
            result='debug',
            data=input.model_dump()
        )

    concurrency: int = 10
    execution_timeout_sec: int = 30
    schedule_timeout_hours: int = 120
    retries: int = 5
    backoff_max_seconds: int = 10
    backoff_factor: float = 1.5
