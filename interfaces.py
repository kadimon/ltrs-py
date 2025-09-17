from typing import Literal, Any, TypedDict

from pydantic import BaseModel


class WorkerLabels(TypedDict, total=False):
    ip: Literal['ru', 'rs']

class InputBase(BaseModel):
    url: str
    task_id: str = 'default'

class InputLitresPartnersBook(InputBase):
    book_id: int = 0

class InputLivelibBook(InputBase):
    pass

class Output(BaseModel):
    result: Literal['done', 'error', 'empty', 'debug']
    data: dict[str, Any]
