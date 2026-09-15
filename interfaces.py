from typing import Any, Literal, TypedDict

from pydantic import BaseModel


class WorkerLabels(TypedDict, total=False):
    ip: Literal['ru', 'rs']

class InputBase(BaseModel):
    url: str
    task_id: str = 'default'

class InputLitresPartnersBook(InputBase):
    book_id: int = 0

class InputLivelibBook(InputBase):
    # цена, снятая в листинге: у части книг её нет на самой карточке
    price: str | None = None

class InputSeLtrs(InputBase):
    source: str = ''
    query: str = ''
    book_id: int = 0

class Output(BaseModel):
    result: Literal['done', 'error', 'empty', 'debug']
    data: dict[str, Any]
