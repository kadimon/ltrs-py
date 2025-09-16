from typing import Literal, Any, TypedDict

from pydantic import BaseModel


class WorkerLabels(TypedDict, total=False):
    ip: Literal['ru', 'rs']

class InputLitresPartnersBook(BaseModel):
    url: str
    book_id: int = 0

class InputLivelibBook(BaseModel):
    url: str
    site: str = 'default'

class Output(BaseModel):
    result: Literal['done', 'error', 'empty', 'debug']
    data: dict[str, Any]
