from typing import Literal, Any, TypedDict

from pydantic import BaseModel


class WorkerLabels(TypedDict, total=False):
    ip: Literal['ru', 'rs']

class InputLitresPartnersBook(BaseModel):
    url: str
    site: str
    book_id: int

class InputLivelibBook(BaseModel):
    url: str
    site: str

class Output(BaseModel):
    result: Literal['done', 'error', 'empty', 'debug']
    data: dict[str, Any]

class InputEvent(BaseModel):
    url: str
    event: str
    site: str
    customer: str
