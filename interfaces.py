from typing import Literal, Any

from pydantic import BaseModel


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
