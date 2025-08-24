from typing import Literal, Any

from pydantic import BaseModel


class InputLitresPartnersBook(BaseModel):
    url: str
    site: str
    book_id: int

class Output(BaseModel):
    result: Literal['done', 'error', 'empty', 'debug']
    data: dict[str, Any]
