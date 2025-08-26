from typing import Any

from pymongo import AsyncMongoClient

from interfaces import InputLitresPartnersBook
import settings


async def save_book(input: InputLitresPartnersBook, book: dict[str, Any]):
    if settings.DEBUG:
        return

    client = AsyncMongoClient(settings.MONGO_URI)
    col = client['ltrs']['books']

    unique_key = {
        'book_id': input.book_id,
        'site': input.site,
        'url': input.url
    }

    data = unique_key | book

    # Обновляем документ или вставляем новый, если не существует
    await col.update_one(
        unique_key,
        {'$set': data},
        upsert=True
    )

    await client.aclose()
