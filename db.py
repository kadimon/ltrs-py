import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from pymongo import AsyncMongoClient

import settings
from interfaces import InputLitresPartnersBook
from prisma import Prisma


async def save_book_mongo(input: InputLitresPartnersBook, site:str, book: dict[str, Any]):
    if settings.DEBUG:
        return

    client = AsyncMongoClient(settings.MONGO_URI)
    col = client['ltrs']['books']

    unique_key = {
        'book_id': input.book_id,
        'site': site,
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


def str2int(value: str) -> int:
    """Преобразует строку в число с поддержкой k и m."""
    if not isinstance(value, str):
        return int(value)

    value = value.lower().replace("\xa0", "").replace(",", ".").replace(" ", "")
    multiplier = 1
    if value.endswith("k"):
        multiplier = 1_000
        value = value[:-1]
    elif value.endswith("m"):
        multiplier = 1_000_000
        value = value[:-1]
    try:
        return int(float(value) * multiplier)
    except ValueError:
        return 0


def str2float(value: str) -> float:
    """Преобразует строку в число с плавающей точкой."""
    if not isinstance(value, str):
        return float(value)

    value = value.replace("\xa0", "").replace(",", ".").replace(" ", "")
    try:
        return float(value)
    except ValueError:
        return 0.0


class DbSamizdatPrisma:
    def __init__(self):
        self.con: Optional[Prisma] = None

    async def __aenter__(self):
        self.con = Prisma()
        await self.con.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.con:
            await self.con.disconnect()

    async def check_book_exist(self, url: str) -> bool:
        return await self.con.book.find_unique(
            where={"url": url},
        ) is not None

    async def check_book_have_cover(self, url: str) -> bool:
        return await self.con.book.find_first(
            where={"url": url, "coverImage": {"not": None}},
        ) is not None

    async def create_book(self, book_data: Dict[str, Any]) -> None:
        if settings.DEBUG:
            return

        book = await self.clear_item(book_data)
        await self.con.book.create(data=book)

    async def update_book(self, book_data: Dict[str, Any]) -> None:
        if settings.DEBUG:
            return

        book = await self.clear_item(book_data)
        book["deleted"] = None

        persons_data_fields = {
            "authors_data": "AUTHOR",
            "artists_data": "ARTIST",
            "publishers_data": "PUBLISHER",
            "owners_data": "OWNER",
            "translators_data": "TRANSLATOR",
            "voices_data": "VOICE",
            "editors_data": "EDITOR",
        }

        async with self.con.tx() as tx:
            for person_field, role in persons_data_fields.items():
                if persons := book.get(person_field):
                    await tx.bookperson.delete_many(
                        where={
                            "book": {"url": book["url"]},
                            "role": role,
                        }
                    )

                    for person in persons:
                        person_in_db = await tx.person.upsert(
                            where={'url': person['url']},
                            data={'create': person, 'update': person},
                        )

                        await tx.bookperson.create(
                            data={
                                "book": {"connect": {"url": book["url"]}},
                                "person": {"connect": {"id": person_in_db.id}},
                                "role": role,
                            }
                        )

                    del book[person_field]

            await tx.book.update(
                where={"url": book["url"]},
                data=book
            )

    async def mark_book_deleted(self, url: str, source: str) -> None:
        if settings.DEBUG:
            return

        book = await self.con.book.find_first(
            where={"url": url},
        )

        if not book:
            await self.con.book.create(
                data={
                    "url": url,
                    "source": source,
                    "title": "Not Exist",
                    "deleted": datetime.utcnow(),
                }
            )
        elif not book.deleted:
            await self.con.book.update(
                where={"url": url},
                data={"deleted": datetime.utcnow()},
            )

    async def create_metrics(self, metrics_data: Dict[str, Any]) -> None:
        if settings.DEBUG:
            return

        metrics = await self.clear_item(metrics_data)
        metrics = await self.convert_metrics(metrics)
        await self.con.metrics.create(data=metrics)

    async def clear_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        item_clear = {}
        for k, v in item.items():
            if not v:
                continue

            if k in (
                # "title",
                # "author",
            ):
                v = re.sub(
                    r'[!"#$%&\'()*+\-./:;<=>?@[\\\]^`{|}~]',
                    '',
                    v,
                )
                v = re.sub(r"\n|\s{2,}", " ", v)

            if isinstance(v, list):
                if all(isinstance(i, str) for i in v):
                    item_clear[k] = [i.strip().replace("\xa0", " ") for i in v]
                else:
                    item_clear[k] = v
            elif isinstance(v, str):
                item_clear[k] = v.strip().replace("\xa0", " ")
            else:
                item_clear[k] = v

        # Преобразуем нужные поля в int
        fields2int = [
            'isbn',
            'age_rating',
        ]
        for field in fields2int:
            val = item.get(field)
            if val and not isinstance(val, int):
                item_clear[field] = str2int(val)

        return item_clear

    async def convert_metrics(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        fields2int = [
            "views",
            "votes",
            "added_to_lib",
            "read_process",
            "read_stoped",
            "read_on_pause",
            "read_later",
            "read_finished",
            "downloaded",
            "likes",
            "unlike",
            "comments",
            "pages_count",
            "characters_count",
            "chapters_count",
        ]
        for field in fields2int:
            val = metrics.get(field)
            if val and not isinstance(val, int):
                metrics[field] = str2int(val)

        fields2float = [
            "rating",
            "price",
            "price_discount",
            "price_old",
            "price_audio",
        ]
        for field in fields2float:
            val = metrics.get(field)
            if val:
                metrics[field] = str2float(val)

        fields2json_int_val = ["site_ratings", "awards"]
        for field in fields2json_int_val:
            if field_data := metrics.get(field):
                metrics[field] = json.dumps([[k, str2int(v)] for k, v in field_data.items()])

        return metrics

    async def get_all_books_urls(self, source: str) -> List[str]:
        books = await self.con.book.find_many(
            where={"source": source},
        )
        return [b.url for b in books]

    async def get_priority_persons_urls(self, source: str) -> List[str]:
        persons = await self.con.person.find_many(
            where={"for_scrape": True, "books": {"some": {"book": {"source": source}}}},
        )
        return [p.url for p in persons]
