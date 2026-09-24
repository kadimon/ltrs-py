import hashlib
from io import BytesIO
from pathlib import Path
from urllib.parse import urljoin

import httpx
import puremagic
from aiobotocore.session import get_session
from furl import furl
from PIL import Image
from playwright.async_api import Page
from usp.tree import sitemap_tree_for_homepage

import settings


async def upload_cover(img_bytes: bytes, cover_url: str) -> str:
    """Проверяет картинку и кладёт её в S3. Возвращает имя файла.

    `cover_url` должен быть уже без query: имя файла — md5 от него.
    Исключения не глушит — это делают вызывающие.
    """
    with Image.open(BytesIO(img_bytes)) as _:
        pass

    cover_url_data = furl(cover_url)
    file_check = puremagic.magic_string(img_bytes, cover_url_data.pathstr)[0]
    extension = file_check.extension
    mime_type = file_check.mime_type

    cover_name = hashlib.md5(cover_url.encode()).hexdigest() + extension

    session = get_session()
    async with session.create_client(
            's3',
            endpoint_url=settings.AWS_ENDPOINT_URL,
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY
        ) as client:
            await client.put_object(
                Bucket=settings.AWS_COVERS_BUCKET,
                Key=f'{settings.AWS_COVERS_DIR}/{cover_name}',
                Body=img_bytes,
                ContentType=mime_type,
            )

    return cover_name


async def save_cover(page: Page, cover_url: str, timeout: int = 10_000) -> str | None:
    page_url = page.url
    cover_url = urljoin(page_url, cover_url).split('?', 1)[0]

    headers = {
        'referer': page_url,
        'cookie': 'PHPSESSID=a1;',
    }

    try:
        img_resp = await page.request.get(cover_url, headers=headers, timeout=timeout)
        if not img_resp.ok:
            return None

        return await upload_cover(await img_resp.body(), cover_url)

    except Exception as e:
        return None


async def save_cover_httpx(client: httpx.AsyncClient, cover_url: str, timeout: float = 10) -> str | None:
    """То же, что `save_cover`, но через уже открытую httpx-сессию.

    Качаем по полному URL (query вроде `?image_hash=` может быть нужен CDN),
    а имя файла считаем от URL без query — как и в `save_cover`.
    """
    try:
        img_resp = await client.get(cover_url, timeout=timeout)
        if img_resp.status_code != 200:
            return None

        return await upload_cover(img_resp.content, cover_url.split('?', 1)[0])

    except Exception as e:
        return None


def sitemap(url: str) -> list[str]:
    tree = sitemap_tree_for_homepage(url, use_robots=False)
    all_pages = [page.url for page in tree.all_pages()]

    return list(set(all_pages))

async def detect_new_tab_url(page: Page, timeout: int = 5000):
    try:
        new_page = await page.context.wait_for_event('page', timeout=timeout)
        # ждём, пока вкладка завершит навигацию (все редиректы)
        await new_page.wait_for_event('framenavigated', timeout=timeout)
        return new_page.url
    except Exception:
        return None
