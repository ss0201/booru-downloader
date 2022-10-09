import argparse
import asyncio
import concurrent.futures
import json
import os
from typing import Union

import requests
from pygelbooru import API_GELBOORU, API_RULE34, Gelbooru
from pygelbooru.gelbooru import GelbooruImage


def main():
    parser = argparse.ArgumentParser(description="Download images from Gelbooru/Rule34")
    parser.add_argument(
        "--source",
        help="Source site",
        type=str,
        default="gelbooru",
        choices=["gelbooru", "rule34"],
    )
    parser.add_argument("--tags", help="Tags to search for", nargs="+")
    parser.add_argument("--output", help="Output directory")
    parser.add_argument("--page", help="Page to start from", type=int, default=0)
    parser.add_argument(
        "--parallel", help="Number of parallel downloads", type=int, default=5
    )
    args = parser.parse_args()

    with open("credentials.json", "r") as f:
        credentials = json.loads(f.read())

    print("Creating Gelbooru client...")
    api: str = API_GELBOORU
    if args.source == "gelbooru":
        api = API_GELBOORU
    elif args.source == "rule34":
        api = API_RULE34
    gelbooru = Gelbooru(credentials["api_key"], credentials["user_id"], api=api)

    print("Preparing for downloading images...")
    asyncio.run(
        download_all_images(gelbooru, args.tags, args.output, args.page, args.parallel),
        debug=True,
    )


async def download_all_images(
    gelbooru: Gelbooru, tags: list[str], output_dir: str, start_page: int, parallel: int
):
    LIMIT = 100
    page = start_page
    while True:
        count = await download_images(gelbooru, tags, output_dir, LIMIT, page, parallel)
        if count == 0:
            break
        page += 1


async def download_images(
    gelbooru: Gelbooru,
    tags: list[str],
    output_dir: str,
    limit: int,
    page: int,
    parallel: int,
):
    print(f"Searching posts: page={page}")
    image_or_images: Union[
        list[GelbooruImage], GelbooruImage
    ] = await gelbooru.search_posts(tags=tags, limit=limit, page=page)

    images: list[GelbooruImage]
    if isinstance(image_or_images, list):
        images = image_or_images
    else:
        images = [image_or_images]

    print("Downloading images...")
    os.makedirs(output_dir, exist_ok=True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=parallel) as executor:
        futures = [
            executor.submit(download_image, output_dir, image) for image in images
        ]
        for future in futures:
            future.result()

    return len(images)


def download_image(output_dir: str, image: GelbooruImage):
    print(f"Downloading {image.file_url}")
    response = requests.get(image.file_url, timeout=(10, 30))

    if not response.ok:
        print(
            f"Failed to download {image.file_url}. Status code: {response.status_code}, reason: {response.reason}"
        )
        return

    output_path = os.path.join(output_dir, image.filename)
    with open(output_path, "wb") as f:
        f.write(response.content)
    print(f"Downloaded to {output_path}")


if __name__ == "__main__":
    main()
