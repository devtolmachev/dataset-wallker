import asyncio
from concurrent.futures import ThreadPoolExecutor
from curses import tigetflag
import io
import os
from pathlib import Path
from pprint import pprint
from subprocess import Popen
import subprocess
import tempfile
import aiobotocore.client
import aiobotocore.session
import tempdir
import uvicorn
import uvloop
import s3fs


AWS_ACCESS_KEY_ID = "d8402d0971e14f61be19a46e48463811"
AWS_SECRET_ACCESS_KEY = "85dab4c391d943bb9c935da94473bc33"
ENDPOINT_URL = "https://s3.ru-1.storage.selcloud.ru"

# s3 = s3fs.S3FileSystem(
#     endpoint_url=ENDPOINT_URL,
#     key=AWS_ACCESS_KEY_ID,
#     secret=AWS_SECRET_ACCESS_KEY,
# )

bucket = "Heap"


async def walk_s3_storage(top, s3, max_deepth: int = None):
    files = await s3.ls(top, detail=True)

    _files = [
        o["Key"]
        for o in files
        if o["type"] == "file" and not o["Key"].endswith("/")
    ]
    dirs = [o["Key"] for o in files if o["type"] == "directory"]

    yield (top, dirs, _files)

    for d in dirs:
        if max_deepth is not None:
            max_deepth -= 1

        if isinstance(max_deepth, int) and max_deepth <= 0:
            return

        async for i in walk_s3_storage(d, s3, max_deepth=max_deepth):
            yield i


files = []
import filetype, tempdir

tempdirectory = tempdir.TempDir()


async def append_files(obj):
    global files, tempdirectory

    root, dirs, _files = obj
    for fn in _files:
        try:
            with s3.open(fn, "rb") as buffer:
                file = filetype.guess(buffer.read())

                if hasattr(file, "extension") and file.extension == "dcm":
                    print(f"[READ] {buffer.full_name}", flush=True)
                    files.append(fn)

        except Exception as e:
            print(e.__class__.__name__, str(e))
            pass


def list_files_recursively(
    s3: s3fs.S3FileSystem, path: str | Path, extension: str = "dcm"
) -> list[str]:
    path = Path(str(path))
    files = []
    temp_dir = tempdir.TempDir()

    def scan_dirs(obj):
        nonlocal files
        root, _, fns = obj

        for fn in fns:
            if fn.endswith("/") or not fn:
                continue
            try:
                rpath = os.path.join(root, fn)
                lpath = os.path.join(temp_dir.name, rpath)
                
                l_parrent = os.path.split(lpath)[0]
                if not os.path.exists(l_parrent):
                    os.makedirs(l_parrent, exist_ok=True)
                
                s3.get_file(rpath, lpath)
                file = filetype.guess(lpath)
                if hasattr(file, "extension") and file.extension == extension:
                    print(str(rpath))
                    files.append(str(rpath))

            except Exception as e:
                print(e.__class__.__name__, str(e))
                continue
    
    with temp_dir:
        with ThreadPoolExecutor() as executor:
            executor.map(
                scan_dirs,
                s3.walk(bucket),
            )
        ...
    
    print(len(files))
    return files


async def main():
    s3 = s3fs.S3FileSystem(
        endpoint_url=ENDPOINT_URL,
        key=AWS_ACCESS_KEY_ID,
        secret=AWS_SECRET_ACCESS_KEY,
    )

    # for obj in s3.walk(bucket, maxdepth=3):
    #     print(obj)

    print(list_files_recursively(s3, bucket))


# for i, (root, dirs, files) in enumerate(walk_s3_storage(bucket, max_deepth=4)):
#     # print(i)
#     print(root, files)

# with tempdirectory:
#     with ThreadPoolExecutor() as e:
#         paths = walk_s3_storage(bucket)
#         e.map(append_files, paths)


# asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
asyncio.new_event_loop().run_until_complete(main())

exit()

import boto3

session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

manager = session.resource("s3", endpoint_url=ENDPOINT_URL).buckets
bucket = next((b for b in manager.all() if b.name == "Heap"), None)
if not bucket:
    raise TypeError(f"bucket : {bucket}")

with tempdir.TempDir() as temp:
    prefix = "boto3"
    os.makedirs(prefix, exist_ok=True)

    def download_files(summary):
        key = summary.key
        return print(key)
        if os.path.isdir(key):
            return
        dirpath = f"{prefix}/{os.path.split(summary.key)[0]}"
        if not os.path.exists(dirpath):
            os.makedirs(dirpath, exist_ok=True)

        fn = f"{prefix}/{summary.key}"
        print(fn, key)
        os.makedirs(dirpath, exist_ok=True)
        with open(fn, "w"):
            bucket.download_file(key, fn)

    iterator = bucket.objects.all()
    # for summary in iterator:
    #     dirpath = f"{prefix}/{os.path.split(summary.key)[0]}"
    #     fn = f"{prefix}/{summary.key}"
    #     print(dirpath)
    #     os.makedirs(dirpath, exist_ok=True)
    #     with open(fn, "w") as f:
    #         bucket.download_file(summary.key, fn)
    #     exit()

    with ThreadPoolExecutor(32) as e:
        e.map(download_files, iterator)
