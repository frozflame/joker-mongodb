#!/usr/bin/env python3
# coding: utf-8
from __future__ import annotations

import logging
import subprocess
from pathlib import Path
from urllib.parse import urljoin

import requests

from joker.mongodb.utils import Pathlike

_logger = logging.getLogger(__name__)


def _download(url: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True) as resp:
        resp.raise_for_status()
        with open(path, "wb") as fout:
            for chunk in resp.iter_content(chunk_size=8192):
                fout.write(chunk)
    _logger.info("downloaded %s", path)


class DumpSuite:
    def __init__(self, dir_: Pathlike, source: str):
        self.dir_ = Path(dir_)
        self.source = source

    def fetch(self, db_name: str, collection_name: str):
        rel_paths = [
            f"{db_name}/{collection_name}.bson.gz",
            f"{db_name}/{collection_name}.metadata.json.gz",
        ]
        for path in rel_paths:
            url = urljoin(self.source, path)
            _download(url, self.dir_ / path)

    def batch_fetch(self, names: list[str]):
        for name in names:
            db_name, collection_name = name.split(".", maxsplit=1)
            self.fetch(db_name, collection_name)

    def restore(self, host: str = "mongodb", port: int = 27017):
        cmd = [
            "mongorestore",
            f"--dir={self.dir_}",
            "--gzip",
            "--drop",
            f"--host={host}",
            f"--port={port}",
        ]
        subprocess.run(cmd, check=True)
