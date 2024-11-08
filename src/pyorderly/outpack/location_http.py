import shutil
from pathlib import Path
from urllib.parse import urljoin

import requests
from typing_extensions import Self, override

from pyorderly.outpack.location_driver import LocationDriver
from pyorderly.outpack.metadata import MetadataCore, PacketFile, PacketLocation


def raise_http_error(response: requests.Response):
    if response.headers.get("Content-Type") == "application/json":
        result = response.json()
        # Unfortunately the schema is a bit inconsistent. Packit uses a
        # singular `error` whereas outpack_server uses a list of
        # `errors`.
        if "error" in result:
            detail = result["error"]["detail"]
        else:
            detail = result["errors"][0]["detail"]

        msg = f"{response.status_code} Error: {detail}"
        raise requests.HTTPError(msg)
    else:
        response.raise_for_status()


class OutpackHTTPClient(requests.Session):
    def __init__(self, url: str, authentication=None):
        super().__init__()
        self._base_url = url
        self._authentication = authentication

    @override
    def request(self, method, path, *args, **kwargs):
        if self._authentication is not None:
            headers = kwargs.setdefault("headers", {})
            headers.update(self._authentication())

        url = urljoin(self._base_url, path)
        response = super().request(method, url, *args, **kwargs)
        if not response.ok:
            raise_http_error(response)
        return response


class OutpackLocationHTTP(LocationDriver):
    def __init__(self, url: str, authentication=None):
        self._base_url = url
        self._client = OutpackHTTPClient(url, authentication)

    def __enter__(self) -> Self:
        self._client.__enter__()
        return self

    def __exit__(self, *args):
        self._client.__exit__(*args)

    @override
    def list_packets(self) -> dict[str, PacketLocation]:
        response = self._client.get("metadata/list").json()
        data = response["data"]
        return {
            entry["packet"]: PacketLocation.from_dict(entry) for entry in data
        }

    @override
    def metadata(self, ids: list[str]) -> dict[str, str]:
        result = {}
        for i in ids:
            result[i] = self._client.get(f"metadata/{i}/text").text

        return result

    @override
    def fetch_file(self, packet: MetadataCore, file: PacketFile, dest: str):
        response = self._client.get(f"file/{file.hash}", stream=True)
        with open(dest, "wb") as f:
            shutil.copyfileobj(response.raw, f)

    @override
    def list_unknown_packets(self, ids: list[str]) -> list[str]:
        response = self._client.post(
            "/packets/missing",
            json={
                "ids": ids,
                "unpacked": True,
            },
        ).json()
        return response["data"]

    @override
    def list_unknown_files(self, hashes: list[str]) -> list[str]:
        response = self._client.post(
            "/files/missing",
            json={
                "hashes": hashes,
            },
        ).json()
        return response["data"]

    @override
    def push_file(self, src: Path, hash: str):
        with open(src, "rb") as f:
            self._client.post(f"file/{hash}", data=f)

    @override
    def push_metadata(self, src: Path, hash: str):
        with open(src, "rb") as f:
            self._client.post(f"packet/{hash}", data=f)
