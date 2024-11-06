from typing import Dict

import pytest
import requests
import responses
from requests import HTTPError

from pyorderly.outpack.hash import hash_file
from pyorderly.outpack.location import outpack_location_add
from pyorderly.outpack.location_http import (
    OutpackHTTPClient,
    OutpackLocationHTTP,
)
from pyorderly.outpack.location_pull import (
    outpack_location_pull_metadata,
    outpack_location_pull_packet,
)
from pyorderly.outpack.location_push import outpack_location_push
from pyorderly.outpack.metadata import PacketFile, PacketLocation
from pyorderly.outpack.static import LOCATION_LOCAL
from pyorderly.outpack.util import read_string

from ..helpers import (
    create_random_packet,
    create_temporary_root,
    create_temporary_roots,
)
from ..helpers.outpack_server import start_outpack_server


def test_can_list_packets(tmp_path):
    root = create_temporary_root(
        tmp_path,
        use_file_store=True,
        require_complete_tree=True,
        path_archive=None,
    )
    ids = [create_random_packet(tmp_path) for _ in range(3)]
    packets = root.index.location(LOCATION_LOCAL)

    def filter_out_time(data: Dict[str, PacketLocation]) -> Dict[str, dict]:
        # outpack_server doesn't roundtrip the floating-point time field very
        # well, which leads to flaky tests.
        return {
            id: {k: v for k, v in entry.to_dict().items() if k != "time"}
            for id, entry in data.items()
        }

    with start_outpack_server(tmp_path) as url:
        location = OutpackLocationHTTP(url)
        assert location.list().keys() == set(ids)
        assert filter_out_time(location.list()) == filter_out_time(packets)


def test_can_fetch_metadata(tmp_path):
    root = create_temporary_root(
        tmp_path,
        use_file_store=True,
        require_complete_tree=True,
        path_archive=None,
    )
    ids = [create_random_packet(tmp_path) for _ in range(3)]
    metadata = {
        k: read_string(root.path / ".outpack" / "metadata" / k) for k in ids
    }

    with start_outpack_server(tmp_path) as url:
        location = OutpackLocationHTTP(url)
        assert location.metadata([]) == {}
        assert location.metadata([ids[0]]) == {ids[0]: metadata[ids[0]]}
        assert location.metadata(ids) == metadata


def test_can_fetch_files(tmp_path_factory):
    root = create_temporary_root(
        tmp_path_factory.mktemp("server"),
        use_file_store=True,
        require_complete_tree=True,
        path_archive=None,
    )
    id = create_random_packet(root)
    files = root.index.metadata(id).files

    dest = tmp_path_factory.mktemp("data") / "result"

    with start_outpack_server(root) as url:
        location = OutpackLocationHTTP(url)

        location.fetch_file(root.index.metadata(id), files[0], dest)

        assert str(hash_file(dest)) == files[0].hash


def test_errors_if_file_not_found(tmp_path_factory):
    root = create_temporary_root(
        tmp_path_factory.mktemp("server"),
        use_file_store=True,
        require_complete_tree=True,
        path_archive=None,
    )
    id = create_random_packet(root)

    dest = tmp_path_factory.mktemp("data") / "result"

    with start_outpack_server(root) as url:
        location = OutpackLocationHTTP(url)
        packet = root.index.metadata(id)
        f = PacketFile(
            path="unknown_data.txt",
            hash="md5:c7be9a2c3cd8f71210d9097e128da316",
            size=12,
        )

        msg = f"'{f.hash}' not found"
        with pytest.raises(requests.HTTPError, match=msg):
            location.fetch_file(packet, f, dest)


def test_can_add_http_location(tmp_path):
    root = create_temporary_root(
        tmp_path,
        use_file_store=True,
        require_complete_tree=True,
        path_archive=None,
    )
    outpack_location_add(
        "upstream", "http", {"url": "http://example.com/path"}, root
    )


def test_can_pull_metadata(tmp_path):
    root = create_temporary_roots(
        tmp_path,
        use_file_store=True,
        require_complete_tree=True,
        path_archive=None,
    )
    id = create_random_packet(root["src"])

    with start_outpack_server(root["src"]) as url:
        outpack_location_add(
            "upstream",
            "http",
            {"url": url},
            root=root["dst"],
        )
        assert id not in root["dst"].index.all_metadata()

        outpack_location_pull_metadata(root=root["dst"])
        assert id in root["dst"].index.all_metadata()


def test_can_pull_packet(tmp_path):
    root = create_temporary_roots(
        tmp_path,
        use_file_store=True,
        require_complete_tree=True,
        path_archive=None,
    )
    id = create_random_packet(root["src"])

    with start_outpack_server(root["src"]) as url:
        outpack_location_add(
            "upstream",
            "http",
            {"url": url},
            root=root["dst"],
        )

        outpack_location_pull_metadata(root=root["dst"])
        assert id not in root["dst"].index.unpacked()
        outpack_location_pull_packet(id, root=root["dst"])
        assert id in root["dst"].index.unpacked()


@responses.activate
def test_http_client_errors():
    responses.get(
        "https://example.com/text-error", status=400, body="Request failed"
    )
    responses.get(
        "https://example.com/packit-error",
        status=400,
        json={"error": {"detail": "Custom error message"}},
    )
    responses.get(
        "https://example.com/outpack-error",
        status=400,
        json={"errors": [{"detail": "Custom error message"}]},
    )

    client = OutpackHTTPClient("https://example.com")
    with pytest.raises(HTTPError, match="400 Client Error: Bad Request"):
        client.get("/text-error")
    with pytest.raises(HTTPError, match="400 Error: Custom error message"):
        client.get("/packit-error")
    with pytest.raises(HTTPError, match="400 Error: Custom error message"):
        client.get("/outpack-error")


def test_can_push_packet(tmp_path):
    root = create_temporary_roots(
        tmp_path,
        use_file_store=True,
        require_complete_tree=True,
        path_archive=None,
    )
    id = create_random_packet(root["dst"])

    with start_outpack_server(root["src"]) as url:
        outpack_location_add(
            "upstream",
            "http",
            {"url": url},
            root=root["dst"],
        )

        print(outpack_location_push([id], "upstream", root=root["dst"]))

        # assert id not in root["dst"].index.all_metadata()

        # outpack_location_pull_metadata(root=root["dst"])
        # assert id in root["dst"].index.all_metadata()
