import pytest
from pyorderly.outpack.hash import hash_validate_string
import requests
import responses
from requests import HTTPError

from pyorderly.outpack.filestore import FileStore
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
    create_random_file,
    create_random_packet,
    create_random_packet_chain,
    create_temporary_root,
    create_temporary_roots,
)
from ..helpers.outpack_server import start_outpack_server


def test_can_list_packets(tmp_path) -> None:
    root = create_temporary_root(
        tmp_path,
        use_file_store=True,
        require_complete_tree=True,
        path_archive=None,
    )
    ids = [create_random_packet(tmp_path) for _ in range(3)]
    packets = root.index.location(LOCATION_LOCAL)

    def filter_out_time(data: dict[str, PacketLocation]) -> dict[str, dict]:
        # outpack_server doesn't roundtrip the floating-point time field very
        # well, which leads to flaky tests.
        return {
            id: {k: v for k, v in entry.to_dict().items() if k != "time"}
            for id, entry in data.items()
        }

    with start_outpack_server(tmp_path) as url:
        location = OutpackLocationHTTP(url)
        assert location.list_packets().keys() == set(ids)
        assert filter_out_time(location.list_packets()) == filter_out_time(
            packets
        )


def test_can_fetch_metadata(tmp_path) -> None:
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


def test_can_fetch_files(tmp_path_factory) -> None:
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


def test_errors_if_file_not_found(tmp_path_factory) -> None:
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


def test_can_add_http_location(tmp_path) -> None:
    root = create_temporary_root(
        tmp_path,
        use_file_store=True,
        require_complete_tree=True,
        path_archive=None,
    )
    outpack_location_add(
        "upstream", "http", {"url": "http://example.com/path"}, root
    )


def test_can_pull_metadata(tmp_path) -> None:
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


def test_can_pull_packet(tmp_path) -> None:
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
def test_http_client_errors() -> None:
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


def test_can_push_files(tmp_path) -> None:
    file = create_random_file(tmp_path / "data")
    h = str(hash_file(file))
    with start_outpack_server(tmp_path / "server") as url:
        loc = OutpackLocationHTTP(url)
        loc.push_file(file, h)

    store = FileStore(tmp_path / "server" / ".outpack" / "files")
    assert store.exists(h)


def test_can_list_unknown_files(tmp_path) -> None:
    known_files = [create_random_file(tmp_path / "data") for _ in range(5)]
    unknown_files = [create_random_file(tmp_path / "data") for _ in range(5)]
    known_hashes = [str(hash_file(f)) for f in known_files]
    unknown_hashes = [str(hash_file(f)) for f in unknown_files]

    with start_outpack_server(tmp_path / "server") as url:
        loc = OutpackLocationHTTP(url)
        for f, h in zip(known_files, known_hashes):
            loc.push_file(f, h)

        result = loc.list_unknown_files(known_hashes + unknown_hashes)
        assert set(result) == set(unknown_hashes)


@pytest.mark.parametrize("use_file_store", [True, False])
def test_can_push_packet(tmp_path, use_file_store) -> None:
    root = create_temporary_root(
        tmp_path / "root",
        use_file_store=use_file_store,
    )

    id = create_random_packet(root)
    packet = root.index.location(LOCATION_LOCAL)[id]

    with start_outpack_server(tmp_path / "server") as url:
        outpack_location_add(
            "upstream",
            "http",
            {"url": url},
            root=root,
        )
        outpack_location_push(id, "upstream", root=root)

        metadata = OutpackLocationHTTP(url).metadata([id])
        hash_validate_string(metadata[id], packet.hash, "packet")


@pytest.mark.parametrize("use_file_store", [True, False])
def test_can_push_packet_chain(tmp_path, use_file_store) -> None:
    root = create_temporary_root(
        tmp_path / "root",
        use_file_store=use_file_store,
    )

    ids = create_random_packet_chain(root, length=4)

    with start_outpack_server(tmp_path / "server") as url:
        outpack_location_add("upstream", "http", {"url": url}, root=root)
        plan = outpack_location_push(ids["d"], "upstream", root=root)
        assert len(plan.packets) == 4

        loc = OutpackLocationHTTP(url)
        assert set(loc.list_packets().keys()) == set(ids.values())


def test_can_list_unknown_packets(tmp_path) -> None:
    root = create_temporary_root(tmp_path / "root")
    known_ids = [create_random_packet(root) for _ in range(5)]
    unknown_ids = [create_random_packet(root) for _ in range(5)]

    with start_outpack_server(tmp_path / "server") as url:
        outpack_location_add("upstream", "http", {"url": url}, root=root)
        outpack_location_push(known_ids, "upstream", root=root)

        loc = OutpackLocationHTTP(url)
        result = loc.list_unknown_packets(known_ids + unknown_ids)
        assert set(result) == set(unknown_ids)
