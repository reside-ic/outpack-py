from contextlib import contextmanager
from pathlib import Path

import paramiko
import pytest

from pyorderly.outpack.hash import hash_file
from pyorderly.outpack.location import outpack_location_add
from pyorderly.outpack.location_pull import (
    outpack_location_pull_metadata,
    outpack_location_pull_packet,
)
from pyorderly.outpack.location_ssh import OutpackLocationSSH, parse_ssh_url
from pyorderly.outpack.metadata import PacketFile
from pyorderly.outpack.static import LOCATION_LOCAL
from pyorderly.outpack.util import read_string

from ..helpers import (
    SSHServer,
    create_random_packet,
    create_temporary_root,
    create_temporary_roots,
)


@contextmanager
def start_ssh_location(root: Path, path: str = "", known_hosts=None):
    """Start an SSH server and expose it as an location driver."""
    with SSHServer(root) as server:
        if known_hosts is None:
            known_hosts = [server.host_key_entry]

        with OutpackLocationSSH(
            url=server.url(path),
            known_hosts=known_hosts,
            password="",
        ) as location:
            yield location


def test_can_parse_url():
    assert parse_ssh_url("ssh://example.com/") == (
        None,
        "example.com",
        None,
        "",
    )
    assert parse_ssh_url("ssh://example.com/foobar") == (
        None,
        "example.com",
        None,
        "foobar",
    )
    assert parse_ssh_url("ssh://myuser@example.com/foobar") == (
        "myuser",
        "example.com",
        None,
        "foobar",
    )
    assert parse_ssh_url("ssh://myuser@example.com:1234/foobar") == (
        "myuser",
        "example.com",
        1234,
        "foobar",
    )
    assert parse_ssh_url("ssh://example.com//") == (
        None,
        "example.com",
        None,
        "/",
    )
    assert parse_ssh_url("ssh://example.com//foo/bar") == (
        None,
        "example.com",
        None,
        "/foo/bar",
    )


def test_errors_on_invalid_url():
    msg = "Protocol of SSH url must be 'ssh'"
    with pytest.raises(Exception, match=msg):
        parse_ssh_url("/home/alice")

    with pytest.raises(Exception, match=msg):
        parse_ssh_url("www.example.com")

    with pytest.raises(Exception, match=msg):
        parse_ssh_url("http://www.example.com")

    msg = "No path specified for SSH location"
    with pytest.raises(Exception, match=msg):
        parse_ssh_url("ssh://example.com")


def test_errors_on_missing_repo(tmp_path):
    (tmp_path / "bar").mkdir()

    msg = "Path 'foo' on remote 127.0.0.1 does not exist"
    with pytest.raises(Exception, match=msg):
        with start_ssh_location(tmp_path, "foo"):
            pass

    msg = "Path 'bar' on remote 127.0.0.1 is not a valid outpack repository"
    with pytest.raises(Exception, match=msg):
        with start_ssh_location(tmp_path, "bar"):
            pass


def test_errors_if_host_key_is_unknown(tmp_path):
    create_temporary_root(tmp_path)

    msg = "Server .* not found in known_hosts"
    with pytest.raises(paramiko.SSHException, match=msg):
        with start_ssh_location(tmp_path, known_hosts=[]):
            pass


def test_can_read_config(tmp_path):
    create_temporary_root(tmp_path / "foo", require_complete_tree=True)
    create_temporary_root(tmp_path / "bar", require_complete_tree=False)

    with start_ssh_location(tmp_path / "foo") as location:
        assert location.config.core.require_complete_tree

    with start_ssh_location(tmp_path / "bar") as location:
        assert not location.config.core.require_complete_tree


def test_can_list_packets(tmp_path):
    root = create_temporary_root(tmp_path)
    ids = [create_random_packet(tmp_path) for _ in range(3)]
    packets = root.index.location(LOCATION_LOCAL)

    with start_ssh_location(tmp_path) as location:
        assert location.list_packets().keys() == set(ids)
        assert location.list_packets() == packets


def test_can_fetch_metadata(tmp_path):
    root = create_temporary_root(tmp_path)
    ids = [create_random_packet(tmp_path) for _ in range(3)]
    metadata = {
        k: read_string(root.path / ".outpack" / "metadata" / k) for k in ids
    }

    with start_ssh_location(tmp_path) as location:
        assert location.metadata([]) == {}
        assert location.metadata([ids[0]]) == {ids[0]: metadata[ids[0]]}
        assert location.metadata(ids) == metadata


@pytest.mark.parametrize("use_file_store", [True, False])
def test_can_fetch_files(tmp_path, use_file_store):
    root = create_temporary_root(tmp_path, use_file_store=use_file_store)
    id = create_random_packet(tmp_path)
    files = root.index.metadata(id).files

    with start_ssh_location(tmp_path) as location:
        dest = tmp_path / "data"
        location.fetch_file(root.index.metadata(id), files[0], dest)
        assert str(hash_file(dest)) == files[0].hash


@pytest.mark.parametrize("use_file_store", [True, False])
def test_errors_if_file_not_found(tmp_path, use_file_store):
    root = create_temporary_root(tmp_path, use_file_store=use_file_store)
    id = create_random_packet(tmp_path)

    with start_ssh_location(tmp_path) as location:
        packet = root.index.metadata(id)
        f = PacketFile(
            path="unknown_data.txt",
            hash="md5:c7be9a2c3cd8f71210d9097e128da316",
            size=12,
        )
        dest = tmp_path / "dest"

        msg = f"Hash '{f.hash}' not found at location"
        with pytest.raises(Exception, match=msg):
            location.fetch_file(packet, f, dest)


def test_can_add_ssh_location(tmp_path):
    root = create_temporary_root(tmp_path)
    outpack_location_add(
        "upstream", "ssh", {"url": "ssh://example.com/path"}, root
    )


def test_cannot_add_invalid_ssh_url(tmp_path):
    root = create_temporary_root(tmp_path)

    msg = "Protocol of SSH url must be 'ssh'"
    with pytest.raises(Exception, match=msg):
        outpack_location_add(
            "upstream", "ssh", {"url": "http://example.com/path"}, root
        )


def test_can_pass_username_in_url(tmp_path):
    root = create_temporary_roots(tmp_path)
    with SSHServer(tmp_path, allowed_users=["alice"]) as server:
        outpack_location_add(
            "alice_upstream",
            "ssh",
            {
                "url": server.url("src", username="alice"),
                "known_hosts": [server.host_key_entry],
                "password": "",
            },
            root=root["dst"],
        )

        outpack_location_add(
            "bob_upstream",
            "ssh",
            {
                "url": server.url("src", username="bob"),
                "known_hosts": [server.host_key_entry],
                "password": "",
            },
            root=root["dst"],
        )

        outpack_location_pull_metadata("alice_upstream", root=root["dst"])

        with pytest.raises(paramiko.AuthenticationException):
            outpack_location_pull_metadata("bob_upstream", root=root["dst"])


def test_can_pull_metadata(tmp_path):
    root = create_temporary_roots(tmp_path)
    id = create_random_packet(root["src"])

    with SSHServer(tmp_path) as server:
        url = server.url("src")
        outpack_location_add(
            "upstream",
            "ssh",
            {
                "url": url,
                "known_hosts": [server.host_key_entry],
                "password": "",
            },
            root=root["dst"],
        )
        assert id not in root["dst"].index.all_metadata()

        outpack_location_pull_metadata(root=root["dst"])
        assert id in root["dst"].index.all_metadata()


def test_can_pull_packet(tmp_path):
    root = create_temporary_roots(tmp_path)
    id = create_random_packet(root["src"])

    with SSHServer(tmp_path) as server:
        url = server.url("src")
        outpack_location_add(
            "upstream",
            "ssh",
            {
                "url": url,
                "known_hosts": [server.host_key_entry],
                "password": "",
            },
            root=root["dst"],
        )
        outpack_location_pull_metadata(root=root["dst"])
        assert id not in root["dst"].index.unpacked()
        outpack_location_pull_packet(id, root=root["dst"])
        assert id in root["dst"].index.unpacked()


def test_can_use_abolute_path(tmp_path):
    root = create_temporary_roots(tmp_path, ["foo", "bar"])
    ids = {k: create_random_packet(v) for k, v in root.items()}

    path = root["foo"].path
    assert path.is_absolute()
    with start_ssh_location(tmp_path, path=path.as_posix()) as location:
        assert location.list_packets().keys() == {ids["foo"]}

    with start_ssh_location(tmp_path, path="bar") as location:
        assert location.list_packets().keys() == {ids["bar"]}
