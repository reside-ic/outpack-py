import os

import pytest

from pyorderly.outpack.hash import hash_file
from pyorderly.outpack.location_path import OutpackLocationPath
from pyorderly.outpack.metadata import PacketFile
from pyorderly.outpack.util import read_string

from ..helpers import create_random_packet, create_temporary_root


def test_can_construct_location_path_object(tmp_path):
    root = create_temporary_root(tmp_path)
    loc = OutpackLocationPath(root.path)
    dat = loc.list_packets()
    assert dat == {}


def test_location_path_requires_existing_directory(tmp_path):
    with pytest.raises(Exception) as e:
        OutpackLocationPath(tmp_path / "file")
    assert e.match("Expected 'path' to be an existing directory")


def test_location_path_requires_exact_root(tmp_path):
    root = create_temporary_root(tmp_path)
    subdir = root.path / "subdir"
    os.makedirs(subdir)
    with pytest.raises(Exception) as e:
        OutpackLocationPath(subdir)
    assert e.match("Did not find existing outpack root in")

    OutpackLocationPath(root.path)


def test_location_path_returns_list_of_packet_ids(tmp_path):
    root = create_temporary_root(tmp_path)
    ids = [create_random_packet(root) for _ in range(3)]
    path = root.path
    loc = OutpackLocationPath(path)

    dat = loc.list_packets()
    assert all(packet_id in dat.keys() for packet_id in ids)

    expected_hashes = [packet.hash for packet in dat.values()]

    hashes = []
    for packet_id in ids:
        file_hash = hash_file(
            path / ".outpack" / "metadata" / packet_id, "sha256"
        )
        hashes.append(file_hash)
        assert str(file_hash) in expected_hashes

    assert len(hashes) == len(expected_hashes)


def test_location_path_can_return_metadata(tmp_path):
    root = create_temporary_root(tmp_path)
    ids = [create_random_packet(root) for _ in range(3)]
    path = root.path
    loc = OutpackLocationPath(path)

    metadata_path = tmp_path / ".outpack" / "metadata"
    str = [read_string(metadata_path / packet_id) for packet_id in ids]

    assert loc.metadata([ids[1]])[ids[1]] == str[1]
    for idx, i in enumerate(loc.metadata(ids).values()):
        assert i == str[idx]
    first = [ids[0], ids[0]]
    # Only returns one item even though we pass 2 in, dicts cannot
    # have the same key multiple times
    assert loc.metadata(first) == {ids[0]: str[0]}


def test_requesting_nonexistent_metadata_errors(tmp_path):
    root = create_temporary_root(tmp_path)
    ids = [create_random_packet(root) for _ in range(2)]
    path = root.path
    loc = OutpackLocationPath(path)

    errs = ["20220317-125935-ee5fd50e", "20220317-130038-48ffb8ba"]
    with pytest.raises(Exception) as e:
        loc.metadata([errs[0]])
    assert e.match("Some packet ids not found: '20220317-125935-ee5fd50e'")

    with pytest.raises(Exception) as e:
        loc.metadata(errs)
    assert e.match("Some packet ids not found: '.+', '.+'")
    assert e.match("20220317-130038-48ffb8ba")
    assert e.match("20220317-125935-ee5fd50e")

    with pytest.raises(Exception) as e:
        loc.metadata([ids[0], errs[0], ids[1]])
    assert e.match("Some packet ids not found: '20220317-125935-ee5fd50e'")


@pytest.mark.parametrize("use_file_store", [True, False])
def test_can_locate_files_from_store(tmp_path, use_file_store):
    root = create_temporary_root(tmp_path, use_file_store=use_file_store)
    path = root.path

    loc = OutpackLocationPath(path)

    id = create_random_packet(tmp_path)
    packet = root.index.metadata(id)
    files = root.index.metadata(id).files

    dest = tmp_path / "dest"

    loc.fetch_file(packet, files[0], dest)
    assert str(hash_file(dest)) == files[0].hash


@pytest.mark.parametrize("use_file_store", [True, False])
def test_sensible_error_if_file_not_found_in_store(tmp_path, use_file_store):
    root = create_temporary_root(tmp_path, use_file_store=use_file_store)
    id = create_random_packet(tmp_path)
    packet = root.index.metadata(id)

    loc = OutpackLocationPath(root.path)
    f = PacketFile(
        path="data.txt",
        hash="md5:c7be9a2c3cd8f71210d9097e128da316",
        size=12,
    )
    dest = tmp_path / "dest"

    msg = "Hash 'md5:c7be9a2c3cd8f71210d9097e128da316' not found at location"
    with pytest.raises(Exception, match=msg):
        loc.fetch_file(packet, f, dest)

    assert not dest.exists()
