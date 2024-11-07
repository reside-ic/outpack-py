from pathlib import Path

import pytest

from pyorderly.outpack.config import read_config
from pyorderly.outpack.filestore import FileStore
from pyorderly.outpack.index import Index
from pyorderly.outpack.init import outpack_init
from pyorderly.outpack.root import root_open
from pyorderly.outpack.util import transient_working_directory

from .. import helpers


def test_can_open_existing_root():
    r = root_open("example")
    assert r.config == read_config("example")
    assert isinstance(r.files, FileStore)
    assert isinstance(r.index, Index)


def test_can_open_root_with_no_store(tmp_path):
    outpack_init(tmp_path)
    r = root_open(tmp_path)
    assert r.config == read_config(tmp_path)
    assert r.files is None
    assert isinstance(r.index, Index)


def test_can_open_root_from_a_subdir():
    r = root_open("example", locate=False)
    assert root_open("example/src", locate=True).path == r.path
    assert root_open("example/src/data", locate=True).path == r.path


def test_can_open_root_from_wd():
    p = Path("example").resolve()
    with transient_working_directory("example"):
        assert root_open(None, locate=True).path == p
        assert root_open(None, locate=False).path == p


def test_can_error_if_recursion_to_find_root_fails():
    p = Path("example").resolve()
    with transient_working_directory("example/src/data"):
        assert root_open(None, locate=True).path == p

        with pytest.raises(Exception, match="Did not find existing outpack"):
            assert root_open(None, locate=False).path == p


def test_roots_are_handed_back():
    r = root_open("example")
    assert root_open(r, locate=True) == r
    assert root_open(r, locate=False) == r


def test_paths_must_be_directories(tmp_path):
    p = tmp_path / "p"
    p.touch()

    with pytest.raises(Exception, match="to be an existing directory"):
        assert root_open(p)


def test_can_find_file_by_hash(tmp_path):
    outpack_init(tmp_path, use_file_store=False, path_archive="archive")
    id = [helpers.create_random_packet(tmp_path) for _ in range(3)]
    root = root_open(tmp_path)
    meta = root.index.metadata(id[1])
    hash = meta.files[0].hash
    assert (
        root.archive.find_file(hash)
        == root.path / "archive" / "data" / id[1] / "data.txt"
    )

    with pytest.raises(FileNotFoundError):
        root.archive.find_file(hash[:-1])


def test_can_reject_corrupted_files(tmp_path, capsys):
    outpack_init(tmp_path, use_file_store=False, path_archive="archive")
    id = [helpers.create_random_packet(tmp_path) for _ in range(3)]
    root = root_open(tmp_path)
    meta = root.index.metadata(id[1])
    hash = meta.files[0].hash
    path = root.path / "archive" / "data" / id[1] / "data.txt"
    with open(path, "a") as f:
        f.write("1")

    msg = "File not found in archive"
    with pytest.raises(FileNotFoundError, match=msg):
        root.archive.find_file(hash)

    captured = capsys.readouterr()
    assert (
        captured.out
        == f"Rejecting file from archive 'data.txt' in 'data/{id[1]}'\n"
    )

    msg = "File not found in archive, or corrupt: 'data.txt'"
    with pytest.raises(FileNotFoundError, match=msg):
        root.export_file(id[1], "data.txt", tmp_path / "result.txt")


@pytest.mark.parametrize("use_file_store", [True, False])
def test_can_export_files_from_root(tmp_path, use_file_store):
    outpack_init(
        tmp_path,
        use_file_store=use_file_store,
        path_archive=None if use_file_store else "archive",
    )

    id = helpers.create_random_packet(tmp_path)
    r = root_open(tmp_path)
    r.export_file(id, "data.txt", tmp_path / "result.txt")
    assert (tmp_path / "result.txt").exists()


def test_root_path_is_absolute(tmp_path):
    helpers.create_temporary_root(tmp_path / "foo")
    (tmp_path / "bar").mkdir()

    with transient_working_directory(tmp_path / "foo"):
        r = root_open(None)
        assert r.path.is_absolute()

    with transient_working_directory(tmp_path / "bar"):
        r = root_open("../foo")
        assert r.path.is_absolute()
