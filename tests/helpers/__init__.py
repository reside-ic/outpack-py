import pickle
import random
import shutil
import string
import textwrap
from contextlib import contextmanager
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import Optional

import pytest

from pyorderly.outpack.init import outpack_init
from pyorderly.outpack.location import outpack_location_add_path
from pyorderly.outpack.metadata import MetadataCore, PacketDepends
from pyorderly.outpack.packet import Packet, insert_packet
from pyorderly.outpack.root import OutpackRoot, root_open
from pyorderly.outpack.schema import outpack_schema_version
from pyorderly.outpack.util import openable_temporary_file
from pyorderly.run import orderly_run

from .ssh_server import SSHServer  # noqa: F401


@contextmanager
def create_packet(root, name, *, packet_id=None, parameters=None):
    """
    Create an Outpack packet.

    This function should be used as a context manager in a `with` block. The
    packet can be populated in the block's body. The packet gets completed and
    added to the repository when the context manager is exited.
    """
    root = root_open(root, locate=False)
    with TemporaryDirectory() as src:
        p = Packet(root, src, name, id=packet_id, parameters=parameters)
        try:
            yield p
        except BaseException:
            p.end(succesful=False)
            raise
        else:
            metadata = p.end(succesful=True)
            insert_packet(root, Path(src), metadata)


def create_random_file(dir: Path) -> Path:
    dir.mkdir(exist_ok=True, parents=True)
    with NamedTemporaryFile(delete=False, dir=dir, mode="w") as f:
        d = [f"{random.random()}\n" for _ in range(10)]
        f.writelines(d)

    return Path(f.name)


def create_random_packet(
    root, name="data", *, parameters=None, packet_id=None
) -> str:
    d = [f"{random.random()}\n" for _ in range(10)]

    with create_packet(
        root, name=name, parameters=parameters, packet_id=packet_id
    ) as p:
        path_data = p.path / "data.txt"
        with open(path_data, "w") as f:
            f.writelines(d)

    return p.id


## Create a chain of packets a, b, c, ... that depend on each other
def create_random_packet_chain(root, length, base=None):
    ids = {}
    for i in range(length):
        name = chr(i + ord("a"))
        with create_packet(root, name) as p:
            if base is not None:
                p.use_dependency(base, {"input.txt": "data.txt"})

            d = [f"{random.random()}\n" for _ in range(10)]
            with open(p.path / "data.txt", "w") as f:
                f.writelines(d)

            ids[name] = p.id
            base = p.id

    return ids


def create_temporary_root(path, **kwargs) -> OutpackRoot:
    outpack_init(path, **kwargs)
    return root_open(path, locate=False)


def create_temporary_roots(
    path, names=("src", "dst"), *, add_location=False, **kwargs
):
    root = {}
    for name in names:
        root[name] = create_temporary_root(path / name, **kwargs)

    if add_location:
        for i in range(len(names) - 1):
            outpack_location_add_path(
                names[i], root[names[0]], root=root[names[i + 1]]
            )

    return root


def copy_examples(names, root):
    if isinstance(names, str):
        names = [names]
    for nm in names:
        path_src = root.path / "src" / nm
        path_src.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(Path("tests/orderly/examples") / nm, path_src)


def copy_shared_resources(names, root):
    shared_path = root.path / "shared"
    shared_path.mkdir(exist_ok=True)

    if isinstance(names, str):
        names = [names]
    for nm in names:
        src = Path("tests/orderly/shared/") / nm
        dst = shared_path / nm
        if src.is_dir():
            shutil.copytree(src, dst)
        else:
            shutil.copyfile(src, dst)


def create_metadata_depends(id: str, depends: Optional[list[str]] = None):
    if depends is None:
        depends = []
    dependencies = [
        PacketDepends(dependency_id, "", []) for dependency_id in depends
    ]
    return {
        id: MetadataCore(
            outpack_schema_version(),
            id,
            "name_" + random_characters(4),
            {},
            {},
            [],
            dependencies,
            None,
            None,
        )
    }


def random_characters(n):
    return "".join(
        random.choice(string.ascii_letters + string.digits) for _ in range(n)
    )


# Like Rs rep function, useful for setting up test values
def rep(x, each):
    ret = []
    if isinstance(each, int):
        each = [each] * len(x)
    if len(x) != len(each):
        msg = (
            "Repeats must be int or same length as the thing you want to repeat"
        )
        raise Exception(msg)
    for item, times in zip(x, each):
        ret.extend([item] * times)

    return ret


def touch_files(*files):
    """
    Create empty files at the given paths.

    If necessary, parent directories are created for each file.
    """
    for f in files:
        f.parent.mkdir(parents=True, exist_ok=True)
        f.touch()


def write_file(path, text):
    """
    Write text to a path.

    If necessary, parent directories are created for each file.
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text)


def run_snippet(name, code, root, **kwargs):
    """
    Run a snippet of Python code as an Orderly report.

    The snippet is wrapped in a function and may use return statements. The
    snippet's return value is returned by this function, alongside the packet
    ID.
    """
    # The obvious way of returning the snippet's result would be to write it as
    # an artefact of the packet. This is visible in the packet's metadata and
    # would cause noise in the expectations of tests using this function.
    #
    # We work around that by returning the value out-of-band, in a temporary
    # file outside of the report's directory. That way it is completely
    # transparent to the caller
    with openable_temporary_file() as output:
        wrapped = f"""
def body():
{textwrap.indent(code, "    ")}

import pickle
with open({output.name!r}, "wb") as f:
    pickle.dump(body(), f)
"""
        write_file(root.path / "src" / name / f"{name}.py", wrapped)
        id = orderly_run(name, root=root, **kwargs)
        result = pickle.load(output)  # noqa: S301
        return id, result


@contextmanager
def report_raises(match):
    with pytest.raises(Exception, match="Running orderly report failed!") as e:
        yield
    excinfo = pytest.ExceptionInfo.from_exception(e.value.__cause__)
    excinfo.match(match)
