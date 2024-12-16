import datetime
import os
import tempfile
import time
from contextlib import contextmanager
from itertools import filterfalse, tee
from pathlib import Path, PurePath
from typing import Dict, List, Optional, TypeVar, Union


def find_file_descend(filename, path):
    path = Path(path)
    root = Path(path.anchor)

    while path != root:
        attempt = path / filename
        if attempt.exists():
            return str(attempt.parent)
        path = path.parent

    return None


def iso_time_str(t):
    return time.strftime("%Y%m%d-%H%M%S", time.gmtime(t))


def time_to_num(x):
    return x.timestamp()


def num_to_time(x):
    return datetime.datetime.fromtimestamp(x, datetime.timezone.utc)


# Recursively find all normal files below 'path', returning them
# relative to that path.
#
# This excludes any files contained in a `__pycache__` directory.
def all_normal_files(path):
    result = []
    for root, dirs, files in os.walk(path):
        base = Path(root).relative_to(path)
        result.extend(str(base.joinpath(f)) for f in files)

        if "__pycache__" in dirs:
            dirs.remove("__pycache__")
    return result


@contextmanager
def transient_working_directory(path):
    origin = os.getcwd()
    try:
        if path is not None:
            os.chdir(path)
        yield
    finally:
        if path is not None:
            os.chdir(origin)


def assert_file_exists(
    path: Union[str, List[str]], *, workdir=None, name="File"
):
    with transient_working_directory(workdir):
        if isinstance(path, list):
            missing = [str(p) for p in path if not os.path.exists(p)]
        else:
            missing = [] if os.path.exists(path) else [path]

    if len(missing):
        missing_str = ", ".join(missing)
        msg = f"{name} does not exist: {missing_str}"
        raise Exception(msg)


def assert_relative_path(path: str, name: str):
    path = Path(path)

    # On Windows, this is not equivalent to path.is_absolute().
    # There are paths which are neither absolute nor relative, but somewhere
    # between the two. These are paths such as `C:foo` or `\foo`. The former has
    # a drive but no root, and the latter has a root but no drive. We want to
    # exclude both scenarios. On POSIX, drive will always be empty and this is
    # equivalent to calling `is_absolute()`.
    #
    # See https://github.com/python/cpython/issues/44626 for some discussion.
    # Unfortunately, while the issue was closed, the `is_relative` function
    # mentioned was never added.
    if path.drive or path.root:
        msg = f"Expected {name} path '{path}' to be a relative path"
        raise Exception(msg)

    if ".." in path.parts:
        msg = f"Path '{path}' must not contain '..' component"
        raise Exception(msg)


def expand_dirs(paths, *, workdir=None):
    if len(paths) == 0:
        return []
    ret = []
    with transient_working_directory(workdir):
        for p in paths:
            if os.path.isdir(p):
                ret += [os.path.join(p, f) for f in all_normal_files(p)]
            else:
                ret.append(str(p))
    return ret


def match_value(arg, choices, name):
    if arg not in choices:
        choices_str = "', '".join(choices)
        msg = f"{name} must be one of '{choices_str}'"
        raise Exception(msg)


def relative_path_array(files: Union[str, List[str]], name: str) -> List[str]:
    if not isinstance(files, list):
        files = [files]

    for f in files:
        assert_relative_path(f, name)

    return files


def relative_path_mapping(
    files: Union[str, List[str], Dict[str, str]], name: str
) -> Dict[str, str]:
    if isinstance(files, str):
        files = {files: files}
    elif isinstance(files, list):
        files = {f: f for f in files}

    for k, v in files.items():
        assert_relative_path(k, name)
        assert_relative_path(v, name)

    return files


def read_string(path):
    with open(path) as f:
        lines = f.read().rstrip()
    return lines


def format_list(x):
    return ", ".join("'" + item + "'" for item in x)


def pl(x, singular, plural=None):
    if plural is None:
        plural = singular + "s"

    if isinstance(x, int):
        length = x
    else:
        length = len(x)
    return f"{singular if length == 1 else plural}"


def partition(pred, iterable):
    """Partition entries into false entries and true entries.

    This is slightly modified version of partition from itertools
    recipes https://docs.python.org/dev/library/itertools.html#itertools-recipes
    If *pred* is slow, consider wrapping it with functools.lru_cache().
    """
    # partition(is_odd, range(10)) --> 1 3 5 7 9 and 0 2 4 6 8
    t1, t2 = tee(iterable)
    return list(filter(pred, t1)), list(filterfalse(pred, t2))


@contextmanager
def openable_temporary_file(*, mode: str = "w+b", dir: Optional[str] = None):
    # On Windows, a NamedTemporaryFile with `delete=True` cannot be reopened,
    # which makes its name pretty useless. On Python 3.12, a new
    # delete_on_close flag is solves this issue, but we can't depend on that
    # yet. This block mimicks that feature.
    #
    # https://bugs.python.org/issue14243
    # https://github.com/mrc-ide/outpack-py/pull/33#discussion_r1500522877
    f = tempfile.NamedTemporaryFile(mode=mode, dir=dir, delete=False)
    try:
        yield f
    finally:
        f.close()
        try:
            os.unlink(f.name)
        except OSError:
            pass


Paths = TypeVar("Paths", str, List[str], Dict[str, str])


def as_posix_path(paths: Paths) -> Paths:
    """
    Convert a native path into a posix path.

    This is used when exporting paths into packet metadata, ensuring the
    produced packets are portable across platforms.
    """
    if isinstance(paths, dict):
        return {as_posix_path(k): as_posix_path(v) for (k, v) in paths.items()}
    elif isinstance(paths, list):
        return [as_posix_path(v) for v in paths]
    else:
        return PurePath(paths).as_posix()


T = TypeVar("T")


def as_list(x: Union[T, List[T]]) -> List[T]:
    if isinstance(x, list):
        return x
    else:
        return [x]
