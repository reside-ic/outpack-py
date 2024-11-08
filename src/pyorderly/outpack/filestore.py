import os
import os.path
import shutil
import stat
from contextlib import contextmanager
from errno import ENOENT
from pathlib import Path

from pyorderly.outpack.hash import Hash, hash_parse, hash_validate_file
from pyorderly.outpack.util import openable_temporary_file


class FileStore:
    def __init__(self, path):
        self._path = Path(path)
        os.makedirs(path, exist_ok=True)

    def filename(self, hash: str, *, allow_missing: bool = False) -> Path:
        dat = hash_parse(hash)
        path = self._path / dat.algorithm / dat.value[:2] / dat.value[2:]
        if not allow_missing and not path.exists():
            msg = f"Hash '{hash}' not found in store"
            raise FileNotFoundError(ENOENT, msg)
        return path

    def get(self, hash, dst, *, overwrite=False):
        src = self.filename(hash)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        if not overwrite and os.path.exists(dst):
            msg = f"Failed to copy '{src}' to '{dst}', file already exists"
            raise Exception(msg)
        shutil.copyfile(src, dst)

    def exists(self, hash: str) -> bool:
        return self.filename(hash, allow_missing=True).exists()

    def put(self, src, hash, *, move=False):
        hash_validate_file(src, hash)
        dst = self.filename(hash, allow_missing=True)
        if not os.path.exists(dst):
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            if move:
                shutil.move(src, dst)
            else:
                shutil.copyfile(src, dst)
            # Make file readonly for everyone
            dst.chmod(0o444)
        return hash

    def ls(self):
        # Lots of ways of pulling this off with higer order functions
        # (os.walk, Path.glob etc), but this is probably clearest.
        ret = []
        for algorithm in os.listdir(self._path):
            path_alg = self._path / algorithm
            for prefix in os.listdir(path_alg):
                path_prefix = os.path.join(path_alg, prefix)
                for suffix in os.listdir(path_prefix):
                    ret.append(Hash(algorithm, prefix + suffix))
        return ret

    def destroy(self) -> None:
        def onerror(func, path, _exc_info):
            """
            Error handler for ``shutil.rmtree``.

            If the error is due to an access error (read only file)
            it attempts to add write permission and then retries.

            If the error is for another reason it re-raises the error.
            We manually remove write permission in ``put`` above so this
            is expected.

            Note we only need this on windows, on Linux shutils.rmtree will
            successfully remove the dir and its contents without having
            to add write permission to individual files

            Usage : ``shutil.rmtree(path, onerror=onerror)``
            """
            if not os.access(path, os.W_OK):
                os.chmod(path, stat.S_IWUSR)
                func(path)
            else:
                # This is always called by rmtree from within a try-catch
                # block, but the linter doesn't see that and complains about
                # the argument-less `raise` statement.
                raise  # noqa: PLE0704

        shutil.rmtree(self._path, onerror=onerror)

    @contextmanager
    def tmp(self):
        path = self._path / "tmp"
        path.mkdir(exist_ok=True)

        with openable_temporary_file(dir=path) as f:
            # Close the file (but don't delete it). We are only interested in
            # using the path.
            f.close()
            yield f.name
