import itertools
import shutil
from collections.abc import Iterable
from errno import ENOENT
from pathlib import Path

from pyorderly.outpack.filestore import FileStore
from pyorderly.outpack.hash import Hash, hash_file, hash_parse
from pyorderly.outpack.index import Index
from pyorderly.outpack.metadata import MetadataCore


class Archive:
    def __init__(self, path: Path, index: Index):
        self._path = Path(path)
        self._index = index

    def _find_file_in_packet(self, id: str, hash: Hash):
        meta = self._index.metadata(id)
        for f in meta.files:
            if f.hash == str(hash):
                path = self._path / meta.name / meta.id / f.path
                if hash_file(path, hash.algorithm) == hash:
                    return path
                else:
                    msg = (
                        f"Rejecting file from archive '{f.path}' "
                        f"in '{meta.name}/{meta.id}'"
                    )
                    print(msg)

        return None

    def find_file(self, hash: str, *, candidates: Iterable[str] = ()) -> Path:
        hash_parsed = hash_parse(hash)
        packets = set(self._index.unpacked()).difference(candidates)
        for id in itertools.chain(candidates, packets):
            path = self._find_file_in_packet(id, hash_parsed)
            if path is not None:
                return path

        msg = "File not found in archive, or corrupt"
        raise FileNotFoundError(ENOENT, msg)

    def import_packet(self, meta: MetadataCore, path: Path) -> Path:
        dest = self._path / meta.name / meta.id
        for f in meta.files:
            f_dest = dest / f.path
            f_dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(path / f.path, f_dest)
        return dest

    def import_packet_from_store(
        self, meta: MetadataCore, store: FileStore
    ) -> Path:
        dest = self._path / meta.name / meta.id
        for f in meta.files:
            store.get(f.hash, dest / f.path, overwrite=True)
        return dest
