import os
import shutil
from collections.abc import Iterable
from pathlib import Path
from typing import Optional, Union

from pyorderly.outpack.archive import Archive
from pyorderly.outpack.config import read_config
from pyorderly.outpack.filestore import FileStore
from pyorderly.outpack.index import Index
from pyorderly.outpack.metadata import PacketLocation
from pyorderly.outpack.schema import validate
from pyorderly.outpack.util import find_file_descend


class OutpackRoot:
    files: Optional[FileStore] = None
    archive: Optional[Archive] = None

    def __init__(self, path):
        self.path = Path(path)
        self.config = read_config(path)
        self.index = Index(path)

        if self.config.core.use_file_store:
            self.files = FileStore(self.path / ".outpack" / "files")
        if self.config.core.path_archive is not None:
            self.archive = Archive(
                self.path / self.config.core.path_archive, self.index
            )

    def find_file_by_hash(
        self, hash: str, *, candidates: Iterable[str] = ()
    ) -> Path:
        """
        Find a file in the repository, based on its hash.

        A list of candidate packet IDs may be specified, in which case these
        packets are searched in priority. This can speed up the search somewhat.
        """
        if self.files is not None:
            return self.files.filename(hash)
        elif self.archive is not None:
            return self.archive.find_file(hash, candidates=candidates)
        else:
            msg = "Neither filestore nor archive"
            raise Exception(msg)

    def export_file(self, id: str, there: str, dest: Path):
        meta = self.index.metadata(id)
        hash = meta.file_hash(there)
        try:
            src = self.find_file_by_hash(hash, candidates=[id])
        except FileNotFoundError as e:
            e.filename = there
            raise

        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, dest)


def root_open(
    path: Union[OutpackRoot, str, os.PathLike, None], *, locate: bool = False
) -> OutpackRoot:
    if isinstance(path, OutpackRoot):
        return path

    if path is None:
        path = Path.cwd()
    else:
        path = Path(path).absolute()

    if not path.is_dir():
        msg = "Expected 'path' to be an existing directory"
        raise Exception(msg)
    if locate:
        path_outpack = find_file_descend(".outpack", path)
        has_outpack = path_outpack is not None
        pass
    else:
        has_outpack = path.joinpath(".outpack").is_dir()
        path_outpack = path
    if not has_outpack:
        msg = f"Did not find existing outpack root in '{path}'"
        raise Exception(msg)
    return OutpackRoot(path_outpack)


def mark_known(root, packet_id, location, hash, time):
    dat = PacketLocation(packet_id, time, str(hash))
    validate(dat.to_dict(), "outpack/location.json")
    dest = root.path / ".outpack" / "location" / location / packet_id
    dest.parent.mkdir(parents=True, exist_ok=True)
    with open(dest, "w") as f:
        f.write(dat.to_json(separators=(",", ":")))
