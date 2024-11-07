import time
from pathlib import Path

from pyorderly.outpack.copy_files import copy_files
from pyorderly.outpack.hash import (
    hash_file,
    hash_parse,
    hash_string,
    hash_validate_file,
)
from pyorderly.outpack.ids import outpack_id, validate_outpack_id
from pyorderly.outpack.location_pull import outpack_location_pull_packet
from pyorderly.outpack.metadata import (
    MetadataCore,
    PacketDepends,
    PacketFile,
)
from pyorderly.outpack.root import mark_known, root_open
from pyorderly.outpack.schema import outpack_schema_version, validate
from pyorderly.outpack.search import as_query, search_unique
from pyorderly.outpack.tools import git_info
from pyorderly.outpack.util import all_normal_files, as_posix_path


# TODO: most of these fields should be private.
class Packet:
    def __init__(
        self, root, path, name, *, parameters=None, id=None, locate=True
    ):
        self.root = root_open(root, locate=locate)
        self.path = Path(path)
        if id is None:
            self.id = outpack_id()
        else:
            validate_outpack_id(id)
            self.id = id
        self.name = name
        self.parameters = parameters or {}
        self.depends = []
        self.files = []
        self.time = {"start": time.time()}
        self.git = git_info(self.path)
        self.custom = {}
        self.metadata = None
        self.immutable = {}

    def use_dependency(self, query, files=None, search_options=None):
        if files is None:
            files = {}

        query = as_query(query)

        id = search_unique(
            query, options=search_options, root=self.root, this=self.parameters
        )

        # We only need the whole packet if `require_complete_tree` is True.
        # In other cases, `copy_files` can download individual files.
        needs_pull = self.root.config.core.require_complete_tree and (
            id not in self.root.index.unpacked()
        )
        if needs_pull:
            outpack_location_pull_packet(
                id, options=search_options, root=self.root
            )

        result = copy_files(
            id, files, self.path, options=search_options, root=self.root
        )
        for f in result.files.keys():
            self.mark_file_immutable(f)

        d = PacketDepends(id, str(query), PacketDepends.files_from_dict(files))
        self.depends.append(d)
        return result

    def mark_file_immutable(self, path):
        path_full = self.path / path
        if path in self.immutable:
            hash_validate_file(path_full, self.immutable[path])
        else:
            hash_algorithm = self.root.config.core.hash_algorithm
            self.immutable[path] = hash_file(path_full, hash_algorithm)

    def add_custom_metadata(self, key, value):
        if key in self.custom:
            msg = f"metadata for '{key}' has already been added for this packet"
            raise Exception(msg)
        self.custom[key] = value

    def end(self, *, succesful=True):
        if self.metadata:
            msg = f"Packet '{id}' already ended"
            raise Exception(msg)
        self.time["end"] = time.time()
        hash_algorithm = self.root.config.core.hash_algorithm
        self.files = [
            PacketFile.from_file(self.path, as_posix_path(f), hash_algorithm)
            for f in all_normal_files(self.path)
        ]
        _check_immutable_files(self.files, self.immutable)
        self.metadata = self._build_metadata()

        validate(self.metadata.to_dict(), "outpack/metadata.json")
        if not succesful:
            self.path.joinpath("outpack.json").write_text(
                self.metadata.to_json()
            )

        return self.metadata

    def _build_metadata(self):
        return MetadataCore(
            outpack_schema_version(),
            self.id,
            self.name,
            self.parameters,
            self.time,
            self.files,
            self.depends,
            self.git,
            self.custom,
        )


def insert_packet(root, path, meta):
    # check that we have not already inserted this packet; in R we
    # look to see if it's unpacked but actually the issue is if it is
    # present as metadata at all.
    if root.config.core.use_file_store:
        for p in meta.files:
            root.files.put(path / p.path, p.hash)

    if root.archive is not None:
        root.archive.import_packet(meta, path)

    json = meta.to_json(separators=(",", ":"))
    hash_meta = hash_string(json, root.config.core.hash_algorithm)
    path_meta = root.path / ".outpack" / "metadata" / meta.id
    path_meta.parent.mkdir(parents=True, exist_ok=True)
    with open(path_meta, "w") as f:
        f.write(json)

    mark_known(root, meta.id, "local", hash_meta, time.time())


def _check_immutable_files(files, immutable):
    if not immutable:
        return
    found = {x.path: x.hash for x in files}
    for p, h in immutable.items():
        if p not in found:
            msg = f"File was deleted after being added: {p}"
            raise Exception(msg)
        if hash_parse(found[p]) != h:
            msg = f"File was changed after being added: {p}"
            raise Exception(msg)
