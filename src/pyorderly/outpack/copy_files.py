from dataclasses import dataclass
from pathlib import Path

from pyorderly.outpack.location_pull import (
    location_build_pull_plan,
    location_pull_files,
)
from pyorderly.outpack.metadata import PacketFile
from pyorderly.outpack.root import OutpackRoot
from pyorderly.outpack.search_options import SearchOptions


@dataclass
class Plan:
    id: str
    name: str
    files: dict[str, PacketFile]


def copy_files(
    id: str,
    files: dict[str, str],
    dest: Path,
    options: SearchOptions,
    root: OutpackRoot,
) -> Plan:
    plan = _plan_copy_files(root, id, files)

    try:
        for here, there in plan.files.items():
            root.export_file(id, there.path, dest / here)

    except FileNotFoundError as e:
        if not options.allow_remote:
            msg = f"File `{e.filename}` from packet {id} is not available locally."
            raise Exception(msg) from e
        else:
            copy_files_from_remote(id, plan.files, dest, options, root)

    return plan


def copy_files_from_remote(
    id: str,
    files: dict[str, PacketFile],
    dest: Path,
    options: SearchOptions,
    root: OutpackRoot,
):
    plan = location_build_pull_plan(
        [id],
        options.location,
        recursive=False,
        files={id: [f.hash for f in files.values()]},
        root=root,
    )

    with location_pull_files(plan.files, root) as store:
        for here, there in files.items():
            store.get(there.hash, dest / here, overwrite=True)


def _validate_files(files):
    if isinstance(files, str):
        files = {files: files}
    if isinstance(files, list):
        files = {x: x for x in files}
    return files


def _plan_copy_files(root, id, files):
    meta = root.index.metadata(id)
    files = _validate_files(files)
    known = {f.path: f for f in meta.files}

    plan = {}
    for here, there in files.items():
        # TODO: check absolute paths
        if here.endswith("/") or there.endswith("/"):
            msg = "Directories not yet supported for export"
            raise Exception(msg)

        f = known.get(there, None)
        if f is None:
            msg = f"Packet '{id}' does not contain the requested path '{there}'"
            raise Exception(msg)
        plan[here] = f
    return Plan(id, meta.name, plan)
