import itertools
import os
import time
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Optional, Union

import humanize

from pyorderly.outpack.filestore import FileStore
from pyorderly.outpack.hash import hash_validate_string
from pyorderly.outpack.location import (
    LocationDriver,
    _location_driver,
    location_resolve_valid,
)
from pyorderly.outpack.metadata import (
    MetadataCore,
    PacketFileWithLocation,
    PacketLocation,
)
from pyorderly.outpack.root import (
    OutpackRoot,
    mark_known,
    root_open,
)
from pyorderly.outpack.search_options import SearchOptions
from pyorderly.outpack.static import LOCATION_LOCAL
from pyorderly.outpack.util import format_list, partition, pl


def outpack_location_pull_metadata(
    location: Union[None, str, list[str]] = None, root=None, *, locate=True
):
    root = root_open(root, locate=locate)
    location_name = location_resolve_valid(
        location,
        root,
        include_local=False,
        include_orphan=False,
        allow_no_locations=True,
    )
    for name in location_name:
        with _location_driver(name, root) as driver:
            available = list(driver.list_packets().values())
            known_packets = {
                k: v
                for loc in root.index.all_locations().values()
                for k, v in loc.items()
            }

            _validate_hashes(name, available, known_packets)
            _pull_missing_metadata(driver, root, name, available)
            _mark_all_known(root, name, available)

    # TODO: mrc-4601 deorphan recovered packets


def _store_packet_metadata(
    root: OutpackRoot,
    location_name: str,
    packet: PacketLocation,
    metadata: str,
):
    hash_validate_string(
        metadata,
        packet.hash,
        f"metadata for '{packet.packet}' from '{location_name}'",
        [
            "This is bad news, I'm afraid. Your location is sending data "
            "that does not match the hash it says it does. Please let us "
            "know how this might have happened.",
            _get_remove_location_hint(location_name),
        ],
    )

    path_metadata = root.path / ".outpack" / "metadata"
    os.makedirs(path_metadata, exist_ok=True)
    filename = path_metadata / packet.packet
    with open(filename, "w") as f:
        f.writelines(metadata)


def _get_remove_location_hint(location_name):
    return (
        f'Probably all you can do at this point is '
        f'remove this location from your configuration '
        f'by running '
        f'outpack_location_remove("{location_name}")'
    )


def _validate_hashes(
    location_name: str,
    location_packets: list[PacketLocation],
    known_packets: dict[str, PacketLocation],
):
    mismatched_hashes = set()
    for packet in location_packets:
        packet_here = known_packets.get(packet.packet)
        if packet_here is not None and packet_here.hash != packet.hash:
            mismatched_hashes.add(packet.packet)

    if mismatched_hashes:
        id_text = "', '".join(mismatched_hashes)
        msg = (
            f"Location '{location_name}' has conflicting metadata\n"
            f"This is really bad news. We have been offered metadata "
            f"from '{location_name}' that has a different hash to "
            f"metadata that we have already imported from other "
            f"locations. I'm not going to import this new metadata, "
            f"but there's no guarantee that the older metadata is "
            f"actually what you want!\nConflicts for: '{id_text}'\n"
            f"We would be interested in this case, please let us know\n"
            f"{_get_remove_location_hint(location_name)}"
        )
        raise Exception(msg)


def _mark_all_known(
    root: OutpackRoot, location_name: str, packets: list[PacketLocation]
):
    known_already = root.index.packets_in_location(root)
    for packet in packets:
        if packet.packet not in known_already:
            mark_known(
                root,
                packet.packet,
                location_name,
                packet.hash,
                packet.time,
            )


def outpack_location_pull_packet(
    ids: Union[str, list[str]],
    *,
    options: Optional[SearchOptions] = None,
    recursive: Optional[bool] = None,
    root: Union[str, OutpackRoot, None] = None,
    locate: bool = True,
):
    root = root_open(root, locate=locate)

    if isinstance(ids, str):
        ids = [ids]

    if options is None:
        actual_options = SearchOptions(allow_remote=True)
    else:
        actual_options = SearchOptions.create(options)

    if not actual_options.allow_remote:
        msg = "'allow_remote' must be True"
        raise Exception(msg)

    if recursive is None:
        recursive = root.config.core.require_complete_tree

    if root.config.core.require_complete_tree and not recursive:
        msg = """'recursive' must be True (or None) with your configuration
Because 'core.require_complete_tree' is true, we can't do a \
non-recursive pull, as this might leave an incomplete tree"""
        raise Exception(msg)

    plan = location_build_pull_plan(
        ids, actual_options.location, recursive=recursive, root=root
    )

    ## Warn people of extra pulls and skips
    if plan.info.n_extra > 0:
        print(
            f"Also pulling {plan.info.n_extra} "
            f"{pl(plan.info.n_extra, 'packet')}, which are "
            f"dependencies of those requested"
        )
    if plan.info.n_skip > 0:
        print(
            f"Skipping {plan.info.n_skip} of {plan.info.n_total} "
            f"{pl(plan.info.n_total, 'packet')} which are already "
            f"unpacked"
        )

    with location_pull_files(plan.files, root) as store:
        n_packets = len(plan.packets)
        time_start = time.time()
        for idx, packet in enumerate(plan.packets.values()):
            if root.archive is not None:
                print(
                    f"Writing files for '{packet.packet}' (packet {idx + 1}/"
                    f"{n_packets})"
                )
                meta = root.index.metadata(packet.packet)
                root.archive.import_packet_from_store(meta, store)

            mark_known(
                root, packet.packet, LOCATION_LOCAL, packet.hash, time.time()
            )

    print(
        f"Unpacked {n_packets} {pl(n_packets, 'packet')} in "
        f"{humanize.time.precisedelta(int(time.time() - time_start))}."
    )

    return list(plan.packets.keys())


# This approach may be suboptimal in the case where the user does not
# already have a file store, as it means that files will be copied
# around and hashed more than ideal:
#
# * hash the candidate file
# * rehash on entry into the file store
# * copy into the file store
# * copy from the file store into the final location
#
# So in the case where a hash is only present once in a chain of
# packets being pulled this will be one too many hashes and one too
# many copies.
#
# However, this approach makes the logic fairly easy to deal with,
# and copes well with data races and corruption of data on disk
# (e.g., users having edited files that we rely on, or editing them
# after we hash them the first time).
@contextmanager
def location_pull_files(
    files: list[PacketFileWithLocation], root: OutpackRoot
) -> Generator[FileStore, None, None]:
    store = root.files
    cleanup_store = False
    if store is not None:
        exists, missing = partition(lambda file: store.exists(file.hash), files)

        if exists:
            print(
                f"Found {len(exists)} {pl(exists, 'file')} in the "
                f"file store"
            )
    elif root.archive is not None:
        print("Looking for suitable files already on disk")
        store = _temporary_filestore(root)
        cleanup_store = True

        missing = []
        no_found = 0
        for file in files:
            try:
                path = root.archive.find_file(file.hash)
            except FileNotFoundError:
                missing.append(file)
            else:
                store.put(path, file.hash)
                no_found += 1

        print(f"Found {no_found} {pl(no_found, 'file')} on disk ")
    else:
        msg = "Neither filestore nor archive"
        raise RuntimeError(msg)

    if len(missing) == 0:
        print("All files available locally, no need to fetch any")
    else:
        locations = {file.location for file in missing}
        total_size = humanize.naturalsize(sum(file.size for file in missing))
        print(
            f"Need to fetch {len(missing)} {pl(missing, 'file')} "
            f"({total_size}) from {len(locations)} "
            f"{pl(locations, 'location')}"
        )
        for location in locations:
            from_this_location = [
                file for file in missing if file.location == location
            ]
            with _location_driver(location, root) as driver:
                _location_pull_hash_store(
                    from_this_location,
                    location,
                    driver,
                    store,
                    root,
                )

    try:
        yield store
    finally:
        if cleanup_store:
            store.destroy()


def _location_pull_hash_store(
    files: list[PacketFileWithLocation],
    location_name: str,
    driver: LocationDriver,
    store: FileStore,
    root: OutpackRoot,
):
    no_of_files = len(files)
    # TODO: show a nice progress bar for users
    for idx, file in enumerate(files):
        print(
            f"Fetching file {idx + 1}/{no_of_files} "
            f"({humanize.naturalsize(file.size)}) from '{location_name}'"
        )
        with store.tmp() as path:
            packet = root.index.metadata(file.packet_id)
            driver.fetch_file(packet, file, path)
            store.put(path, file.hash)


def _pull_missing_metadata(
    driver: LocationDriver,
    root: OutpackRoot,
    location_name: str,
    packets: list[PacketLocation],
):
    known_here = root.index.all_metadata()

    to_pull = [p for p in packets if p.packet not in known_here]
    metadata = driver.metadata([p.packet for p in to_pull])

    for packet in to_pull:
        _store_packet_metadata(
            root, location_name, packet, metadata[packet.packet]
        )


@dataclass
class PullPlanInfo:
    n_extra: int
    n_skip: int
    n_total: int


@dataclass
class LocationPullPlan:
    packets: dict[str, PacketLocation]
    files: list[PacketFileWithLocation]
    info: PullPlanInfo


@dataclass
class PullPlanPackets:
    requested: list[str]
    full: list[str]
    skip: set[str]
    fetch: set[str]


def location_build_pull_plan(
    packet_ids: list[str],
    locations: Optional[list[str]],
    *,
    files: Optional[dict[str, list[str]]] = None,
    recursive: bool,
    root: OutpackRoot,
) -> LocationPullPlan:
    """
    Create a plan to pull packets from one or more locations.

    Parameters
    ----------
    packet_ids :
        A list of packet IDs to pull.

    locations :
        A list of location names from which to pull packets. If None, all
        configured locations will be considered.

    files :
        A filter restricting, for each packet, which file hashes to pull. This
        allows a subset of a packet's files to pulled. If None, or if a packet
        ID is mising from the dictionary, the entire packet is pulled.

    recursive :
        If True, all transitive dependencies of the requested packets will be
        pulled as well.

    root :
        The root object used to determine the location configuration and which
        files are missing and need pulling.
    """
    if files is None:
        files = {}

    packets = _location_build_pull_plan_packets(
        packet_ids, root, recursive=recursive
    )
    locations = _location_build_pull_plan_location(packets, locations, root)
    files_location = _location_build_pull_plan_files(
        packets.fetch, locations, files, root
    )
    fetch = _location_build_packet_locations(packets.fetch, locations, root)

    info = PullPlanInfo(
        n_extra=len(packets.full) - len(packets.requested),
        n_skip=len(packets.skip),
        n_total=len(packets.full),
    )

    return LocationPullPlan(packets=fetch, files=files_location, info=info)


def _location_build_pull_plan_packets(
    packet_ids: list[str], root: OutpackRoot, *, recursive: Optional[bool]
) -> PullPlanPackets:
    requested = packet_ids
    index = root.index
    if recursive:
        full = _find_all_dependencies(packet_ids, index.all_metadata())
    else:
        full = packet_ids

    skip = set(full).intersection(index.unpacked())
    fetch = set(full).difference(skip)

    return PullPlanPackets(
        requested=requested, full=full, skip=skip, fetch=fetch
    )


def _find_all_dependencies(
    packet_ids: list[str], metadata: dict[str, MetadataCore]
) -> list[str]:
    ret = set(packet_ids)
    packets = set(packet_ids)
    while packets:
        dependency_ids = {
            dependencies.packet
            for packet_id in packets
            if packet_id in metadata.keys()
            for dependencies in (
                []
                if metadata.get(packet_id) is None
                else metadata[packet_id].depends
            )
        }
        packets = dependency_ids.difference(ret)
        ret = packets.union(ret)

    return sorted(ret)


def _location_build_pull_plan_location(
    packets: PullPlanPackets, locations: Optional[list[str]], root: OutpackRoot
) -> list[str]:
    location_names = location_resolve_valid(
        locations,
        root,
        include_local=False,
        include_orphan=False,
        allow_no_locations=len(packets.fetch) == 0,
    )

    known_packets = [
        root.index.packets_in_location(location_name)
        for location_name in location_names
    ]
    missing = packets.fetch.difference(itertools.chain(*known_packets))
    if missing:
        extra = missing.difference(packets.requested)
        if extra:
            hint = (
                f"{len(extra)} missing "
                f"{pl(extra, 'packet was', 'packets were')} "
                f"requested as "
                f"{pl(extra, 'dependency', 'dependencies')} "
                f"of the {pl(extra, 'one')} you asked for: "
                f"{format_list(extra)}"
            )
        else:
            # In the case where the above is used, we probably have
            # up-to-date metadata, so we don't display this.
            hint = "Do you need to run 'outpack_location_pull_metadata()'?"

        msg = (
            f"Failed to find {pl(missing, 'packet')} "
            f"{format_list(missing)}\n"
            f"Looked in {pl(location_names, 'location')} "
            f"{format_list(location_names)}\n" + hint
        )
        raise Exception(msg)

    return location_names


def _location_build_pull_plan_files(
    packet_ids: set[str],
    locations: list[str],
    files: dict[str, list[str]],
    root: OutpackRoot,
) -> list[PacketFileWithLocation]:
    metadata = root.index.all_metadata()

    # Find first location within the set which contains each packet
    # We've already checked earlier that the file is in at least 1
    # location so we don't have to worry about that here
    all_files = []
    seen_hashes = set()
    for location_name in locations:
        location_packets = set(root.index.packets_in_location(location_name))
        packets_in_location = location_packets.intersection(packet_ids)
        for packet_id in packets_in_location:
            for file in metadata[packet_id].files:
                if packet_id in files and file.hash not in files[packet_id]:
                    continue
                file_with_location = PacketFileWithLocation.from_packet_file(
                    file, location_name, packet_id
                )
                if file.hash not in seen_hashes:
                    seen_hashes.add(file.hash)
                    all_files.append(file_with_location)

    return all_files


def _location_build_packet_locations(
    packets: set[str], locations: list[str], root: OutpackRoot
) -> dict[str, PacketLocation]:
    packets_fetch = {}
    for location in locations:
        packets_from_location = root.index.location(location)
        packets_in_this_location = packets & packets_from_location.keys()
        for packet_id in packets_in_this_location:
            packets_fetch[packet_id] = packets_from_location[packet_id]
    return packets_fetch


def _temporary_filestore(root: OutpackRoot) -> FileStore:
    return FileStore(root.path / "orderly" / "pull")
