import collections
import os
import shutil
from typing import List

from outpack.config import Location, update_config
from outpack.hash import hash_validate_string
from outpack.location_path import OutpackLocationPath
from outpack.metadata import PacketLocation
from outpack.packet import mark_known
from outpack.root import root_open
from outpack.static import (
    LOCATION_LOCAL,
    LOCATION_ORPHAN,
    LOCATION_RESERVED_NAME,
)


def outpack_location_list(root=None, *, locate=True):
    root = root_open(root, locate)
    return list(root.config.location.keys())


def outpack_location_add(name, type, args, root=None, *, locate=True):
    root = root_open(root, locate)

    if name in LOCATION_RESERVED_NAME:
        msg = f"Cannot add a location with reserved name '{name}'"
        raise Exception(msg)

    _location_check_new_name(root, name)

    loc = Location(name, type, args)

    if type == "path":
        root_open(loc.args["path"], locate=False)
    elif type in ("http", "custom"):  # pragma: no cover
        msg = f"Cannot add a location with type '{type}' yet."
        raise Exception(msg)

    config = root.config
    config.location[name] = loc
    update_config(config, root.path)


def outpack_location_remove(name, root=None, *, locate=True):
    root = root_open(root, locate)

    if name in LOCATION_RESERVED_NAME:
        msg = f"Cannot remove default location '{name}'"
        raise Exception(msg)

    _location_check_exists(root, name)
    config = root.config

    # TODO: mark packets as orphaned mrc-4601

    location_path = root.path / ".outpack" / "location" / name
    if location_path.exists():
        ## Skipped on covr because this dir won't exist until packet pulling implemented
        shutil.rmtree(location_path)  # pragma: no cover

    root.index.rebuild()
    config.location.pop(name)
    update_config(config, root.path)


def outpack_location_rename(old, new, root=None, *, locate=True):
    root = root_open(root, locate)

    if old in LOCATION_RESERVED_NAME:
        msg = f"Cannot rename default location '{old}'"
        raise Exception(msg)

    _location_check_new_name(root, new)
    _location_check_exists(root, old)

    config = root.config
    new_loc = config.location.pop(old)
    new_loc.name = new
    config.location[new] = new_loc
    update_config(config, root.path)


def location_resolve_valid(
    location, root, *, include_local, include_orphan, allow_no_locations
):
    if location is None:
        location = outpack_location_list(root)
    elif isinstance(location, str):
        if location not in outpack_location_list(root):
            msg = f"Unknown location: '{location}'"
            raise Exception(msg)
        location = [location]
    elif isinstance(location, collections.abc.Iterable) and all(
        isinstance(item, str) for item in location
    ):
        unknown = set(location).difference(outpack_location_list(root))
        if len(unknown) > 0:
            unknown_text = "', '".join(unknown)
            msg = f"Unknown location: '{unknown_text}'"
            raise Exception(msg)
    else:
        msg = (
            "Invalid input for 'location'; expected None or a list of "
            "strings"
        )
        raise Exception(msg)

    if not include_local and LOCATION_LOCAL in location:
        location.remove(LOCATION_LOCAL)
    if not include_orphan and LOCATION_ORPHAN in location:  # pragma: no cover
        location.remove(LOCATION_ORPHAN)

    if len(location) == 0 and not allow_no_locations:
        msg = "No suitable location found"
        raise Exception(msg)

    return location


def outpack_location_pull_metadata(location=None, root=None, *, locate=True):
    root = root_open(root, locate)
    location_name = location_resolve_valid(
        location,
        root,
        include_local=False,
        include_orphan=False,
        allow_no_locations=True,
    )
    for name in location_name:
        driver = _location_driver(name, root)
        _pull_all_metadata(driver, root, name)
        known_packets = []
        for packet_location in root.index.all_locations().values():
            known_packets.extend(list(packet_location.values()))
        _validate_hashes(driver, name, known_packets)
        _mark_all_known(driver, root, name)

    # TODO: mrc-4601 deorphan recovered packets


def _pull_all_metadata(driver, root, location_name):
    known_there = driver.list()
    known_here = root.index.all_metadata().keys()
    for packet_id in known_there:
        if packet_id not in known_here:
            _pull_packet_metadata(driver, root, location_name, packet_id)


def _get_remove_location_hint(location_name):
    return (
        f'Probably all you can do at this point is '
        f'remove this location from your configuration '
        f'by running '
        f'orderly_location_remove("{location_name}")'
    )


def _pull_packet_metadata(driver, root, location_name, packet_id):
    metadata = driver.metadata(packet_id)[packet_id]
    expected_hash = driver.list()[packet_id].hash

    hash_validate_string(
        metadata,
        expected_hash,
        f"metadata for '{packet_id}' from '{location_name}'",
        [
            "This is bad news, I'm afraid. Your location is sending data "
            "that does not match the hash it says it does. Please let us "
            "know how this might have happened.",
            _get_remove_location_hint(location_name),
        ],
    )

    path_metadata = root.path / ".outpack" / "metadata"
    os.makedirs(path_metadata, exist_ok=True)
    filename = path_metadata / packet_id
    with open(filename, "w") as f:
        f.writelines(metadata)


def _validate_hashes(driver, location_name, packets: List[PacketLocation]):
    mismatched_hashes = set()
    known_there = driver.list()
    for packet in packets:
        if known_there.get(packet.packet) is not None:
            hash_there = known_there[packet.packet].hash
            hash_here = packet.hash
            if hash_there != hash_here:
                mismatched_hashes.add(packet.packet)

    if len(mismatched_hashes) > 0:
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


def _mark_all_known(driver, root, location_name):
    try:
        known_here = root.index.location(location_name)
    except KeyError:
        known_here = {}

    known_there = driver.list()
    for packet_id in known_there:
        if packet_id not in known_here.keys():
            mark_known(
                root,
                packet_id,
                location_name,
                known_there[packet_id].hash,
                known_there[packet_id].time,
            )


def _location_check_new_name(root, name):
    if _location_exists(root, name):
        msg = f"A location with name '{name}' already exists"
        raise Exception(msg)


def _location_check_exists(root, name):
    if not _location_exists(root, name):
        msg = f"No location with name '{name}' exists"
        raise Exception(msg)


def _location_exists(root, name):
    return name in outpack_location_list(root)


def _location_driver(location_name, root):
    location = root.config.location[location_name]
    if location.type == "path":
        return OutpackLocationPath(location.args["path"])
    elif location.type == "http":  # pragma: no cover
        msg = "Http remote not yet supported"
        raise Exception(msg)
    elif location.type == "custom":  # pragma: no cover
        msg = "custom remote not yet supported"
        raise Exception(msg)
