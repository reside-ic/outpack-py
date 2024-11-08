import collections
import shutil
from pathlib import PurePath
from typing import Union

from pyorderly.outpack.config import Location, update_config
from pyorderly.outpack.location_driver import LocationDriver
from pyorderly.outpack.location_http import OutpackLocationHTTP
from pyorderly.outpack.location_packit import outpack_location_packit
from pyorderly.outpack.location_path import OutpackLocationPath
from pyorderly.outpack.location_ssh import OutpackLocationSSH, parse_ssh_url
from pyorderly.outpack.root import OutpackRoot, root_open
from pyorderly.outpack.static import (
    LOCATION_LOCAL,
    LOCATION_ORPHAN,
    LOCATION_RESERVED_NAME,
)


def outpack_location_list(root=None, *, locate=True) -> list[str]:
    root = root_open(root, locate=locate)
    return list(root.config.location.keys())


def outpack_location_add(name, type, args, root=None, *, locate=True):
    root = root_open(root, locate=locate)

    if name in LOCATION_RESERVED_NAME:
        msg = f"Cannot add a location with reserved name '{name}'"
        raise Exception(msg)

    _location_check_new_name(root, name)

    loc = Location(name, type, args)

    if type == "path":
        root_open(loc.args["path"], locate=False)
    elif type == "ssh":
        parse_ssh_url(loc.args["url"])
    elif type in ("custom",):  # pragma: no cover
        msg = f"Cannot add a location with type '{type}' yet."
        raise Exception(msg)

    config = root.config
    config.location[name] = loc
    update_config(config, root.path)


def outpack_location_add_path(name, path, root=None, *, locate=True):
    if isinstance(path, OutpackRoot):
        path = str(path.path)
    elif isinstance(path, PurePath):
        path = str(path)

    outpack_location_add(name, "path", {"path": path}, root=root, locate=locate)


def outpack_location_remove(name, root=None, *, locate=True):
    root = root_open(root, locate=locate)

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
    root = root_open(root, locate=locate)

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
    location: Union[None, str, list[str]],
    root: OutpackRoot,
    *,
    include_local: bool,
    include_orphan: bool,
    allow_no_locations: bool,
) -> list[str]:
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
        if unknown:
            unknown_text = "', '".join(unknown)
            msg = f"Unknown location: '{unknown_text}'"
            raise Exception(msg)
        location = list(location)
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


def _location_driver(location_name, root) -> LocationDriver:
    location = root.config.location[location_name]
    if location.type == "path":
        return OutpackLocationPath(location.args["path"])
    elif location.type == "ssh":
        return OutpackLocationSSH(
            location.args["url"],
            location.args.get("known_hosts"),
            location.args.get("password"),
        )
    elif location.type == "http":
        return OutpackLocationHTTP(location.args["url"])
    elif location.type == "packit":
        return outpack_location_packit(
            location.args["url"], location.args.get("token")
        )
    elif location.type == "custom":
        msg = "custom remote not yet supported"
        raise Exception(msg)

    msg = "invalid location type"
    raise Exception(msg)
