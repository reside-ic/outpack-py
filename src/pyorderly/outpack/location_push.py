from dataclasses import dataclass
from typing import Union

from pyorderly.outpack.location import _location_driver, location_resolve_valid
from pyorderly.outpack.location_driver import LocationDriver
from pyorderly.outpack.location_pull import _find_all_dependencies
from pyorderly.outpack.root import OutpackRoot, root_open
from pyorderly.outpack.static import LOCATION_LOCAL
from pyorderly.outpack.util import as_list


@dataclass
class LocationPushPlan:
    packets: list[str]
    files: list[str]


def outpack_location_push(
    ids: Union[str, list[str]],
    location: str,
    *,
    root: Union[str, OutpackRoot, None] = None,
    locate: bool = True,
) -> LocationPushPlan:
    ids = as_list(ids)
    root = root_open(root, locate=locate)
    (location_name,) = location_resolve_valid(
        [location],
        root,
        include_local=False,
        include_orphan=False,
        allow_no_locations=False,
    )

    with _location_driver(location_name, root) as driver:
        plan = location_build_push_plan(driver, ids, root)
        for h in plan.files:
            try:
                # This uses all the packets being pushed as candidates to find
                # the file. We could be more precise and only include the
                # packets that contain that particular file, but the push plan
                # doesn't record this (yet).
                path = root.find_file_by_hash(h, candidates=plan.packets)
            except FileNotFoundError as e:
                msg = "Did not find suitable file, can't push this packet"
                raise Exception(msg) from e

            driver.push_file(path, h)

        packets = root.index.location(LOCATION_LOCAL)
        for id in plan.packets:
            path = root.path / ".outpack" / "metadata" / id
            driver.push_metadata(path, packets[id].hash)

    return plan


def location_build_push_plan(
    driver: LocationDriver, packet_ids: list[str], root: OutpackRoot
) -> LocationPushPlan:
    metadata = root.index.all_metadata()
    all_packets = _find_all_dependencies(packet_ids, metadata)
    missing_packets = driver.list_unknown_packets(all_packets)

    all_files = list(
        {f.hash for id in missing_packets for f in metadata[id].files}
    )
    missing_files = driver.list_unknown_files(all_files)

    # In principle, we want the packets to be in reverse topological order: all
    # dependencies need to be pushed first before a packet can be pushed.
    #
    # Lexicographic order is a pretty good approximation because packet IDs are
    # prefixed by their date and time. This might break down if clocks aren't
    # well adjusted, or if producing a packet chain on a system with a poor
    # clock resolution (ie. Windows).
    ordered_packets = sorted(missing_packets)

    return LocationPushPlan(packets=ordered_packets, files=missing_files)
