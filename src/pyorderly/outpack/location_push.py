from dataclasses import dataclass
from typing import List, Union

from pyorderly.outpack.location import _location_driver, location_resolve_valid
from pyorderly.outpack.location_driver import LocationDriver
from pyorderly.outpack.location_pull import _find_all_dependencies
from pyorderly.outpack.root import OutpackRoot, find_file_by_hash, root_open
from pyorderly.outpack.static import LOCATION_LOCAL
from pyorderly.outpack.util import as_list


@dataclass
class LocationPushPlan:
    packets: List[str]
    files: List[str]


def outpack_location_push(
    ids: Union[str, List[str]],
    location: str,
    *,
    root: Union[str, OutpackRoot, None] = None,
    locate: bool = True,
):
    root = root_open(root, locate=locate)
    (location_name,) = location_resolve_valid(
        [location],
        root,
        include_local=False,
        include_orphan=False,
        allow_no_locations=False,
    )

    with _location_driver(location_name, root) as driver:
        plan = location_build_push_plan(driver, as_list(ids), root)
        for h in plan.files:
            if root.files is not None:
                path = root.files.filename(h)
            else:
                path = find_file_by_hash(root, h)
                if path is None:
                    msg = "Did not find suitable file, can't push this packet"
                    raise Exception(msg)
            driver.push_file(path, h)

        packets = root.index.location(LOCATION_LOCAL)
        for id in plan.packets:
            path = root.path / ".outpack" / "metadata" / id
            driver.push_metadata(path, packets[id].hash)


def location_build_push_plan(
    driver: LocationDriver, packet_ids: List[str], root: OutpackRoot
) -> LocationPushPlan:
    metadata = root.index.all_metadata()
    all_packets = _find_all_dependencies(packet_ids, metadata)
    missing_packets = driver.list_unknown_packets(all_packets)

    all_files = list(
        {f.hash for id in missing_packets for f in metadata[id].files}
    )
    missing_files = driver.list_unknown_files(all_files)

    return LocationPushPlan(packets=missing_packets, files=missing_files)
