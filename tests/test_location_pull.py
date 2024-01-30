import contextlib
import io
import json
import os
import re
from operator import itemgetter

import pytest

from outpack.hash import hash_file
from outpack.ids import outpack_id
from outpack.location import outpack_location_add, location_resolve_valid, \
    outpack_location_list
from outpack.location_pull import find_all_dependencies, \
    outpack_location_pull_metadata, outpack_location_pull_packet
from outpack.packet import Packet
from outpack.util import read_string
from .helpers import create_metadata_depends, create_temporary_root, \
    create_random_packet, create_temporary_roots, create_random_packet_chain


def test_can_pull_metadata_from_a_file_base_location(tmp_path):
    root_upstream = create_temporary_root(
        tmp_path / "upstream", use_file_store=True
    )

    ids = [create_random_packet(root_upstream) for _ in range(3)]
    root_downstream = create_temporary_root(
        tmp_path / "downstream", use_file_store=True
    )

    outpack_location_add(
        "upstream",
        "path",
        {"path": str(root_upstream.path)},
        root=root_downstream,
    )
    assert outpack_location_list(root_downstream) == ["local", "upstream"]

    outpack_location_pull_metadata("upstream", root=root_downstream)

    metadata = root_downstream.index.all_metadata()
    assert len(metadata) == 3
    assert set(metadata.keys()) == set(ids)
    assert metadata == root_upstream.index.all_metadata()

    packets = root_downstream.index.location("upstream")
    assert len(packets) == 3
    assert set(packets.keys()) == set(ids)


def test_can_pull_empty_metadata(tmp_path):
    root_upstream = create_temporary_root(
        tmp_path / "upstream", use_file_store=True
    )
    root_downstream = create_temporary_root(
        tmp_path / "downstream", use_file_store=True
    )

    outpack_location_add(
        "upstream",
        "path",
        {"path": str(root_upstream.path)},
        root=root_downstream,
    )
    outpack_location_pull_metadata("upstream", root=root_downstream)

    index = root_downstream.index.data
    assert len(index.metadata) == 0


def test_can_pull_metadata_from_subset_of_locations(tmp_path):
    root = {"a": create_temporary_root(tmp_path / "a", use_file_store=True)}

    location_names = ["x", "y", "z"]
    for name in location_names:
        root[name] = create_temporary_root(tmp_path / name, use_file_store=True)
        outpack_location_add(
            name, "path", {"path": str(root[name].path)}, root=root["a"]
        )

    assert outpack_location_list(root["a"]) == ["local", "x", "y", "z"]

    ids = {}
    for name in location_names:
        ids[name] = [create_random_packet(root[name]) for _ in range(3)]

    outpack_location_pull_metadata(["x", "y"], root=root["a"])
    index = root["a"].index

    assert set(index.all_metadata()) == set(ids["x"] + ids["y"])
    locations = index.all_locations()
    assert set(locations.keys()) == {"local", "x", "y"}
    assert len(locations["local"]) == 0
    assert len(locations["x"]) == 3
    assert len(locations["y"]) == 3

    x_metadata = root["x"].index.all_metadata().keys()
    y_metadata = root["y"].index.all_metadata().keys()
    for packet_id in index.all_metadata().keys():
        if packet_id in ids["x"]:
            assert packet_id in x_metadata
        else:
            assert packet_id in y_metadata

    outpack_location_pull_metadata(root=root["a"])
    index = root["a"].index

    assert set(index.all_metadata()) == set(ids["x"] + ids["y"] + ids["z"])
    locations = index.all_locations()
    assert set(locations.keys()) == {"local", "x", "y", "z"}
    assert len(locations["local"]) == 0
    assert len(locations["x"]) == 3
    assert len(locations["y"]) == 3
    assert len(locations["z"]) == 3
    z_metadata = root["z"].index.all_metadata().keys()
    for packet_id in index.all_metadata().keys():
        if packet_id in ids["x"]:
            assert packet_id in x_metadata
        elif packet_id in ids["y"]:
            assert packet_id in y_metadata
        else:
            assert packet_id in z_metadata


def test_cant_pull_metadata_from_an_unknown_location(tmp_path):
    root = create_temporary_root(tmp_path)
    with pytest.raises(Exception) as e:
        outpack_location_pull_metadata("upstream", root=root)

    assert e.match("Unknown location: 'upstream'")


def test_noop_to_pull_metadata_from_no_locations(tmp_path):
    root = create_temporary_root(tmp_path)
    outpack_location_pull_metadata("local", root=root)
    outpack_location_pull_metadata(root=root)


def test_handle_metadata_where_hash_does_not_match_reported(tmp_path):
    here = create_temporary_root(tmp_path / "here")
    there = create_temporary_root(tmp_path / "there")
    outpack_location_add("server", "path", {"path": str(there.path)}, root=here)
    packet_id = create_random_packet(there)

    path_metadata = there.path / ".outpack" / "metadata" / packet_id
    parsed = json.loads(read_string(path_metadata))
    with open(path_metadata, "w") as f:
        f.write(json.dumps(parsed, indent=4))

    with pytest.raises(Exception) as e:
        outpack_location_pull_metadata(root=here)

    assert e.match(
        f"Hash of metadata for '{packet_id}' from 'server' does not match:"
    )
    assert e.match("This is bad news")
    assert e.match("remove this location")


def test_handle_metadata_where_two_locations_differ_in_hash_for_same_id(
    tmp_path,
):
    root = {}

    for name in ["a", "b", "us"]:
        root[name] = create_temporary_root(tmp_path / name)

    packet_id = outpack_id()
    create_random_packet(root["a"], packet_id=packet_id)
    create_random_packet(root["b"], packet_id=packet_id)

    outpack_location_add(
        "a", "path", {"path": str(root["a"].path)}, root=root["us"]
    )
    outpack_location_add(
        "b", "path", {"path": str(root["b"].path)}, root=root["us"]
    )

    outpack_location_pull_metadata(location="a", root=root["us"])

    with pytest.raises(Exception) as e:
        outpack_location_pull_metadata(location="b", root=root["us"])

    assert e.match(
        "We have been offered metadata from 'b' that has a different"
    )
    assert e.match(f"Conflicts for: '{packet_id}'")
    assert e.match("please let us know")
    assert e.match("remove this location")


def test_can_pull_metadata_through_chain_of_locations(tmp_path):
    root = {}
    for name in ["a", "b", "c", "d"]:
        root[name] = create_temporary_root(tmp_path / name)

    # More interesting topology, with a chain of locations, but d also
    # knowing directly about an earlier location
    # > a -> b -> c -> d
    # >       `-------/
    outpack_location_add(
        "a", "path", {"path": str(root["a"].path)}, root=root["b"]
    )
    outpack_location_add(
        "b", "path", {"path": str(root["b"].path)}, root=root["c"]
    )
    outpack_location_add(
        "b", "path", {"path": str(root["b"].path)}, root=root["d"]
    )
    outpack_location_add(
        "c", "path", {"path": str(root["c"].path)}, root=root["d"]
    )

    # Create a packet and make sure it's in both b and c
    create_random_packet(root["a"])
    outpack_location_pull_metadata(root=root["b"])
    # TODO: complete test once orderly_location_pull_packet


def test_informative_error_when_no_locations_configured(tmp_path):
    root = create_temporary_root(tmp_path)

    locations = location_resolve_valid(
        None,
        root,
        include_local=False,
        include_orphan=False,
        allow_no_locations=True,
    )
    assert locations == []

    with pytest.raises(Exception) as e:
        location_resolve_valid(
            None,
            root,
            include_local=False,
            include_orphan=False,
            allow_no_locations=False,
        )

    assert e.match("No suitable location found")

    with pytest.raises(Exception) as e:
        outpack_location_pull_packet(outpack_id(), root=root)
    assert e.match("No suitable location found")


def test_can_resolve_dependencies_where_there_are_none():
    metadata = create_metadata_depends("a")
    deps = find_all_dependencies(["a"], metadata)
    assert deps == ["a"]

    metadata = (create_metadata_depends("a") |
                create_metadata_depends("b", ["a"]))
    deps = find_all_dependencies(["a"], metadata)
    assert deps == ["a"]


def test_can_find_dependencies():
    metadata = (create_metadata_depends("a") |
                create_metadata_depends("b") |
                create_metadata_depends("c") |
                create_metadata_depends("d", ["a", "b"]) |
                create_metadata_depends("e", ["b", "c"]) |
                create_metadata_depends("f", ["a", "c"]) |
                create_metadata_depends("g", ["a", "f", "c"]) |
                create_metadata_depends("h", ["a", "b", "c"]) |
                create_metadata_depends("i", ["f"]) |
                create_metadata_depends("j", ["i", "e", "a"]))

    assert find_all_dependencies(["a"], metadata) == ["a"]
    assert find_all_dependencies(["b"], metadata) == ["b"]
    assert find_all_dependencies(["c"], metadata) == ["c"]

    assert find_all_dependencies(["d"], metadata) == ["a", "b", "d"]
    assert find_all_dependencies(["e"], metadata) == ["b", "c", "e"]
    assert find_all_dependencies(["f"], metadata) == ["a", "c", "f"]

    assert (find_all_dependencies(["g"], metadata) ==
            ["a", "c", "f", "g"])
    assert (find_all_dependencies(["h"], metadata) ==
            ["a", "b", "c", "h"])
    assert (find_all_dependencies(["i"], metadata) ==
            ["a", "c", "f", "i"])
    assert (find_all_dependencies(["j"], metadata) ==
            ["a", "b", "c", "e", "f", "i", "j"])


def test_can_find_multiple_dependencies_at_once():
    metadata = (create_metadata_depends("a") |
                create_metadata_depends("b") |
                create_metadata_depends("c") |
                create_metadata_depends("d", ["a", "b"]) |
                create_metadata_depends("e", ["b", "c"]) |
                create_metadata_depends("f", ["a", "c"]) |
                create_metadata_depends("g", ["a", "f", "c"]) |
                create_metadata_depends("h", ["a", "b", "c"]) |
                create_metadata_depends("i", ["f"]) |
                create_metadata_depends("j", ["i", "e", "a"]))

    assert find_all_dependencies([], metadata) == []
    assert (find_all_dependencies(["c", "b", "a"], metadata) ==
            ["a", "b", "c"])
    assert (find_all_dependencies(["d", "e", "f"], metadata) ==
            ["a", "b", "c", "d", "e", "f"])


def test_can_pull_packet_from_location_into_another_file_store(tmp_path):
    root = create_temporary_roots(tmp_path, use_file_store=True)

    id = create_random_packet(root["src"])
    outpack_location_add("src", "path",
                         {"path": str(root["src"].path)},
                         root=root["dst"])
    outpack_location_pull_metadata(root=root["dst"])
    outpack_location_pull_packet(id, root=root["dst"])

    index = root["dst"].index
    assert index.unpacked() == [id]
    assert os.path.exists(root["dst"].path / "archive" / "data" / id /
                          "data.txt")

    meta = index.metadata(id)
    assert all(root["dst"].files.exists(file.hash) for file in meta.files)


def test_can_pull_packet_from_one_location_to_another_archive(tmp_path):
    root = create_temporary_roots(tmp_path, use_file_store=False)

    id = create_random_packet(root["src"])
    outpack_location_add("src", "path",
                         {"path": str(root["src"].path)},
                         root=root["dst"])
    outpack_location_pull_metadata(root=root["dst"])
    outpack_location_pull_packet(id, root=root["dst"])

    index = root["dst"].index
    assert index.unpacked() == [id]
    assert os.path.exists(root["dst"].path / "archive" / "data" / id /
                          "data.txt")


def test_detect_and_avoid_modified_files_in_source_repository(tmp_path):
    root = create_temporary_roots(tmp_path, use_file_store=False)

    tmp_dir = tmp_path / "new_dir"
    os.mkdir(tmp_dir)
    with open(tmp_dir / "a.txt", "w") as f:
        f.writelines("my data a")
    with open(tmp_dir / "b.txt", "w") as f:
        f.writelines("my data b")
    ids = [None] * 2
    for i in range(len(ids)):
        p = Packet(root["src"], tmp_dir, "data")
        p.end()
        ids[i] = p.id

    outpack_location_add("src", "path",
                         {"path": str(root["src"].path)},
                         root["dst"])
    outpack_location_pull_metadata(root=root["dst"])

    ## When I corrupt the file in the first ID by truncating it:
    src_data = root["src"].path / "archive" / "data"
    dest_data = root["dst"].path / "archive" / "data"
    with open(src_data / ids[0] / "a.txt", "w") as f:
        f.truncate(0)

    ## and then try and pull it, user is warned
    f = io.StringIO()
    with contextlib.redirect_stdout(f):
        outpack_location_pull_packet(ids[0], root=root["dst"])

    print(f.getvalue())
    assert re.search(
        r"Rejecting file from archive 'a\.txt' in 'data/",
        f.getvalue())

    assert (hash_file(dest_data / ids[0] / "a.txt") ==
            hash_file(src_data / ids[1] / "a.txt"))
    assert (hash_file(dest_data / ids[0] / "b.txt") ==
            hash_file(src_data / ids[1] / "b.txt"))


def test_do_not_unpack_packet_twice(tmp_path):
    root = create_temporary_roots(tmp_path)

    id = create_random_packet(root["src"])
    outpack_location_add("src", "path",
                         {"path": str(root["src"].path)},
                         root["dst"])
    outpack_location_pull_metadata(root=root["dst"])

    assert outpack_location_pull_packet(id, root=root["dst"]) == [id]
    assert outpack_location_pull_packet(id, root=root["dst"]) == []


def test_sensible_error_if_packet_not_known(tmp_path):
    root = create_temporary_roots(tmp_path)
    id = create_random_packet(root["src"])
    outpack_location_add("src", "path",
                         {"path": str(root["src"].path)},
                         root=root["dst"])

    with pytest.raises(Exception) as e:
        outpack_location_pull_packet(id, root=root["dst"])
    assert e.match(f"Failed to find packet '{id}'")
    assert e.match("Looked in location 'src'")
    assert e.match("Do you need to run 'outpack_location_pull_metadata'?")


def test_error_if_dependent_packet_not_known(tmp_path):
    root = create_temporary_roots(tmp_path, ["a", "c"],
                                  require_complete_tree=True)
    root["b"] = create_temporary_root(tmp_path / "b",
                                      require_complete_tree=False)

    ids = create_random_packet_chain(root["a"], 5)
    outpack_location_add("a", "path",
                         {"path": str(root["a"].path)},
                         root=root["b"])
    outpack_location_pull_metadata(root=root["b"])
    outpack_location_pull_packet(ids["e"], root=root["b"])

    outpack_location_add("b", "path",
                         {"path": str(root["b"].path)},
                         root=root["c"])
    outpack_location_pull_metadata(root=root["c"])

    with pytest.raises(Exception) as e:
        outpack_location_pull_packet(ids["e"], root=root["c"])
    print(e.value)
    assert e.match(f"Failed to find packet '{ids['d']}")
    assert e.match("Looked in location 'b'")
    assert e.match("1 missing packet was requested as dependency of the "
                   f"one you asked for: '{ids['d']}'")


def test_can_pull_a_tree_recursively(tmp_path):
    root = create_temporary_roots(tmp_path)
    ids = create_random_packet_chain(root["src"], 3)

    outpack_location_add("src", "path",
                         {"path": str(root["src"].path)},
                         root["dst"])
    outpack_location_pull_metadata(root=root["dst"])
    pulled_packets = outpack_location_pull_packet(ids["c"], recursive=True,
                                                  root=root["dst"])
    assert set(pulled_packets) == set(itemgetter("a", "b", "c")(ids))

    assert (set(root["dst"].index.unpacked()) ==
            set(root["src"].index.unpacked()))

    pulled_packets = outpack_location_pull_packet(ids["c"], recursive=True,
                                                  root=root["dst"])
    assert pulled_packets == []


def test_can_filter_locations(tmp_path):
    location_names = ["dst", "a", "b", "c", "d"]
    root = create_temporary_roots(tmp_path, location_names, use_file_store=True)
    for name in location_names:
        if name != "dst":
            outpack_location_add(name, "path",
                                 {"path": str(root[name].path)},
                                 root=root["dst"])

    ids_a = [create_random_packet(root["a"]) for _ in range(3)]
    outpack_location_add("a", "path",
                         {"path": str(root["a"].path)},
                         root=root["b"])
    outpack_location_pull_metadata(root=root["b"])
    outpack_location_pull_packet(ids_a, root=root["b"])

    ids_b = ids_a + [create_random_packet(root["b"]) for _ in range(3)]
    ids_c = [create_random_packet(root["c"]) for _ in range(3)]
    outpack_location_add("a", "path",
                         {"path": str(root["a"].path)},
                         root=root["d"])
    outpack_location_add("c", "path",
                         {"path": str(root["c"].path)},
                         root=root["d"])
    outpack_location_pull_metadata(root=root["d"])
    outpack_location_pull_packet(ids_a, root=root["d"])
    outpack_location_pull_packet(ids_c, root=root["d"])
    ids_d = ids_c + [create_random_packet(root["d"]) for _ in range(3)]

    outpack_location_pull_metadata(root=root["dst"])

    ids = set(ids_a + ids_b + ids_c + ids_d)


def test_nonrecursive_pulls_are_prevented_by_configuration(tmp_path):
    root = create_temporary_roots(tmp_path, require_complete_tree=True)
    ids = create_random_packet_chain(root["src"], 3)

    with pytest.raises(Exception) as e:
        outpack_location_pull_packet(ids["c"], recursive=False,
                                     root=root["dst"])

    assert e.match("'recursive' must be True \(or None\) with your "
                   "configuration")


def test_if_recursive_pulls_are_required_pulls_are_default_recursive(tmp_path):
    root = create_temporary_roots(tmp_path, ["src", "shallow"],
                                  require_complete_tree=False)
    root["deep"] = create_temporary_root(tmp_path, require_complete_tree=True)

    ids = create_random_packet_chain(root["src"], 3)

    for r in [root["shallow"], root["deep"]]:
        outpack_location_add("src", "path",
                             {"path": str(root["src"].path)},
                             root=r)
        outpack_location_pull_metadata(root=r)

    outpack_location_pull_packet(ids["c"], recursive=None, root=root["shallow"])
    assert root["shallow"].index.unpacked() == [ids["c"]]

    outpack_location_pull_packet(ids["c"], recursive=None, root=root["deep"])
    assert root["deep"].index.unpacked() == list(ids.values())

## TODO: Uncomment when wired up with searching
# def test_can_pull_packets_as_result_of_query(tmp_path):
#     TODO
# def test_pull_packet_errors_if_allow_remote_is_false(tmp_path):
#     root = create_temporary_roots(tmp_path)
#     id = create_random_packet(root["src"])
#
#     outpack_location_add("src", "path",
#                          {"path": str(root["src"].path)},
#                          root=root["dst"])
#     outpack_location_pull_metadata(root=root["dst"])
#
#     with pytest.raises(Exception):
#         outpack_location_pull_packet(None, options={"allow_remote" = False}, root=root["dst"])
#     assert e.match("If specifying 'options', 'allow_remote' must be True")


def test_skip_files_in_file_store(tmp_path):
    root = create_temporary_roots(tmp_path, use_file_store=True)

    ids = create_random_packet_chain(root["src"], 3)
    outpack_location_add("src", "path",
                         {"path": str(root["src"].path)},
                         root=root["dst"])

    outpack_location_pull_metadata(root=root["dst"])
    outpack_location_pull_packet(ids["a"], root=root["dst"])

    f = io.StringIO()
    with contextlib.redirect_stdout(f):
        outpack_location_pull_packet(ids["b"], root=root["dst"])
    text = f.getvalue()
    assert re.search("Found 1 file in the file store", text)
    assert re.search(
        r"Need to fetch 3 files \([0-9]* Bytes\) from 1 location", text)
