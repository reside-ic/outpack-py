import re

from click.testing import CliRunner
from orderly.cli import cli
from pytest_unordered import unordered

from outpack.config import Location, read_config
from outpack.location import outpack_location_add_path
from outpack.root import OutpackRoot

from .. import helpers


def invoke(*args, expected_exit_code=0):
    def cast(x):
        if isinstance(x, OutpackRoot):
            return str(x.path)
        else:
            return str(x)

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(cli, [cast(x) for x in args])
    assert result.exit_code == expected_exit_code

    return result


def test_can_init_repository(tmp_path):
    invoke("init", tmp_path)

    cfg = read_config(tmp_path)
    assert cfg.core.path_archive == "archive"
    assert not cfg.core.use_file_store
    assert not cfg.core.require_complete_tree


def test_can_init_repository_with_file_store(tmp_path):
    invoke("init", tmp_path, "--use-file-store")

    cfg = read_config(tmp_path)
    assert cfg.core.path_archive is None
    assert cfg.core.use_file_store


def test_can_init_repository_with_file_store_and_archive(tmp_path):
    invoke("init", tmp_path, "--use-file-store", "--archive", "storage")

    cfg = read_config(tmp_path)
    assert cfg.core.path_archive == "storage"
    assert cfg.core.use_file_store


def test_run_prints_packet_id(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    helpers.copy_examples("data", root)

    result = invoke("run", "data", "--root", tmp_path)
    id = result.stdout.strip()

    assert (tmp_path / "archive" / "data" / id).exists()
    assert root.index.metadata(id).name == "data"


def test_can_pass_parameters_when_running(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    helpers.copy_examples(["parameters"], root)

    result = invoke(
        "run",
        "parameters",
        *("-n", "a", "3.14"),
        *("-p", "b", "foo"),
        *("--root", root),
    )
    id = result.stdout.strip()
    assert root.index.metadata(id).parameters == {"a": 3.14, "b": "foo"}

    result = invoke(
        "run",
        "parameters",
        *("-b", "a", "true"),
        *("-b", "b", "false"),
        *("--root", root),
    )
    id = result.stdout.strip()
    assert root.index.metadata(id).parameters == {"a": True, "b": False}


def test_can_distinguish_int_or_float_parameters(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    helpers.copy_examples(["parameters"], root)

    result = invoke(
        "run",
        "parameters",
        *("-n", "a", "3"),
        *("-n", "b", "3."),
        *("--root", root),
    )
    id = result.stdout.strip()
    params = root.index.metadata(id).parameters
    assert isinstance(params["a"], int)
    assert isinstance(params["b"], float)


def test_parameters_are_not_converted_implicitly(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    helpers.copy_examples(["parameters"], root)

    result = invoke(
        "run",
        "parameters",
        *("-p", "a", "3"),
        *("-p", "b", "true"),
        *("--root", root),
    )
    id = result.stdout.strip()
    params = root.index.metadata(id).parameters
    assert isinstance(params["a"], str)
    assert isinstance(params["b"], str)
    assert params == {"a": "3", "b": "true"}


def test_cannot_pass_same_parameter_multiple_times(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    helpers.copy_examples(["parameters"], root)

    result = invoke(
        "run",
        "parameters",
        *("-p", "a", "foo"),
        *("-p", "a", "bar"),
        *("-n", "b", "5"),
        *("-b", "b", "false"),
        *("--root", root),
        expected_exit_code=1,
    )
    assert (
        result.stderr.strip()
        == "Parameters were specified multiple times: 'a', 'b'"
    )


def test_can_search_packets(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    ids = {helpers.create_random_packet(root, name="data") for _ in range(3)}

    result = invoke("search", "name == 'data'", "--root", tmp_path)
    assert set(result.stdout.splitlines()) == ids

    result = invoke(
        "search", "name == 'other'", "--root", root, expected_exit_code=1
    )
    assert result.stderr.strip() == "No packets matching the query were found"


def test_search_options(tmp_path):
    root = helpers.create_temporary_roots(tmp_path, ["dst", "x", "y"])
    id_x = helpers.create_random_packet(name="data", root=root["x"])
    id_y = helpers.create_random_packet(name="data", root=root["y"])

    outpack_location_add_path("x", root["x"], root=root["dst"])
    outpack_location_add_path("y", root["y"], root=root["dst"])

    result = invoke(
        "search",
        "--allow-remote",
        "--pull-metadata",
        "name == 'data'",
        *("--root", root["dst"]),
    )
    assert result.stdout.splitlines() == unordered(id_x, id_y)

    result = invoke(
        "search",
        "--allow-remote",
        "--location=x",
        "name == 'data'",
        *("--root", root["dst"]),
    )
    assert result.stdout.splitlines() == [id_x]

    result = invoke(
        "search",
        "name == 'data'",
        *("--root", root["dst"]),
        expected_exit_code=1,
    )
    assert result.stderr.strip() == "No packets matching the query were found"


def test_can_add_locations(tmp_path):
    root = helpers.create_temporary_roots(tmp_path)

    invoke(
        "location",
        "add",
        "foo",
        root["src"],
        *("--root", root["dst"]),
    )

    invoke(
        "location",
        "add",
        "bar",
        "ssh://127.0.0.1/foo",
        *("--root", root["dst"]),
    )

    config = read_config(root["dst"].path)
    assert config.location == {
        "local": Location("local", "local", None),
        "foo": Location("foo", "path", {"path": str(root["src"].path)}),
        "bar": Location("bar", "ssh", {"url": "ssh://127.0.0.1/foo"}),
    }


def test_can_manage_locations(tmp_path):
    root = helpers.create_temporary_roots(tmp_path)

    result = invoke("location", "list", "--root", root["dst"])
    assert result.stdout.splitlines() == ["local"]

    invoke(
        "location",
        "add",
        "foo",
        root["src"],
        *("--root", root["dst"]),
    )

    result = invoke("location", "list", "--root", root["dst"])
    assert result.stdout.splitlines() == unordered("local", "foo")

    invoke(
        "location",
        "rename",
        "foo",
        "bar",
        *("--root", root["dst"]),
    )

    result = invoke("location", "list", "--root", root["dst"])
    assert result.stdout.splitlines() == unordered("local", "bar")

    invoke(
        "location",
        "remove",
        "bar",
        *("--root", root["dst"]),
    )

    result = invoke("location", "list", "--root", root["dst"])
    assert result.stdout.splitlines() == ["local"]


def test_cannot_add_location_with_unknown_protocol(tmp_path):
    root = helpers.create_temporary_root(tmp_path)
    result = invoke(
        "location",
        "add",
        "origin",
        "myproto://example.com",
        *("--root", root),
        expected_exit_code=1,
    )
    assert re.search(
        "^Unsupported location protocol: 'myproto'$",
        result.stderr,
        re.MULTILINE,
    )


def test_flexible_root_argument(tmp_path):
    root = helpers.create_temporary_root(tmp_path / "a")
    helpers.copy_examples("data", root)

    # For subcommands that accept a --root option, we allow that option to be
    # passed at any point in the sequence, including at the start, on the
    # parent command.

    invoke("--root", root, "run", "data")
    invoke("run", "--root", root, "data")
    invoke("run", "data", "--root", root)

    invoke("--root", root, "location", "list")
    invoke("location", "--root", root, "list")
    invoke("location", "list", "--root", root)

    # init doesn't accept a --root option, instead it uses a positional
    # argument. Make sure we don't accidentally accept the former.
    result = invoke(
        "--root", root, "init", tmp_path / "b", expected_exit_code=2
    )
    assert re.search(
        "^Error: No such option: --root$",
        result.stderr,
        re.MULTILINE,
    )
