import sys
from collections import Counter
from urllib.parse import urlparse

import click

import outpack.search
from orderly.cli.options import (
    NumericalParamType,
    PropagatingGroup,
    with_root,
    with_search_options,
)
from orderly.run import orderly_run
from outpack.init import outpack_init
from outpack.location import (
    outpack_location_add,
    outpack_location_list,
    outpack_location_remove,
    outpack_location_rename,
)
from outpack.util import format_list


@click.group(cls=PropagatingGroup)
@with_root(propagate=True)
def cli():
    """Run 'orderly <command> --help' to see the options available for each subcommand."""
    pass


@cli.command()
@click.argument("path")
@click.option(
    "--archive",
    help="Path to the archive in which packets will be stored.",
    type=click.Path(),
)
@click.option(
    "--use-file-store",
    is_flag=True,
    help="Store packets in a content-addressed store.",
)
@click.option(
    "--require-complete-tree",
    is_flag=True,
    help=(
        "Require a complete tree. Whenever packets are pulled from remote"
        " locations, orderly will ensure all of that packet's dependencies are"
        " pulled as well."
    ),
)
def init(path, archive, use_file_store, require_complete_tree):
    """Initialize at new orderly repository at the specified path."""
    kwargs = {}
    if archive is not None:
        kwargs["path_archive"] = archive
    elif use_file_store:
        kwargs["path_archive"] = None
    else:
        # In this case we let outpack_init set the default value for the archive
        # path, ie. `archive`.
        pass

    outpack_init(
        path,
        use_file_store=use_file_store,
        require_complete_tree=require_complete_tree,
        **kwargs,
    )


@cli.command()
@click.argument("name")
@click.option(
    "-p",
    "--parameter",
    nargs=2,
    type=(str, str),
    multiple=True,
    help="Pass a string parameter to the report",
    metavar="NAME VALUE",
)
@click.option(
    "-n",
    "--numeric-parameter",
    nargs=2,
    type=(str, NumericalParamType()),
    multiple=True,
    help="Pass a numerical parameter to the report",
    metavar="NAME VALUE",
)
@click.option(
    "-b",
    "--bool-parameter",
    nargs=2,
    type=(str, bool),
    multiple=True,
    help="Pass a boolean parameter to the report",
    metavar="NAME VALUE",
)
@with_search_options
@with_root
def run(
    name,
    root,
    parameter,
    numeric_parameter,
    bool_parameter,
    search_options,
):
    """
    Run a report.

    Parameters to the report may be specified using the -p/-n/-b options,
    depending on the parameter's type. These options may be specified multiple
    times.
    """
    all_parameters = parameter + numeric_parameter + bool_parameter
    counts = Counter(key for (key, _) in all_parameters)
    duplicates = [key for key, n in counts.items() if n > 1]
    if duplicates:
        click.echo(
            f"Parameters were specified multiple times: {format_list(duplicates)}",
            err=True,
        )
        sys.exit(1)

    id = orderly_run(
        name,
        root=root,
        parameters=dict(all_parameters),
        search_options=search_options,
    )
    click.echo(id)


@cli.command()
@click.argument("query")
@with_search_options
@with_root
def search(query, root, search_options):
    """
    Search existing packets by query.

    By default, the search will only return packets present in the local
    repository. The --allow-remote option allows packets that are known locally
    but only present remotely to also be returned.
    """
    packets = outpack.search.search(query, root=root, options=search_options)
    if packets:
        for id in packets:
            click.echo(id)
    else:
        click.echo("No packets matching the query were found", err=True)
        sys.exit(1)


@cli.group()
@with_root(propagate=True)
def location():
    """
    Manage remote locations.

    Run 'orderly location <command> --help' to see the options available for each subcommand.
    """
    pass


@location.command("list")
@with_root
def location_list(root):
    """List configured remote locations."""
    locations = outpack_location_list(root=root)
    for name in locations:
        click.echo(name)


@location.command("rename")
@click.argument("old")
@click.argument("new")
@with_root
def location_rename(root, old, new):
    """Rename a remote location."""
    outpack_location_rename(old, new, root=root)


@location.command("remove")
@click.argument("name")
@with_root
def location_remove(root, name):
    """Remove a remote location."""
    outpack_location_remove(name, root=root)


@location.command("add")
@click.argument("name")
@click.argument("location", metavar="(PATH | URL)")
@with_root
def location_add(root, name, location):
    """
    Add a new remote location.

    The location may be specified as a path, for outpack repositories that are
    directly accesible through the file system, or as URLs. Currently only SSH
    URLs are supported.
    """
    # On windows it could be slightly ambiguous whether "C://foo" is a URL or a
    # file path, and we always interpret that as a URL. Users can easily
    # disambiguate by using either backslashes or using a single forward slash.
    if "://" not in location:
        outpack_location_add(name, "path", {"path": location}, root=root)
    else:
        parts = urlparse(location)

        if parts.scheme in {"ssh"}:
            outpack_location_add(
                name, parts.scheme, {"url": location}, root=root
            )
        else:
            click.echo(
                f"Unsupported location protocol: '{parts.scheme}'", err=True
            )
            sys.exit(1)
