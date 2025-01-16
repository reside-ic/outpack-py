import functools

import click

from outpack.search_options import SearchOptions


class NumericalParamType(click.ParamType):
    """
    A type for numerical parameters.

    It automatically deduces whether the parameter should be interpreted as an
    int or a float.
    """

    name = "number"

    def convert(self, value, param, ctx):
        if isinstance(value, float):
            return value

        try:
            return int(value)
        except ValueError:
            pass

        try:
            return float(value)
        except ValueError:
            self.fail(f"{value!r} is not a valid numerical value", param, ctx)


# Almost all sub-commands accept a --root option, so it would make sense for it
# to be accepted early in the command syntax. For instance, we want to accept
# `orderly --root /path run foo`. Normally, with click, for this to be accepted
# we would have to define the option on the top-level command.
#
# However there is at least one command (init) which does not take a --root
# option, which would prevent us from doing this. Additionally, if the option
# is defined on the top-level command, we cannot call the tool with the option
# further down, ie. `orderly run --root /path foo`.
#
# To work around this we apply a bit of magic: we add a hidden option on the
# top-level command with a callback that stores the value in a seperate field
# on the context. When the top-level command dispatches to the sub-command, we
# extract these saved arguments and pass them down.
class PropagatingContext(click.Context):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.propagated_options = []


class PropagatingGroup(click.Group):
    context_class = PropagatingContext

    def resolve_command(self, ctx, args):

        # Rewrite the subcommands arguments to include the propagated options.
        #
        # args[0] is always the name of the subcommand. We pass that first so
        # the superclasses' resolve_command method still works, followed by our
        # injected arguments, and finally the rest of the original arguments.
        extended_args = [args[0], *ctx.propagated_options, *args[1:]]
        return super().resolve_command(ctx, extended_args)

    @staticmethod
    def propagate(ctx, param, value):
        if not isinstance(ctx, PropagatingContext):
            msg = "Propagating option on non-propagating group"
            raise RuntimeError(msg)
        if len(param.opts) > 1:
            msg = "Options with multiple syntaxes is not supported"
            raise NotImplementedError(msg)
        if value is not None:
            ctx.propagated_options += [param.opts[0], value]


PropagatingGroup.group_class = PropagatingGroup


def with_root(f=None, *, propagate=False):
    if f is None:
        # This happens when the function is called as a decorator factory.
        # Return the actual decorator and forward the options.
        return lambda f: with_root(f, propagate=propagate)

    return click.option(
        "--root",
        help=(
            "Path to the orderly repository. If not specified, the current"
            " working directory is used."
        ),
        type=click.Path(),
        hidden=propagate,
        expose_value=not propagate,
        callback=PropagatingGroup.propagate if propagate else None,
    )(f)


def with_search_options(f):
    @click.option(
        "--pull-metadata",
        is_flag=True,
        help="Synchronize metadata from remote locations.",
    )
    @click.option(
        "--allow-remote",
        is_flag=True,
        help="Search for packets in remote locations.",
    )
    @click.option(
        "--location",
        multiple=True,
        help=(
            "Locations in which packets are searched. This option may be"
            " specified multiple times to allow multiple locations. If none are"
            " specified, all configured locations are searched."
        ),
        metavar="NAME",
    )
    @click.pass_context
    @functools.wraps(f)
    def inner(ctx, allow_remote, pull_metadata, location, *args, **kwargs):
        options = SearchOptions(
            allow_remote=allow_remote,
            pull_metadata=pull_metadata,
            location=list(location) if location else None,
        )
        return ctx.invoke(f, *args, **kwargs, search_options=options)

    return inner
