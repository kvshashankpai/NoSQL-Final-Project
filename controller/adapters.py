"""Adapter launch commands from config/project_config.json. No database access."""

import sys

from etl_common.config import adapter_config


class StartError(Exception):
    """The run could not start; nothing was written anywhere."""


def launcher(pipeline):
    """The command that starts this pipeline's adapter, as a list of arguments.

    `{python}` in the configured launcher is replaced by the current interpreter.
    """
    cmd = adapter_config(pipeline)["launcher"]
    if not cmd:
        raise StartError(f"No launcher configured for {pipeline} in config/project_config.json")
    return [sys.executable if part == "{python}" else part for part in cmd]


def canonical_name(pipeline):
    return adapter_config(pipeline)["name"]
