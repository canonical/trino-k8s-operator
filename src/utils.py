#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Collection of helper methods for Trino Charm."""

import logging
import secrets
import string
import os
import json
from jinja2 import Environment, FileSystemLoader
from ops.model import Container
import re

logger = logging.getLogger(__name__)


def generate_password() -> str:
    """Create randomized string for use as app passwords.

    Returns:
        String of 32 randomized letter+digit characters
    """
    return "".join(
        [
            secrets.choice(string.ascii_letters + string.digits)
            for _ in range(32)
        ]
    )


def push(container: Container, content: str, path: str) -> None:
    """Write a file and contents to a container.

    Args:
        container: container to push the files into
        content: the text content to write to a file path
        path: the full path of the desired file
    """
    container.push(path, content, make_dirs=True)

def render(template_name, context):
    """Render the template with the given name using the given context dict.

    Args:
        template_name: File name to read the template from.
        context: Dict used for rendering.

    Returns:
        A dict containing the rendered template.
    """
    charm_dir = os.path.abspath(
        os.path.join(os.path.dirname(__file__), os.pardir)
    )
    loader = FileSystemLoader(os.path.join(charm_dir, "templates"))
    return (
        Environment(loader=loader, autoescape=True)
        .get_template(template_name)
        .render(**context)
    )

def string_to_dict(string):
    pairs = string.split()
    dictionary = {}
    for pair in pairs:
        key, value = pair.split('=')
        dictionary[key] = value
    return dictionary

def validate_membership(connector_fields, conn_input, name):
    required = connector_fields["required"]
    optional = connector_fields["optional"]

    for field in required:
        if field not in conn_input:
            raise ValueError(f"{name!r} {field!r} is required")

    for field in conn_input:
        if field not in required and field not in optional:
            raise ValueError(f"{name!r} {field!r} is not allowed")

def validate_jdbc_pattern(conn_dict, conn_name):
    if not re.match("jdbc:[a-z0-9]+:(?s:.*)$", conn_dict["connection-url"]):
        raise ValueError(f"{conn_name!r} has an invalid jdbc format")

def format_properties_file(dictionary):
    conn_config = ""
    for key, value in dictionary.items():
        output += f"{key}={value}\n"
    return conn_config
