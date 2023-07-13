#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Collection of helper methods for Trino Charm."""

import logging
import os
import re
import secrets
import string
import bcrypt

from jinja2 import Environment, FileSystemLoader
from ops.model import Container

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
    """Convert a string to a dictionary with = delimiter.

    Args:
        string: The string to be converted

    Returns:
        dictionary: The converted dictionary
    """
    pairs = string.split()
    dictionary = {}
    for pair in pairs:
        key, value = pair.split("=")
        dictionary[key] = value
    return dictionary


def validate_membership(connector_fields, conn_input):
    """Validate if user input fields match those allowed by Trino.

    Args:
        connector_fields: Allowed and required Trino fields by connector
        conn_input: User input connection fields

    Raises:
        ValueError: In the case where a required field is missing
        ValueError: In the case where a provided field is not accepted
    """
    required = connector_fields["required"]
    optional = connector_fields["optional"]

    missing_fields = []
    for field in required:
        if field not in conn_input:
            missing_fields.append(field)
    if missing_fields:
        raise ValueError(f"field(s) {missing_fields!r} are required")

    illegal_fields = []
    for field in conn_input:
        if field not in required and field not in optional:
            illegal_fields.append(field)
    if illegal_fields:
        raise ValueError(f"field(s) {illegal_fields!r} are not allowed")


def validate_jdbc_pattern(conn_input, conn_name):
    """Validate the format of postgresql jdbc string.

    Args:
        conn_input: user input connector dictionary
        conn_name: user input connector name

    Raises:
        ValueError: In the case the jdbc string is invalid
    """
    if not re.match("jdbc:[a-z0-9]+:(?s:.*)$", conn_input["connection-url"]):
        raise ValueError(f"{conn_name!r} has an invalid jdbc format")


def format_properties_file(dictionary):
    """Convert string into format required by properties file.

    Args:
        dictionary: Configuration dictionary to be converted

    Return:
        conn_config: String of connector connfiguration
    """
    conn_config = ""
    for key, value in dictionary.items():
        conn_config += f"{key}={value}\n"
    return conn_config

def bcrypt_pwd(password):
    """Bycrypts password.

    Args:
        password: plain text password

    Return:
        bcrypt_password: encrypted password
    """
    bcrypt_password = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt(rounds=12)).decode("utf-8")
    return bcrypt_password
