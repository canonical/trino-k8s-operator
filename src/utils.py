#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Collection of helper methods for Trino Charm."""

import logging
import secrets
import string
import os
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

def read(file_name, file_directory):
    """Read the file with the given name using the given directory.

    Args:
        file_name: File name to read from.

    Returns:
        A string of the file provided's content.
    """
    charm_dir = os.path.abspath(
        os.path.join(os.path.dirname(__file__), os.pardir)
    )
    template_path = os.path.join(charm_dir, file_directory)
    file_path = os.path.join(template_path, file_name)
    with open(file_path) as f: content = f.read()
    return content
