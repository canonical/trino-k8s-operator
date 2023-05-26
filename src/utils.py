#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Collection of helper methods for Trino Charm."""

import base64
import logging
import re
import secrets
import string

from ops.model import Container


logger = logging.getLogger(__name__)


def generate_password() -> str:
    """Creates randomized string for use as app passwords.

    Returns:
        String of 32 randomized letter+digit characters
    """
    return "".join([secrets.choice(string.ascii_letters + string.digits) for _ in range(32)])


def parse_tls_file(raw_content: str) -> str:
    """Parse TLS files from both plain text or base64 format."""
    if re.match(r"(-+(BEGIN|END) [A-Z ]+-+)", raw_content):
        return raw_content
    return base64.b64decode(raw_content).decode("utf-8")


def push(container: Container, content: str, path: str) -> None:
    """Wrapper for writing a file and contents to a container.

    Args:
        container: container to push the files into
        content: the text content to write to a file path
        path: the full path of the desired file
    """
    container.push(path, content, make_dirs=True)