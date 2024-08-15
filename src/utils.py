#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Collection of helper methods for Trino Charm."""

import logging
import os
import re
import secrets
import string

import yaml
from jinja2 import Environment, FileSystemLoader
from ops.pebble import ExecError

from literals import JAVA_ENV

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


def render(template_name, env=None):
    """Pushes configuration files to application.

    Args:
        template_name: template_file.
        env: (Optional) The subset of config values for the file.

    Returns:
        content: template content.
    """
    # get the absolute path of templates directory.
    charm_dir = os.path.abspath(
        os.path.join(os.path.dirname(__file__), os.pardir)
    )
    templates_path = os.path.join(charm_dir, "templates")

    # handle jinja files.
    if "jinja" in template_name:
        loader = FileSystemLoader(templates_path)
        content = (
            Environment(loader=loader, autoescape=True)
            .get_template(template_name)
            .render(**env)
        )

    # handle properties files.
    else:
        file_path = os.path.join(templates_path, template_name)
        with open(file_path, "r") as file:
            content = file.read()
    return content


def string_to_dict(string_value):
    """Convert a string to a dictionary with = delimiter.

    Args:
        string_value: The string to be converted

    Returns:
        dictionary: The converted dictionary
    """
    pairs = string_value.splitlines()
    dictionary = {}
    for pair in pairs:
        key, value = pair.split("=", maxsplit=1)
        dictionary[key] = value
    return dictionary


def validate_membership(connector_fields, conn_input):
    """Validate if user input fields match those allowed by Trino.

    Args:
        connector_fields: Allowed and required Trino fields by connector
        conn_input: User input connection fields

    Raises:
        ValueError: In the case where a required field is missing
                    In the case where a provided field is not accepted
    """
    required = connector_fields["required"]
    optional = connector_fields["optional"]

    missing_fields = set(required) - set(conn_input)
    if missing_fields:
        raise ValueError(f"field(s) {missing_fields!r} are required")

    illegal_fields = set(conn_input) - (set(required) | set(optional))
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


def handle_exec_error(func):
    """Handle ExecError while executing command on application container.

    Args:
        func: The function to decorate.

    Returns:
        wrapper: A decorated function that raises an error on failure.
    """

    def wrapper(*args, **kwargs):
        """Execute wrapper for the decorated function and handle errors.

        Args:
            args: Positional arguments passed to the decorated function.
            kwargs: Keyword arguments passed to the decorated function.

        Returns:
            result: The result of the decorated function if successful.

        Raises:
            ExecError: In case the command fails to execute successfully.
        """
        try:
            result = func(*args, **kwargs)
            return result
        except ExecError:
            logger.exception(f"Failed to execute {func.__name__}:")
            raise

    return wrapper


def create_cert_and_catalog_dicts(config):
    """Identify certs and connection values from config.

    Args:
        config: the catalog-config file content.

    Returns:
        certs: dictionary of certificates.
        catalogs: dictionary of catalog values.
    """
    catalogs_with_certs = yaml.safe_load(config)
    catalogs = catalogs_with_certs.get("catalogs")
    certs = catalogs_with_certs.get("certs")
    return certs, catalogs


def add_cert_to_truststore(container, name, cert, storepass, conf_path):
    """Add CA to JKS truststore.

    Args:
        container: Trino container.
        name: Certificate file name.
        cert: Certificate content.
        storepass: Truststore password.
        conf_path: The conf directory.

    Raises:
        ExecError: In case of error during keytool certificate import
    """
    java_home = JAVA_ENV["JAVA_HOME"]
    command = [
        f"{java_home}/bin/keytool",
        "-import",
        "-v",
        "-alias",
        name,
        "-file",
        f"{name}.crt",
        "-keystore",
        "truststore.jks",
        "-storepass",
        storepass,
        "-noprompt",
    ]
    try:
        process = container.exec(
            command,
            working_dir=conf_path,
        )
        stdout, _ = process.wait_output()
        logger.info(stdout)
    except ExecError as e:
        expected_error_string = f"alias <{name}> already exists"
        if expected_error_string in str(e.stdout):
            logger.debug(expected_error_string)
            return
        logger.error(e.stdout)
        raise
