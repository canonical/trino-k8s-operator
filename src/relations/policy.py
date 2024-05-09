# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Defines policy relation event handling methods."""

import logging
from pathlib import Path

import yaml
from ops import framework
from ops.pebble import ExecError

from literals import (
    JAVA_ENV,
    RANGER_PLUGIN_FILES,
    RANGER_PLUGIN_HOME,
    TRINO_PLUGIN_DIR,
    TRINO_PORTS,
    UNIX_TYPE_MAPPING,
)
from log import log_event_handler
from utils import handle_exec_error, render

logger = logging.getLogger(__name__)


class PolicyRelationHandler(framework.Object):
    """Client for trino policy relations.

    Attrs:
        ranger_abs_path: The absolute path for Ranger plugin home directory.
    """

    @property
    def ranger_abs_path(self):
        """Return the Ranger plugin absolute path."""
        return Path(RANGER_PLUGIN_HOME)

    def __init__(self, charm, relation_name="policy"):
        """Construct.

        Args:
            charm: The charm to attach the hooks to.
            relation_name: The name of the relation defaults to policy.
        """
        super().__init__(charm, "policy")
        self.charm = charm
        self.relation_name = relation_name

        # Handle database relation.
        self.framework.observe(
            charm.on[self.relation_name].relation_created,
            self._on_relation_created,
        )
        self.framework.observe(
            charm.on[self.relation_name].relation_changed,
            self._on_relation_changed,
        )
        self.framework.observe(
            charm.on[self.relation_name].relation_broken,
            self._on_relation_broken,
        )

    @log_event_handler(logger)
    def _on_relation_created(self, event):
        """Handle policy relation created.

        Args:
            event: The relation created event.
        """
        if not self.charm.unit.is_leader():
            return

        service = self._prepare_service(event)

        if event.relation:
            event.relation.data[self.charm.app].update(service)

    @log_event_handler(logger)
    def _on_relation_changed(self, event):
        """Handle policy relation changed.

        Args:
            event: Relation changed event.
        """
        if not self.charm.unit.is_leader():
            return

        container = self.charm.model.unit.get_container(self.charm.name)
        if not container.can_connect():
            event.defer()
            return

        self.charm.state.policy_manager_url = event.relation.data[
            event.app
        ].get("policy_manager_url")
        if not self.charm.state.policy_manager_url:
            return

        self.charm.state.policy_relation = f"relation_{event.relation.id}"
        self._configure_ranger_plugin(container)

        users_and_groups = event.relation.data[event.app].get(
            "user-group-configuration"
        )
        if users_and_groups:
            try:
                self._synchronize(users_and_groups, container)
            except ExecError:
                logger.exception("Failed to synchronize groups:")
                event.defer()
        self.charm._restart_trino(container)

    def _prepare_service(self, event):
        """Prepare service to be created in Ranger.

        Args:
            event: Relation created event

        Returns:
            service: Service values to be set in relation databag
        """
        host = self.charm.app.name
        port = TRINO_PORTS["HTTP"]
        uri = f"{host}:{port}"

        service_name = (
            self.charm.config.get("ranger-service-name")
            or f"relation_{event.relation.id}"
        )
        service = {
            "name": service_name,
            "type": "trino",
            "jdbc.driverClassName": "io.trino.jdbc.TrinoDriver",
            "jdbc.url": f"jdbc:trino://{uri}",
        }
        return service

    @log_event_handler(logger)
    def _on_relation_broken(self, event):
        """Handle policy relation broken.

        Args:
            event: Relation broken event.

        Raises:
            ExecError: When failure to disable Ranger plugin.
        """
        if not self.charm.unit.is_leader():
            return

        container = self.charm.model.unit.get_container(self.charm.name)
        if not container.can_connect():
            event.defer()
            return

        try:
            self._disable_ranger_plugin(container)
            self.charm.state.ranger_enabled = False
            logger.info("Ranger plugin disabled successfully")
        except ExecError as err:
            raise ExecError(f"Unable to disable Ranger plugin: {err}") from err

        self.charm._restart_trino(container)

    @handle_exec_error
    def _configure_ranger_plugin(self, container):
        """Enable the ranger plugin for Trino.

        Args:
            container: The application container.
        """
        self._push_plugin_files(
            container,
            self.charm.state.policy_manager_url,
            self.charm.state.policy_relation,
        )
        self._run_plugin_entrypoint(container)
        self.charm.state.ranger_enabled = True
        logger.info("Ranger plugin is enabled.")

    @handle_exec_error
    def _run_plugin_entrypoint(self, container):
        """Enable ranger plugin.

        Args:
            container: The application container
        """
        command = [
            "bash",
            "enable-trino-plugin.sh",
        ]
        container.exec(
            command,
            working_dir=str(self.ranger_abs_path),
            environment=JAVA_ENV,
        ).wait()

    def _push_plugin_files(
        self, container, policy_manager_url, policy_relation
    ):
        """Configure the Ranger plugin install.properties file.

        Args:
            container: The application container
            policy_manager_url: The url of the policy manager
            policy_relation: The relation name and id of policy relation
        """
        policy_context = {
            "POLICY_MGR_URL": policy_manager_url,
            "REPOSITORY_NAME": self.charm.config.get("ranger-service-name")
            or policy_relation,
        }
        for template, file in RANGER_PLUGIN_FILES.items():
            content = render(template, policy_context)
            if file == "access-control.properties":
                path = self.charm.trino_abs_path.joinpath(file)
            else:
                path = self.ranger_abs_path.joinpath(file)
            container.push(path, content, make_dirs=True, permissions=0o744)

    @handle_exec_error
    def _synchronize(self, config, container):
        """Handle synchronization of Ranger users, groups and group membership.

        Args:
            config: String of user and group configuration from Ranger relation.
            container: Trino application container.
        """
        data = yaml.safe_load(config)
        self._sync(container, data["users"], "user")
        self._sync(container, data["groups"], "group")
        self._sync(container, data["memberships"], "membership")
        logger.info("User synchronization successful!")

    @handle_exec_error
    def _sync(self, container, apply_objects, member_type):
        """Synchronize Unix users and groups.

        Args:
            container: The container to run the command in.
            apply_objects: The users and group mappings to be applied to Trino.
            member_type: The type of Unix member, "user", "group" or "membership".
        """
        # get existing values
        existing = self._get_unix(container, member_type)

        # get values to apply
        apply = self._transform_apply_values(apply_objects, member_type)

        # create members
        to_create = [item for item in apply if item not in existing]
        self._create_members(container, member_type, to_create)

        # delete memberships
        if member_type == "membership":
            to_delete = [item for item in existing if item not in apply]
            self._delete_memberships(container, to_delete)

    @handle_exec_error
    def _get_unix(self, container, member_type):
        """Get a list of Unix users or groups from the specified container.

        Args:
            container: The container to run the command in.
            member_type: The type of Unix member, "user", "group" or "membership".

        Returns:
            values: Either a list of usernames/groups or a list of (group, user) tuples.
        """
        member_type_mapping = UNIX_TYPE_MAPPING
        command = ["getent", member_type_mapping[member_type]]

        out = container.exec(command).wait_output()

        # Split the output to rows.
        rows = out[0].strip().split("\n")
        if member_type == "membership":
            # Create a list of (group, user) tuples.
            members = [(row.split(":")[0], row.split(":")[3]) for row in rows]
            values = []
            for group, users in members:
                values += [
                    (group, user.strip())
                    for user in users.split(",")
                    if user.strip()
                ]
        else:
            # Split the output to rows and create a list of user or group values.
            values = [row.split(":")[0] for row in rows]
        return values

    def _transform_apply_values(self, data, member_type):
        """Get list of users, groups or memberships to apply from configuration file.

        Args:
            data: User, group or membership data.
            member_type: The type of Unix member, "user", "group" or "membership".

        Returns:
            List of users, groups or memberships to apply.
        """
        if member_type in ["user", "group"]:
            return [member["name"] for member in data]

        membership_tuples = [
            (membership["groupname"], user)
            for membership in data
            for user in membership["users"]
        ]
        return membership_tuples

    @handle_exec_error
    def _create_members(self, container, member_type, to_create):
        """Create Unix users, groups or memberships.

        Args:
            container: The container to run the command in.
            member_type: The type of Unix member, "user", "group" or "membership".
            to_create: List of users, groups or memberships to create.
        """
        for member in to_create:
            logger.debug(f"Attempting to create {member_type}: {member}")

            if member_type == "group":
                command = [f"{member_type}add", member]
            elif member_type == "user":
                command = [f"{member_type}add", "-c", "ranger", member]
            elif member_type == "membership":
                command = ["usermod", "-aG", member[0], member[1]]

            container.exec(command).wait()

    @handle_exec_error
    def _delete_memberships(self, container, to_delete):
        """Delete Unix group memberships.

        Args:
            container: The container to run the command in.
            to_delete: List of memberships to delete.
        """
        ranger_users = self._get_ranger_users(container)
        for membership in to_delete:
            if membership[1] in ranger_users:
                logger.debug(f"Attempting to delete membership {membership}")
                container.exec(
                    ["deluser", membership[1], membership[0]]
                ).wait()

    @handle_exec_error
    def _get_ranger_users(self, container):
        """Get users for which the Gecos information contains `ranger`.

        Args:
            container: The container to run the command in.

        Returns:
            ranger_users: The users created by the Ranger relation.
        """
        out = container.exec(["getent", "passwd"]).wait_output()
        rows = out[0].strip().split("\n")
        ranger_users = []

        for row in rows:
            user = row.strip().split(":")
            if "ranger" in user[4]:
                ranger_users.append(user[0])

        return ranger_users

    @handle_exec_error
    def _disable_ranger_plugin(self, container):
        """Disable ranger plugin.

        Args:
            container: application container
        """
        if container.exists(f"{TRINO_PLUGIN_DIR}/ranger"):
            command = [
                "bash",
                "disable-trino-plugin.sh",
            ]
            container.exec(
                command,
                working_dir=str(self.ranger_abs_path),
                environment=JAVA_ENV,
            ).wait()

            container.remove_path(
                self.charm.trino_abs_path.joinpath(
                    "access-control.properties"
                ),
                recursive=True,
            )
