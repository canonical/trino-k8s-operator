# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Defines policy relation event handling methods."""

import logging

import yaml
from ops import framework
from ops.model import BlockedStatus
from ops.pebble import ExecError

from literals import (
    JAVA_ENV,
    RANGER_PLUGIN_FILE,
    RANGER_PLUGIN_VERSION,
    RANGER_POLICY_PATH,
    TRINO_PORTS,
    UNIX_TYPE_MAPPING,
)
from log import log_event_handler
from utils import handle_exec_error, render

logger = logging.getLogger(__name__)


class PolicyRelationHandler(framework.Object):
    """Client for trino policy relations.

    Attributes:
        plugin_version: the version of the Ranger plugin
        ranger_plugin_path: the path of the unpacked ranger plugin
    """

    plugin_version = RANGER_PLUGIN_VERSION["path"]
    ranger_plugin_path = f"/root/ranger-{plugin_version}-trino-plugin"

    def __init__(self, charm, relation_name="policy"):
        """Construct.

        Args:
            charm: The charm to attach the hooks to.
            relation_name: the name of the relation defaults to policy.
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
            event: relation created event.
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
            event: relation changed event.
        """
        if not self.charm.unit.is_leader():
            return

        container = self.charm.model.unit.get_container(self.charm.name)
        if not container.can_connect():
            return

        policy_manager_url = event.relation.data[event.app].get(
            "policy_manager_url", None
        )
        if not policy_manager_url:
            return

        policy_relation = f"relation_{event.relation.id}"

        try:
            self._unpack_plugin(container)
            self._configure_plugin_properties(
                container, policy_manager_url, policy_relation
            )
            self._enable_plugin(container)
        except ExecError:
            logger.exception("Failed to enable Ranger plugin:")
            self.charm.unit.status = BlockedStatus(
                "Failed to enable Ranger plugin."
            )
            return

        users_and_groups = event.relation.data[event.app].get(
            "user-group-configuration", None
        )
        if users_and_groups:
            try:
                self._synchronize(users_and_groups, container)
            except ExecError:
                logger.exception("Failed to synchronize groups:")
                event.defer()
            except Exception:
                logger.exception(
                    "An exception occurred while sychronizing Ranger groups:"
                )
                self.charm.unit.status = BlockedStatus(
                    "Failed to synchronize Ranger groups."
                )
        self.charm._restart_trino(container)

    def _prepare_service(self, event):
        """Prepare service to be created in Ranger.

        Args:
            event: relation created event

        Returns:
            service: service values to be set in relation databag
        """
        host = self.charm.config["application-name"]
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
            event: relation broken event.
        """
        if not self.charm.unit.is_leader():
            return

        container = self.charm.model.unit.get_container(self.charm.name)
        if not container.can_connect():
            return

        if not container.exists(self.ranger_plugin_path):
            return

        self._disable_ranger_plugin(container)

        if container.exists(RANGER_POLICY_PATH):
            container.remove_path(RANGER_POLICY_PATH, recursive=True)

        if container.exists(self.ranger_plugin_path):
            container.remove_path(self.ranger_plugin_path, recursive=True)

        self.charm._restart_trino(container)

    def _enable_plugin(self, container):
        """Enable ranger plugin.

        Args:
            container: application container

        Raises:
            ExecError: in case unable to enable trino plugin
        """
        command = [
            "bash",
            "enable-trino-plugin.sh",
        ]
        try:
            container.exec(
                command,
                working_dir=self.ranger_plugin_path,
                environment=JAVA_ENV,
            ).wait_output()
            logger.info("Ranger plugin enabled successfully")
        except ExecError as err:
            logger.error(err.stdout)
            raise

    def _unpack_plugin(self, container):
        """Unpack ranger plugin tar.

        Args:
            container: application container

        Raises:
            ExecError: in case unable to enable trino plugin
        """
        if container.exists(self.ranger_plugin_path):
            return

        tar_version = RANGER_PLUGIN_VERSION["tar"]

        command = [
            "tar",
            "xf",
            f"ranger-{tar_version}-trino-plugin.tar.gz",
        ]
        try:
            container.exec(command, working_dir="/root").wait_output()
            logger.info("Ranger plugin unpacked successfully")
        except ExecError as err:
            logger.error(err.stdout)
            raise

    def _configure_plugin_properties(
        self, container, policy_manager_url, policy_relation
    ):
        """Configure the Ranger plugin install.properties file.

        Args:
            container: The application container
            policy_manager_url: The url of the policy manager
            policy_relation: The relation name and id of policy relation
        """
        ranger_properties_path = f"/root/ranger-{self.plugin_version}-trino-plugin/install.properties"
        policy_context = {
            "POLICY_MGR_URL": policy_manager_url,
            "REPOSITORY_NAME": self.charm.config.get("ranger-service-name")
            or policy_relation,
        }
        properties = render(RANGER_PLUGIN_FILE, policy_context)
        container.push(
            ranger_properties_path,
            properties,
            make_dirs=True,
            permissions=0o744,
        )

    @handle_exec_error
    def _synchronize(self, config, container):
        """Handle synchronization of Ranger users, groups and group membership.

        Args:
            config: string of user and group configuration from Ranger relation.
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
            member_type: The type of Unix object to create, either "user" or "group".
        """
        # get existing values
        existing_values = self._get_unix(container, member_type)

        # get values to apply
        apply_values = self._transfornm_apply_values(
            apply_objects, member_type
        )

        # create memnbers
        to_create = [
            item for item in apply_values if item not in existing_values
        ]
        self._create_members(container, member_type, to_create)

        # delete memberships
        if member_type == "memberships":
            to_delete = [
                item for item in existing_values if item not in apply_values
            ]
            self._delete_membership(container, to_delete)

    @handle_exec_error
    def _get_unix(self, container, member_type):
        """Get a list of Unix users or groups from the specified container.

        Args:
            container: The container to run the command in.
            member_type: The Unix object to retrieve, either "user", "group" or "membership".

        Returns:
            values: Either a list of usernames/groups or a list of (group, user) tuples.
        """
        member_type_mapping = UNIX_TYPE_MAPPING
        command = ["getent", member_type_mapping[member_type]]

        out = container.exec(command).wait_output()

        # Split the output to rows.
        rows = out[0].strip().split("\n")
        if member_type == "membership":
            # Create a dictionary of field 0 (group) and field 3 (username(s)).
            group_users = [
                (row.split(":")[0], row.split(":")[3]) for row in rows
            ]
            values = []
            for group, users in group_users:
                values += [
                    (group, user.strip())
                    for user in users.split(",")
                    if user.strip()
                ]
        else:
            # Split the output to rows and create a list of user/group values.
            values = [row.split(":")[0] for row in rows]
        return values

    def _transfornm_apply_values(self, data, member_type):
        """Get list of users, groups or memberships to apply from configuration file.

        Args:
            data: user group or membership data.
            member_type: One of "user", "group" or "membership".

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
            member_type: The Unix object to retrieve, either "user", "group" or "membership".
            to_create: List of users, groups or memberships to create.
        """
        for member in to_create:
            if member_type == "group":
                command = [f"{member_type}add", member]
            if member_type == "user":
                command = [f"{member_type}add", "-c", "ranger", member]
            elif member_type == "membership":
                command = ["usermod", "-aG", member[0], member[1]]

            container.exec(command).wait_output()

    @handle_exec_error
    def _delete_memberships(self, container, to_delete):
        """Delete Unix group memberships.

        Args:
            container: The container to run the command in.
            to_delete: List of memberships to delete.
        """
        for membership in to_delete:
            user_info = self._get_user_gecos(container, membership[1])
            if "ranger" in user_info:
                container.exec(
                    ["deluser", membership[1], membership[0]]
                ).wait_output()

    @handle_exec_error
    def _get_user_gecos(self, container, username):
        """Get the Gecos information for a specific user.

        Args:
            container: The container to run the command in.
            username: The username for which to retrieve the Gecos information.

        Returns:
            user_info: The Gecos information for the user.
        """
        out = container.exec(["getent", "passwd", username]).wait_output()
        user_info = out[0].strip().split(":")[4]
        return user_info

    def _disable_ranger_plugin(self, container):
        """Disable ranger plugin.

        Args:
            container: application container

        Raises:
            ExecError: in case unable to enable trino plugin
        """
        command = [
            "bash",
            "disable-trino-plugin.sh",
        ]
        try:
            container.exec(
                command,
                working_dir=self.ranger_plugin_path,
                environment=JAVA_ENV,
            ).wait_output()
            logger.info("Ranger plugin disabled successfully")
        except ExecError as err:
            logger.error(err.stdout)
            raise