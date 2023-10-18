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
from utils import render

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

    def _synchronize(self, config, container):
        """Handle synchronization of Ranger users, groups and group membership.

        Args:
            config: string of user and group configuration from Ranger relation.
            container: Trino application container.

        Raises:
            ExecError: in case of error while sychnronizing group membership.
        """
        data = yaml.safe_load(config)
        try:
            self._sync(container, data["users"], "user")
            self._sync(container, data["groups"], "group")
            existing_combinations = self._get_existing_memberships(
                container, "membership"
            )
            removable_combinations = self._create_memberships(
                container, data["memberships"], existing_combinations
            )
            self._delete_memberships(container, removable_combinations)
        except ExecError:
            logger.exception(
                "An error occurred while syncing users and group memberships:"
            )
            raise

    def _sync(self, container, apply_objects, member_type):
        """Synchronize Unix users and groups.

        Args:
            container: The container to run the command in.
            apply_objects: The users and group mappings to be applied to Trino.
            member_type: The type of Unix object to create, either "user" or "group".

        Raises:
            ExecError: in case getting existing Unix users fails.
                       in case creating a Unix user or group fails.
        """
        try:
            existing_objects = self._get_unix(container, member_type)
        except ExecError:
            logger.exception(
                f"An error occurred while retrieving Unix {member_type}:"
            )
            raise

        for apply_object in apply_objects:
            apply_name = apply_object.get("name")
            matching = next(
                (
                    existing_object
                    for existing_object in existing_objects
                    if existing_object == apply_name
                ),
                None,
            )
            if not matching:
                try:
                    _ = container.exec(
                        [f"{member_type}add", apply_name]
                    ).wait_output()
                    logger.info(f"Created {member_type}: {apply_name}")
                except ExecError:
                    logger.exception(
                        f"An error occurred while creating {member_type}: {apply_name}:"
                    )
                    raise

    def _get_unix(self, container, member_type):
        """Get a list of Unix users or groups from the specified container.

        Args:
            container: The container to run the command in.
            member_type: The Unix object to retrieve, either "user", "group" or "membership".

        Raises:
            ExecError: in case the command cannot be executed.

        Returns:
            values: Either a list of usernames/groups or a dictionary of groups mapped to users.
        """
        member_type_mapping = UNIX_TYPE_MAPPING
        command = ["getent", member_type_mapping[member_type]]

        try:
            out = container.exec(command).wait_output()
        except ExecError:
            logger.exception(f"Failed to execute command {command}:")
            raise

        if member_type == "membership":
            # Split the output to rows.
            rows = out[0].strip().split("\n")
            # Create a dictionary of field 0 (group) and field 3 (username(s)).
            values = {row.split(":")[0]: row.split(":")[3] for row in rows}
        else:
            # Split the output to rows and create a list of user/group values.
            values = [row.split(":")[0] for row in out[0].strip().split("\n")]
        return values

    def _get_existing_memberships(self, container, member_type):
        """Get a list of Unix users or groups from the specified container.

        Args:
            container: The container to run the command in.
            member_type: The type of Unix object to retrieve: "membership".

        Raises:
            ExecError: in case the command cannot be executed.

        Returns:
            existing_combination: existing Unix memberships.
        """
        try:
            existing_memberships = self._get_unix(container, member_type)
        except ExecError:
            logger.exception("Failed to get Unix group memberships:")
            raise

        existing_combinations = set()
        for group, value in existing_memberships.items():
            users = [user.strip() for user in value.split(",")]
            for user in users:
                if user:
                    existing_combinations.add((group, user))

        return existing_combinations

    def _create_memberships(
        self, container, apply_memberships, existing_combinations
    ):
        """Add Unix users to groups.

        Args:
            container: The container to run the command in.
            apply_memberships: The membership combinations to apply.
            existing_combinations: Dictionary of existing Unix group membership.

        Raises:
            ExecError: in case where adding a user to a group fails.

        Returns:
            existing_combinations: dictionary of memberships not defined by Ranger.
        """
        for apply_membership in apply_memberships:
            group_name = apply_membership["groupname"]
            users = apply_membership["users"]
            for user_name in users:
                if (group_name, user_name) in existing_combinations:
                    existing_combinations.remove((group_name, user_name))
                else:
                    try:
                        container.exec(
                            ["usermod", "-aG", group_name, user_name]
                        ).wait_output()
                        logger.info(
                            f"Created group membership {group_name}:{user_name}"
                        )
                    except ExecError:
                        logger.exception(
                            f"Failed to add user {user_name} to {group_name}:"
                        )
                        raise
        return existing_combinations

    def _delete_memberships(self, container, removable_combinations):
        """Delete Unix group memberships.

        Args:
            container: The container to run the command in.
            removable_combinations: The membership to remove.

        Raises:
            ExecError: in case removing a user from a group fails.
        """
        for combination in removable_combinations:
            try:
                container.exec(
                    ["deluser", combination[1], combination[0]]
                ).wait_output()
                logger.info(
                    f"Removed group membership {combination[1]}:{combination[0]}"
                )
            except ExecError:
                logger.exception(
                    f"Failed to delete user {combination[1]} from {combination[0]}:"
                )
                raise

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
