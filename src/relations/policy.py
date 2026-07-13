# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Defines policy relation event handling methods."""

import logging

from ops import framework

from literals import RANGER_PLUGIN_FILES, TRINO_HOME, TRINO_PORTS
from utils import handle_exec_error, render

logger = logging.getLogger(__name__)


class PolicyRelationHandler(framework.Object):
    """Client for trino policy relations.

    Event observation is centralized in the charm; this object exposes logic
    methods invoked by the charm reconciler.
    """

    def __init__(self, charm, relation_name="policy"):
        """Construct.

        Args:
            charm: The charm to attach the hooks to.
            relation_name: The name of the relation defaults to policy.
        """
        super().__init__(charm, "policy")
        self.charm = charm
        self.relation_name = relation_name

    def publish_service_data(self):
        """Publish the Trino service definition to related Ranger managers."""
        if not self.charm.unit.is_leader():
            return

        for relation in self.charm.model.relations[self.relation_name]:
            service = self._prepare_service(relation)
            relation.data[self.charm.app].update(service)

    def read_policy_manager_url(self):
        """Return the policy manager URL published by the Ranger provider.

        Returns:
            The policy manager URL, or None when unavailable.
        """
        for relation in self.charm.model.relations[self.relation_name]:
            if relation.app is None:
                continue
            url = relation.data[relation.app].get("policy_manager_url")
            if url:
                return url
        return None

    def _prepare_service(self, relation):
        """Prepare service to be created in Ranger.

        Args:
            relation: The policy relation.

        Returns:
            service: Service values to be set in relation databag
        """
        host = self.charm.app.name
        port = TRINO_PORTS["HTTP"]
        namespace = self.model.name
        uri = f"{host}.{namespace}.svc.cluster.local:{port}"

        service_name = self.charm.config.ranger_service_name or f"relation_{relation.id}"
        service = {
            "name": service_name,
            "type": "trino",
            "jdbc.driverClassName": "io.trino.jdbc.TrinoDriver",
            "jdbc.url": f"jdbc:trino://{uri}",
        }
        return service

    @handle_exec_error
    def _configure_ranger_plugin(self, container):
        """Render and push the Ranger plugin files.

        Args:
            container: The application container.

        Returns:
            Mapping of pushed file name to rendered content, for plan hashing.
        """
        rendered = self._push_plugin_files(
            container,
            self.charm.state.policy_manager_url,
            self.charm.state.policy_relation,
        )
        logger.info("Ranger plugin is enabled.")
        return rendered

    def _push_plugin_files(self, container, policy_manager_url, policy_relation):
        """Configure the Ranger plugin install.properties file.

        Args:
            container: The application container
            policy_manager_url: The url of the policy manager
            policy_relation: The relation name and id of policy relation

        Returns:
            Mapping of pushed file name to rendered content.
        """
        opensearch = self.charm.state.opensearch or {}
        policy_context = {
            "TRINO_HOME": TRINO_HOME,
            "POLICY_MGR_URL": policy_manager_url,
            "REPOSITORY_NAME": self.charm.config.ranger_service_name or policy_relation,
            "RANGER_RELATION": True,
            "OPENSEARCH_INDEX": opensearch.get("index"),
            "OPENSEARCH_HOST": opensearch.get("host"),
            "OPENSEARCH_PORT": opensearch.get("port"),
            "OPENSEARCH_PWD": opensearch.get("password"),
            "OPENSEARCH_USER": opensearch.get("username"),
            "OPENSEARCH_ENABLED": opensearch.get("is_enabled"),
        }
        rendered = {}
        for template, file in RANGER_PLUGIN_FILES.items():
            content = render(template, policy_context)
            path = self.charm.trino_abs_path.joinpath(file)
            container.push(path, content, make_dirs=True, permissions=0o744)
            rendered[file] = content
        return rendered
