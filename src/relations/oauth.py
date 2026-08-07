# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""OAuth relation handler for Trino SSO."""

import logging
from typing import Optional

from charms.hydra.v0.oauth import (
    ClientConfig,
    ClientConfigError,
    OauthProviderConfig,
    OAuthRequirer,
)
from ops import ModelError, SecretNotFoundError, framework

from literals import (
    OAUTH_GRANT_TYPES,
    OAUTH_RELATION_NAME,
    OAUTH_SCOPE,
    OIDC_CALLBACK_PATH,
)
from log import log_event_handler

logger = logging.getLogger(__name__)


class OAuthRelationHandler(framework.Object):
    """Manage Trino's OAuth client registration and provider information."""

    def __init__(self, charm):
        """Construct the OAuth requirer.

        Args:
            charm: The Trino charm instance.
        """
        super().__init__(charm, OAUTH_RELATION_NAME)
        self.charm = charm
        self.requirer = OAuthRequirer(
            charm,
            client_config=None,
            relation_name=OAUTH_RELATION_NAME,
        )

        charm.framework.observe(self.requirer.on.oauth_info_changed, self._on_relation_event)
        charm.framework.observe(self.requirer.on.oauth_info_removed, self._on_relation_event)
        charm.framework.observe(
            charm.on[OAUTH_RELATION_NAME].relation_created,
            self._on_relation_event,
        )
        charm.framework.observe(
            self.requirer.on.invalid_client_config,
            self._on_invalid_client_config,
        )

    @property
    def is_related(self) -> bool:
        """Return whether an OAuth provider relation exists."""
        return self.charm.model.get_relation(OAUTH_RELATION_NAME) is not None

    @property
    def provider_info(self) -> Optional[OauthProviderConfig]:
        """Return live provider information once client registration is complete."""
        if not self.is_related or not self.requirer.is_client_created():
            return None
        try:
            provider = self.requirer.get_provider_info()
            relation = self.charm.model.get_relation(OAUTH_RELATION_NAME)
            if provider and relation and relation.app:
                secret_id = relation.data[relation.app].get("client_secret_id")
                if secret_id:
                    secret = self.charm.model.get_secret(id=secret_id)
                    provider.client_secret = secret.get_content(refresh=True)["secret"]
            return provider
        except (KeyError, ModelError, SecretNotFoundError) as err:
            logger.info("OAuth client secret is not available yet: %s", err)
            return None

    def publish_client_config(self) -> None:
        """Publish the ingress-derived OAuth client configuration."""
        if not self.is_related or self.charm.config.charm_function == "worker":
            return

        ingress_url = self.charm.ingress.url
        if not ingress_url or not ingress_url.startswith("https://"):
            return

        client_config = ClientConfig(
            redirect_uri=f"{ingress_url.rstrip('/')}{OIDC_CALLBACK_PATH}",
            scope=OAUTH_SCOPE,
            grant_types=OAUTH_GRANT_TYPES,
        )
        self.requirer.update_client_config(client_config)

    @log_event_handler(logger)
    def _on_relation_event(self, event) -> None:
        """Reconcile when provider data changes or is removed.

        Args:
            event: The OAuth library event.
        """
        self.charm._reconcile()

    @log_event_handler(logger)
    def _on_invalid_client_config(self, event) -> None:
        """Log invalid client configuration reported by the library.

        Args:
            event: The OAuth library validation event.
        """
        logger.error("Invalid OAuth client configuration: %s", event.error)


__all__ = ["ClientConfigError", "OAuthRelationHandler"]
