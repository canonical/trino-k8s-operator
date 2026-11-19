#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Fake opensearch_client provider for testing failed Ranger audit delivery."""

import logging
import secrets

from ops.charm import CharmBase, RelationEvent
from ops.main import main
from ops.model import ActiveStatus, SecretNotFoundError

logger = logging.getLogger(__name__)

RELATION_NAME = "opensearch-client"
SECRET_LABEL = "opensearch-stub-user"  # nosec B105
# Nothing listens on port 1 in the requirer's pod, so connections are refused.
UNREACHABLE_ENDPOINT = "127.0.0.1:1"


class OpenSearchStubCharm(CharmBase):
    """Publish credentials and an unreachable endpoint over opensearch_client."""

    def __init__(self, *args):
        """Construct.

        Args:
            args: Ignore.
        """
        super().__init__(*args)
        self.framework.observe(self.on.start, self._on_start)
        self.framework.observe(self.on[RELATION_NAME].relation_joined, self._publish)
        self.framework.observe(self.on[RELATION_NAME].relation_changed, self._publish)

    def _on_start(self, _):
        """Handle start event."""
        self.unit.status = ActiveStatus()

    def _publish(self, event: RelationEvent):
        """Share the user secret and the unreachable endpoint with the requirer.

        Args:
            event: The relation event.
        """
        if not self.unit.is_leader():
            return
        try:
            secret = self.model.get_secret(label=SECRET_LABEL)
        except SecretNotFoundError:
            secret = self.app.add_secret(
                {"username": "audit", "password": secrets.token_hex(16)},
                label=SECRET_LABEL,
            )
        secret.grant(event.relation)
        event.relation.data[self.app].update(
            {"endpoints": UNREACHABLE_ENDPOINT, "secret-user": secret.get_info().id}
        )
        logger.info("Published unreachable endpoint %s", UNREACHABLE_ENDPOINT)


if __name__ == "__main__":  # pragma: nocover
    main(OpenSearchStubCharm)
