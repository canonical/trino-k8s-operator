# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Integration tests for the Trino OAuth relation."""

import logging
import time

import jubilant
import pytest
import yaml
from helpers import (
    APP_NAME,
    TRAEFIK_NAME,
    TRINO_USER,
    fast_forward_ctx,
    get_unit,
    query_trino,
    wait_for_apps,
)

logger = logging.getLogger(__name__)

OAUTH_INTEGRATOR_NAME = "oauth-external-idp-integrator"
OAUTH_INTEGRATOR_CHANNEL = "latest/edge"
CERTIFICATES_NAME = "self-signed-certificates"
CERTIFICATES_CHANNEL = "latest/edge"
TRINO_CONFIG_PATH = "/usr/lib/trino/etc/config.properties"

OAUTH_STUB_CONFIG = {
    "issuer_url": "https://accounts.google.com",
    "authorization_endpoint": "https://accounts.google.com/o/oauth2/auth",
    "token_endpoint": "https://oauth2.googleapis.com/token",  # nosec B105
    "introspection_endpoint": "https://oauth2.googleapis.com/tokeninfo",
    "userinfo_endpoint": "https://www.googleapis.com/oauth2/v1/userinfo",
    "jwks_endpoint": "https://www.googleapis.com/oauth2/v3/certs",
    "scope": "openid profile email",
    "client_id": "stub-client-id",
    "client_secret": "stub-client-secret",  # nosec B105
}


def _read_trino_config(juju: jubilant.Juju) -> str:
    """Read Trino's rendered config.properties file."""
    return juju.ssh(
        f"{APP_NAME}/0",
        "cat",
        TRINO_CONFIG_PATH,
        container="trino",
    )


def _wait_for_trino_config(
    juju: jubilant.Juju,
    *,
    contains: tuple[str, ...],
    absent: tuple[str, ...] = (),
    timeout: float = 300,
) -> str:
    """Wait for Trino's rendered configuration to contain the expected values."""
    deadline = time.monotonic() + timeout
    config = ""
    while time.monotonic() < deadline:
        config = _read_trino_config(juju)
        if all(value in config for value in contains) and all(
            value not in config for value in absent
        ):
            return config
        time.sleep(5)
    raise TimeoutError(
        "Trino OAuth configuration did not converge; "
        f"missing={tuple(value for value in contains if value not in config)!r}, "
        f"unexpected={tuple(value for value in absent if value in config)!r}"
    )


def _wait_for_oauth_client_data(juju: jubilant.Juju, timeout: float = 300) -> dict[str, str]:
    """Wait for the OAuth provider to observe Trino's client registration data."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        raw = juju.cli("show-unit", f"{OAUTH_INTEGRATOR_NAME}/0", "--format=yaml")
        unit_data = yaml.safe_load(raw).get(f"{OAUTH_INTEGRATOR_NAME}/0", {})
        for relation in unit_data.get("relation-info", []):
            if relation.get("endpoint") != "oauth":
                continue
            app_data = relation.get("application-data", {})
            if app_data.get("redirect_uri"):
                return app_data
        time.sleep(5)
    raise TimeoutError("OAuth client registration data was not published within the timeout")


@pytest.fixture(name="deploy-oauth", scope="module")
def deploy_oauth(juju: jubilant.Juju, charm: str, charm_image: str):
    """Deploy a minimal Trino, ingress, and external OAuth integrator stack."""
    juju.deploy(
        charm,
        APP_NAME,
        resources={"trino-image": charm_image},
        config={"charm-function": "all"},
        num_units=1,
        trust=True,
    )
    juju.deploy(
        TRAEFIK_NAME,
        config={"routing_mode": "subdomain", "external_hostname": "example.com"},
        trust=True,
    )
    juju.deploy(CERTIFICATES_NAME, channel=CERTIFICATES_CHANNEL)
    juju.integrate(f"{APP_NAME}:ingress", f"{TRAEFIK_NAME}:ingress")
    wait_for_apps(juju, [APP_NAME, TRAEFIK_NAME], status="active", timeout=1200)

    juju.deploy(
        OAUTH_INTEGRATOR_NAME,
        channel=OAUTH_INTEGRATOR_CHANNEL,
        config=OAUTH_STUB_CONFIG,
    )
    juju.integrate(f"{APP_NAME}:oauth", f"{OAUTH_INTEGRATOR_NAME}:oauth")


@pytest.mark.incremental
@pytest.mark.usefixtures("deploy-oauth")
class TestOAuth:
    """Exercise OAuth registration, TLS enforcement, and removal."""

    def test_oauth_requires_https_ingress(self, juju: jubilant.Juju):
        """An OAuth relation blocks Trino until ingress publishes an HTTPS URL."""
        wait_for_apps(juju, [APP_NAME], status="blocked", timeout=600)

        unit = get_unit(juju, APP_NAME)
        assert unit.workload_status.message == "OAuth requires an HTTPS ingress URL"

    def test_oauth_configures_trino_after_tls(self, juju: jubilant.Juju):
        """TLS enables client registration and generic OIDC workload configuration."""
        wait_for_apps(juju, [CERTIFICATES_NAME], status="active", timeout=600)
        juju.integrate(
            f"{TRAEFIK_NAME}:certificates",
            f"{CERTIFICATES_NAME}:certificates",
        )

        wait_for_apps(
            juju,
            [APP_NAME, TRAEFIK_NAME, OAUTH_INTEGRATOR_NAME, CERTIFICATES_NAME],
            status="active",
            timeout=1200,
        )

        client_data = _wait_for_oauth_client_data(juju)
        assert client_data["redirect_uri"].startswith("https://")
        assert client_data["redirect_uri"].endswith("/oauth2/callback")
        assert client_data["scope"] == "openid profile email"

        _wait_for_trino_config(
            juju,
            contains=(
                "http-server.authentication.type=oauth2,PASSWORD",
                "http-server.authentication.oauth2.issuer=https://accounts.google.com",
                "http-server.authentication.oauth2.auth-url="
                "https://accounts.google.com/o/oauth2/auth",
                "http-server.authentication.oauth2.token-url=https://oauth2.googleapis.com/token",
                "http-server.authentication.oauth2.userinfo-url="
                "https://www.googleapis.com/oauth2/v1/userinfo",
                "http-server.authentication.oauth2.jwks-url="
                "https://www.googleapis.com/oauth2/v3/certs",
                "http-server.authentication.oauth2.oidc.discovery=false",
                "http-server.authentication.oauth2.scopes=openid,profile,email",
                "web-ui.authentication.type=oauth2",
            ),
        )

    def test_removing_oauth_restores_password_authentication(self, juju: jubilant.Juju):
        """Removing the relation disables OAuth without disrupting password auth."""
        juju.remove_relation(
            f"{APP_NAME}:oauth",
            f"{OAUTH_INTEGRATOR_NAME}:oauth",
        )
        wait_for_apps(juju, [APP_NAME], status="active", timeout=600)

        _wait_for_trino_config(
            juju,
            contains=("http-server.authentication.type=PASSWORD",),
            absent=(
                "http-server.authentication.type=oauth2,PASSWORD",
                "http-server.authentication.oauth2.issuer=",
                "http-server.authentication.oauth2.client-secret=",
            ),
        )

        result = query_trino(
            get_unit(juju, APP_NAME).address,
            TRINO_USER,
            "SELECT current_user",
        )
        assert result[0][0] == TRINO_USER


# ---------------------------------------------------------------------------
# Proxy configuration sourced from the Juju model (integration).
#
# Appended here, rather than a new module, because the OAuth proxy property
# is only rendered in the OAuth branch of config.jinja, and this module
# already deploys an OAuth-enabled Trino -- reusing it avoids an extra
# deployment purely to exercise this behaviour. No proxy service is deployed
# or contacted; a syntactically
# valid, non-routable proxy URL is used solely to verify that Juju really
# exports `JUJU_CHARM_*` to the charm container and that Airlift accepts the
# rendered property at startup -- both risks are invisible to unit tests,
# which mock `os.environ` and assert on rendered text only.
# ---------------------------------------------------------------------------

MODEL_PROXY_URL = "http://192.0.2.1:3128"
MODEL_NO_PROXY = "localhost,127.0.0.1,.svc.cluster.local"
ZZUSERZZ = "ZZUSERZZ"  # nosec B105
ZZPASSZZ = "ZZPASSZZ"  # nosec B105
JVM_CONFIG_PATH = "/usr/lib/trino/etc/jvm.config"


def _read_trino_jvm_config(juju: jubilant.Juju) -> str:
    """Read Trino's rendered jvm.config file."""
    return juju.ssh(
        f"{APP_NAME}/0",
        "cat",
        JVM_CONFIG_PATH,
        container="trino",
    )


@pytest.mark.incremental
@pytest.mark.usefixtures("deploy-oauth")
class TestModelProxyConfiguration:
    """Exercise model-config-derived proxy settings.

    Placed in this module and reusing its already-deployed, OAuth-enabled
    Trino application: the OAuth proxy property is only rendered when the
    OAuth relation is active, and no additional application deployment is
    introduced to keep CI cost low.
    """

    def test_model_proxy_config_renders_and_returns_to_active(self, juju: jubilant.Juju):
        """Setting juju-https-proxy/juju-no-proxy at model level stays active.

        This is appended to test_oauth.py because the OAuth proxy property is
        only rendered in the OAuth branch of config.jinja, and reuses the
        already-deployed OAuth-enabled Trino so no extra deployment is
        required. No proxy service is deployed or contacted; the proxy URL is
        syntactically valid but non-routable, proving Trino accepts the
        rendered configuration without needing to reach a live proxy.
        """
        juju.model_config(
            {
                "juju-https-proxy": MODEL_PROXY_URL,
                "juju-no-proxy": MODEL_NO_PROXY,
            }
        )

        with fast_forward_ctx(juju, "10s"):
            wait_for_apps(juju, [APP_NAME], status="active", timeout=600)

            config = _wait_for_trino_config(
                juju,
                contains=("oauth2-jwk.http-client.http-proxy=192.0.2.1:3128",),
            )
        assert "oauth2-jwk.http-client.http-proxy.secure" not in config

        jvm_config = _read_trino_jvm_config(juju)
        assert "-Dhttps.proxyHost=192.0.2.1" in jvm_config
        assert "-Dhttps.proxyPort=3128" in jvm_config
        assert "-Dhttp.nonProxyHosts=localhost|127.0.0.1|.svc.cluster.local" in jvm_config

    def test_credential_bearing_proxy_blocks_without_exposing_credentials(
        self, juju: jubilant.Juju
    ):
        """An authenticated proxy URL blocks the unit without leaking credentials.

        Continues from the previous test on the same OAuth-enabled deployment
        (see module docstring for the placement rationale). Sentinel credential
        values are used so this assertion cannot pass by accident.
        """
        juju.model_config({"juju-https-proxy": f"http://{ZZUSERZZ}:{ZZPASSZZ}@192.0.2.1:3128"})

        wait_for_apps(juju, [APP_NAME], status="blocked", timeout=600)

        status_output = juju.cli("status", "--format=yaml")
        assert ZZUSERZZ not in status_output
        assert ZZPASSZZ not in status_output

    def test_unsetting_model_proxy_converges_via_update_status(self, juju: jubilant.Juju):
        """Unsetting the model proxy returns the unit to active via update-status.

        Continues from the previous test on the same OAuth-enabled deployment
        (see module docstring for the placement rationale). Model config changes
        fire no `config-changed` event, so this specifically proves convergence
        through the `update-status` hook.
        """
        juju.model_config(reset=["juju-https-proxy", "juju-no-proxy"])

        with fast_forward_ctx(juju, "10s"):
            wait_for_apps(juju, [APP_NAME], status="active", timeout=600)
