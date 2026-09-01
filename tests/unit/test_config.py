#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for proxy configuration derivation.

Pins the pure `utils.py` logic that turns Juju model proxy configuration
(`JUJU_CHARM_HTTP_PROXY`, `JUJU_CHARM_HTTPS_PROXY`, `JUJU_CHARM_NO_PROXY`) and
the `additional-jvm-options` escape hatch into JVM proxy flags and the OAuth
`http-proxy` property.

Sentinel credential values (``ZZUSERZZ`` / ``ZZPASSZZ``) are used instead of
short strings so assertions that they are absent from error messages and log
output cannot pass by accident.

`utils` is imported as a module, not by name, so that a not-yet-implemented
symbol raises `AttributeError` inside the individual test that needs it,
rather than an `ImportError` that would abort collection of the whole test
module before implementation exists.
"""

import logging

import pytest

import utils

ZZUSERZZ = "ZZUSERZZ"  # nosec B105
ZZPASSZZ = "ZZPASSZZ"  # nosec B105


# ---------------------------------------------------------------------------
# Proxy URL parsing and validation
# ---------------------------------------------------------------------------


def test_http_with_port():
    """An http:// URL with an explicit port parses to host/port/not-secure."""
    parsed = utils.parse_proxy_url("http://proxy.corp:3128", "juju-http-proxy")
    assert parsed.host == "proxy.corp"
    assert parsed.port == 3128
    assert parsed.secure is False


def test_https_with_port():
    """An https:// URL with an explicit port parses to host/port/secure."""
    parsed = utils.parse_proxy_url("https://proxy.corp:8443", "juju-https-proxy")
    assert parsed.host == "proxy.corp"
    assert parsed.port == 8443
    assert parsed.secure is True


def test_http_no_port_defaults_to_80():
    """An http:// URL without a port defaults to port 80."""
    parsed = utils.parse_proxy_url("http://proxy.corp", "juju-http-proxy")
    assert parsed.port == 80


def test_https_no_port_defaults_to_443():
    """An https:// URL without a port defaults to port 443."""
    parsed = utils.parse_proxy_url("https://proxy.corp", "juju-https-proxy")
    assert parsed.port == 443


def test_http_scheme_on_https_setting_yields_port_80_not_443():
    """`juju-https-proxy=http://proxy.corp` must yield port 80, not 443.

    The port derives from the proxy URL's own scheme, never from the name of
    the Juju setting that carried it.
    """
    parsed = utils.parse_proxy_url("http://proxy.corp", "juju-https-proxy")
    assert parsed.port == 80
    assert parsed.secure is False


def test_ipv4_host():
    """A bare IPv4 host and port parse through unchanged."""
    parsed = utils.parse_proxy_url("http://10.0.0.5:3128", "juju-http-proxy")
    assert parsed.host == "10.0.0.5"
    assert parsed.port == 3128


def test_empty_string_is_not_configured():
    """An empty string is treated as "not configured", with no error."""
    assert utils.parse_proxy_url("", "juju-http-proxy") is None


def test_none_is_not_configured():
    """An unset (None) value behaves identically to an empty string."""
    assert utils.parse_proxy_url(None, "juju-http-proxy") is None


def test_no_host_is_rejected():
    """A scheme with no host cannot be used and is rejected."""
    with pytest.raises(utils.ProxyConfigError):
        utils.parse_proxy_url("http://", "juju-http-proxy")


def test_not_a_url_is_rejected():
    """A value that is not a URL at all is rejected."""
    with pytest.raises(utils.ProxyConfigError):
        utils.parse_proxy_url("not a url", "juju-http-proxy")


def test_unparsable_port_is_rejected():
    """A non-numeric port cannot be used and is rejected."""
    with pytest.raises(utils.ProxyConfigError):
        utils.parse_proxy_url("http://proxy.corp:notaport", "juju-http-proxy")


def test_malformed_credential_bearing_value_is_rejected_without_sentinels():
    """A malformed, credential-bearing sentinel value is rejected.

    Neither sentinel may leak into the raised error message.
    """
    value = f"http://{ZZUSERZZ}:{ZZPASSZZ}@proxy.corp:3128"
    with pytest.raises(utils.ProxyConfigError) as exc_info:
        utils.parse_proxy_url(value, "juju-http-proxy")
    message = str(exc_info.value)
    assert ZZUSERZZ not in message
    assert ZZPASSZZ not in message


def test_username_only_is_rejected():
    """A username-only proxy URL (no password) is rejected."""
    with pytest.raises(utils.ProxyConfigError) as exc_info:
        utils.parse_proxy_url(f"http://{ZZUSERZZ}@proxy.corp:3128", "juju-http-proxy")
    assert ZZUSERZZ not in str(exc_info.value)


def test_scheme_less_url_is_rejected():
    """A scheme-less value is rejected rather than guessed at."""
    with pytest.raises(utils.ProxyConfigError):
        utils.parse_proxy_url("proxy.corp:3128", "juju-http-proxy")


def test_socks_scheme_is_rejected():
    """Only http/https CONNECT proxies are supported; SOCKS is rejected."""
    with pytest.raises(utils.ProxyConfigError):
        utils.parse_proxy_url("socks5://proxy.corp:1080", "juju-http-proxy")


def test_path_is_rejected_not_truncated():
    """A proxy URL carrying a path is rejected, not silently truncated."""
    with pytest.raises(utils.ProxyConfigError):
        utils.parse_proxy_url("http://proxy.corp:3128/pac", "juju-http-proxy")


def test_query_is_rejected():
    """A proxy URL carrying a query string is rejected."""
    with pytest.raises(utils.ProxyConfigError):
        utils.parse_proxy_url("http://proxy.corp:3128?a=b", "juju-http-proxy")


def test_ipv6_host_bare_without_brackets():
    """An IPv6 proxy host is returned bare, without brackets."""
    parsed = utils.parse_proxy_url("http://[::1]:3128", "juju-http-proxy")
    assert parsed.host == "::1"
    assert parsed.port == 3128


def test_ipv6_host_no_port_defaults_to_80():
    """An IPv6 proxy host without a port defaults to port 80."""
    parsed = utils.parse_proxy_url("http://[::1]", "juju-http-proxy")
    assert parsed.host == "::1"
    assert parsed.port == 80


def test_fragment_is_rejected_not_truncated():
    """A proxy URL carrying a fragment is rejected, not silently truncated."""
    with pytest.raises(utils.ProxyConfigError):
        utils.parse_proxy_url("http://proxy.corp:3128#fragment", "juju-http-proxy")


def test_password_only_is_rejected_without_sentinel_in_error_or_logs(caplog):
    """A password-only proxy URL is rejected; the sentinel never leaks."""
    value = f"http://:{ZZPASSZZ}@proxy.corp:3128"
    with caplog.at_level(logging.DEBUG):
        with pytest.raises(utils.ProxyConfigError) as exc_info:
            utils.parse_proxy_url(value, "juju-http-proxy")
    assert ZZPASSZZ not in str(exc_info.value)
    assert ZZPASSZZ not in caplog.text


# ---------------------------------------------------------------------------
# JVM proxy flag derivation
# ---------------------------------------------------------------------------


def test_only_http_proxy_emits_http_family_only():
    """Only JUJU_CHARM_HTTP_PROXY set emits only the http.* flags."""
    result = utils.jvm_proxy_options("http://p:3128", None, None)
    assert "-Dhttp.proxyHost=p" in result
    assert "-Dhttp.proxyPort=3128" in result
    assert "-Dhttps.proxyHost=" not in result
    assert "-Dhttps.proxyPort=" not in result


def test_only_https_proxy_emits_https_family_only():
    """Only JUJU_CHARM_HTTPS_PROXY set emits only the https.* flags."""
    result = utils.jvm_proxy_options(None, "https://p:8443", None)
    assert "-Dhttps.proxyHost=p" in result
    assert "-Dhttps.proxyPort=8443" in result
    assert "-Dhttp.proxyHost=" not in result
    assert "-Dhttp.proxyPort=" not in result


def test_both_proxies_set_to_different_hosts_are_independent():
    """Both proxies set, to different hosts, both families are emitted."""
    result = utils.jvm_proxy_options("http://phttp:80", "https://phttps:443", None)
    assert "-Dhttp.proxyHost=phttp" in result
    assert "-Dhttp.proxyPort=80" in result
    assert "-Dhttps.proxyHost=phttps" in result
    assert "-Dhttps.proxyPort=443" in result


def test_none_set_emits_no_flags():
    """No model proxy configuration emits no proxy flags at all."""
    result = utils.jvm_proxy_options(None, None, None)
    assert result == ""


def test_no_proxy_alone_emits_non_proxy_hosts_only():
    """`JUJU_CHARM_NO_PROXY` alone still emits `-Dhttp.nonProxyHosts`.

    This is inert to the JVM (which only consults it once a proxyHost is set)
    but is expected behaviour.
    """
    result = utils.jvm_proxy_options(None, None, "localhost,127.0.0.1")
    assert result == "-Dhttp.nonProxyHosts=localhost|127.0.0.1"


# ---------------------------------------------------------------------------
# nonProxyHosts conversion
# ---------------------------------------------------------------------------


def test_comma_to_pipe_conversion():
    """Comma-separated entries convert to pipe-separated."""
    result = utils.jvm_proxy_options(None, None, "localhost,127.0.0.1")
    assert result == "-Dhttp.nonProxyHosts=localhost|127.0.0.1"


def test_wildcard_entry_preserved_verbatim():
    """A `*.svc.cluster.local` wildcard entry is preserved verbatim."""
    result = utils.jvm_proxy_options(None, None, "*.svc.cluster.local")
    assert "-Dhttp.nonProxyHosts=*.svc.cluster.local" in result


def test_cidr_entry_dropped():
    """A CIDR entry is dropped, leaving only the literal host."""
    result = utils.jvm_proxy_options(None, None, "localhost,10.0.0.0/8")
    assert result == "-Dhttp.nonProxyHosts=localhost"


def test_multiple_dropped_cidrs_produce_one_aggregated_warning(caplog):
    """Multiple dropped CIDR entries produce exactly one aggregated warning."""
    with caplog.at_level(logging.WARNING):
        result = utils.jvm_proxy_options(None, None, "10.0.0.0/8,192.168.0.0/16,localhost")
    assert result == "-Dhttp.nonProxyHosts=localhost"
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "10.0.0.0/8" in warnings[0].message
    assert "192.168.0.0/16" in warnings[0].message


def test_only_cidr_entries_emit_no_flag_but_one_warning(caplog):
    """If every entry is a CIDR, no flag is emitted, but one warning is logged."""
    with caplog.at_level(logging.WARNING):
        result = utils.jvm_proxy_options(None, None, "10.0.0.0/8,192.168.0.0/16")
    assert result == ""
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1


def test_entries_with_surrounding_whitespace_are_trimmed():
    """Whitespace around entries is trimmed."""
    result = utils.jvm_proxy_options(None, None, " localhost , 127.0.0.1 ")
    assert result == "-Dhttp.nonProxyHosts=localhost|127.0.0.1"


def test_empty_segments_are_dropped():
    """Empty segments produced by stray commas are dropped."""
    result = utils.jvm_proxy_options(None, None, "a,,b,")
    assert result == "-Dhttp.nonProxyHosts=a|b"


def test_no_implicit_entries_are_added():
    """Localhost/127.0.0.1/discovery host are never appended implicitly."""
    result = utils.jvm_proxy_options("http://p:80", None, None)
    assert "nonProxyHosts" not in result


# ---------------------------------------------------------------------------
# Batch 5 (partial) -- OAuth proxy property derivation, pure logic
# ---------------------------------------------------------------------------


def test_oauth_properties_http_only_no_secure():
    """A plaintext model proxy yields the property with no `.secure` flag."""
    props = utils.oauth_proxy_properties("http://p:3128", None)
    assert props["http_proxy"] == "p:3128"
    assert not props.get("secure")


def test_oauth_properties_https_sets_secure_true():
    """An https:// model proxy yields the property and `.secure=true`."""
    props = utils.oauth_proxy_properties(None, "https://p:8443")
    assert props["http_proxy"] == "p:8443"
    assert props["secure"] is True


def test_oauth_properties_prefers_https_over_http():
    """When both are set, `juju-https-proxy` is preferred (Decision 4)."""
    props = utils.oauth_proxy_properties("http://phttp:80", "https://phttps:443")
    assert props["http_proxy"] == "phttps:443"


def test_oauth_properties_falls_back_to_http_proxy():
    """Only `juju-http-proxy` set: the property is derived from it."""
    props = utils.oauth_proxy_properties("http://phttp:80", None)
    assert props["http_proxy"] == "phttp:80"


def test_oauth_properties_empty_when_no_proxy_configured():
    """No model proxy configured: no OAuth proxy properties at all."""
    assert utils.oauth_proxy_properties(None, None) == {}


def test_oauth_properties_ipv6_host_bracketed():
    """The OAuth property brackets an IPv6 host, unlike the bare JVM flag."""
    props = utils.oauth_proxy_properties("http://[::1]:3128", None)
    assert props["http_proxy"] == "[::1]:3128"


# ---------------------------------------------------------------------------
# Precedence validation helper (additional-jvm-options host/port pairing)
# ---------------------------------------------------------------------------


def test_validate_jvm_proxy_overrides_accepts_matched_pairs():
    """A host+port supplied together in additional-jvm-options is valid."""
    utils.validate_jvm_proxy_overrides("-Dhttps.proxyHost=other-proxy -Dhttps.proxyPort=8080")


def test_validate_jvm_proxy_overrides_rejects_host_without_port():
    """A proxyHost override without a matching proxyPort is invalid."""
    with pytest.raises(utils.ProxyConfigError):
        utils.validate_jvm_proxy_overrides("-Dhttps.proxyHost=other-proxy")


def test_validate_jvm_proxy_overrides_rejects_port_without_host():
    """A proxyPort override without a matching proxyHost is invalid."""
    with pytest.raises(utils.ProxyConfigError):
        utils.validate_jvm_proxy_overrides("-Dhttps.proxyPort=8080")


def test_validate_jvm_proxy_overrides_accepts_unrelated_options():
    """An unrelated JVM flag alongside a valid pair does not raise."""
    utils.validate_jvm_proxy_overrides("-Xmx4G -Dhttp.proxyHost=h -Dhttp.proxyPort=1")


def test_validate_jvm_proxy_overrides_accepts_empty():
    """No additional-jvm-options at all is valid (nothing to check)."""
    utils.validate_jvm_proxy_overrides("")
    utils.validate_jvm_proxy_overrides(None)
