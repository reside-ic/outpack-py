import re

import pytest
import responses
from responses import matchers
from responses.registries import OrderedRegistry

from pyorderly.outpack.location import outpack_location_add
from pyorderly.outpack.location_packit import (
    GITHUB_ACCESS_TOKEN_URL,
    GITHUB_CLIENT_ID,
    GITHUB_DEVICE_CODE_URL,
    OAuthDeviceClient,
    outpack_location_packit,
    packit_authorisation,
)
from pyorderly.outpack.location_pull import outpack_location_pull_metadata

from ..helpers import create_temporary_root


# This fixture automatically gets invoked by all tests in the file, and will
# clear the authorisation cache to avoid carrying tokens over.
@pytest.fixture(autouse=True)
def clear_authentication_cache():
    try:
        yield
    finally:
        packit_authorisation.cache_clear()


@responses.activate(assert_all_requests_are_fired=True)
def test_can_pass_packit_token():
    responses.get(
        "https://example.com/packit/api/outpack/metadata/list",
        json={"status": "success", "data": []},
        match=[matchers.header_matcher({"Authorization": "Bearer mytoken"})],
    )

    location = outpack_location_packit("https://example.com", token="mytoken")
    location.list()


@responses.activate(assert_all_requests_are_fired=True)
def test_can_pass_github_personal_token():
    responses.post(
        "https://example.com/packit/api/auth/login/api",
        match=[matchers.json_params_matcher({"token": "ghp_token"})],
        json={"token": "mytoken"},
    )
    responses.get(
        "https://example.com/packit/api/outpack/metadata/list",
        match=[matchers.header_matcher({"Authorization": "Bearer mytoken"})],
        json={"status": "success", "data": []},
    )

    location = outpack_location_packit("https://example.com", token="ghp_token")
    location.list()


@responses.activate(assert_all_requests_are_fired=True)
def test_authentication_is_cached():
    auth_response = responses.post(
        "https://example.com/packit/api/auth/login/api",
        match=[matchers.json_params_matcher({"token": "ghp_token"})],
        json={"token": "mytoken"},
    )
    list_response = responses.get(
        "https://example.com/packit/api/outpack/metadata/list",
        match=[matchers.header_matcher({"Authorization": "Bearer mytoken"})],
        json={"status": "success", "data": []},
    )

    location = outpack_location_packit("https://example.com", token="ghp_token")
    location.list()

    assert auth_response.call_count == 1
    assert list_response.call_count == 1

    location.list()

    assert auth_response.call_count == 1
    assert list_response.call_count == 2


@responses.activate(
    registry=OrderedRegistry, assert_all_requests_are_fired=True
)
def test_can_perform_interactive_authentication(capsys):
    responses.post(
        GITHUB_DEVICE_CODE_URL,
        json={
            "device_code": "xxxxx",
            "user_code": "1234-5678",
            "verification_uri": "https://github.com/login/device",
            "expires_in": 3600,
            "interval": 0,
        },
        match=[
            matchers.urlencoded_params_matcher(
                {
                    "client_id": GITHUB_CLIENT_ID,
                    "scope": "read:org",
                }
            )
        ],
    )

    m = matchers.urlencoded_params_matcher(
        {
            "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
            "device_code": "xxxxx",
            "client_id": GITHUB_CLIENT_ID,
        }
    )

    responses.post(
        GITHUB_ACCESS_TOKEN_URL,
        json={"error": "authorization_pending"},
        match=[m],
    )

    responses.post(
        GITHUB_ACCESS_TOKEN_URL,
        json={"access_token": "ghp_token", "token_type": "bearer"},
        match=[m],
    )

    responses.post(
        "https://example.com/packit/api/auth/login/api",
        match=[matchers.json_params_matcher({"token": "ghp_token"})],
        json={"token": "mytoken"},
    )

    responses.get(
        "https://example.com/packit/api/outpack/metadata/list",
        match=[matchers.header_matcher({"Authorization": "Bearer mytoken"})],
        json={"status": "success", "data": []},
    )

    location = outpack_location_packit("https://example.com")
    location.list()

    captured = capsys.readouterr()
    assert "enter the code <1234-5678>" in captured.out


@responses.activate(assert_all_requests_are_fired=True)
def test_oauth_failed_authentication():
    responses.post(
        GITHUB_DEVICE_CODE_URL,
        json={
            "device_code": "xxxxx",
            "user_code": "1234-5678",
            "verification_uri": "https://github.com/login/device",
            "expires_in": 3600,
            "interval": 0,
        },
    )

    responses.post(GITHUB_ACCESS_TOKEN_URL, json={"error": "access_denied"})

    with OAuthDeviceClient(
        GITHUB_CLIENT_ID,
        GITHUB_DEVICE_CODE_URL,
        GITHUB_ACCESS_TOKEN_URL,
    ) as client:
        msg = "Error while fetching access token: access_denied"
        with pytest.raises(Exception, match=msg):
            client.authenticate("read:org")


@responses.activate(assert_all_requests_are_fired=True)
def test_oauth_failed_authentication_with_description():
    responses.post(
        GITHUB_DEVICE_CODE_URL,
        json={
            "device_code": "xxxxx",
            "user_code": "1234-5678",
            "verification_uri": "https://github.com/login/device",
            "expires_in": 3600,
            "interval": 0,
        },
    )

    responses.post(
        GITHUB_ACCESS_TOKEN_URL,
        json={
            "error": "access_denied",
            "error_description": "Access was denied",
        },
    )

    with OAuthDeviceClient(
        GITHUB_CLIENT_ID,
        GITHUB_DEVICE_CODE_URL,
        GITHUB_ACCESS_TOKEN_URL,
    ) as client:
        msg = "Error while fetching access token: Access was denied (access_denied)"
        with pytest.raises(Exception, match=re.escape(msg)):
            client.authenticate("read:org")


@responses.activate(assert_all_requests_are_fired=True)
def test_can_add_packit_location(tmp_path):
    responses.get(
        "https://example.com/packit/api/outpack/metadata/list",
        json={"status": "success", "data": []},
        match=[matchers.header_matcher({"Authorization": "Bearer mytoken"})],
    )

    root = create_temporary_root(tmp_path)
    outpack_location_add(
        "upstream",
        "packit",
        {"url": "https://example.com", "token": "mytoken"},
        root,
    )
    outpack_location_pull_metadata("upstream", root=root)
