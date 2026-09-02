import os
from unittest.mock import patch

from flask import Flask

import app as aycf_app


def _request(remote_addr: str = "127.0.0.1"):
    flask_app = Flask(__name__)
    return flask_app.test_request_context("/", environ_base={"REMOTE_ADDR": remote_addr})


def test_aycf_trusts_direct_loopback_when_bound_to_loopback():
    with patch.dict(os.environ, {"AYCF_BIND_HOST": "127.0.0.1"}, clear=False), _request():
        assert aycf_app._trusted_local_request() is True


def test_aycf_does_not_trust_remote_client():
    with patch.dict(os.environ, {"AYCF_BIND_HOST": "127.0.0.1"}, clear=False), _request("192.0.2.10"):
        assert aycf_app._trusted_local_request() is False


def test_aycf_does_not_bypass_password_on_non_loopback_bind():
    with patch.dict(os.environ, {"AYCF_BIND_HOST": "0.0.0.0"}, clear=False), _request():
        assert aycf_app._trusted_local_request() is False


def test_local_password_can_be_forced_explicitly():
    with patch.dict(os.environ, {"AYCF_BIND_HOST": "127.0.0.1", "AYCF_REQUIRE_LOCAL_PASSWORD": "true"}, clear=False), _request():
        assert aycf_app._trusted_local_request() is False
