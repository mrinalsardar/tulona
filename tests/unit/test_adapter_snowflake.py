from unittest import mock

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from tulona.adapter import snowflake
from tulona.adapter.snowflake import SnowflakeConnection

BASE = {
    "account": "acc",
    "warehouse": "wh",
    "database": "db",
    "schema": "sch",
    "user": "usr",
}


@pytest.fixture(autouse=True)
def fresh_singleton(monkeypatch):
    monkeypatch.setattr(SnowflakeConnection, "_instance", None)


@pytest.fixture
def mocks(monkeypatch):
    create_engine = mock.Mock(side_effect=lambda *a, **kw: mock.sentinel.engine)
    url = mock.Mock(return_value="url")
    monkeypatch.setattr(snowflake, "create_engine", create_engine)
    monkeypatch.setattr(snowflake, "URL", url)
    return create_engine, url


def write_key(path, passphrase=None):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    encryption = (
        serialization.BestAvailableEncryption(passphrase)
        if passphrase
        else serialization.NoEncryption()
    )
    path.write_bytes(
        key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=encryption,
        )
    )
    return str(path)


def test_password_auth(mocks):
    create_engine, url = mocks
    creds = {**BASE, "password": "pw", "role": "r", "client_session_keep_alive": True}
    conn = SnowflakeConnection(creds)
    assert conn.get_engine() is mock.sentinel.engine
    assert url.call_args.kwargs["password"] == "pw"
    assert url.call_args.kwargs["role"] == "r"
    assert create_engine.call_args.kwargs["connect_args"] == {
        "CLIENT_SESSION_KEEP_ALIVE": True
    }


def test_password_auth_defaults(mocks):
    create_engine, url = mocks
    SnowflakeConnection({**BASE, "password": "pw"})
    assert url.call_args.kwargs["role"] is None
    assert create_engine.call_args.kwargs["connect_args"] == {
        "CLIENT_SESSION_KEEP_ALIVE": False
    }


def test_singleton(mocks):
    create_engine, _ = mocks
    first = SnowflakeConnection({**BASE, "password": "pw"})
    second = SnowflakeConnection({**BASE, "password": "other"})
    assert first is second
    assert create_engine.call_count == 1


def test_private_key_without_passphrase(mocks, tmp_path):
    create_engine, url = mocks
    key_path = write_key(tmp_path / "key.p8")
    conn = SnowflakeConnection({**BASE, "private_key": key_path})
    assert conn.get_engine() is mock.sentinel.engine
    connect_args = create_engine.call_args.kwargs["connect_args"]
    assert isinstance(connect_args["private_key"], bytes)
    assert connect_args["CLIENT_SESSION_KEEP_ALIVE"] is False
    assert "password" not in url.call_args.kwargs


def test_private_key_with_passphrase(mocks, tmp_path):
    create_engine, _ = mocks
    key_path = write_key(tmp_path / "key.p8", passphrase=b"secret")
    SnowflakeConnection(
        {
            **BASE,
            "private_key": key_path,
            "private_key_passphrase": "secret",
            "client_session_keep_alive": True,
        }
    )
    connect_args = create_engine.call_args.kwargs["connect_args"]
    assert isinstance(connect_args["private_key"], bytes)
    assert connect_args["CLIENT_SESSION_KEEP_ALIVE"] is True


def test_private_key_invalid_extension(mocks):
    with pytest.raises(ValueError, match="is not a valid private key"):
        SnowflakeConnection({**BASE, "private_key": "key.pem"})


def test_externalbrowser_auth(mocks):
    create_engine, url = mocks
    conn = SnowflakeConnection({**BASE, "authenticator": "externalbrowser", "role": "r"})
    assert conn.get_engine() is mock.sentinel.engine
    assert url.call_args.kwargs["authenticator"] == "externalbrowser"
    assert url.call_args.kwargs["role"] == "r"


def test_externalbrowser_auth_defaults(mocks):
    _, url = mocks
    SnowflakeConnection({**BASE, "authenticator": "externalbrowser"})
    assert url.call_args.kwargs["role"] is None


def test_other_authenticator_creates_no_engine(mocks):
    create_engine, _ = mocks
    conn = SnowflakeConnection({**BASE, "authenticator": "oauth"})
    create_engine.assert_not_called()
    assert not hasattr(conn, "engine")
