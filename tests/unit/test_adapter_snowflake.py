from unittest.mock import MagicMock, patch

import pytest

from tulona.adapter.snowflake import SnowflakeConnection


@pytest.mark.parametrize("passphrase", [123456, "123456"])
def test_private_key_passphrase_converted_to_str(tmp_path, passphrase):
    key_file = tmp_path / "key.p8"
    key_file.write_bytes(b"dummy")
    credentials = {
        "account": "acc",
        "warehouse": "wh",
        "database": "db",
        "schema": "sc",
        "user": "usr",
        "private_key": str(key_file),
        "private_key_passphrase": passphrase,
    }

    SnowflakeConnection._instance = None
    try:
        with patch(
            "tulona.adapter.snowflake.serialization.load_pem_private_key",
            return_value=MagicMock(),
        ) as load_key, patch("tulona.adapter.snowflake.create_engine"):
            SnowflakeConnection(credentials)
        assert load_key.call_args.kwargs["password"] == b"123456"
    finally:
        SnowflakeConnection._instance = None
