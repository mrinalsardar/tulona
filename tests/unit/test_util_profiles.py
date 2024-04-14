import pytest

from tulona.util.profiles import extract_profile_name, get_connection_profile


@pytest.mark.parametrize(
    "project,datasource,expected",
    [
        ({"datasources": {"ds1": {"connection_profile": "pgdb"}}}, "ds1", "pgdb"),
    ],
)
def test_extract_profile_name(project, datasource, expected):
    actual = extract_profile_name(project, datasource)
    assert actual == expected


@pytest.mark.parametrize(
    "profile,project,datasource,expected",
    [
        (
            {"profiles": {"pgdb": {"foo": "bar"}}},
            {"datasources": {"ds1": {"connection_profile": "pgdb"}}},
            "ds1",
            {"foo": "bar"},
        ),
    ],
)
def test_get_connection_profile(profile, config, expected):
    actual = get_connection_profile(profile, config)
    assert actual == expected
