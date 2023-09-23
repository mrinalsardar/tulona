import pytest
from tulona.config.project import Project


class TestProject:
    @pytest.fixture(scope='class')
    def project(self):
        p = Project()
        p.project_config_raw = {
            "version": "2.0",
            "name": "test_project",
            "config-version": 2,
            "connection_profiles": {
                "postgres1": {
                    "type": "postgres",
                    "host": "db_pg",
                    "port": 5432,
                    "username": "postgres",
                    "password": "postgres"
                },
                "mysql1": {
                    "type": "mysql",
                    "host": "db_mysql",
                    "port": 3306,
                    "username": "user",
                    "password": "password"
                }
            },
            "databases": [{
                "postgres1": "postgres",
                "mysql1": "db"
            }]
        }

        return p


    def test_get_eligible_connection_list(self, project):
        expected = ['postgres1', 'mysql1']
        actual = project.get_eligible_connection_list(
            connection_profiles=project.project_config_raw['connection_profiles'],
            databases=project.project_config_raw['databases']
        )

        assert sorted(actual) == sorted(expected)

    def test_get_eligible_connection_profiles(self, project):
        expected = [
            [
                {
                    "database": "postgres",
                    "type": "postgres",
                    "host": "db_pg",
                    "port": 5432,
                    "username": "postgres",
                    "password": "postgres"
                },
                {
                    "database": "db",
                    "type": "mysql",
                    "host": "db_mysql",
                    "port": 3306,
                    "username": "user",
                    "password": "password"
                }
            ]
        ]

        actual = project.get_eligible_connection_profiles()

        assert sorted(actual) == sorted(expected)
