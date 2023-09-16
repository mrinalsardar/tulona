import logging
from tulona.config.porject import load_project_config, get_eligible_connections

log = logging.getLogger(__name__)


class ConnectionTest:
    def get_connection_profiles(self):
        project_conf = load_project_config()
        connection_profiles = project_conf.connection_profiles
        databases = project_conf.databases

        eligible_connections = get_eligible_connections(
            connection_profiles=connection_profiles,
            databases=databases
        )

        self.eligible_profiles = []

        for cf in eligible_connections:
            profile = {cf: connection_profiles[cf]}

            for dbcomb in databases:
                if cf in dbcomb:
                    profile[cf]['dbname'] = dbcomb[cf]
                    break

            self.eligible_profiles.append(profile)


    def test_connection(self, profile_name, profile):
        connection_str = f"{get_db_driver(dbconf.type)}://{dbconf.username}:{dbconf.password}@{dbconf.host}:{dbconf.port}/{dbconf.dbname}"

    def test_all_eligible_connections(self):
        self.get_connection_profiles()

        for profile_name in self.eligible_profiles:
            try:
                self.test_connection(
                    profile_name=profile_name,
                    profile=self.eligible_profiles[profile_name]
                )
            except Exception as e:
                pass


