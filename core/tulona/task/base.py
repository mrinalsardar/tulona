from abc import ABCMeta, abstractmethod
from typing import Dict

from tulona.adapter.connection import ConnectionManager
from tulona.exceptions import TulonaNotImplementedError


class BaseTask(metaclass=ABCMeta):

    def get_connection_manager(self, conn_profile: Dict) -> ConnectionManager:
        conman = ConnectionManager(conn_profile)
        conman.get_engine()
        return conman

    @abstractmethod
    def execute(self):
        raise TulonaNotImplementedError(
            "This method needs to be implemented in child class"
        )
