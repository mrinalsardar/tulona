from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from tulona.exceptions import (
    TulonaNotImplementedError
)


class BaseTask(metaclass=ABCMeta):
    @abstractmethod
    def execute(self):
        raise TulonaNotImplementedError(
            "This method needs to be implemented in child class"
        )