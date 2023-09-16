from dataclasses import dataclass
from typing import Union


@dataclass
class BaseConnectionManager:

    host: str
    username: str
    port: Union[int, str]
    password: str  # mandatory
    database: str
    dbtype: str