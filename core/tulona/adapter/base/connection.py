from dataclasses import dataclass
from typing import Union


@dataclass
class BaseConnectionManager:
    dbtype: str
    host: str
    port: Union[int, str]
    username: str
    password: str
    database: str