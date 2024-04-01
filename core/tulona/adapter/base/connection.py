from dataclasses import dataclass
from typing import Dict


@dataclass
class BaseConnectionManager:
    conn_profile: Dict
