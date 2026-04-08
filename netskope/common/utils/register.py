"""Register."""

import socket
from netskope.common.utils import DBConnector

connector = DBConnector()
connector.collection("machines").update_one(
    {"hostname": socket.gethostname()},
    {
        "$set": {
            "hostname": socket.gethostname(),
            "alive": True,
            "update": False,
        }
    },
    upsert=True,
)
