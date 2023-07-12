from pydantic import SecretStr
from ..api import DBConfig

class AstraDBConfig(DBConfig):
    client_id: SecretStr
    client_secret: SecretStr
    cloud_config: str
    keyspace: str = "vectordb"

    def to_dict(self) -> dict:
        return {
            "client_id": self.client_id.get_secret_value(),
            "client_secret": self.client_secret.get_secret_value(),
            "cloud_config": self.cloud_config,
            "keyspace": self.keyspace
        }
