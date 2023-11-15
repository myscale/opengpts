import os
import orjson
from typing import List
from datetime import datetime
from clickhouse_connect import get_client
from clickhouse_connect.driver import Client
from langchain.schema.messages import messages_from_dict

from .base import BaseStorage



class MyScaleStorage(BaseStorage):
    """Redis storage for backend"""

    def __init__(self):
        super().__init__()
        self._db = os.environ.get("MYSCALE_DATABASE", "opengpts")
        client = self._get_myscale_client()
        client.command(f"CREATE DATABASE IF NOT EXISTS {self._db}")
        client.command(f"""CREATE TABLE IF NOT EXISTS {self._db}.threads(
                        thread_id String,
                        assistant_id String,  -- foreign key
                        user_id String,
                        name String,
                        updated_at DateTime
                        ) ORDER BY (assistant_id, thread_id)""")
        client.command(f"""CREATE TABLE IF NOT EXISTS {self._db}.assistants(
                        assistant_id String,
                        user_id String,
                        name String,
                        config JSON,
                        updated_at DateTime,
                        public Boolean,
                        ) ORDER BY (assistant_id, user_id)""")

    def _get_myscale_client(self) -> Client:
        """Get a MyScale client."""
        url = os.environ.get("MYSCALE_URL")
        port = os.environ.get("MYSCALE_PORT", "443")
        username = os.environ.get("MYSCALE_USER")
        password = os.environ.get("MYSCALE_PASS")
        if not (url and port and username and password):
            raise ValueError("MyScale connection details are not set! "
                             "Please check url, port, user and password to your cluster.")
        return get_client(
            host=url,
            port=int(port),
            username=username,
            password=password,
        )
    
    def thread_messages_key(self, user_id: str, thread_id: str) -> str:
        # Needs to match key used by RedisChatMessageHistory
        # TODO we probably want to align this with the others
        return f"message_store:{user_id}:{thread_id}"

    def list_assistants(self, user_id: str) -> List[dict]:
        client = self._get_myscale_client()
        query = f"SELECT * FROM {self._db}.assistants WHERE user_id = '{user_id}'"
        return [r for r in client.query(query).named_results()]

    def list_public_assistants(self, assistant_ids: List[str]) -> List[dict]:
        client = self._get_myscale_client()
        ids_str = ",".join([f"'{a_id}'" for a_id in assistant_ids])
        query = (f"SELECT * FROM {self._db}.assistants WHERE public=true "
                 f"AND assistant_id IN [{ids_str}]")
        return [r for r in client.query(query).named_results()]
    
    def put_assistant(
        self,
        user_id: str,
        assistant_id: str,
        *,
        name: str,
        config: dict,
        public: bool = False
    ):
        saved = {
            "user_id": user_id,
            "assistant_id": assistant_id,
            "name": name,
            "config": config,
            "updated_at": datetime.utcnow(),
            "public": public,
        }
        client = self._get_myscale_client()
        keys, values = list(zip(*saved.items()))
        client.insert(table="assistants", database=self._db, 
                      data=[values], column_names=keys)
        return saved

    def list_threads(self, user_id: str):
        client = self._get_myscale_client()
        query = f"SELECT * FROM {self._db}.threads WHERE user_id = '{user_id}'"
        return [r for r in client.query(query).named_results()]

    def get_thread_messages(self, user_id: str, thread_id: str):
        client = self._get_myscale_client()
        query = (f"SELECT * FROM {self._db}.messages "
                 f"WHERE session_id = '{self.thread_messages_key(user_id, thread_id)}'")
        return [r for r in client.query(query).named_results()]

    def put_thread(self, user_id: str, thread_id: str, *, assistant_id: str, name: str):
        saved = {
            "user_id": user_id,
            "thread_id": thread_id,
            "assistant_id": assistant_id,
            "name": name,
            "updated_at": datetime.utcnow(),
        }
        client = self._get_myscale_client()
        keys, values = list(zip(*saved.items()))
        client.insert(table="threads", database=self._db, 
                      data=[values], column_names=keys)
        return saved
