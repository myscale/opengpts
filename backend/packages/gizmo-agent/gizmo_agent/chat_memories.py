import os
import time
import json
import hashlib
from typing import Any, Dict
from langchain.schema import BaseMessage

from functools import partial
from langchain.memory import RedisChatMessageHistory, SQLChatMessageHistory
from langchain.memory.chat_message_histories.sql import DefaultMessageConverter, BaseMessageConverter
from sqlalchemy import Column, Text
try:
    from sqlalchemy.orm import declarative_base
except ImportError:
    from sqlalchemy.ext.declarative import declarative_base
from clickhouse_sqlalchemy import (
    types, engines
)
from langchain.schema.messages import messages_from_dict

def create_message_model(table_name, DynamicBase):  # type: ignore
    """
    Create a message model for a given table name.
    Args:
        table_name: The name of the table to use.
        DynamicBase: The base class to use for the model.
    Returns:
        The model class.
    """

    # Model decleared inside a function to have a dynamic table name
    class Message(DynamicBase):
        __tablename__ = table_name
        id = Column(types.Float64)
        session_id = Column(Text)
        msg_id = Column(Text, primary_key=True)
        type = Column(Text)
        addtionals = Column(Text)
        message = Column(Text)
        __table_args__ = (
            engines.ReplacingMergeTree(
                partition_by='session_id',
                order_by=('id', 'msg_id')),
            {'comment': 'Store Chat History'}
        )

    return Message


class DefaultClickhouseMessageConverter(DefaultMessageConverter):
    """The default message converter for SQLChatMessageHistory."""

    def __init__(self, table_name: str):
        self.model_class = create_message_model(table_name, declarative_base())

    def to_sql_model(self, message: BaseMessage, session_id: str) -> Any:
        tstamp = time.time()
        msg_id = hashlib.sha256(f"{session_id}_{message}_{tstamp}".encode('utf-8')).hexdigest()
        return self.model_class(
            id=tstamp, 
            msg_id=msg_id,
            session_id=session_id, 
            type=message.type,
            addtionals=json.dumps(message.additional_kwargs),
            message=json.dumps({
                "type": message.type, 
                "additional_kwargs": {"timestamp": tstamp},
                "data": message.dict()})
        )
    
    def from_sql_model(self, sql_message: Any) -> BaseMessage:
        msg_dump = json.loads(sql_message.message)
        msg = messages_from_dict([msg_dump])[0]
        msg.additional_kwargs = msg_dump["additional_kwargs"]
        return msg
    
    def get_sql_model_class(self) -> Any:
        return self.model_class
    
def build_myscale_chat_history(session_id: str) -> BaseMessageConverter:
    db = os.environ.get("MYSCALE_DATABASE", "opengpts")
    url = os.environ.get("MYSCALE_URL")
    port = os.environ.get("MYSCALE_PORT", "443")
    username = os.environ.get("MYSCALE_USER")
    password = os.environ.get("MYSCALE_PASS")
    conn_str = f'clickhouse://{username}:{password}@{url}:{port}'
    return SQLChatMessageHistory(
        session_id,
        connection_string=f'{conn_str}/{db}?protocol={"https" if port == "443" else "http"}',
        custom_message_converter=DefaultClickhouseMessageConverter("messages"))

ChatMemoryImpls: Dict[str, BaseMessageConverter] = {
    "redis": lambda: partial(RedisChatMessageHistory, url=os.environ["REDIS_URL"]),
    "myscale": lambda: build_myscale_chat_history,
}