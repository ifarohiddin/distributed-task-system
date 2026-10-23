import json
import zlib
from types import SimpleNamespace
from unittest.mock import MagicMock


def decode(body: bytes) -> dict:
    return json.loads(zlib.decompress(body).decode())


def encode(task: dict) -> bytes:
    return zlib.compress(json.dumps(task).encode())


def fake_connection():
    """Haqiqiy RabbitMQ'siz ishlaydigan soxta ulanish va kanal."""
    connection = MagicMock(name="connection")
    connection.is_closed = False
    channel = MagicMock(name="channel")
    connection.channel.return_value = channel
    return connection, channel


def delivery(tag=1):
    return SimpleNamespace(delivery_tag=tag)
