import json

from src.serializer.base import Serializer


class JSONSerializer(Serializer):
    def __init__(self):
        pass

    @staticmethod
    def serialize(message: dict) -> bytes:
        try:
            json_message = json.dumps(message)
            bytes_message = json_message.encode("utf8")
            return bytes_message
        except Exception as exc:
            print(exc)

    @staticmethod
    def deserialize(message: bytes) -> dict:
        try:
            message_json = message.decode('utf8')
            json_message = json.loads(message_json)
            return json_message
        except Exception as exc:
            print(exc)
