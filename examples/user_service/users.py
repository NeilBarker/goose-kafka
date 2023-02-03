"""
This example shows a simple microservice that consumer user registration events for
an imaginary service and prints them to stdout.

For this example, all Kafka messages are encoded using JSON.

"""

import json
from dataclasses import dataclass
from datetime import date

from goose_kafka.abstractions import Event, KafkaConsumer, MessageType
from goose_kafka.consumer import run


# Define a model for the messages we expect to receive
@dataclass
class UserRegistered(Event):
    username: str
    date_of_birth: date
    place_of_birth: str


def deserialiser(message: MessageType) -> UserRegistered:
    return UserRegistered(**json.loads(message.value()))  # type: ignore


def task(event: UserRegistered) -> None:
    print(event)


if __name__ == "__main__":
    config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "user_group_1",
        "auto.offset.reset": "earliest",
    }
    kafka_consumer = KafkaConsumer.from_config(config, topic="user_registration_events")
    run(kafka_consumer, deserialiser, task)
