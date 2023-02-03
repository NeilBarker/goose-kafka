import json
from unittest.mock import Mock, call

from goose_kafka.abstractions import FakeKafkaConsumer, KafkaConsumer
from goose_kafka.consumer import run
from goose_kafka.testing_fixtures import KafkaProducer

from ..users import UserRegistered, deserialiser  # type: ignore


def make_n_messages(n: int) -> list[dict[str, str]]:
    usernames = ["username"] * n
    dobs = ["01/01/2000"] * n
    places = ["Earth"] * n
    return [
        dict(
            username=usernames[idx], date_of_birth=dobs[idx], place_of_birth=places[idx]
        )
        for idx in range(n)
    ]


def test_task_is_called_once_for_each_message(  # type: ignore
    message_factory,
    mock_task,
) -> None:
    message_bodies = make_n_messages(10)
    messages = [message_factory(value=json.dumps(body)) for body in message_bodies]
    consumer = FakeKafkaConsumer(messages=messages)

    run(
        consumer,
        deserialiser=deserialiser,
        event_processor=mock_task,
        stop_after_n=10,
    )

    expected_calls = [
        call(UserRegistered(**body)) for body in message_bodies  # type: ignore
    ]
    mock_task.assert_has_calls(expected_calls)


class TestIntegration:
    """Tests require a running broker."""

    bootstrap_servers = "localhost:9092"
    group_id = "python_group_1"

    @property
    def consumer_config(self) -> dict[str, str]:
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": "earliest",
        }

    def test_task_is_called_once_for_each_message(
        self,
        mock_task: Mock,
        kafka_producer: KafkaProducer,
    ) -> None:
        kafka_consumer = KafkaConsumer.from_config(
            self.consumer_config, topic=kafka_producer.topic
        )

        body = {
            "username": "test",
            "date_of_birth": "01/01/2000",
            "place_of_birth": "Earth",
        }
        kafka_producer.produce(key="12345", value=json.dumps(body))

        run(
            kafka_consumer,
            deserialiser=deserialiser,
            event_processor=mock_task,
            stop_after_n=1,
        )
        mock_task.assert_called_once_with(UserRegistered(**body))  # type: ignore
