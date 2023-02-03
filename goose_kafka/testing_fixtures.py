import os
from typing import Callable, Optional, Union
from unittest.mock import Mock
from uuid import uuid4

import pytest  # type: ignore
from confluent_kafka import KafkaError, Producer  # type: ignore
from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore
from mypy_extensions import DefaultNamedArg

from goose_kafka.abstractions import FakeKafkaMessage

BOOTSTRAP_SERVERS = os.getenv("BOOSTRAP_SERVERS", "localhost:9092")


@pytest.fixture
def mock_task() -> Mock:
    return Mock()


def _message_factory(
    *, value: Optional[Union[str, bytes]] = None, error: Optional[KafkaError] = None
) -> FakeKafkaMessage:
    return FakeKafkaMessage(value=value, error=error)


@pytest.fixture
def message_factory() -> Callable[
    [
        DefaultNamedArg(Union[str, bytes, None], "value"),  # noqa: F821
        DefaultNamedArg(Optional[KafkaError], "error"),  # noqa:F821
    ],
    FakeKafkaMessage,
]:
    """Return a callable factory function that itself returns a `FakeKafkaMessage`.

    For a message with a `value` you should call the factory function with the
    serialised value. Example for a JSON encode value:

    ```python
    message = message_factory(value=json.dumps(body))
    ```

    For a message with an error you should call the factory function with the error:

    ```python
    errant_message = message_factory(
        error=KafkaError(error=1, reason="Something went wrong...")
    )
    ```

    """
    return _message_factory


class KafkaProducer:
    """Basic producer for publishing messages into a single topic.

    NOTE: This is suitable for testing only, not as a standalone producer.

    """

    def __init__(self, producer: Producer, topic: str) -> None:
        self.producer = producer
        self.topic = topic

    def produce(self, key, value) -> None:  # type: ignore
        self.producer.produce(self.topic, value=value, key=key)
        # Blocks until messages are sent
        self.producer.flush()


@pytest.fixture
def kafka_producer() -> KafkaProducer:  # type: ignore
    """Returns a test-isolated `KafkaProducer`.

    The topic name is randomly generated to ensure that tests running in parallel are
    isolated from one another. You can get the topic name in your test by accessing the
    `topic` attribute of the `KafkaProducer` instance.

    The topic and all messages are destroyed once the test function using this fixture
    finishes running.

    The producer expects to find a Kafka server running at `BOOTSTRAP_SERVERS`.

    """
    topic_name = str(uuid4())
    config = dict({"bootstrap.servers": BOOTSTRAP_SERVERS})
    admin = AdminClient(config)

    # Create a new topic
    topic = NewTopic(topic=topic_name, num_partitions=1, replication_factor=1)
    result = admin.create_topics([topic])
    for future in result.values():
        future.result()  # Wait for the topic to be created, raises exception on error

    producer = KafkaProducer(Producer(config), topic_name)
    yield producer

    # Delete the topic
    admin = AdminClient(config)
    result = admin.delete_topics([topic_name])
    for future in result.values():
        future.result()
