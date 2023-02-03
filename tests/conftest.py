import json
import uuid

import pytest  # type: ignore
from confluent_kafka import KafkaError  # type: ignore

from goose_kafka.abstractions import FakeKafkaMessage
from goose_kafka.testing_fixtures import (  # noqa
    kafka_producer,
    message_factory,
    mock_task,
)


@pytest.fixture
def valid_message_body() -> dict[str, str]:
    return dict(order_id=str(uuid.uuid4()), order_url="https://example.com")


@pytest.fixture
def valid_message(  # type: ignore
    valid_message_body, message_factory  # noqa
) -> FakeKafkaMessage:
    return message_factory(value=json.dumps(valid_message_body))


@pytest.fixture
def errant_message(message_factory) -> FakeKafkaMessage:  # type: ignore  # noqa
    error = KafkaError(error=1, reason="Something went wrong...")
    return message_factory(error=error)
