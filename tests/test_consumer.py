import json
import os
import uuid
from dataclasses import dataclass
from typing import Callable, Optional, Union
from unittest.mock import MagicMock, Mock, call, patch

import pytest  # type: ignore
from confluent_kafka import KafkaError  # type: ignore
from mypy_extensions import DefaultNamedArg

from goose_kafka.abstractions import (
    Event,
    FakeKafkaConsumer,
    FakeKafkaMessage,
    KafkaConsumer,
    MessageType,
)
from goose_kafka.consumer import iter_messages, run
from goose_kafka.testing_fixtures import KafkaProducer


@dataclass
class FakeEvent(Event):
    order_id: uuid.UUID
    order_url: str


def event_deserialiser(raw_message: MessageType) -> FakeEvent:
    """Basic deserialiser for tests only that loads JSON into 'FakeEvent's."""
    return FakeEvent(**json.loads(raw_message.value()))  # type: ignore


def make_n_messages(n: int) -> list[dict[str, Union[uuid.UUID, str]]]:
    ids = [str(uuid.uuid4()) for _ in range(n)]
    urls = ["httos: //example.com"] * n
    return [dict(order_id=ids[idx], order_url=urls[idx]) for idx in range(n)]


class TestIterMessages:
    def test_valid_message_is_yielded(self, valid_message: FakeKafkaMessage) -> None:
        consumer = FakeKafkaConsumer(messages=[valid_message])
        deserialised = next(iter_messages(consumer))  # type: ignore
        assert isinstance(deserialised, FakeKafkaMessage)

    @patch("goose_kafka.consumer.log")
    def test_message_with_error_is_handled(
        self, mock_log: MagicMock, errant_message: FakeKafkaMessage
    ) -> None:
        consumer = FakeKafkaConsumer(messages=[errant_message])
        with pytest.raises(StopIteration):
            next(iter_messages(consumer, stop_after_n=1))

        mock_log.warning.assert_called_once()

    @patch("goose_kafka.consumer.log")
    @patch.object(FakeKafkaConsumer, "poll")
    def test_unexpected_exception_is_handled(
        self, mock_poll: MagicMock, mock_log: MagicMock, valid_message: FakeKafkaMessage
    ) -> None:
        """Test that an unexpected exception somwhere in the poll loop does not stop
        further message consumption.

        In this test, the poll loop will yield a result on the second run through of the
        loop. The first run through will generate an exception and log message.

        """
        mock_poll.side_effect = [
            RuntimeError("Something bad happened..."),
            valid_message,
        ]
        consumer = FakeKafkaConsumer(messages=[])

        deserialised = next(iter_messages(consumer, stop_after_n=1))
        mock_log.exception.assert_called_once()
        assert isinstance(deserialised, FakeKafkaMessage)

    @patch.object(FakeKafkaConsumer, "poll")
    @patch.object(FakeKafkaConsumer, "close")
    def test_keyboard_interrupt_breaks_the_loop(
        self, mock_close: MagicMock, mock_poll: MagicMock
    ) -> None:
        mock_poll.side_effect = KeyboardInterrupt()
        consumer = FakeKafkaConsumer(messages=[])

        try:
            next(iter_messages(consumer, stop_after_n=1))
        except StopIteration:
            pass
        mock_close.assert_called_once()


class TestRun:
    def test_task_is_called_once_for_each_message(
        self,
        message_factory: Callable[
            [
                DefaultNamedArg(Union[str, bytes, None], "value"),  # noqa: F821
                DefaultNamedArg(Optional[KafkaError], "error"),  # noqa:F821
            ],
            FakeKafkaMessage,
        ],
        mock_task: Mock,
    ) -> None:
        # Create 10 messages and a fake consumer
        message_bodies = make_n_messages(10)
        messages = [message_factory(value=json.dumps(body)) for body in message_bodies]
        consumer = FakeKafkaConsumer(messages=messages)

        run(
            consumer,
            deserialiser=event_deserialiser,
            event_processor=mock_task,
            stop_after_n=10,
        )

        # The task should be called with the deserialised messages
        expected_calls = [
            call(FakeEvent(**body)) for body in message_bodies  # type: ignore
        ]
        mock_task.assert_has_calls(expected_calls)

    @patch("goose_kafka.consumer.log")
    def test_deserialiser_exception_does_not_break_loop(
        self,
        mock_log: MagicMock,
        message_factory: Callable[
            [
                DefaultNamedArg(Union[str, bytes, None], "value"),  # noqa: F821
                DefaultNamedArg(Optional[KafkaError], "error"),  # noqa:F821
            ],
            FakeKafkaMessage,
        ],
        mock_task: Mock,
    ) -> None:
        """Tests that an error deserialising a message does not stop processing of
        subsequent valid messages.

        In this test, the first message cannot be deserialised, but the second message
        is valid.

        """
        invalid_message = message_factory(value=json.dumps(dict(unknown_format=42)))
        valid_message = message_factory(
            value=json.dumps(dict(order_id=1, order_url="https://example.com"))
        )
        consumer = FakeKafkaConsumer(messages=[invalid_message, valid_message])

        run(
            consumer,
            deserialiser=event_deserialiser,
            event_processor=mock_task,
            stop_after_n=2,
        )

        mock_log.exception.assert_called_once()
        mock_task.assert_called_once_with(
            FakeEvent(**json.loads(valid_message.value()))  # type: ignore
        )

    @patch("goose_kafka.consumer.log")
    def test_task_exception_does_not_break_the_loop(
        self,
        mock_log: MagicMock,
        message_factory: Callable[
            [
                DefaultNamedArg(Union[str, bytes, None], "value"),  # noqa: F821
                DefaultNamedArg(Optional[KafkaError], "error"),  # noqa:F821
            ],
            FakeKafkaMessage,
        ],
        mock_task: Mock,
    ) -> None:
        """Tests that an error running the event processor funciton does not stop
        processing of subsequent valid messages.

        In this test, the first message cannot be processed but the second message is
        valid.

        """
        mock_task.side_effect = [RuntimeError("Something bad happened..."), None]

        message_bodies = make_n_messages(2)
        consumer = FakeKafkaConsumer(
            messages=[
                message_factory(value=json.dumps(body)) for body in message_bodies
            ]
        )

        run(
            consumer,
            deserialiser=event_deserialiser,
            event_processor=mock_task,
            stop_after_n=2,
        )

        mock_log.exception.assert_called_once()
        assert mock_task.call_count == 2


class TestIntegration:
    """These tests require a running broker."""

    bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
    group_id = "oval-kafka-group-1"

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
        valid_message_body: dict[str, str],
    ) -> None:
        kafka_consumer = KafkaConsumer.from_config(
            self.consumer_config, topic=kafka_producer.topic
        )

        # Publish a message
        kafka_producer.produce(key="12345", value=json.dumps(valid_message_body))

        # Listen for the message
        run(
            kafka_consumer,
            deserialiser=event_deserialiser,
            event_processor=mock_task,
            stop_after_n=1,
        )

        mock_task.assert_called_once_with(
            FakeEvent(**valid_message_body)  # type: ignore
        )

    def test_successive_tests(
        self,
        mock_task: Mock,
        kafka_producer: KafkaProducer,
        valid_message_body: dict[str, str],
    ) -> None:
        """Runs the same test again to prove that the kafka producer fixture is able
        to delete and create topics between test functions.

        """
        kafka_consumer = KafkaConsumer.from_config(
            self.consumer_config, topic=kafka_producer.topic
        )

        # Publish a message
        kafka_producer.produce(key="12345", value=json.dumps(valid_message_body))

        # Listen for the message
        run(
            kafka_consumer,
            deserialiser=event_deserialiser,
            event_processor=mock_task,
            stop_after_n=1,
        )

        mock_task.assert_called_once_with(
            FakeEvent(**valid_message_body)  # type: ignore
        )
