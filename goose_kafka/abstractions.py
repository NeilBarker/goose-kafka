"""
This module defines a series of protocols that abstract the Confluent Kafka "Consumer
and 'Message' classes. This allows for substitutes to be used in unit-tests and for
other kafka Python packages to be used if necessary - just implement a wrapper that
complies with the protocols.

"""

from typing import Optional, Protocol, TypeVar, Union

from confluent_kafka import Consumer, KafkaError, Message  # type: ignore


class Event:
    """Base class for models of Kafka events (messages)."""


class MessageType(Protocol):
    """Protocol for a Kafka message class."""

    def error(self) -> Optional[KafkaError]:
        ...

    def value(self) -> Optional[Union[str, bytes]]:
        ...


class KafkaMessage(MessageType):
    """Wrapper around the confluent-kafka `Message` class."""

    def __init__(self, message: Message) -> None:
        self.message = message
        super().__init__()

    def error(self) -> Optional[KafkaError]:
        return self.message.error()

    def value(self) -> Optional[Union[str, bytes]]:
        return self.message.value()


class FakeKafkaMessage(MessageType):
    """A fake message type for unit-tests."""

    def __init__(
        self,
        value: Optional[Union[str, bytes]] = None,
        error: Optional[KafkaError] = None,
    ):
        self._value = value
        self._error = error
        super().__init__()

    def error(self) -> Optional[KafkaError]:
        return self._error

    def value(self) -> Optional[Union[str, bytes]]:
        return self._value


class ConsumerType(Protocol):
    """Protocol for a Kafka consumer class."""

    def poll(self, timout: Optional[float] = None) -> Optional[MessageType]:
        ...

    def close(self) -> None:
        ...


class KafkaConsumer(ConsumerType):
    """Wrapper around the confluent-kafka `Consumer` class."""

    def __init__(self, consumer: Consumer) -> None:
        self.consumer = consumer
        super().__init__()

    @classmethod
    def from_config(cls, config: dict[str, str], topic: str) -> "KafkaConsumer":
        consumer = Consumer(config)
        consumer.subscribe([topic])
        return cls(consumer)

    def poll(self, timeout: Optional[float] = None) -> Optional[KafkaMessage]:
        if raw_message := self.consumer.poll(timeout):
            return KafkaMessage(raw_message)
        return None

    def close(self) -> None:
        self.consumer.close()


class FakeKafkaConsumer(ConsumerType):
    """A fake consumer for unit-tests to decouple tests from having a running broker.

    To simulate a broker, any messages in the `messages` list are returned from calling
    `poll` in the same order as they are added to the list.

    Attributes:
        messsages: A list of messages to return one by one from the `poll` method.
            Messages are popped and returned from index 0.

    """

    def __init__(self, messages: list[FakeKafkaMessage]) -> None:
        self.messages = messages
        super().__init__()

    def poll(self, timout: Optional[float] = None) -> Optional[FakeKafkaMessage]:
        try:
            message = self.messages.pop(0)
        except IndexError:
            return None
        else:
            return message

    def close(self) -> None:
        return None


_MessageType = TypeVar("_MessageType", bound=MessageType, contravariant=True)


class Deserialiser(Protocol[_MessageType]):
    def __call__(self, __raw_message: _MessageType) -> Event:
        ...


EventType = TypeVar("EventType", bound=Event, contravariant=True)


class EventProcessor(Protocol[EventType]):
    def __call__(self, __event: EventType) -> None:
        ...
