import logging
from typing import Iterator, Optional

from goose_kafka.abstractions import (
    ConsumerType,
    Deserialiser,
    EventProcessor,
    MessageType,
)

log = logging.getLogger(__name__)


def iter_messages(
    consumer: ConsumerType,
    stop_after_n: Optional[int] = None,
    poll_timeout: float = 0.5,
) -> Iterator[MessageType]:
    """Yield `MessageType` instances from the topic consumer.

    Args:
        consumer: The consumer instance listening to the relevant topic. The consumer
            will be `close`d once polling exits.
        stop_after_n: If provided the consumer will terminate after processing the
            specified number of messages. This is generally required for unit-testing
            only.
        poll_timeout: The number of seconds to wait for a response from the message
            broker.

    Yields:
        `MessageType` instances.

    """
    n_messages_seen = 0
    while True:
        try:
            # Check exit condition
            if stop_after_n and n_messages_seen >= stop_after_n:
                log.debug(
                    f"Message processing limit hit ({stop_after_n}) messsages),"
                    " shutting down..."
                )
                break

            # Attempt to get a message
            if not (raw_message := consumer.poll(poll_timeout)):
                continue

            # At this point we have a message

            if stop_after_n is not None:
                n_messages_seen += 1

            if error := raw_message.error():
                log.warning(
                    "The Kafka message contains an error, moving on to the next "
                    f"message - error details: '{error}'"
                )
                continue

            yield raw_message
            log.debug("Processed message")

        except KeyboardInterrupt:
            log.debug("Kill signal received, shutting down...")
            break
        except Exception:
            log.exception(
                "Unexpected exception in Kafka consumer poll loop, moving on to the "
                "next message"
            )
            continue

    # Leave group and commit final offsets
    consumer.close()


def run(
    consumer: ConsumerType,
    deserialiser: Deserialiser,
    event_processor: EventProcessor,
    stop_after_n: Optional[int] = None,
) -> None:
    """Run the consumer poll loop.

    Each message received by the consumer is deserialised and then processed using
    the `event_processor` callable.

    """
    for raw_message in iter_messages(
        consumer=consumer,
        stop_after_n=stop_after_n,
    ):
        try:
            message = deserialiser(raw_message)
        except Exception:
            log.exception(
                "Error in deserialiser function, moving onto the next message"
            )
        else:
            try:
                event_processor(message)
            except Exception:
                log.exception(
                    "Error in event processor function, moving onto the next message"
                )
