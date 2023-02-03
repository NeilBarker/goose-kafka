# goose-kafka

Helper library to simplify creating Kafka Consumer services

## Kafka Consumer

goose-kafka provides an abstraction over the `confluent-kafka` Kafka Consumer class that
allows for unit tests to be decoupled from having a running broker. An isolated producer
test fixture is provided for when testing with a running broker is necessary.

Using goose-kafka it is possible to get a consumer microservice up and running with just
a few lines of code - see the examples directory.

### Expected Message Format

The current version expects the message body to be in the `value` key fo the message.
The message `key` is currently not used.

## Development

### Installation

From the root of the project:

```
docker compose build
poetry install
```

### Running the tests

From the root of the project folder:

```
docker compose up -d
pytest tests -vv
```

To update the coverage file and html reports:

```
pytest tests --cov goose_kafka --cov-report html -vv
```
