# IoT Hub Telemetry Processor Service

Stream processing service for telemetry validation, deduplication, and routing across Kafka topics.

## Purpose

The Telemetry Processor Service transforms raw telemetry messages into validated internal platform events.

## Responsibilities

- consume raw telemetry from Kafka
- validate message structure and business constraints
- verify device and metric bindings
- perform deduplication
- route messages to downstream topics
- emit invalid or out-of-window telemetry to dedicated flows

## Owned data

- technical processing state
- deduplication keys
- local configuration projections or cache, if required

## Integrations

### Inbound
- Kafka raw telemetry topic
- registry-derived configuration
- Redis for deduplication window support

### Outbound
- validated telemetry topic
- invalid telemetry topic
- expired / out-of-window telemetry topic

## Technology

- Python
- Kafka
- Redis
- Docker
