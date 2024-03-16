# Venty
Event Driven Tooling built around [CloudEvents](https://cloudevents.io/) 
 
 
 ## Features
  * [Event Producer](venty/event_producer.py) for easy cloudevent creation.
  * [Event Channel Interface](venty/event_channel.py)
    * [HTTP](venty/http_event_channel.py)
    * [In Memory](venty/in_memory_event_channel.py) 
    * Queues (Planned)
    * Topics (Planned)
 * [Simple Event Store Interface](venty/event_store.py)
   * [In Memory Event Store Implementation](venty/in_memory_event_store.py)
   * [Simple SQL Event Store Implementation](venty/sql_event_store.py) 
   * DynamoDB Event Store Implementation (Planned)
 * [Aggregate Store Implementation](venty/aggregate_store.py)
    * Based on the event store interface.
 * [Strong Types](venty/strong_types.py) for event driven development.
 * Log Formatter as CloudEvents (Planned)
 * Correlation-ID and Causation-ID augmentation (Planned) 
 * Claim Check (Planned)
 
 
 ## Configuration
 You can configure the venty package via [the package settings](venty/settings.md)