# Venty
Event Driven Tooling built around [CloudEvents](https://cloudevents.io/) 
 
 
 ## Features
 * [Simple Event Store Interface](venty/event_store.py)
   * [In Memory Event Store Implementation](venty/in_memory_event_store.py)
   * [Simple SQL Event Store Implementation](venty/sql_event_store.py) 
   * DynamoDB Event Store Implementation (Planned)
 * [Aggregate Store Implementation](venty/aggregate_store.py)
    * Based on the event store interface.
 * [Strong Types](venty/strong_types.py) for event driven development.
 * Event Channel Interface (Planned)
    * Queues 
    * Topics
    * HTTP 
 * Log Formatter as CloudEvents (Planned)
 * Event Producers (Planned)
 * Correlation-ID and Causation-ID augmentation (Planned) 
 * Claim Check (Planned)
 