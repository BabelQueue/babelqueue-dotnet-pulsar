# Changelog

All notable changes to `BabelQueue.Pulsar` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [1.0.0] - 2026-06-13

### Added
- Initial release. An Apache Pulsar transport on `BabelQueue.Core` + `DotPulsar`:
  `PulsarPublisher` (canonical-envelope send with the §5 property projection — `bq-job` =
  URN, `bq-trace-id` = `trace_id`, `bq-message-id` = `meta.id`, plus `bq-schema-version`/
  `bq-source-lang`/`bq-attempts`; native `DeliverAtTime` for delays) and `PulsarConsumer`
  (receive → URN-routed `BabelHandler`s → `Acknowledge`; a throwing handler redelivers for
  at-least-once; `attempts` reconciled to `max(bq-attempts, RedeliveryCount)` — Pulsar's
  redelivery count is 0-based so it maps directly with no −1, and the `max` keeps a
  republish-driven retry and a native redelivery in agreement; `OnError`/`OnUnknownUrn`
  hooks). .NET 8, xUnit, analyzers-as-errors, ≥90% line coverage; the DotPulsar
  `IProducer`/`IConsumer`/`IMessage` interfaces are mocked with Moq (no Pulsar, no network).
  The envelope is unchanged (`schema_version: 1`); Apache Pulsar is purely additive.
