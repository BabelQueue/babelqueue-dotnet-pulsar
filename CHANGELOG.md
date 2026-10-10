# Changelog

All notable changes to `BabelQueue.Pulsar` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

## [1.1.0] - 2026-10-10

### Added
- Optional `OnRoutingMismatch` hook on the consumer options: called with the property URN, the body URN
  and the envelope when `bq-job` differs from the body's `job`. Defaults to a no-op; a throwing
  hook is swallowed. New public API, so the next release is a MINOR.

### Fixed
- `PulsarConsumer` now version-gates on the `bq-schema-version` message property **before** decoding
  the body (§5.7 step 3, MUST). A present value that is not exactly the core's supported schema version
  (`EnvelopeCodec.SchemaVersion`; compared exactly as sent, with no trimming; so `"2"`, `"x"`, `"01"` and `" 1"` are rejected) skips decoding and routing:
  `OnError` receives a `BabelQueueException` naming the property (with an empty envelope) and the
  message is redelivered, like a non-conformant envelope. A missing/blank (empty or ASCII-whitespace-only: space, tab, LF, VT, FF, CR; NBSP, U+0085, U+FEFF etc. are not blank) property and a supported
  value behave as before (the post-decode `Accepts` check is unchanged). No public API or wire change;
  `schema_version` stays 1.
- `PulsarConsumer` now follows contract §5.2/§5.7: it routes on the `bq-job` message property
  (the body's `job`/`urn` is only a fallback when the property is absent or blank), and takes
  `attempts` from the authoritative `bq-attempts` property (falling back to the body's value when
  it is absent or does not match `^[0-9]+$` — ASCII digits only, no sign or whitespace, within `int`), still raised to `RedeliveryCount` when that is higher. Previously it
  routed on the decoded body URN and ignored `bq-attempts`. When `bq-job` differs from the body's `job`, the new optional `OnRoutingMismatch` hook is called (property URN, body URN, envelope/message); the property still routes, a throwing hook is swallowed and `OnError` is untouched. No breaking public API or wire-format change (only the optional hook is added).

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
