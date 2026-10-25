# Changelog

All notable changes to `BabelQueue.Pulsar` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

## [1.2.0] - 2026-10-25

Implements ADR-0036 decisions 1, 2 and 4 (post-R1 parity). The envelope stays at `schema_version: 1`; every change is a
consumer-side settle decision. Ships as a **MINOR**.

### Added

- `PulsarConsumerOptions.MaxAttempts` (default **3**): the SDK-owned bound on redelivery, the total number of deliveries a
  message gets, the first included (`1` = no retry; a value below 1 makes the `PulsarConsumer` constructor throw
  `ArgumentOutOfRangeException`). The counted attempt is `reconciled + 1` (`bq-attempts`, falling back to the body's
  `attempts`, floored by `RedeliveryCount`). No native `DeadLetterPolicy` is used (DotPulsar 5.3.1 has none; ADR-0014 is
  amended by ADR-0036).
- `PulsarConsumerOptions.DeadLetterProducer`: a producer bound to `<queue>.dlq`. Setting it turns dead-lettering on. An
  exhausted message is written there as the canonical envelope plus the `dead_letter` block (`reason` `failed` or
  `unknown_urn`, `dead_letter.attempts` = the counted attempt, `lang: "dotnet"`), and the original is acknowledged only
  after the write succeeded; a failed write leaves it unacknowledged (redelivered) and is reported to `OnError`. With it
  unset, an exhausted message is acknowledged and dropped (a `Trace` warning is written).
- Retry by re-publish (the Go model, broker-bindings 5.5): a retry re-publishes the message to the consumer's own topic with
  `bq-attempts` + 1 (the body re-encoded by the codec with `attempts` in sync, not the original bytes: Go parity; a poison body is re-published as the empty envelope) and acknowledges the original, so `MaxAttempts` bounds the retries on
  **every** subscription type. `PulsarConsumerOptions.Client` (the `IPulsarClient` the consumer was built from) lets the
  consumer create that producer itself on `consumer.Topic`; `PulsarConsumerOptions.RetryProducer` supplies one instead
  (used as given, never disposed by the consumer; no API break). `PulsarConsumer` now implements `IAsyncDisposable` to dispose
  the producer it created.
- **Behaviour change (fail-closed):** a consumer whose subscription type is not reported (the DotPulsar `SubscriptionType`
  default is `Exclusive`) is treated as Exclusive, so without a `Client` or `RetryProducer` the constructor throws instead of
  risking an unbounded nack loop. A `Shared` or `Key_Shared` subscription that is reported as such keeps the nack path.
- With neither `Client` nor `RetryProducer` a retry stays a negative acknowledgement, which advances `RedeliveryCount` only on
  Shared and Key_Shared subscriptions. The `PulsarConsumer` constructor therefore throws `InvalidOperationException` for an
  Exclusive or Failover subscription without one of them, instead of looping forever at run time.
- Conformance v1.7.0 runner: the `pulsar_exhaustion` section (the ten `on_unknown_urn` rows are reported as `skip`, citing
  ADR-0036, because the .NET consumer has no unknown-URN strategy option) and `poison_dlq`; the test project writes
  `results.json` to `BQ_CONFORMANCE_RESULTS`, `ci.yml` uploads it and `release.yml` attaches it to the GitHub release.

### Changed

- The `failed` / `unknown_urn` dead-letter record keeps the routed `bq-job` property and the other incoming properties (Go parity); `bq-attempts` is the counted attempt.
- A failing `Acknowledge` after a successful handler is no longer treated as a handler failure (it no longer republishes or dead-letters a handled message); it is reported to `OnError` and the message stays unacknowledged (redelivered when the consumer reconnects or is reopened; DotPulsar has no client-side ack timeout), so the poll loop keeps running.
- **Behaviour change:** a message that keeps failing is no longer redelivered without limit. With the defaults it is
  dropped after its third delivery (dead-lettered to `<queue>.dlq` when a `DeadLetterProducer` is set). A deployment that
  relied on unbounded redelivery must raise `MaxAttempts`.
- **Behaviour change:** an unmapped URN with no `OnUnknownUrn` hook follows the same bounded path (retry, then
  `<queue>.dlq` with `reason: "unknown_urn"` or a drop).
- **Behaviour change:** a body that never reaches a handler (not valid UTF-8, not a JSON object, a JSON object with no valid
  `job`/`urn` identity per ADR-0032 section 6 / ADR-0036 decision 4, any other non-conformant envelope, or a message
  rejected by the `bq-schema-version` gate) takes the poison route instead of being rejected and redelivered forever. With
  dead-lettering on it is written as the ADR-0035 record (`reason: "poison"`, original bytes preserved, the message's
  properties carried over) and acknowledged without a retry; with it off it takes the bounded retry and is then dropped.
- `OnError` is invoked after the message is settled, and an exception thrown by the hook is swallowed (it can no longer
  leave a message unsettled). A handler that throws `OperationCanceledException` while the loop is being cancelled leaves
  the message unsettled instead of counting an attempt.
- Depends on `BabelQueue.Core` 1.13.0 (was 1.0.0) for `PoisonRecord` and `DeadLetters`.
- Vendored conformance suite updated to v1.7.0; CI pins `BQ_CONFORMANCE_VERSION` 1.7.0 (the `fetch.sh` sha256 is unchanged).
  The test project now copies `conformance/CONFORMANCE_VERSION` next to the test assembly.
- CI pins the `fetch.sh` it downloads by sha256 (`BQ_FETCH_SH_SHA256`) before running it.

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
