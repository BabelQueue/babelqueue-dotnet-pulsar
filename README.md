# BabelQueue — Apache Pulsar (.NET)

`BabelQueue.Pulsar` — an Apache Pulsar transport for
[BabelQueue](https://babelqueue.com), built on
[DotPulsar](https://github.com/apache/pulsar-dotpulsar) and the framework-agnostic
[`BabelQueue.Core`](https://github.com/BabelQueue/babelqueue-dotnet).

A canonical-envelope **publisher** and a URN-routed **consumer**, so a Pulsar-based .NET
service speaks the same wire contract (envelope shape, URN identity, trace propagation) as
the Java, Python, Go and Node SDKs. Implements
[§5 of the broker-bindings contract](https://babelqueue.com/docs/spec/1.x/broker-bindings#apache-pulsar).

## Install

```bash
dotnet add package BabelQueue.Pulsar
```

It pulls `BabelQueue.Core` and `DotPulsar` transitively.

## Use

```csharp
await using var client = PulsarClient.Builder().ServiceUrl(new Uri("pulsar://localhost:6650")).Build();

// produce
await using var producer = client.NewProducer(Schema.ByteArray).Topic("orders").Create();
var id = await new PulsarPublisher(producer)
    .PublishAsync("urn:babel:orders:created", new Dictionary<string, object?> { ["order_id"] = 1042 });

// consume (Shared subscription)
await using var consumer = client.NewConsumer(Schema.ByteArray)
    .Topic("orders").SubscriptionName("babelqueue").SubscriptionType(SubscriptionType.Shared).Create();

var handlers = new Dictionary<string, BabelHandler>
{
    ["urn:babel:orders:created"] = async (env, msg, ct) =>
    {
        // env.Data, env.TraceId, env.Attempts ...
    },
};
var babel = new PulsarConsumer(consumer, handlers, new PulsarConsumerOptions
{
    OnError = (err, env, msg) => Console.Error.WriteLine(err),
});
await babel.RunAsync(cancellationToken); // poll until cancelled
```

Delayed delivery: `PublishAsync(urn, data, traceId, TimeSpan.FromMinutes(5))` → native
`DeliverAtTime`. The consumer routes on the `bq-job` property (§5.2/§5.7 step 2); the body's
`job`/`urn` is consulted only when the property is absent or blank (a message produced
outside BabelQueue).

Before decoding the body, the consumer version-gates on the `bq-schema-version` property
(§5.7 step 3): a value that is not exactly the schema version the core supports, compared exactly as sent with no
trimming (`"2"`, `"x"`, `"01"`, `" 1"`, …) is rejected without decoding — `OnError` is called with an empty envelope and the
message is settled on its **first delivery** (ADR-0037): with a `DeadLetterProducer` its body is written **exactly as
received** (byte for byte, no `dead_letter` block) to `<queue>.dlq` and acknowledged after the write (a failed write leaves it
unacknowledged — negative acknowledgement — and is reported); without one it is acknowledged and dropped (a `Trace`
warning). It is never retried and never reaches a handler or `OnUnknownUrn`. A missing or blank (empty or
ASCII-whitespace-only: space, tab, LF, VT, FF, CR; NBSP, U+0085, U+FEFF etc. are not blank) property changes nothing; a `"1"` property still goes through the post-decode `Accepts` check.

## Contract mapping (§5)

| Envelope | Apache Pulsar |
| :--- | :--- |
| body | message payload (byte-identical across SDKs) |
| `job` (URN) | property `bq-job` (consumer routes on this) |
| `trace_id` | property `bq-trace-id` |
| `meta.id` | property `bq-message-id` |
| `meta.schema_version` | property `bq-schema-version` |
| `meta.lang` | property `bq-source-lang` |
| `meta.created_at` | `PublishTime` (mirror; body authoritative) |
| `attempts` | property `bq-attempts` (authoritative), cross-checked against `RedeliveryCount` |
| reserve / ack / retry | `Acknowledge` / re-publish with `bq-attempts` + 1 (`Client` / `RetryProducer`) or redeliver-unacknowledged (Shared / Key_Shared) |

Pulsar properties are string→string, so `bq-attempts` carries the contract `attempts` and is
**authoritative**. The consumer reads it (falling back to the body's `attempts` when it is
absent or does not match `^[0-9]+$` — ASCII digits only, no sign/whitespace, within `int`) and reconciles to `max(bq-attempts, RedeliveryCount)`:
`RedeliveryCount` is 0-based (0 on first delivery) so it maps directly to `attempts` with
**no −1**, and the `max` never lowers a higher carried-over count — so a republish-driven retry
(the Go/Python transports increment `bq-attempts` and re-send) and a native redelivery both
converge on the same number. The poll loop never stops on a bad message — observe via `OnError`/`OnUnknownUrn`.
If `bq-job` differs from the body's `job`, the property still routes and the optional `OnRoutingMismatch` hook is called with the property URN, body URN and envelope/message (a throwing hook is swallowed; settling is unaffected; `OnError` is not involved). The envelope is
unchanged (`schema_version` stays `1`); Pulsar is purely additive.

## Retries, dead-lettering and poison (ADR-0036)

Pulsar has no broker-side redelivery limit and this SDK sets no native `DeadLetterPolicy`, so the bound is owned by the
consumer:

```csharp
await using var dlq = client.NewProducer(Schema.ByteArray).Topic("orders.dlq").Create();

await using var babel = new PulsarConsumer(consumer, handlers, new PulsarConsumerOptions
{
    MaxAttempts = 3,             // default: the total number of deliveries, the first included (1 = no retry)
    DeadLetterProducer = dlq,    // on: exhausted messages go to <queue>.dlq; off (null): they are acknowledged and dropped
    Client = client,             // the consumer creates its retry producer (re-publish with bq-attempts + 1) from it
});
```

- A handler that throws, an unmapped URN with no `OnUnknownUrn` hook, and a poison body while dead-lettering is off are
  retried while `counted < MaxAttempts` (`counted` = the reconciled `attempts` + 1). Afterwards the message is dead-lettered
  (`DeadLetterProducer` set: the envelope plus a `dead_letter` block, `reason` `failed` or `unknown_urn`,
  `dead_letter.attempts` = `counted`) or acknowledged and dropped (a `Trace` warning). **Behaviour change:** before, such a
  message was redelivered forever; with the defaults it is now dropped after its third delivery.
- A retry is a re-publish to the consumer's own topic with `bq-attempts` + 1 and an acknowledgement, so the count travels
  with the message and the bound holds on **every** subscription type. Give the consumer the `Client` it was built from
  (`PulsarConsumerOptions.Client`) and it creates that retry producer itself on `consumer.Topic` (single topic; disposed by
  `PulsarConsumer.DisposeAsync`), or pass your own `RetryProducer` (used as given, never disposed by the consumer).
- With neither, a retry is a negative acknowledgement (`RedeliverUnacknowledgedMessages`), which advances `RedeliveryCount`
  on Shared and Key_Shared subscriptions only. On an **Exclusive** or **Failover** subscription it would never reach
  `MaxAttempts`, so the `PulsarConsumer` constructor throws `InvalidOperationException` instead: set `Client` or
  `RetryProducer` there.
- **Poison.** A body that cannot reach a handler — invalid UTF-8, not a JSON object, an object without a valid `job`/`urn`,
  any other non-conformant envelope — is never handed to one (a rejected `bq-schema-version` is not poison: see above). With a
  `DeadLetterProducer` it is written to `<queue>.dlq` as the ADR-0035 record (`reason: "poison"`,
  original bytes preserved) and acknowledged, without a retry; without one it takes the bounded retry above and is dropped.
- The original message is acknowledged only after the dead-letter write (or the retry re-publish) succeeded; a failed
  write leaves it for redelivery and is reported to `OnError`.

`OnError` and `OnRoutingMismatch` are synchronous `Action` delegates (`OnUnknownUrn` returns a
`Task` and may be async). Do not assign an `async` lambda to them: it compiles to `async void`, so an exception thrown after its
first `await` escapes the consumer's try/catch and can crash the process. Keep hooks
synchronous, or hand async work off yourself and observe its failures.

## Build & test

```bash
dotnet test
```

The DotPulsar `IProducer` / `IConsumer` / `IMessage` interfaces are mocked with Moq — no
Pulsar, no network. The vendored conformance suite (v1.8.0) is run too; set `BQ_CONFORMANCE_RESULTS=/path/results.json` to
get the `results.json` of the run. xUnit; analyzers run as errors; ≥90% line coverage enforced.

## License

MIT
