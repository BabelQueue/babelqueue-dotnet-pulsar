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
message is redelivered, the same path as a non-conformant envelope. A missing or blank (empty or
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
| reserve / ack / retry | `Acknowledge` / redeliver-unacknowledged |

Pulsar properties are string→string, so `bq-attempts` carries the contract `attempts` and is
**authoritative**. The consumer reads it (falling back to the body's `attempts` when it is
absent or does not match `^[0-9]+$` — ASCII digits only, no sign/whitespace, within `int`) and reconciles to `max(bq-attempts, RedeliveryCount)`:
`RedeliveryCount` is 0-based (0 on first delivery) so it maps directly to `attempts` with
**no −1**, and the `max` never lowers a higher carried-over count — so a republish-driven retry
(the Go/Python transports increment `bq-attempts` and re-send) and a native redelivery both
converge on the same number. A throwing handler redelivers the message
(`RedeliverUnacknowledgedMessages`), so the broker re-sends it (at-least-once); with a native
`DeadLetterPolicy` it eventually moves to the cross-language `<queue>.dlq` topic. The poll
loop never stops on a bad message — observe via `OnError`/`OnUnknownUrn`. If `bq-job` differs from the body's `job`, the property still routes and the optional `OnRoutingMismatch` hook is called with the property URN, body URN and envelope/message (a throwing hook is swallowed; settling is unaffected; `OnError` is not involved). The envelope is
unchanged (`schema_version` stays `1`); Pulsar is purely additive.

`OnError` and `OnRoutingMismatch` are synchronous `Action` delegates (`OnUnknownUrn` returns a
`Task` and may be async). Do not assign an `async` lambda to them: it compiles to `async void`, so an exception thrown after its
first `await` escapes the consumer's try/catch and can crash the process. Keep hooks
synchronous, or hand async work off yourself and observe its failures.

## Build & test

```bash
dotnet test
```

The DotPulsar `IProducer` / `IConsumer` / `IMessage` interfaces are mocked with Moq — no
Pulsar, no network. xUnit; analyzers run as errors; ≥90% line coverage enforced.

## License

MIT
