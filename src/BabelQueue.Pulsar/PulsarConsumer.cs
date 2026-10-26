using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;

namespace BabelQueue.Pulsar;

/// <summary>
/// Receives from a Pulsar subscription, validates each message, routes it to the handler
/// registered for its URN, and <c>Acknowledge</c>s it on success. Per §5.2/§5.7 the routing key
/// is the <c>bq-job</c> message property (the body's <c>job</c>/<c>urn</c> is consulted only when
/// the property is absent or blank, i.e. a message produced outside BabelQueue).
/// <c>attempts</c> comes from the authoritative <c>bq-attempts</c> property (valid only if it matches
/// <c>^[0-9]+$</c> and fits an <c>int</c>; otherwise the body's value is used) and is reconciled to
/// <c>max(that, RedeliveryCount)</c> for the handler: Pulsar's redelivery count is 0-based, so
/// it maps directly with no −1, and the max never lowers the carried-over count. When <c>bq-job</c> and
/// the body's <c>job</c> differ, the property still routes and the optional <c>OnRoutingMismatch</c>
/// hook is notified (a throwing hook is swallowed; the message is settled normally). The loop
/// never stops on a bad message — observe via the option hooks.
/// <para>
/// <b>Bounded redelivery (ADR-0036 decisions 1 and 2).</b> Pulsar has no broker-side redelivery limit and this SDK sets
/// no native <c>DeadLetterPolicy</c>, so the bound is owned here. A throwing handler, an unmapped URN with no
/// <c>OnUnknownUrn</c> hook, and a poison body while dead-lettering is off are settled by the same decision, with
/// <c>counted = reconciled attempts + 1</c> and <see cref="PulsarConsumerOptions.MaxAttempts"/> (default 3, the total
/// number of deliveries): <c>counted &lt; MaxAttempts</c> retries the message — by a re-publish to its own topic with
/// <c>bq-attempts</c> + 1 and an acknowledgement when a retry producer is available — <see cref="PulsarConsumerOptions.RetryProducer"/>,
/// or one the consumer creates from <see cref="PulsarConsumerOptions.Client"/> (bounded on every subscription type, the Go model) —
/// otherwise by a negative acknowledgement (allowed on Shared and Key_Shared subscriptions only, where the native redelivery count
/// advances; the constructor rejects an Exclusive or Failover consumer without a retry producer); otherwise it is dead-lettered to <see cref="PulsarConsumerOptions.DeadLetterProducer"/> (the envelope plus a
/// <c>dead_letter</c> block, <c>reason</c> <c>failed</c> or <c>unknown_urn</c>, <c>attempts</c> = <c>counted</c>) when
/// that is set, or acknowledged and dropped (a trace warning) when it is not.
/// </para>
/// <para>
/// <b>Poison route.</b> A body that cannot become a handled envelope — not valid UTF-8, not a JSON object, an object with
/// no valid <c>job</c>/<c>urn</c> identity (ADR-0036 decision 4), any other non-conformant envelope
/// (<see cref="EnvelopeCodec.Accepts"/>) — is never handed to a
/// handler. With dead-lettering on it is written as the ADR-0035 record (<c>reason: "poison"</c>, the original bytes
/// preserved) and acknowledged, <b>without a retry</b>; with dead-lettering off it takes the bounded retry above and is
/// finally dropped. The original is acknowledged only after the dead-letter write succeeded; a failed write redelivers it.
/// </para>
/// <para>
/// Per §5.7 step 3 the <c>bq-schema-version</c> property is checked <b>before</b> the body is
/// decoded: when it is present and is not exactly the schema version the core supports
/// (<see cref="EnvelopeCodec.SchemaVersion"/>, compared exactly as sent, as its canonical decimal string, with no
/// trimming — so <c>"2"</c>, <c>"x"</c>, <c>"01"</c> and <c>" 1"</c> are all unknown), the body is never decoded or routed;
/// <c>OnError</c> is notified (with an empty, undecoded envelope) and the message is settled on its <b>first delivery</b> (ADR-0037 decision 1):
/// with dead-lettering on the body is written <b>exactly as received</b> (no <c>dead_letter</c> block) to the dead-letter topic and acknowledged after the write
/// (a failed write leaves it unacknowledged); with dead-lettering off it is acknowledged and dropped. It is never retried and never reaches a handler or <c>OnUnknownUrn</c>. A missing or blank (empty or ASCII-whitespace-only: space, tab, LF, VT, FF, CR) property changes nothing, and a
/// <c>"1"</c> property still goes through the post-decode <see cref="EnvelopeCodec.Accepts"/> check.
/// </para>
/// </summary>
public sealed class PulsarConsumer : IAsyncDisposable
{
    private const string JobProperty = "bq-job";
    private const string AttemptsProperty = "bq-attempts";
    private const string SchemaVersionProperty = "bq-schema-version";

    private readonly IConsumer<byte[]> _consumer;
    private readonly IReadOnlyDictionary<string, BabelHandler> _handlers;
    private readonly PulsarConsumerOptions _options;
    private readonly int _maxAttempts;
    private readonly IProducer<byte[]>? _deadLetterProducer;
    private readonly IProducer<byte[]>? _retryProducer;
    private readonly IProducer<byte[]>? _ownedRetryProducer;

    public PulsarConsumer(
        IConsumer<byte[]> consumer,
        IReadOnlyDictionary<string, BabelHandler> handlers,
        PulsarConsumerOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(consumer);
        ArgumentNullException.ThrowIfNull(handlers);
        _consumer = consumer;
        _handlers = handlers;
        _options = options ?? new PulsarConsumerOptions();
        if (_options.MaxAttempts < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                _options.MaxAttempts,
                "PulsarConsumerOptions.MaxAttempts must be at least 1 (the total number of deliveries, the first included).");
        }

        _maxAttempts = _options.MaxAttempts;
        _deadLetterProducer = _options.DeadLetterProducer;
        _retryProducer = _options.RetryProducer;
        if (_retryProducer is null)
        {
            if (_options.Client is not null)
            {
                // The consumer owns the producer it creates (disposed by DisposeAsync); a supplied RetryProducer is the caller's.
                var topic = consumer.Topic;
                if (string.IsNullOrWhiteSpace(topic))
                {
                    throw new InvalidOperationException(
                        "PulsarConsumerOptions.Client is set but the consumer reports no topic to create the retry producer on; "
                        + "set PulsarConsumerOptions.RetryProducer explicitly.");
                }

                _ownedRetryProducer = _options.Client.NewProducer(DotPulsar.Schema.ByteArray).Topic(topic).Create();
                _retryProducer = _ownedRetryProducer;
            }
            else if (consumer.SubscriptionType is SubscriptionType.Exclusive or SubscriptionType.Failover)
            {
                // A negative acknowledgement does not advance RedeliveryCount here, so a retry that is not a re-publish
                // would never reach MaxAttempts (ADR-0036: every message terminates). Fail at setup, not at run time.
                throw new InvalidOperationException(
                    $"An {consumer.SubscriptionType} subscription needs a retry producer: a negative acknowledgement does not advance "
                    + "RedeliveryCount there, so MaxAttempts would never be reached. Set PulsarConsumerOptions.Client (the consumer then "
                    + "creates the producer on its topic) or PulsarConsumerOptions.RetryProducer.");
            }
        }
    }

    /// <summary>Disposes the retry producer the consumer created from <see cref="PulsarConsumerOptions.Client"/>; a producer supplied by the caller and the underlying consumer are left to the caller.</summary>
    public ValueTask DisposeAsync() => _ownedRetryProducer is null ? ValueTask.CompletedTask : _ownedRetryProducer.DisposeAsync();

    /// <summary>Receive one message, route it, and settle it (acknowledge or redeliver).</summary>
    public async Task ReceiveAsync(CancellationToken cancellationToken = default)
    {
        var message = await _consumer.Receive(cancellationToken).ConfigureAwait(false);
        await HandleAsync(message, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Poll until <paramref name="cancellationToken"/> is cancelled.</summary>
    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await ReceiveAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    private async Task HandleAsync(IMessage<byte[]> message, CancellationToken cancellationToken)
    {
        var properties = message.Properties;
        var attemptsProperty = RawProperty(properties, AttemptsProperty);

        // §5.7 step 3: version-gate on the property before decoding the body.
        // The value is compared exactly as sent (no trimming), like the Go/Python/Node consumers (GR-5).
        var declaredVersion = RawProperty(properties, SchemaVersionProperty);
        if (declaredVersion is not null && !IsBlankSchemaVersion(declaredVersion)
            && !string.Equals(
                declaredVersion,
                EnvelopeCodec.SchemaVersion.ToString(CultureInfo.InvariantCulture),
                StringComparison.Ordinal))
        {
            // ADR-0037 decision 1: settled on the first delivery, whatever bq-attempts / RedeliveryCount / MaxAttempts say.
            // The body is deliberately not decoded, so hand OnError an empty envelope. The consumer has no
            // unknown-URN strategy option (it behaves as `fail`) and the OnUnknownUrn hook never applies here.
            await SettleVersionGatedAsync(
                message,
                Reconcile(new Envelope(null, null, null, null, 0, null), attemptsProperty, message.RedeliveryCount),
                new BabelQueueException(
                    $"Rejected a Pulsar message: unsupported {SchemaVersionProperty} property '{declaredVersion}' "
                    + $"(supported: {EnvelopeCodec.SchemaVersion}); body not decoded."),
                cancellationToken).ConfigureAwait(false);
            return;
        }

        // Invalid UTF-8 is poison (a lenient decode would hand a corrupted payload to a handler).
        var text = StrictUtf8(message.Value());
        var envelope = Reconcile(
            text is null ? new Envelope(null, null, null, null, 0, null) : EnvelopeCodec.Decode(text),
            attemptsProperty,
            message.RedeliveryCount);

        // Handler-before rejection (ADR-0036 decisions 2 and 4): a body that is not valid UTF-8, is not a JSON object,
        // has no valid job/urn identity, or is otherwise non-conformant can never reach a handler, and redelivering
        // it cannot change that: it takes the poison route instead of an endless nack.
        if (text is null || !EnvelopeCodec.Accepts(envelope))
        {
            await SettlePoisonAsync(
                message,
                envelope,
                new BabelQueueException(text is null
                    ? "Rejected a Pulsar message: the body is not valid UTF-8."
                    : "Rejected a non-conformant BabelQueue envelope from Apache Pulsar."),
                cancellationToken).ConfigureAwait(false);
            return;
        }

        // §5.2 / §5.7 step 2: route on bq-job; fall back to the body only when it is absent.
        var routed = Property(properties, JobProperty);
        var bodyUrn = EnvelopeCodec.Urn(envelope);
        var urn = routed ?? bodyUrn;
        if (routed is not null && !string.Equals(routed, bodyUrn, StringComparison.Ordinal))
        {
            // The property wins (§5.7), but the handler sees the body's job: tell the optional
            // OnRoutingMismatch hook. A throwing hook is swallowed — settling continues unchanged.
            NotifyRoutingMismatch(routed, bodyUrn, envelope, message);
        }

        if (!_handlers.TryGetValue(urn, out var handler))
        {
            if (_options.OnUnknownUrn is not null)
            {
                await _options.OnUnknownUrn(envelope, message, cancellationToken).ConfigureAwait(false);
                await AcknowledgeAsync(message, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await SettleFailureAsync(
                    message, envelope, "unknown_urn", new UnknownUrnException(urn), cancellationToken).ConfigureAwait(false);
            }

            return;
        }

        try
        {
            await handler(envelope, message, cancellationToken).ConfigureAwait(false);
        }
#pragma warning disable CA1031 // The consume loop must survive any handler exception.
        catch (Exception error) when (error is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
#pragma warning restore CA1031
        {
            await SettleFailureAsync(message, envelope, "failed", error, cancellationToken).ConfigureAwait(false);
            return;
        }

        // Outside the try: a failing acknowledgement is not a handler failure, and must not republish or dead-letter a
        // message that was handled successfully. It is reported and the message is left unacknowledged (the broker
        // redelivers it when the consumer reconnects or is reopened: DotPulsar has no client-side ack timeout), so a transient broker error does not stop the poll loop.
        try
        {
            await AcknowledgeAsync(message, cancellationToken).ConfigureAwait(false);
        }
#pragma warning disable CA1031 // A failed acknowledgement of a handled message must not stop the poll loop.
        catch (Exception ackError) when (ackError is not OperationCanceledException)
#pragma warning restore CA1031
        {
            Report(new BabelQueueException("Acknowledge failed after a successful handler; the message stays unacknowledged.", ackError), envelope, message);
        }
    }

    /// <summary>
    /// ADR-0036 decision 1: the settle decision for a retryable outcome (<paramref name="reason"/> <c>failed</c> or
    /// <c>unknown_urn</c>). <c>counted = reconciled + 1</c>; below <c>MaxAttempts</c> the message is redelivered,
    /// otherwise it is dead-lettered (producer set) or acknowledged and dropped. Settles first, then reports.
    /// </summary>
    private async Task SettleFailureAsync(
        IMessage<byte[]> message,
        Envelope envelope,
        string reason,
        Exception cause,
        CancellationToken cancellationToken)
    {
        var counted = Counted(envelope.Attempts);
        if (counted < _maxAttempts)
        {
            await RetryAsync(message, envelope, counted, cancellationToken).ConfigureAwait(false);
        }
        else if (_deadLetterProducer is not null)
        {
            await DeadLetterOrRedeliverAsync(
                message,
                () =>
                {
                    var annotated = DeadLetters.Annotate(
                        envelope with { Attempts = counted },
                        reason,
                        QueueName(message),
                        counted,
                        cause.Message,
                        cause.GetType().FullName);
                    // Keep the incoming properties, above all the routed bq-job (§5.2/§5.7; Go keeps the route): a body URN that
                    // differs from it must not redirect a redrive to another handler. bq-attempts is the counted attempt; the
                    // projection of the annotated envelope only fills what the incoming message did not carry.
                    var properties = new Dictionary<string, string>(PulsarProperties.Of(annotated), StringComparer.Ordinal);
                    foreach (var incoming in PulsarProperties.Copy(message.Properties))
                    {
                        properties[incoming.Key] = incoming.Value;
                    }

                    if (Property(message.Properties, JobProperty) is { } routedJob)
                    {
                        properties[JobProperty] = routedJob;
                    }

                    properties[AttemptsProperty] = counted.ToString(CultureInfo.InvariantCulture);
                    return (properties, Encoding.UTF8.GetBytes(EnvelopeCodec.Encode(annotated)));
                },
                envelope,
                cancellationToken).ConfigureAwait(false);
        }
        else
        {
            DropExhausted(message, envelope, reason, counted);
            await AcknowledgeAsync(message, cancellationToken).ConfigureAwait(false);
        }

        Report(cause, envelope, message);
    }

    /// <summary>
    /// ADR-0037 decision 1 (broker-bindings §5.7): a version-gated delivery is never decoded, never retried and never
    /// handed to a handler or the <see cref="PulsarConsumerOptions.OnUnknownUrn"/> hook. With dead-lettering on
    /// (<see cref="PulsarConsumerOptions.DeadLetterProducer"/> set) the body is written <b>exactly as received</b>, byte for
    /// byte, to the dead-letter topic (no <c>dead_letter</c> block, no re-encoding) and the message is acknowledged only after
    /// the write succeeded; a failed write leaves it unacknowledged (negative acknowledgement) and is reported. With
    /// dead-lettering off it is acknowledged and dropped (a trace warning). This consumer has no unknown-URN strategy
    /// option, so it behaves as the `fail` strategy.
    /// </summary>
    private async Task SettleVersionGatedAsync(
        IMessage<byte[]> message, Envelope envelope, Exception cause, CancellationToken cancellationToken)
    {
        if (_deadLetterProducer is null)
        {
            Trace.TraceWarning(
                $"BabelQueue.Pulsar: dropped a message from '{QueueName(message)}' on its first delivery: "
                + $"unsupported {SchemaVersionProperty} property; dead-lettering is off.");
            await AcknowledgeAsync(message, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            await DeadLetterOrRedeliverAsync(
                message,
                () => (PulsarProperties.Copy(message.Properties), message.Value() ?? Array.Empty<byte>()),
                envelope,
                cancellationToken).ConfigureAwait(false);
        }

        Report(cause, envelope, message);
    }

    /// <summary>
    /// ADR-0036 decision 2: the poison route. With dead-lettering on, the ADR-0035 record is written and the message is
    /// acknowledged after it, without any retry; with dead-lettering off the bounded retry of
    /// <see cref="SettleFailureAsync"/> applies (retry, then drop).
    /// </summary>
    private async Task SettlePoisonAsync(
        IMessage<byte[]> message, Envelope envelope, Exception cause, CancellationToken cancellationToken)
    {
        if (_deadLetterProducer is null)
        {
            await SettleFailureAsync(message, envelope, "poison", cause, cancellationToken).ConfigureAwait(false);
            return;
        }

        await DeadLetterOrRedeliverAsync(
            message,
            () =>
            {
                var record = PoisonBody(
                    message.Value() ?? Array.Empty<byte>(),
                    cause.Message,
                    cause.GetType().FullName,
                    DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    QueueName(message),
                    envelope.Attempts);
                return (PulsarProperties.Copy(message.Properties), record);
            },
            envelope,
            cancellationToken).ConfigureAwait(false);
        Report(cause, envelope, message);
    }

    /// <summary>
    /// The retry of ADR-0036 decision 1. With a <see cref="PulsarConsumerOptions.RetryProducer"/> the message is re-published
    /// to its own topic with <c>bq-attempts = counted</c> (and the body's <c>attempts</c> in sync, the body re-encoded by the SDK codec)
    /// and the original is acknowledged after the publish succeeded (the Go model, broker-bindings §5.5): the count is
    /// carried by the message itself, so the bound holds on every subscription type, including Exclusive and Failover
    /// where a negative acknowledgement does not advance <c>RedeliveryCount</c>. A failed publish leaves the original
    /// unacknowledged for redelivery. Without a retry producer the message is negatively acknowledged
    /// (<c>RedeliverUnacknowledgedMessages</c>): the count then advances only where the broker advances
    /// <c>RedeliveryCount</c> (Shared, Key_Shared).
    /// </summary>
    private async Task RetryAsync(
        IMessage<byte[]> message, Envelope envelope, int counted, CancellationToken cancellationToken)
    {
        if (_retryProducer is null)
        {
            await RedeliverAsync(message, cancellationToken).ConfigureAwait(false);
            return;
        }

        Exception? publishError = null;
        try
        {
            var properties = new Dictionary<string, string>(PulsarProperties.Copy(message.Properties), StringComparer.Ordinal)
            {
                [AttemptsProperty] = counted.ToString(CultureInfo.InvariantCulture),
            };
            // The copy carries the body re-encoded by the SDK codec with attempts = counted, not the original bytes (ADR-0036
            // decision 1, Go parity); for a poison body that is the (empty) envelope the decoder produced. The original bytes
            // are not kept on this path (dead-lettering is off for a poison body here and the bound ends in a drop).
            var payload = Encoding.UTF8.GetBytes(EnvelopeCodec.Encode(envelope with { Attempts = counted }));
            var metadata = new MessageMetadata();
            foreach (var property in properties)
            {
                metadata[property.Key] = property.Value;
            }

            await _retryProducer.Send(metadata, payload, cancellationToken).ConfigureAwait(false);
        }
#pragma warning disable CA1031 // Whatever the publish failure was, the original must stay for redelivery, not be lost.
        catch (Exception error) when (error is not OperationCanceledException)
#pragma warning restore CA1031
        {
            publishError = error;
        }

        if (publishError is null)
        {
            await AcknowledgeAsync(message, cancellationToken).ConfigureAwait(false);
            return;
        }

        await RedeliverAsync(message, cancellationToken).ConfigureAwait(false);
        Report(
            new BabelQueueException("Retry re-publish failed; the message stays unacknowledged for redelivery.", publishError),
            envelope,
            message);
    }

    /// <summary>
    /// Writes the dead-letter message and acknowledges the original only after that succeeded; a failed write leaves the
    /// original unacknowledged (negative ack, so it is redelivered and the write is tried again) and is reported.
    /// </summary>
    private async Task DeadLetterOrRedeliverAsync(
        IMessage<byte[]> message,
        Func<(IReadOnlyDictionary<string, string> Properties, byte[] Payload)> build,
        Envelope envelope,
        CancellationToken cancellationToken)
    {
        Exception? writeError = null;
        try
        {
            var (properties, payload) = build();
            var metadata = new MessageMetadata();
            foreach (var property in properties)
            {
                metadata[property.Key] = property.Value;
            }

            await _deadLetterProducer!.Send(metadata, payload, cancellationToken).ConfigureAwait(false);
        }
#pragma warning disable CA1031 // Whatever the write failure was, the original must be redelivered, not lost.
        catch (Exception error) when (error is not OperationCanceledException)
#pragma warning restore CA1031
        {
            writeError = error;
        }

        if (writeError is null)
        {
            await AcknowledgeAsync(message, cancellationToken).ConfigureAwait(false);
            return;
        }

        await RedeliverAsync(message, cancellationToken).ConfigureAwait(false);
        Report(
            new BabelQueueException("Dead-letter write failed; the message stays unacknowledged for redelivery.", writeError),
            envelope,
            message);
    }

    private void DropExhausted(IMessage<byte[]> message, Envelope envelope, string reason, int counted)
    {
        var detail = reason switch
        {
            "unknown_urn" => "no handler is mapped for its URN",
            "poison" => "it is a poison body",
            _ => "its handler kept failing",
        };
        Trace.TraceWarning(
            $"BabelQueue.Pulsar: dropped a message from '{QueueName(message)}' (id '{envelope.Meta?.Id}', reason '{reason}') after "
            + $"{counted} of {_maxAttempts} deliveries: {detail}; dead-lettering is off.");
    }

    /// <summary>Reports a failure to <c>OnError</c> after the message was settled; a throwing hook is swallowed.</summary>
    private void Report(Exception error, Envelope envelope, IMessage<byte[]> message)
    {
        if (_options.OnError is null)
        {
            return;
        }

#pragma warning disable CA1031 // The hook is advisory: it must never alter settling or stop the loop.
        try
        {
            _options.OnError(error, envelope, message);
        }
        catch (Exception)
        {
            // Swallowed by design.
        }
#pragma warning restore CA1031
    }

    /// <summary>The attempt number of the current delivery: the reconciled count plus one (saturating).</summary>
    private static int Counted(int reconciled) => reconciled == int.MaxValue ? int.MaxValue : reconciled + 1;

    private static readonly UTF8Encoding StrictUtf8Encoding = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    private static string? StrictUtf8(byte[]? body)
    {
        try
        {
            return StrictUtf8Encoding.GetString(body ?? Array.Empty<byte>());
        }
        catch (DecoderFallbackException)
        {
            return null;
        }
    }

    /// <summary>
    /// The queue a message came from: the topic of its message id (the specific topic of a multi-topic consumer),
    /// else the consumer's topic, reduced to the last path segment and without a <c>-partition-N</c> suffix.
    /// </summary>
    private string QueueName(IMessage<byte[]> message)
    {
        var topic = message.MessageId?.Topic;
        if (string.IsNullOrEmpty(topic))
        {
            topic = _consumer.Topic;
        }

        if (string.IsNullOrEmpty(topic))
        {
            return "default";
        }

        var slash = topic.LastIndexOf('/');
        var name = slash >= 0 ? topic[(slash + 1)..] : topic;
        var partition = name.LastIndexOf("-partition-", StringComparison.Ordinal);
        if (partition > 0 && IsAsciiDigits(name[(partition + "-partition-".Length)..]))
        {
            name = name[..partition];
        }

        return name.Length == 0 ? "default" : name;
    }

    private static readonly JsonWriterOptions PoisonWriterOptions = new()
    {
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
        Indented = false,
    };

    /// <summary>
    /// The poison DLQ record (ADR-0035, <see cref="PoisonRecord"/>): a body that is a strict JSON object without a
    /// top-level <c>dead_letter</c> key is spliced byte for byte; anything else is carried as <c>raw</c> (plain text when
    /// valid UTF-8, otherwise base64 plus <c>raw_encoding: "base64"</c>). The <c>dead_letter</c> block has reason
    /// <c>poison</c>.
    /// </summary>
    internal static byte[] PoisonBody(byte[] body, string? error, string? exception, long failedAt, string originalQueue, int attempts)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream, PoisonWriterOptions))
        {
            writer.WriteStartObject();
            writer.WriteString("reason", "poison");
            if (error is null) { writer.WriteNull("error"); } else { writer.WriteString("error", error); }
            if (exception is null) { writer.WriteNull("exception"); } else { writer.WriteString("exception", exception); }
            writer.WriteNumber("failed_at", failedAt);
            writer.WriteString("original_queue", originalQueue);
            writer.WriteNumber("attempts", attempts);
            writer.WriteString("lang", EnvelopeCodec.SourceLang);
            writer.WriteEndObject();
        }

        return PoisonRecord.Build(body, Encoding.UTF8.GetString(stream.ToArray()));
    }

    private void NotifyRoutingMismatch(string propertyUrn, string bodyUrn, Envelope envelope, IMessage<byte[]> message)
    {
        if (_options.OnRoutingMismatch is null)
        {
            return;
        }

#pragma warning disable CA1031 // The hook is advisory: it must never alter settling or stop the loop.
        try
        {
            _options.OnRoutingMismatch(propertyUrn, bodyUrn, envelope, message);
        }
        catch (Exception)
        {
            // Swallowed by design.
        }
#pragma warning restore CA1031
    }

    /// <summary>The trimmed property value, or <c>null</c> when the map or the value is missing/blank.</summary>
    private static string? Property(IReadOnlyDictionary<string, string>? properties, string key)
    {
        if (properties is null || !properties.TryGetValue(key, out var value) || string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        return value.Trim();
    }

    /// <summary>The property value exactly as sent (no trimming), or <c>null</c> when the map or the key is missing.</summary>
    private static string? RawProperty(IReadOnlyDictionary<string, string>? properties, string key)
        => properties is not null && properties.TryGetValue(key, out var value) ? value : null;

    /// <summary>
    /// The shared cross-SDK "blank" definition for <c>bq-schema-version</c>: the empty string, or a value made up
    /// <b>only</b> of ASCII whitespace (space, <c>\t</c>, <c>\n</c>, U+000B, <c>\f</c>, <c>\r</c>). Anything else
    /// (NBSP, U+001C–U+001F, U+0085, U+FEFF, …) is <b>not</b> blank, unlike <see cref="string.IsNullOrWhiteSpace"/>.
    /// </summary>
    private static bool IsBlankSchemaVersion(string value)
    {
        foreach (var c in value)
        {
            if (c != ' ' && (c < '\t' || c > '\r'))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary><c>^[0-9]+$</c>: ASCII digits only — no sign, no whitespace, not empty, no Unicode digits.</summary>
    private static bool IsAsciiDigits(string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return false;
        }

        foreach (var c in value)
        {
            if (c is < '0' or > '9')
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Sets <c>attempts</c> from the authoritative <c>bq-attempts</c> property (§5.5): that value
    /// replaces the body's, falling back to the body's when the property is absent or does not
    /// match <c>^[0-9]+$</c> (or overflows <c>int</c>); the result is then raised to <c>RedeliveryCount</c> if that is higher.
    /// Pulsar's redelivery count is 0-based (0 on first delivery) so it maps directly to
    /// <c>attempts</c> with no −1; the max never lowers the carried-over count.
    /// </summary>
    private static Envelope Reconcile(Envelope envelope, string? attemptsProperty, uint redeliveryCount)
    {
        var attempts = envelope.Attempts;
        if (IsAsciiDigits(attemptsProperty)
            && int.TryParse(attemptsProperty, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed))
        {
            attempts = parsed;
        }

        var native = redeliveryCount > int.MaxValue ? int.MaxValue : (int)redeliveryCount;
        attempts = Math.Max(attempts, native);
        return attempts == envelope.Attempts ? envelope : envelope with { Attempts = attempts };
    }

    private ValueTask AcknowledgeAsync(IMessage<byte[]> message, CancellationToken cancellationToken)
        => _consumer.Acknowledge(message.MessageId, cancellationToken);

    private ValueTask RedeliverAsync(IMessage<byte[]> message, CancellationToken cancellationToken)
        => _consumer.RedeliverUnacknowledgedMessages(new[] { message.MessageId }, cancellationToken);
}
