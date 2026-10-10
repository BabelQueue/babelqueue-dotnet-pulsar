using System.Globalization;
using System.Text;
using DotPulsar.Abstractions;

namespace BabelQueue.Pulsar;

/// <summary>
/// Receives from a Pulsar subscription, validates each message, routes it to the handler
/// registered for its URN, and <c>Acknowledge</c>s it on success. Per §5.2/§5.7 the routing key
/// is the <c>bq-job</c> message property (the body's <c>job</c>/<c>urn</c> is consulted only when
/// the property is absent or blank, i.e. a message produced outside BabelQueue). A throwing
/// handler redelivers the message — Pulsar increments <c>RedeliveryCount</c> (at-least-once).
/// <c>attempts</c> comes from the authoritative <c>bq-attempts</c> property (valid only if it matches
/// <c>^[0-9]+$</c> and fits an <c>int</c>; otherwise the body's value is used) and is reconciled to
/// <c>max(that, RedeliveryCount)</c> for the handler: Pulsar's redelivery count is 0-based, so
/// it maps directly with no −1, and the max never lowers the carried-over count. When <c>bq-job</c> and
/// the body's <c>job</c> differ, the property still routes and the optional <c>OnRoutingMismatch</c>
/// hook is notified (a throwing hook is swallowed; the message is settled normally). The loop
/// never stops on a bad message — observe via the option hooks.
/// </summary>
public sealed class PulsarConsumer
{
    private const string JobProperty = "bq-job";
    private const string AttemptsProperty = "bq-attempts";

    private readonly IConsumer<byte[]> _consumer;
    private readonly IReadOnlyDictionary<string, BabelHandler> _handlers;
    private readonly PulsarConsumerOptions _options;

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
    }

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
        var envelope = Reconcile(
            EnvelopeCodec.Decode(Encoding.UTF8.GetString(message.Value())),
            RawProperty(properties, AttemptsProperty),
            message.RedeliveryCount);

        if (!EnvelopeCodec.Accepts(envelope))
        {
            _options.OnError?.Invoke(
                new BabelQueueException("Rejected a non-conformant BabelQueue envelope from Apache Pulsar."),
                envelope, message);
            await RedeliverAsync(message, cancellationToken).ConfigureAwait(false);
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
                _options.OnError?.Invoke(new UnknownUrnException(urn), envelope, message);
                await RedeliverAsync(message, cancellationToken).ConfigureAwait(false);
            }

            return;
        }

        try
        {
            await handler(envelope, message, cancellationToken).ConfigureAwait(false);
            await AcknowledgeAsync(message, cancellationToken).ConfigureAwait(false);
        }
#pragma warning disable CA1031 // The consume loop must survive any handler exception.
        catch (Exception error)
#pragma warning restore CA1031
        {
            // Redeliver releases the message — the broker re-sends it and increments RedeliveryCount.
            _options.OnError?.Invoke(error, envelope, message);
            await RedeliverAsync(message, cancellationToken).ConfigureAwait(false);
        }
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
