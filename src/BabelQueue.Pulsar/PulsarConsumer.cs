using System.Text;
using DotPulsar.Abstractions;

namespace BabelQueue.Pulsar;

/// <summary>
/// Receives from a Pulsar subscription, decodes and validates each message, routes it to the
/// handler registered for its URN, and <c>Acknowledge</c>s it on success. A throwing handler
/// redelivers the message — Pulsar increments <c>RedeliveryCount</c> (at-least-once).
/// <c>attempts</c> is reconciled to <c>max(bq-attempts, RedeliveryCount)</c> for the handler:
/// Pulsar's redelivery count is 0-based, so it maps directly with no −1, and the max never
/// lowers a higher body count carried by a republish-driven retry. The loop never stops on a
/// bad message — observe via the option hooks.
/// </summary>
public sealed class PulsarConsumer
{
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
        var envelope = Reconcile(
            EnvelopeCodec.Decode(Encoding.UTF8.GetString(message.Value())),
            message.RedeliveryCount);

        if (!EnvelopeCodec.Accepts(envelope))
        {
            _options.OnError?.Invoke(
                new BabelQueueException("Rejected a non-conformant BabelQueue envelope from Apache Pulsar."),
                envelope, message);
            await RedeliverAsync(message, cancellationToken).ConfigureAwait(false);
            return;
        }

        var urn = EnvelopeCodec.Urn(envelope);
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

    /// <summary>
    /// Sets <c>attempts</c> to <c>max(current, RedeliveryCount)</c>. Pulsar's redelivery count
    /// is 0-based (0 on first delivery) so it maps directly to <c>attempts</c> with no −1; the
    /// max never lowers a higher body count carried by a message republished from another SDK.
    /// </summary>
    private static Envelope Reconcile(Envelope envelope, uint redeliveryCount)
    {
        var native = (int)redeliveryCount;
        return native > envelope.Attempts ? envelope with { Attempts = native } : envelope;
    }

    private ValueTask AcknowledgeAsync(IMessage<byte[]> message, CancellationToken cancellationToken)
        => _consumer.Acknowledge(message.MessageId, cancellationToken);

    private ValueTask RedeliverAsync(IMessage<byte[]> message, CancellationToken cancellationToken)
        => _consumer.RedeliverUnacknowledgedMessages(new[] { message.MessageId }, cancellationToken);
}
