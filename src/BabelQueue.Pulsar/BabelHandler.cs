using DotPulsar.Abstractions;

namespace BabelQueue.Pulsar;

/// <summary>
/// Processes one decoded, validated envelope and the raw Pulsar message it arrived on.
/// Completing normally acknowledges it (the consumer <c>Acknowledge</c>s it); throwing retries it:
/// the consumer redelivers it (or re-publishes it with <c>bq-attempts</c> + 1 when a
/// <see cref="PulsarConsumerOptions.RetryProducer"/> is set) until <see cref="PulsarConsumerOptions.MaxAttempts"/>
/// deliveries, after which it is dead-lettered or dropped (ADR-0036).
/// </summary>
public delegate Task BabelHandler(Envelope envelope, IMessage<byte[]> message, CancellationToken cancellationToken);
