using DotPulsar.Abstractions;

namespace BabelQueue.Pulsar;

/// <summary>
/// Processes one decoded, validated envelope and the raw Pulsar message it arrived on.
/// Completing normally acknowledges it (the consumer <c>Acknowledge</c>s it); throwing leaves
/// it for the broker to redeliver (the consumer redelivers it, incrementing
/// <c>RedeliveryCount</c>).
/// </summary>
public delegate Task BabelHandler(Envelope envelope, IMessage<byte[]> message, CancellationToken cancellationToken);
