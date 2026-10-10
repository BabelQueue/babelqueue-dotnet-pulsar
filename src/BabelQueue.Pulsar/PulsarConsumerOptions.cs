using DotPulsar.Abstractions;

namespace BabelQueue.Pulsar;

/// <summary>Hooks for <see cref="PulsarConsumer"/>.</summary>
public sealed class PulsarConsumerOptions
{
    /// <summary>
    /// Called for a non-conformant envelope, an unmapped URN (with no
    /// <see cref="OnUnknownUrn"/>), or a throwing handler. The poll loop never stops.
    /// </summary>
    public Action<Exception, Envelope, IMessage<byte[]>>? OnError { get; set; }

    /// <summary>
    /// Optional advisory hook (default: none), called with (property URN, body URN, envelope,
    /// message) when the <c>bq-job</c> property that was routed on differs from the body's
    /// <c>job</c>. Never affects settling; exceptions it throws are swallowed.
    /// </summary>
    public Action<string, string, Envelope, IMessage<byte[]>>? OnRoutingMismatch { get; set; }

    /// <summary>
    /// Called instead of erroring when a URN has no handler; the message is then acknowledged
    /// (dropped).
    /// </summary>
    public Func<Envelope, IMessage<byte[]>, CancellationToken, Task>? OnUnknownUrn { get; set; }
}
