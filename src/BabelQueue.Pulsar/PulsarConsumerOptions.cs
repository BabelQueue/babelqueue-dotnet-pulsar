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
    /// Called instead of erroring when a URN has no handler; the message is then acknowledged
    /// (dropped).
    /// </summary>
    public Func<Envelope, IMessage<byte[]>, CancellationToken, Task>? OnUnknownUrn { get; set; }
}
