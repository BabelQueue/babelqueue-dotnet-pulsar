using DotPulsar.Abstractions;

namespace BabelQueue.Pulsar;

/// <summary>Hooks for <see cref="PulsarConsumer"/>.</summary>
public sealed class PulsarConsumerOptions
{
    /// <summary>
    /// Called for a non-conformant envelope, an unmapped URN (with no
    /// <see cref="OnUnknownUrn"/>), a throwing handler, or a message whose
    /// <c>bq-schema-version</c> property names an unsupported version (rejected before the body is
    /// decoded; the envelope argument is then empty). The poll loop never stops.
    /// </summary>
    public Action<Exception, Envelope, IMessage<byte[]>>? OnError { get; set; }

    /// <summary>
    /// Optional advisory hook (default: none), called with (property URN, body URN, envelope,
    /// message) when the <c>bq-job</c> property that was routed on differs from the body's
    /// <c>job</c>. Never affects settling; exceptions it throws are swallowed. Do not assign an
    /// <c>async</c> lambda: as <c>async void</c>, an exception after its first <c>await</c> escapes.
    /// </summary>
    public Action<string, string, Envelope, IMessage<byte[]>>? OnRoutingMismatch { get; set; }

    /// <summary>
    /// Called instead of erroring when a URN has no handler; the message is then acknowledged
    /// (dropped).
    /// </summary>
    public Func<Envelope, IMessage<byte[]>, CancellationToken, Task>? OnUnknownUrn { get; set; }
}
