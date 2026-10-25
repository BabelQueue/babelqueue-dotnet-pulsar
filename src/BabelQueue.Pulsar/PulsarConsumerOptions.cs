using DotPulsar.Abstractions;

namespace BabelQueue.Pulsar;

/// <summary>Hooks and settle settings for <see cref="PulsarConsumer"/>.</summary>
public sealed class PulsarConsumerOptions
{
    /// <summary>The default for <see cref="MaxAttempts"/>.</summary>
    public const int DefaultMaxAttempts = 3;

    /// <summary>
    /// Called for a poison body (a non-conformant envelope or an undecodable body), a message whose
    /// <c>bq-schema-version</c> property names an unsupported version (ADR-0037), an unmapped URN (with no
    /// <see cref="OnUnknownUrn"/>), or a throwing handler, once per failed delivery, after the message has been
    /// settled. For a rejected message the envelope argument is the decoded one (empty when the body could not be
    /// decoded or was deliberately not decoded). A failed <see cref="DeadLetterProducer"/> write is reported here too
    /// (the message is then redelivered). An exception thrown by the hook is swallowed; the poll loop never stops.
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

    /// <summary>
    /// The total number of deliveries a message gets, the first included (default <see cref="DefaultMaxAttempts"/>;
    /// <c>1</c> means no retry). ADR-0036 decision 1: Pulsar has no broker-side redelivery limit and this SDK sets no
    /// native <c>DeadLetterPolicy</c>, so the bound is owned here. On each delivery the consumer counts
    /// <c>reconciled + 1</c> (<c>bq-attempts</c>, falling back to the body's <c>attempts</c>, floored by
    /// <c>RedeliveryCount</c>); while that is below this value a failure is redelivered, afterwards the message is
    /// dead-lettered (when <see cref="DeadLetterProducer"/> is set) or acknowledged and dropped. A value below 1 is
    /// rejected by the <see cref="PulsarConsumer"/> constructor.
    /// </summary>
    public int MaxAttempts { get; set; } = DefaultMaxAttempts;

    /// <summary>
    /// A producer bound to the dead-letter topic (by convention <c>&lt;queue&gt;.dlq</c>, e.g.
    /// <c>client.NewProducer(Schema.ByteArray).Topic("orders.dlq").Create()</c>). Setting it turns dead-lettering on;
    /// leaving it <c>null</c> turns it off (default): a message that exhausts <see cref="MaxAttempts"/> is then
    /// acknowledged and dropped (with a trace warning). With dead-lettering on, an exhausted message is written as the
    /// canonical envelope plus the additive <c>dead_letter</c> block (<c>reason</c> <c>failed</c> or
    /// <c>unknown_urn</c>, <c>attempts</c> = the counted attempt), and a poison body is written in the ADR-0035 shape
    /// (<c>reason: "poison"</c>, original bytes preserved) without any retry. The original message is acknowledged only
    /// after the write succeeded; a failed write leaves it for redelivery. A message whose <c>bq-schema-version</c> property
    /// names an unsupported version (ADR-0037 decision 1) is written <b>raw</b> (the incoming body byte for byte, no
    /// <c>dead_letter</c> block) on its first delivery; with this producer unset it is acknowledged and dropped.
    /// </summary>
    public IProducer<byte[]>? DeadLetterProducer { get; set; }

    /// <summary>
    /// A producer bound to the consumer's own topic, used to retry a failed message the Go way (broker-bindings §5.5): the
    /// message is re-published with <c>bq-attempts</c> + 1 (and the body's <c>attempts</c> in sync) and the original is
    /// acknowledged. The attempt count then travels with the message, so <see cref="MaxAttempts"/> bounds the retries on
    /// <b>every</b> subscription type. When set it is used as given (the caller owns and disposes it); when it is not set,
    /// the consumer creates one from <see cref="Client"/>. A republished message loses its original order and any
    /// <c>DeliverAt</c> schedule; a failed publish leaves the original for redelivery.
    /// </summary>
    public IProducer<byte[]>? RetryProducer { get; set; }

    /// <summary>
    /// The client the consumer was built from. When <see cref="RetryProducer"/> is not set, the <see cref="PulsarConsumer"/>
    /// creates the retry producer itself (<c>client.NewProducer(Schema.ByteArray).Topic(consumer.Topic).Create()</c>; it
    /// is disposed by <see cref="PulsarConsumer.DisposeAsync"/>). The consumer's topic must be a single topic.
    /// <para>
    /// With neither a client nor a <see cref="RetryProducer"/> a retry is a negative acknowledgement, which advances
    /// <c>RedeliveryCount</c> only on Shared and Key_Shared subscriptions; the <see cref="PulsarConsumer"/> constructor
    /// therefore throws <see cref="InvalidOperationException"/> for an Exclusive or Failover subscription.
    /// </para>
    /// </summary>
    public IPulsarClient? Client { get; set; }
}
