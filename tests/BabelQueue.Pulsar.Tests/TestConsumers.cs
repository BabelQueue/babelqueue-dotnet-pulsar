using DotPulsar;
using DotPulsar.Abstractions;
using Moq;

namespace BabelQueue.Pulsar.Tests;

internal static class TestConsumers
{
    /// <summary>A mocked consumer on a Shared subscription (the default subscription type of the binding, §5.5).</summary>
    public static Mock<IConsumer<byte[]>> Shared()
    {
        var consumer = new Mock<IConsumer<byte[]>>();
        consumer.SetupGet(c => c.SubscriptionType).Returns(SubscriptionType.Shared);
        return consumer;
    }
}
