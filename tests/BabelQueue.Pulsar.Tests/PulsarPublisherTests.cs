using System.Text;
using BabelQueue;
using BabelQueue.Pulsar;
using DotPulsar;
using DotPulsar.Abstractions;
using Moq;
using Xunit;

namespace BabelQueue.Pulsar.Tests;

/// <summary>The publisher projects the envelope onto Pulsar properties — against a mocked producer (no broker).</summary>
public sealed class PulsarPublisherTests
{
    private static Mock<IProducer<byte[]>> Producer(string topic, Action<MessageMetadata, byte[]> capture)
    {
        var producer = new Mock<IProducer<byte[]>>();
        producer.SetupGet(p => p.Topic).Returns(topic);
        producer
            .Setup(p => p.Send(It.IsAny<MessageMetadata>(), It.IsAny<byte[]>(), It.IsAny<CancellationToken>()))
            .Callback<MessageMetadata, byte[], CancellationToken>((m, b, _) => capture(m, b))
            .Returns(new ValueTask<MessageId>(new MessageId(0, 0, 0, 0, topic)));
        return producer;
    }

    [Fact]
    public async Task PublishProjectsPropertiesAndReturnsMessageId()
    {
        MessageMetadata? meta = null;
        byte[]? body = null;
        var producer = Producer("persistent://public/default/orders", (m, b) => { meta = m; body = b; });

        var id = await new PulsarPublisher(producer.Object)
            .PublishAsync("urn:babel:orders:created", new Dictionary<string, object?> { ["order_id"] = 7 }, "trace-1");

        Assert.NotNull(meta);
        Assert.Equal("urn:babel:orders:created", meta!["bq-job"]);
        Assert.Equal("trace-1", meta["bq-trace-id"]);
        Assert.Equal(id, meta["bq-message-id"]);
        Assert.Equal("1", meta["bq-schema-version"]);
        Assert.Equal("0", meta["bq-attempts"]);

        var decoded = EnvelopeCodec.Decode(Encoding.UTF8.GetString(body!));
        Assert.True(EnvelopeCodec.Accepts(decoded));
        Assert.Equal("urn:babel:orders:created", EnvelopeCodec.Urn(decoded));

        producer.Verify(
            p => p.Send(It.IsAny<MessageMetadata>(), It.IsAny<byte[]>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task PublishWithDelaySetsDeliverAtAndBqDelay()
    {
        MessageMetadata? meta = null;
        var producer = Producer("orders", (m, _) => meta = m);
        var before = DateTimeOffset.UtcNow;

        await new PulsarPublisher(producer.Object)
            .PublishAsync("urn:babel:orders:created", null, null, TimeSpan.FromSeconds(30));

        Assert.NotNull(meta);
        Assert.True(meta!.DeliverAtTimeAsDateTimeOffset >= before.AddSeconds(29));
        Assert.Equal("30000", meta["bq-delay"]);
    }

    [Fact]
    public async Task PublishWithoutTraceIdMintsAFreshTrace()
    {
        MessageMetadata? meta = null;
        var producer = Producer("orders", (m, _) => meta = m);

        var id = await new PulsarPublisher(producer.Object)
            .PublishAsync("urn:babel:orders:created", new Dictionary<string, object?> { ["order_id"] = 7 });

        Assert.NotNull(meta);
        Assert.Equal(id, meta!["bq-message-id"]);
        Assert.False(string.IsNullOrEmpty(meta["bq-trace-id"]));
    }
}
