using System.Diagnostics;
using System.Text;
using System.Text.Json;
using BabelQueue;
using BabelQueue.Tracing;
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

    // ADR-0028: a W3C traceparent rides as a message property beside the envelope, never inside it.
    private const string Traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    private const string Tracestate = "vendor=opaque,other=1";

    private static Dictionary<string, string> TraceHeaders() => new()
    {
        ["traceparent"] = Traceparent,
        ["tracestate"] = Tracestate,
    };

    private static HashSet<string> TopLevelKeys(byte[] body)
    {
        using var doc = JsonDocument.Parse(body);
        var keys = new HashSet<string>(StringComparer.Ordinal);
        foreach (var property in doc.RootElement.EnumerateObject())
        {
            keys.Add(property.Name);
            if (property is { Name: "meta", Value.ValueKind: JsonValueKind.Object })
            {
                foreach (var inner in property.Value.EnumerateObject())
                {
                    keys.Add("meta." + inner.Name);
                }
            }
        }
        return keys;
    }

    /// <summary>The message a consumer receives for a sent one: the properties the test knows were set.</summary>
    private static Mock<IMessage<byte[]>> Received(MessageMetadata sent, byte[] body, params string[] keys)
    {
        var properties = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var key in keys)
        {
            if (sent[key] is { } value)
            {
                properties[key] = value;
            }
        }

        var message = new Mock<IMessage<byte[]>>();
        message.Setup(m => m.Value()).Returns(body);
        message.SetupGet(m => m.Properties).Returns(properties);
        return message;
    }

    [Fact]
    public async Task PublishWithHeadersCarriesTraceparentAndLeavesTheBodyAnEnvelope()
    {
        var sent = new List<(MessageMetadata Meta, byte[] Body)>();
        var producer = Producer("orders", (m, b) => sent.Add((m, b)));
        var publisher = new PulsarPublisher(producer.Object);

        await publisher.PublishAsync("urn:babel:orders:created", new Dictionary<string, object?> { ["order_id"] = 7 }, "trace-1");
        await publisher.PublishWithHeadersAsync(
            "urn:babel:orders:created", new Dictionary<string, object?> { ["order_id"] = 7 }, TraceHeaders(), "trace-1");

        var (meta, body) = sent[1];
        var received = Received(meta, body, "bq-job", "traceparent", "tracestate");

        // The received message carries the header verbatim.
        Assert.Equal(Traceparent, received.Object.Properties["traceparent"]);
        Assert.Equal(Tracestate, received.Object.Properties["tracestate"]);
        Assert.Equal(Traceparent, PulsarTraceHeaders.Extract(received.Object)["traceparent"]);

        // The body is the same envelope shape and never mentions the trace header.
        var text = Encoding.UTF8.GetString(body);
        Assert.DoesNotContain("traceparent", text, StringComparison.Ordinal);
        Assert.DoesNotContain(Traceparent, text, StringComparison.Ordinal);
        Assert.Equal(TopLevelKeys(sent[0].Body), TopLevelKeys(body));
        Assert.Equal("urn:babel:orders:created", EnvelopeCodec.Urn(EnvelopeCodec.Decode(text)));

        // The contract projection is untouched.
        Assert.Equal("urn:babel:orders:created", meta["bq-job"]);
        Assert.Equal("trace-1", meta["bq-trace-id"]);
    }

    [Fact]
    public async Task PublishWithoutHeadersAddsNoTraceProperties()
    {
        var sent = new List<MessageMetadata>();
        var producer = Producer("orders", (m, _) => sent.Add(m));
        var publisher = new PulsarPublisher(producer.Object);

        await publisher.PublishAsync("urn:babel:orders:created");
        await publisher.PublishWithHeadersAsync("urn:babel:orders:created", null, new Dictionary<string, string>());
        await publisher.PublishWithHeadersAsync("urn:babel:orders:created", null, null);

        foreach (var meta in sent)
        {
            Assert.True(string.IsNullOrEmpty(meta["traceparent"]));
            Assert.True(string.IsNullOrEmpty(meta["tracestate"]));
        }
    }

    [Fact]
    public async Task PublishWithHeadersNeverClobbersAContractProperty()
    {
        MessageMetadata? meta = null;
        var producer = Producer("orders", (m, _) => meta = m);

        await new PulsarPublisher(producer.Object).PublishWithHeadersAsync(
            "urn:babel:orders:created",
            null,
            new Dictionary<string, string> { ["bq-job"] = "urn:evil", ["bq-attempts"] = "9", ["bq-delay"] = "1", [""] = "x" },
            delay: TimeSpan.FromSeconds(30));

        Assert.Equal("urn:babel:orders:created", meta!["bq-job"]);
        Assert.Equal("0", meta["bq-attempts"]);
        Assert.Equal("30000", meta["bq-delay"]);
    }

    [Fact]
    public async Task ConsumerSpanIsATrueChildOfTheProducerSpanViaTheReceivedProperties()
    {
        var sent = new List<(MessageMetadata Meta, byte[] Body)>();
        var producer = Producer("orders", (m, b) => sent.Add((m, b)));
        var producerTrace = ActivityTraceId.CreateFromString(Traceparent.AsSpan(3, 32));

        await new PulsarPublisher(producer.Object).PublishWithHeadersAsync("urn:babel:orders:created", null, TraceHeaders(), "trace-1");
        var received = Received(sent[0].Meta, sent[0].Body, "traceparent", "tracestate");
        var envelope = EnvelopeCodec.Decode(Encoding.UTF8.GetString(sent[0].Body));

        Activity? consumerSpan = null;
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == Telemetry.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStarted = activity =>
            {
                if (activity.Kind == ActivityKind.Consumer && activity.TraceId == producerTrace)
                {
                    consumerSpan = activity;
                }
            },
        };
        ActivitySource.AddActivityListener(listener);

        await Telemetry.Wrap(_ => Task.CompletedTask, PulsarTraceHeaders.Extract(received.Object))(envelope);

        Assert.NotNull(consumerSpan);
        Assert.Equal("b7ad6b7169203331", consumerSpan!.ParentSpanId.ToHexString());
    }

    [Fact]
    public void ExtractOfNoPropertiesIsEmpty()
    {
        Assert.Empty(PulsarTraceHeaders.Extract((IReadOnlyDictionary<string, string>?)null));
        Assert.Empty(PulsarTraceHeaders.Extract(new Dictionary<string, string>()));
    }

    [Fact]
    public async Task PublishWithHeadersDropsEveryContractPropertyIncludingBqDelay()
    {
        // Parity with the Go reference: contract properties are owned by the transport and never accepted from
        // the carrier, even bq-delay on a publish that has no delay.
        MessageMetadata? meta = null;
        var producer = Producer("orders", (m, _) => meta = m);
        var carrier = new Dictionary<string, string> { ["traceparent"] = Traceparent };
        foreach (var key in new[] { "bq-job", "bq-trace-id", "bq-message-id", "bq-schema-version", "bq-source-lang", "bq-attempts", "bq-delay" })
        {
            carrier[key] = "evil";
        }

        await new PulsarPublisher(producer.Object).PublishWithHeadersAsync("urn:babel:orders:created", null, carrier, "trace-1");

        Assert.True(string.IsNullOrEmpty(meta!["bq-delay"]));
        Assert.Equal("urn:babel:orders:created", meta["bq-job"]);
        Assert.Equal("trace-1", meta["bq-trace-id"]);
        Assert.Equal("0", meta["bq-attempts"]);
        Assert.Equal("1", meta["bq-schema-version"]);
        Assert.Equal(Traceparent, meta["traceparent"]);
    }

    [Fact]
    public void ExtractLeavesOutTheContractProperties()
    {
        var map = PulsarTraceHeaders.Extract(new Dictionary<string, string>
        {
            ["bq-job"] = "urn:babel:orders:created",
            ["bq-delay"] = "30000",
            ["bq-attempts"] = "0",
            ["traceparent"] = Traceparent,
            ["tracestate"] = Tracestate,
        });

        Assert.Equal(new[] { "traceparent", "tracestate" }, new SortedSet<string>(map.Keys, StringComparer.Ordinal));
    }
}
