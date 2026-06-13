using System.Text;
using BabelQueue;
using BabelQueue.Pulsar;
using DotPulsar;
using DotPulsar.Abstractions;
using Moq;
using Xunit;

namespace BabelQueue.Pulsar.Tests;

/// <summary>
/// Consumer behaviour against a mocked Pulsar consumer (no broker): attempts =
/// max(body, RedeliveryCount), Acknowledge on success, redeliver on failure /
/// non-conformant / unmapped URN, and the unknown-URN hooks.
/// </summary>
public sealed class PulsarConsumerTests
{
    private const string Urn = "urn:babel:orders:created";

    private static byte[] Body(int attempts = 0)
    {
        var env = EnvelopeCodec.Make(Urn, new Dictionary<string, object?> { ["order_id"] = 1 }, "orders", null);
        if (attempts > 0)
        {
            env = env with { Attempts = attempts };
        }

        return Encoding.UTF8.GetBytes(EnvelopeCodec.Encode(env));
    }

    private static Mock<IMessage<byte[]>> Message(int redeliveryCount, byte[] body)
    {
        var message = new Mock<IMessage<byte[]>>();
        message.Setup(m => m.Value()).Returns(body);
        message.SetupGet(m => m.RedeliveryCount).Returns((uint)redeliveryCount);
        message.SetupGet(m => m.MessageId).Returns(new MessageId(0, 0, 0, 0, "orders"));
        return message;
    }

    private static Mock<IConsumer<byte[]>> ConsumerWith(IMessage<byte[]> message)
    {
        var consumer = new Mock<IConsumer<byte[]>>();
        consumer.Setup(c => c.Receive(It.IsAny<CancellationToken>()))
            .Returns(new ValueTask<IMessage<byte[]>>(message));
        return consumer;
    }

    private static Dictionary<string, BabelHandler> Handler(Action<Envelope> onHandle) =>
        new()
        {
            [Urn] = (env, _, _) =>
            {
                onHandle(env);
                return Task.CompletedTask;
            },
        };

    [Fact]
    public async Task AttemptsIsRedeliveryCountAndAcknowledges()
    {
        var message = Message(2, Body());
        var consumer = ConsumerWith(message.Object);
        var seen = -1;

        await new PulsarConsumer(consumer.Object, Handler(env => seen = env.Attempts)).ReceiveAsync();

        Assert.Equal(2, seen);
        consumer.Verify(c => c.Acknowledge(It.IsAny<MessageId>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task FirstDeliveryIsZeroAttempts()
    {
        var consumer = ConsumerWith(Message(0, Body()).Object);
        var seen = -1;
        await new PulsarConsumer(consumer.Object, Handler(env => seen = env.Attempts)).ReceiveAsync();
        Assert.Equal(0, seen);
    }

    [Fact]
    public async Task BodyAttemptsAreNeverLoweredByRedeliveryCount()
    {
        // Republish-driven retry carried attempts=5 in the body; redelivery count is only 1.
        var consumer = ConsumerWith(Message(1, Body(attempts: 5)).Object);
        var seen = -1;
        await new PulsarConsumer(consumer.Object, Handler(env => seen = env.Attempts)).ReceiveAsync();
        Assert.Equal(5, seen);
    }

    [Fact]
    public async Task ThrowingHandlerRedeliversAndReportsOnError()
    {
        var message = Message(0, Body());
        var consumer = ConsumerWith(message.Object);
        Exception? reported = null;
        var handlers = new Dictionary<string, BabelHandler>
        {
            [Urn] = (_, _, _) => throw new InvalidOperationException("boom"),
        };
        var options = new PulsarConsumerOptions { OnError = (e, _, _) => reported = e };

        await new PulsarConsumer(consumer.Object, handlers, options).ReceiveAsync();

        Assert.IsType<InvalidOperationException>(reported);
        consumer.Verify(
            c => c.RedeliverUnacknowledgedMessages(It.IsAny<IEnumerable<MessageId>>(), It.IsAny<CancellationToken>()),
            Times.Once);
        consumer.Verify(c => c.Acknowledge(It.IsAny<MessageId>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task UnknownUrnWithHookAcknowledges()
    {
        var consumer = ConsumerWith(Message(0, Body()).Object);
        var called = false;
        var options = new PulsarConsumerOptions
        {
            OnUnknownUrn = (_, _, _) =>
            {
                called = true;
                return Task.CompletedTask;
            },
        };

        await new PulsarConsumer(consumer.Object, new Dictionary<string, BabelHandler>(), options).ReceiveAsync();

        Assert.True(called);
        consumer.Verify(c => c.Acknowledge(It.IsAny<MessageId>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task UnknownUrnWithoutHookRedeliversAndReportsOnError()
    {
        var consumer = ConsumerWith(Message(0, Body()).Object);
        Exception? reported = null;
        var options = new PulsarConsumerOptions { OnError = (e, _, _) => reported = e };

        await new PulsarConsumer(consumer.Object, new Dictionary<string, BabelHandler>(), options).ReceiveAsync();

        Assert.IsType<UnknownUrnException>(reported);
        consumer.Verify(
            c => c.RedeliverUnacknowledgedMessages(It.IsAny<IEnumerable<MessageId>>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task NonConformantEnvelopeRedeliversAndReportsOnError()
    {
        const string badJson = "{\"trace_id\":\"t\",\"data\":{\"x\":1}," +
            "\"meta\":{\"id\":\"m\",\"queue\":\"q\",\"lang\":\"dotnet\",\"schema_version\":1,\"created_at\":1}," +
            "\"attempts\":0}";
        var consumer = ConsumerWith(Message(0, Encoding.UTF8.GetBytes(badJson)).Object);
        Exception? reported = null;
        var options = new PulsarConsumerOptions { OnError = (e, _, _) => reported = e };

        await new PulsarConsumer(consumer.Object, new Dictionary<string, BabelHandler>(), options).ReceiveAsync();

        Assert.IsType<BabelQueueException>(reported);
        consumer.Verify(
            c => c.RedeliverUnacknowledgedMessages(It.IsAny<IEnumerable<MessageId>>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task RunStopsWhenCancelled()
    {
        var consumer = new Mock<IConsumer<byte[]>>();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await new PulsarConsumer(consumer.Object, new Dictionary<string, BabelHandler>()).RunAsync(cts.Token);

        consumer.Verify(c => c.Receive(It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task RunBreaksWhenReceiveIsCancelled()
    {
        var consumer = new Mock<IConsumer<byte[]>>();
        consumer.Setup(c => c.Receive(It.IsAny<CancellationToken>())).Throws(new OperationCanceledException());

        await new PulsarConsumer(consumer.Object, new Dictionary<string, BabelHandler>()).RunAsync(CancellationToken.None);

        consumer.Verify(c => c.Receive(It.IsAny<CancellationToken>()), Times.AtLeastOnce);
    }
}
