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
/// routing on the bq-job property (body fallback), attempts from bq-attempts reconciled with
/// RedeliveryCount, Acknowledge on success, redeliver on failure /
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

    private static Mock<IMessage<byte[]>> Message(
        int redeliveryCount, byte[] body, IReadOnlyDictionary<string, string> properties)
    {
        var message = Message(redeliveryCount, body);
        message.SetupGet(m => m.Properties).Returns(properties);
        return message;
    }

    private static Mock<IConsumer<byte[]>> ConsumerWith(IMessage<byte[]> message)
    {
        var consumer = TestConsumers.Shared();
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
    public async Task BqJobPropertyWinsOverBodyUrn()
    {
        // Body says orders:created, the bq-job property says orders:refunded: the property routes.
        const string other = "urn:babel:orders:refunded";
        var consumer = ConsumerWith(
            Message(0, Body(), new Dictionary<string, string> { ["bq-job"] = other }).Object);
        string? routedTo = null;
        var handlers = new Dictionary<string, BabelHandler>
        {
            [Urn] = (_, _, _) =>
            {
                routedTo = "body";
                return Task.CompletedTask;
            },
            [other] = (_, _, _) =>
            {
                routedTo = "property";
                return Task.CompletedTask;
            },
        };

        await new PulsarConsumer(consumer.Object, handlers).ReceiveAsync();

        Assert.Equal("property", routedTo);
        consumer.Verify(c => c.Acknowledge(It.IsAny<MessageId>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task BqJobPropertyWithoutHandlerIsUnknownEvenIfBodyUrnHasOne()
    {
        var consumer = ConsumerWith(Message(
            0, Body(), new Dictionary<string, string> { ["bq-job"] = "urn:babel:orders:refunded" }).Object);
        Exception? reported = null;
        var handlers = new Dictionary<string, BabelHandler>
        {
            [Urn] = (_, _, _) => throw new InvalidOperationException("must not route on the body"),
        };
        var options = new PulsarConsumerOptions { OnError = (e, _, _) => reported = e };

        await new PulsarConsumer(consumer.Object, handlers, options).ReceiveAsync();

        var unknown = Assert.IsType<UnknownUrnException>(reported);
        Assert.Contains("urn:babel:orders:refunded", unknown.Message);
        consumer.Verify(
            c => c.RedeliverUnacknowledgedMessages(It.IsAny<IEnumerable<MessageId>>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task MissingOrBlankBqJobFallsBackToBodyUrn(bool blank)
    {
        var props = blank
            ? new Dictionary<string, string> { ["bq-job"] = "  " }
            : new Dictionary<string, string> { ["bq-trace-id"] = "t" };
        var consumer = ConsumerWith(Message(0, Body(), props).Object);
        var called = false;

        await new PulsarConsumer(consumer.Object, Handler(_ => called = true)).ReceiveAsync();

        Assert.True(called);
        consumer.Verify(c => c.Acknowledge(It.IsAny<MessageId>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task BqAttemptsPropertyIsAuthoritativeOverBody()
    {
        // Body carries 0, the property says 3, the broker redelivered once: property wins over body.
        var consumer = ConsumerWith(
            Message(1, Body(), new Dictionary<string, string> { ["bq-attempts"] = "3" }).Object);
        var seen = -1;
        await new PulsarConsumer(consumer.Object, Handler(env => seen = env.Attempts)).ReceiveAsync();
        Assert.Equal(3, seen);
    }

    [Fact]
    public async Task BqAttemptsPropertyLowerThanBodyStillWins()
    {
        var consumer = ConsumerWith(
            Message(0, Body(attempts: 5), new Dictionary<string, string> { ["bq-attempts"] = "2" }).Object);
        var seen = -1;
        await new PulsarConsumer(consumer.Object, Handler(env => seen = env.Attempts)).ReceiveAsync();
        Assert.Equal(2, seen);
    }

    [Fact]
    public async Task RedeliveryCountRaisesAHigherNativeCountOverBqAttempts()
    {
        // A native redelivery does not touch bq-attempts; the higher native count is kept.
        var consumer = ConsumerWith(
            Message(4, Body(), new Dictionary<string, string> { ["bq-attempts"] = "1" }).Object);
        var seen = -1;
        await new PulsarConsumer(consumer.Object, Handler(env => seen = env.Attempts)).ReceiveAsync();
        Assert.Equal(4, seen);
    }

    [Theory]
    [InlineData("abc")]
    [InlineData("-1")]
    [InlineData("")]
    [InlineData("+3")]
    [InlineData(" 3")]
    [InlineData("3 ")]
    [InlineData("3\n")]
    [InlineData("\u0663")]
    [InlineData("99999999999")]
    public async Task InvalidBqAttemptsFallsBackToBodyAttempts(string bad)
    {
        var consumer = ConsumerWith(
            Message(0, Body(attempts: 5), new Dictionary<string, string> { ["bq-attempts"] = bad }).Object);
        var seen = -1;
        await new PulsarConsumer(consumer.Object, Handler(env => seen = env.Attempts)).ReceiveAsync();
        Assert.Equal(5, seen);
    }

    [Fact]
    public async Task BqJobBodyJobMismatchCallsRoutingMismatchHookNotOnError()
    {
        const string other = "urn:babel:orders:refunded";
        var consumer = ConsumerWith(
            Message(0, Body(), new Dictionary<string, string> { ["bq-job"] = other }).Object);
        var mismatches = new List<string>();
        var errors = new List<Exception>();
        var called = false;
        var handlers = new Dictionary<string, BabelHandler>
        {
            [other] = (_, _, _) =>
            {
                called = true;
                return Task.CompletedTask;
            },
        };
        var options = new PulsarConsumerOptions
        {
            OnError = (e, _, _) => errors.Add(e),
            OnRoutingMismatch = (property, body, _, _) => mismatches.Add($"{property}|{body}"),
        };

        await new PulsarConsumer(consumer.Object, handlers, options).ReceiveAsync();

        Assert.True(called);
        consumer.Verify(c => c.Acknowledge(It.IsAny<MessageId>(), It.IsAny<CancellationToken>()), Times.Once);
        Assert.Equal(new[] { $"{other}|{Urn}" }, mismatches);
        Assert.Empty(errors);
    }

    [Fact]
    public async Task ThrowingRoutingMismatchHookIsSwallowedAndMessageIsAcknowledged()
    {
        const string other = "urn:babel:orders:refunded";
        var consumer = ConsumerWith(
            Message(0, Body(), new Dictionary<string, string> { ["bq-job"] = other }).Object);
        var errors = new List<Exception>();
        var called = false;
        var handlers = new Dictionary<string, BabelHandler>
        {
            [other] = (_, _, _) =>
            {
                called = true;
                return Task.CompletedTask;
            },
        };
        var options = new PulsarConsumerOptions
        {
            OnError = (e, _, _) => errors.Add(e),
            OnRoutingMismatch = (_, _, _, _) => throw new InvalidOperationException("hook boom"),
        };

        await new PulsarConsumer(consumer.Object, handlers, options).ReceiveAsync();

        Assert.True(called);
        consumer.Verify(c => c.Acknowledge(It.IsAny<MessageId>(), It.IsAny<CancellationToken>()), Times.Once);
        consumer.Verify(
            c => c.RedeliverUnacknowledgedMessages(It.IsAny<IEnumerable<MessageId>>(), It.IsAny<CancellationToken>()),
            Times.Never);
        Assert.Empty(errors);
    }

    [Fact]
    public async Task MismatchWithoutHookIsSilent()
    {
        const string other = "urn:babel:orders:refunded";
        var consumer = ConsumerWith(
            Message(0, Body(), new Dictionary<string, string> { ["bq-job"] = other }).Object);
        var handlers = new Dictionary<string, BabelHandler> { [other] = (_, _, _) => Task.CompletedTask };

        await new PulsarConsumer(consumer.Object, handlers).ReceiveAsync();

        consumer.Verify(c => c.Acknowledge(It.IsAny<MessageId>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task MatchingBqJobDoesNotCallRoutingMismatchHook()
    {
        var consumer = ConsumerWith(
            Message(0, Body(), new Dictionary<string, string> { ["bq-job"] = Urn }).Object);
        var reported = new List<Exception>();
        var options = new PulsarConsumerOptions { OnRoutingMismatch = (p, _, _, _) => reported.Add(new InvalidOperationException(p)) };

        await new PulsarConsumer(consumer.Object, Handler(_ => { }), options).ReceiveAsync();

        Assert.Empty(reported);
    }

    [Fact]
    public async Task BodyWithoutJobIsRejectedEvenWhenBqJobIsPresent()
    {
        const string badBody = "{\"trace_id\":\"t\",\"data\":{\"x\":1},"
            + "\"meta\":{\"id\":\"m\",\"queue\":\"q\",\"lang\":\"dotnet\",\"schema_version\":1,\"created_at\":1},"
            + "\"attempts\":0}";
        var consumer = ConsumerWith(Message(
            0, Encoding.UTF8.GetBytes(badBody), new Dictionary<string, string> { ["bq-job"] = Urn }).Object);
        Exception? reported = null;
        var handlers = new Dictionary<string, BabelHandler>
        {
            [Urn] = (_, _, _) => throw new InvalidOperationException("must not reach the handler"),
        };
        var options = new PulsarConsumerOptions { OnError = (e, _, _) => reported = e };

        await new PulsarConsumer(consumer.Object, handlers, options).ReceiveAsync();

        Assert.IsType<BabelQueueException>(reported);
        consumer.Verify(
            c => c.RedeliverUnacknowledgedMessages(It.IsAny<IEnumerable<MessageId>>(), It.IsAny<CancellationToken>()),
            Times.Once);
        consumer.Verify(c => c.Acknowledge(It.IsAny<MessageId>(), It.IsAny<CancellationToken>()), Times.Never);
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
    public async Task MissingSchemaVersionPropertyDecodesAndDelivers()
    {
        var message = Message(0, Body(), new Dictionary<string, string> { ["bq-job"] = Urn });
        var consumer = ConsumerWith(message.Object);
        var delivered = 0;

        await new PulsarConsumer(consumer.Object, Handler(_ => delivered++)).ReceiveAsync();

        Assert.Equal(1, delivered);
        consumer.Verify(c => c.Acknowledge(It.IsAny<MessageId>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Theory]
    [InlineData("")]
    [InlineData("  ")]
    [InlineData("\t")]
    [InlineData("\u000B")]
    [InlineData("\f")]
    [InlineData("\n")]
    [InlineData("\r")]
    public async Task BlankSchemaVersionPropertyDecodesAndDelivers(string blank)
    {
        // A blank property is treated like a missing one (same rule as Go/Python/Node): not gated.
        var message = Message(
            0, Body(), new Dictionary<string, string> { ["bq-job"] = Urn, ["bq-schema-version"] = blank });
        var consumer = ConsumerWith(message.Object);
        var delivered = 0;

        await new PulsarConsumer(consumer.Object, Handler(_ => delivered++)).ReceiveAsync();

        Assert.Equal(1, delivered);
        consumer.Verify(c => c.Acknowledge(It.IsAny<MessageId>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task SupportedSchemaVersionPropertyDelivers()
    {
        var message = Message(
            0, Body(), new Dictionary<string, string> { ["bq-schema-version"] = EnvelopeCodec.SchemaVersion.ToString() });
        var consumer = ConsumerWith(message.Object);
        var delivered = 0;

        await new PulsarConsumer(consumer.Object, Handler(_ => delivered++)).ReceiveAsync();

        Assert.Equal(1, delivered);
        consumer.Verify(c => c.Acknowledge(It.IsAny<MessageId>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Theory]
    [InlineData("2")]
    [InlineData("x")]
    [InlineData("01")]
    [InlineData("1.0")]
    [InlineData("-1")]
    [InlineData(" 1")]
    [InlineData("1 ")]
    [InlineData("\t1")]
    [InlineData("\u00A0")]
    [InlineData("\u001C")]
    [InlineData("\u0085")]
    [InlineData("\uFEFF")]
    public async Task UnknownSchemaVersionPropertyIsRejectedBeforeDecoding(string version)
    {
        // The body is invalid JSON: if it were decoded the failure would be the generic
        // non-conformant-envelope rejection, not the version-gate message.
        var message = Message(
            0, Encoding.UTF8.GetBytes("{ this is not json"),
            new Dictionary<string, string> { ["bq-schema-version"] = version });
        var consumer = ConsumerWith(message.Object);
        Exception? reported = null;
        Envelope? reportedEnvelope = null;
        var delivered = 0;
        var options = new PulsarConsumerOptions
        {
            OnError = (e, env, _) =>
            {
                reported = e;
                reportedEnvelope = env;
            },
        };

        await new PulsarConsumer(consumer.Object, Handler(_ => delivered++), options).ReceiveAsync();

        Assert.Equal(0, delivered);
        Assert.IsType<BabelQueueException>(reported);
        Assert.Contains("bq-schema-version", reported!.Message);
        Assert.Contains($"'{version}'", reported.Message);
        Assert.Null(reportedEnvelope!.Job);
        message.Verify(m => m.Value(), Times.Never);
        consumer.Verify(
            c => c.RedeliverUnacknowledgedMessages(It.IsAny<IEnumerable<MessageId>>(), It.IsAny<CancellationToken>()),
            Times.Once);
        consumer.Verify(c => c.Acknowledge(It.IsAny<MessageId>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task UnknownSchemaVersionWithValidBodyIsStillRejectedAndNeverRouted()
    {
        var message = Message(
            0, Body(), new Dictionary<string, string> { ["bq-job"] = Urn, ["bq-schema-version"] = "2" });
        var consumer = ConsumerWith(message.Object);
        var delivered = 0;

        await new PulsarConsumer(consumer.Object, Handler(_ => delivered++)).ReceiveAsync();

        Assert.Equal(0, delivered);
        consumer.Verify(
            c => c.RedeliverUnacknowledgedMessages(It.IsAny<IEnumerable<MessageId>>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task SupportedSchemaVersionPropertyStillRunsPostDecodeAccepts()
    {
        // Property says "1" but the body is not a conformant envelope: Accepts still rejects it.
        var message = Message(
            0, Encoding.UTF8.GetBytes("{ this is not json"),
            new Dictionary<string, string> { ["bq-schema-version"] = "1" });
        var consumer = ConsumerWith(message.Object);
        Exception? reported = null;
        var options = new PulsarConsumerOptions { OnError = (e, _, _) => reported = e };

        await new PulsarConsumer(consumer.Object, new Dictionary<string, BabelHandler>(), options).ReceiveAsync();

        Assert.IsType<BabelQueueException>(reported);
        Assert.Contains("non-conformant", reported!.Message);
        consumer.Verify(
            c => c.RedeliverUnacknowledgedMessages(It.IsAny<IEnumerable<MessageId>>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task RunStopsWhenCancelled()
    {
        var consumer = TestConsumers.Shared();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await new PulsarConsumer(consumer.Object, new Dictionary<string, BabelHandler>()).RunAsync(cts.Token);

        consumer.Verify(c => c.Receive(It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task RunBreaksWhenReceiveIsCancelled()
    {
        var consumer = TestConsumers.Shared();
        consumer.Setup(c => c.Receive(It.IsAny<CancellationToken>())).Throws(new OperationCanceledException());

        await new PulsarConsumer(consumer.Object, new Dictionary<string, BabelHandler>()).RunAsync(CancellationToken.None);

        consumer.Verify(c => c.Receive(It.IsAny<CancellationToken>()), Times.AtLeastOnce);
    }
}
