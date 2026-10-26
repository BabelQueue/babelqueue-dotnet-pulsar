using System.Text;
using System.Text.Json;
using BabelQueue;
using BabelQueue.Pulsar;
using DotPulsar;
using DotPulsar.Abstractions;
using Moq;
using Xunit;

namespace BabelQueue.Pulsar.Tests;

/// <summary>
/// ADR-0036 decisions 1, 2 and 4 against a mocked Pulsar consumer (no broker): the SDK-owned max attempts, the
/// <c>&lt;queue&gt;.dlq</c> write, the drop with dead-lettering off, and the poison route for a body that never reaches
/// a handler.
/// </summary>
public sealed class PulsarSettleTests
{
    private const string Urn = "urn:babel:orders:created";

    private sealed record Sent(MessageMetadata Metadata, byte[] Payload)
    {
        public JsonElement Json => JsonDocument.Parse(Payload).RootElement.Clone();

        public JsonElement DeadLetter => Json.GetProperty("dead_letter");
    }

    private static byte[] Body(int attempts = 0)
    {
        var env = EnvelopeCodec.Make(Urn, new Dictionary<string, object?> { ["order_id"] = 1 }, "orders", null);
        if (attempts > 0)
        {
            env = env with { Attempts = attempts };
        }

        return Encoding.UTF8.GetBytes(EnvelopeCodec.Encode(env));
    }

    private static Mock<IMessage<byte[]>> Message(
        byte[] body,
        int redeliveryCount = 0,
        IReadOnlyDictionary<string, string>? properties = null,
        string topic = "persistent://public/default/orders")
    {
        var message = new Mock<IMessage<byte[]>>();
        message.Setup(m => m.Value()).Returns(body);
        message.SetupGet(m => m.RedeliveryCount).Returns((uint)redeliveryCount);
        message.SetupGet(m => m.MessageId).Returns(new MessageId(0, 0, 0, 0, topic));
        message.SetupGet(m => m.Properties).Returns(properties ?? new Dictionary<string, string>());
        return message;
    }

    private static Mock<IConsumer<byte[]>> ConsumerWith(IMessage<byte[]> message)
    {
        var consumer = TestConsumers.Shared();
        consumer.Setup(c => c.Receive(It.IsAny<CancellationToken>()))
            .Returns(new ValueTask<IMessage<byte[]>>(message));
        return consumer;
    }

    private static Mock<IProducer<byte[]>> Dlq(List<Sent> sent, Exception? failWith = null)
    {
        var producer = new Mock<IProducer<byte[]>>();
        producer.SetupGet(p => p.Topic).Returns("persistent://public/default/orders.dlq");
        var setup = producer.Setup(p => p.Send(It.IsAny<MessageMetadata>(), It.IsAny<byte[]>(), It.IsAny<CancellationToken>()));
        if (failWith is not null)
        {
            setup.Throws(failWith);
        }
        else
        {
            setup
                .Callback<MessageMetadata, byte[], CancellationToken>((m, b, _) => sent.Add(new Sent(m, b)))
                .Returns(new ValueTask<MessageId>(new MessageId(0, 0, 0, 0, "orders.dlq")));
        }

        return producer;
    }

    private static Dictionary<string, BabelHandler> Failing(Action? onCall = null) =>
        new()
        {
            [Urn] = (_, _, _) =>
            {
                onCall?.Invoke();
                throw new InvalidOperationException("boom");
            },
        };

    private static Dictionary<string, BabelHandler> Counting(Action onCall) =>
        new() { [Urn] = (_, _, _) => { onCall(); return Task.CompletedTask; } };

    private static void VerifyRedelivered(Mock<IConsumer<byte[]>> consumer, Times times) =>
        consumer.Verify(
            c => c.RedeliverUnacknowledgedMessages(It.IsAny<IEnumerable<MessageId>>(), It.IsAny<CancellationToken>()), times);

    private static void VerifyAcked(Mock<IConsumer<byte[]>> consumer, Times times) =>
        consumer.Verify(c => c.Acknowledge(It.IsAny<MessageId>(), It.IsAny<CancellationToken>()), times);

    // -- decision 1: bounded redelivery --------------------------------------------------------------------------

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    public async Task FailingHandlerBelowTheCapIsRedeliveredNotDeadLettered(int redeliveryCount)
    {
        var sent = new List<Sent>();
        var consumer = ConsumerWith(Message(Body(), redeliveryCount).Object);
        var options = new PulsarConsumerOptions { DeadLetterProducer = Dlq(sent).Object };

        await new PulsarConsumer(consumer.Object, Failing(), options).ReceiveAsync();

        VerifyRedelivered(consumer, Times.Once());
        VerifyAcked(consumer, Times.Never());
        Assert.Empty(sent);
    }

    [Fact]
    public async Task FailingHandlerOnTheThirdDeliveryIsDeadLettered()
    {
        var sent = new List<Sent>();
        var consumer = ConsumerWith(Message(Body(), 2, new Dictionary<string, string>
        {
            ["bq-trace-id"] = "orig-trace",
        }).Object);
        var options = new PulsarConsumerOptions { DeadLetterProducer = Dlq(sent).Object };

        await new PulsarConsumer(consumer.Object, Failing(), options).ReceiveAsync();

        var record = Assert.Single(sent);
        Assert.Equal("failed", record.DeadLetter.GetProperty("reason").GetString());
        Assert.Equal(3, record.DeadLetter.GetProperty("attempts").GetInt32());
        Assert.Equal("orders", record.DeadLetter.GetProperty("original_queue").GetString());
        Assert.Equal("dotnet", record.DeadLetter.GetProperty("lang").GetString());
        Assert.Equal("boom", record.DeadLetter.GetProperty("error").GetString());
        Assert.Equal("System.InvalidOperationException", record.DeadLetter.GetProperty("exception").GetString());
        Assert.Equal(Urn, record.Json.GetProperty("job").GetString());
        Assert.Equal(3, record.Json.GetProperty("attempts").GetInt32());
        Assert.Equal(Urn, record.Metadata["bq-job"]);
        Assert.Equal("3", record.Metadata["bq-attempts"]);
        VerifyAcked(consumer, Times.Once());
        VerifyRedelivered(consumer, Times.Never());
    }

    [Fact]
    public async Task FailedDeadLetterRecordKeepsTheRoutedBqJobWhenItDiffersFromTheBodyUrn()
    {
        // bq-job routes (§5.2/§5.7), the body says another URN: the DLQ record must keep the route (Go parity), or a redrive
        // would run another handler than the one that failed.
        var sent = new List<Sent>();
        var consumer = ConsumerWith(Message(Body(), 2, new Dictionary<string, string>
        {
            ["bq-job"] = "urn:babel:other:routed",
            ["x-custom"] = "keep",
        }).Object);
        var handlers = new Dictionary<string, BabelHandler>
        {
            ["urn:babel:other:routed"] = (_, _, _) => throw new InvalidOperationException("boom"),
        };

        await new PulsarConsumer(consumer.Object, handlers, new PulsarConsumerOptions { DeadLetterProducer = Dlq(sent).Object }).ReceiveAsync();

        var record = Assert.Single(sent);
        Assert.Equal("failed", record.DeadLetter.GetProperty("reason").GetString());
        Assert.Equal("urn:babel:other:routed", record.Metadata["bq-job"]);
        Assert.Equal("keep", record.Metadata["x-custom"]);
        Assert.Equal("3", record.Metadata["bq-attempts"]);
        Assert.Equal(Urn, record.Json.GetProperty("job").GetString());
    }

    [Fact]
    public async Task UnknownUrnDeadLetterRecordKeepsTheRoutedBqJobWhenItDiffersFromTheBodyUrn()
    {
        var sent = new List<Sent>();
        var consumer = ConsumerWith(Message(Body(), 2, new Dictionary<string, string> { ["bq-job"] = "urn:babel:unmapped:routed" }).Object);

        await new PulsarConsumer(
            consumer.Object,
            Counting(() => { }),
            new PulsarConsumerOptions { DeadLetterProducer = Dlq(sent).Object }).ReceiveAsync();

        var record = Assert.Single(sent);
        Assert.Equal("unknown_urn", record.DeadLetter.GetProperty("reason").GetString());
        Assert.Equal("urn:babel:unmapped:routed", record.Metadata["bq-job"]);
    }

    [Fact]
    public async Task DeadLetterRecordFallsBackToTheBodyUrnWhenThereIsNoBqJobProperty()
    {
        var sent = new List<Sent>();
        var consumer = ConsumerWith(Message(Body(), 2).Object);

        await new PulsarConsumer(consumer.Object, Failing(), new PulsarConsumerOptions { DeadLetterProducer = Dlq(sent).Object }).ReceiveAsync();

        Assert.Equal(Urn, Assert.Single(sent).Metadata["bq-job"]);
    }

    [Fact]
    public async Task FailingAcknowledgeAfterASuccessfulHandlerIsReportedNotTreatedAsAHandlerFailure()
    {
        var sent = new List<Sent>();
        var consumer = ConsumerWith(Message(Body(), 2).Object);
        consumer.Setup(c => c.Acknowledge(It.IsAny<MessageId>(), It.IsAny<CancellationToken>()))
            .Throws(new InvalidOperationException("ack failed"));
        var reported = new List<Exception>();
        var options = new PulsarConsumerOptions { DeadLetterProducer = Dlq(sent).Object, OnError = (e, _, _) => reported.Add(e) };

        await new PulsarConsumer(consumer.Object, Counting(() => { }), options).ReceiveAsync();

        Assert.Empty(sent);
        VerifyRedelivered(consumer, Times.Never());
        var error = Assert.Single(reported);
        Assert.Contains("Acknowledge failed", error.Message, StringComparison.Ordinal);
        Assert.Equal("ack failed", error.InnerException!.Message);
    }

    [Fact]
    public async Task RunKeepsPollingAfterAFailedAcknowledge()
    {
        using var cts = new CancellationTokenSource();
        var first = Message(Body()).Object;
        var second = Message(Body()).Object;
        var queue = new Queue<IMessage<byte[]>>(new[] { first, second });
        var consumer = TestConsumers.Shared();
        consumer.Setup(c => c.Receive(It.IsAny<CancellationToken>()))
            .Returns(() => new ValueTask<IMessage<byte[]>>(queue.Dequeue()));
        var acks = 0;
        consumer.Setup(c => c.Acknowledge(It.IsAny<MessageId>(), It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                if (++acks == 1)
                {
                    throw new InvalidOperationException("transient");
                }

                cts.Cancel();
                return ValueTask.CompletedTask;
            });
        var handled = 0;

        await new PulsarConsumer(consumer.Object, Counting(() => handled++)).RunAsync(cts.Token);

        Assert.Equal(2, handled);
        Assert.Equal(2, acks);
    }

    [Fact]
    public async Task ExhaustedWithDeadLetteringOffAcknowledgesAndDrops()
    {
        var consumer = ConsumerWith(Message(Body(), 2).Object);
        var calls = 0;

        await new PulsarConsumer(consumer.Object, Failing(() => calls++)).ReceiveAsync();

        Assert.Equal(1, calls);
        VerifyAcked(consumer, Times.Once());
        VerifyRedelivered(consumer, Times.Never());
    }

    [Fact]
    public async Task EveryDeliveryOfAPersistentlyFailingMessageIsBounded()
    {
        // Simulate the broker: redeliver until the message is acknowledged, with RedeliveryCount advancing each time.
        var sent = new List<Sent>();
        var deliveries = 0;
        var settled = false;
        var count = 0;
        var message = new Mock<IMessage<byte[]>>();
        message.Setup(m => m.Value()).Returns(Body());
        message.SetupGet(m => m.MessageId).Returns(new MessageId(0, 0, 0, 0, "orders"));
        message.SetupGet(m => m.Properties).Returns(new Dictionary<string, string>());
        message.SetupGet(m => m.RedeliveryCount).Returns(() => (uint)count);
        var consumer = TestConsumers.Shared();
        consumer.Setup(c => c.Receive(It.IsAny<CancellationToken>()))
            .Returns(() => new ValueTask<IMessage<byte[]>>(message.Object));
        consumer.Setup(c => c.RedeliverUnacknowledgedMessages(It.IsAny<IEnumerable<MessageId>>(), It.IsAny<CancellationToken>()))
            .Returns(() => { count++; return ValueTask.CompletedTask; });
        consumer.Setup(c => c.Acknowledge(It.IsAny<MessageId>(), It.IsAny<CancellationToken>()))
            .Returns(() => { settled = true; return ValueTask.CompletedTask; });
        var babel = new PulsarConsumer(
            consumer.Object,
            Failing(() => deliveries++),
            new PulsarConsumerOptions { DeadLetterProducer = Dlq(sent).Object });

        for (var i = 0; i < 10 && !settled; i++)
        {
            await babel.ReceiveAsync();
        }

        Assert.True(settled);
        Assert.Equal(3, deliveries);
        Assert.Single(sent);
    }

    [Fact]
    public async Task MaxAttemptsOneDeadLettersTheFirstFailure()
    {
        var sent = new List<Sent>();
        var consumer = ConsumerWith(Message(Body()).Object);
        var options = new PulsarConsumerOptions { MaxAttempts = 1, DeadLetterProducer = Dlq(sent).Object };

        await new PulsarConsumer(consumer.Object, Failing(), options).ReceiveAsync();

        Assert.Equal(1, Assert.Single(sent).DeadLetter.GetProperty("attempts").GetInt32());
        VerifyAcked(consumer, Times.Once());
    }

    [Fact]
    public async Task CustomMaxAttemptsRaisesTheCap()
    {
        var consumer = ConsumerWith(Message(Body(), 3).Object);

        await new PulsarConsumer(consumer.Object, Failing(), new PulsarConsumerOptions { MaxAttempts = 5 }).ReceiveAsync();

        VerifyRedelivered(consumer, Times.Once());
        VerifyAcked(consumer, Times.Never());
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void MaxAttemptsBelowOneIsAConfigurationError(int maxAttempts)
    {
        var consumer = TestConsumers.Shared();

        Assert.Throws<ArgumentOutOfRangeException>(() => new PulsarConsumer(
            consumer.Object,
            new Dictionary<string, BabelHandler>(),
            new PulsarConsumerOptions { MaxAttempts = maxAttempts }));
    }

    [Fact]
    public void MaxAttemptsDefaultsToThree()
    {
        Assert.Equal(3, new PulsarConsumerOptions().MaxAttempts);
        Assert.Null(new PulsarConsumerOptions().DeadLetterProducer);
    }

    [Fact]
    public async Task BqAttemptsPropertyCountsTowardsTheCap()
    {
        var sent = new List<Sent>();
        var consumer = ConsumerWith(Message(Body(), 0, new Dictionary<string, string> { ["bq-attempts"] = "2" }).Object);
        var options = new PulsarConsumerOptions { DeadLetterProducer = Dlq(sent).Object };

        await new PulsarConsumer(consumer.Object, Failing(), options).ReceiveAsync();

        Assert.Equal(3, Assert.Single(sent).DeadLetter.GetProperty("attempts").GetInt32());
    }

    [Fact]
    public async Task AttemptsAtIntMaxAreSaturatedAndExhausted()
    {
        var sent = new List<Sent>();
        var consumer = ConsumerWith(Message(
            Body(), 0, new Dictionary<string, string> { ["bq-attempts"] = int.MaxValue.ToString(System.Globalization.CultureInfo.InvariantCulture) }).Object);
        var options = new PulsarConsumerOptions { DeadLetterProducer = Dlq(sent).Object };

        await new PulsarConsumer(consumer.Object, Failing(), options).ReceiveAsync();

        Assert.Equal(int.MaxValue, Assert.Single(sent).DeadLetter.GetProperty("attempts").GetInt32());
    }

    [Fact]
    public async Task UnknownUrnOnTheThirdDeliveryIsDeadLetteredAsUnknownUrn()
    {
        var sent = new List<Sent>();
        var consumer = ConsumerWith(Message(Body(), 2).Object);
        var options = new PulsarConsumerOptions { DeadLetterProducer = Dlq(sent).Object };

        await new PulsarConsumer(consumer.Object, new Dictionary<string, BabelHandler>(), options).ReceiveAsync();

        var record = Assert.Single(sent);
        Assert.Equal("unknown_urn", record.DeadLetter.GetProperty("reason").GetString());
        Assert.Equal(3, record.DeadLetter.GetProperty("attempts").GetInt32());
        VerifyAcked(consumer, Times.Once());
    }

    [Fact]
    public async Task UnknownUrnOnTheThirdDeliveryWithDeadLetteringOffIsDropped()
    {
        var consumer = ConsumerWith(Message(Body(), 2).Object);
        Exception? reported = null;

        await new PulsarConsumer(
            consumer.Object,
            new Dictionary<string, BabelHandler>(),
            new PulsarConsumerOptions { OnError = (e, _, _) => reported = e }).ReceiveAsync();

        Assert.IsType<UnknownUrnException>(reported);
        VerifyAcked(consumer, Times.Once());
        VerifyRedelivered(consumer, Times.Never());
    }

    [Fact]
    public async Task UnknownUrnHookStillAcknowledgesWithoutAnyDeadLetter()
    {
        var sent = new List<Sent>();
        var consumer = ConsumerWith(Message(Body(), 5).Object);
        var options = new PulsarConsumerOptions
        {
            DeadLetterProducer = Dlq(sent).Object,
            OnUnknownUrn = (_, _, _) => Task.CompletedTask,
        };

        await new PulsarConsumer(consumer.Object, new Dictionary<string, BabelHandler>(), options).ReceiveAsync();

        Assert.Empty(sent);
        VerifyAcked(consumer, Times.Once());
    }

    [Fact]
    public async Task FailedDeadLetterWriteRedeliversAndReportsAndNeverAcknowledges()
    {
        var consumer = ConsumerWith(Message(Body(), 2).Object);
        var reported = new List<Exception>();
        var options = new PulsarConsumerOptions
        {
            DeadLetterProducer = Dlq(new List<Sent>(), new InvalidOperationException("broker down")).Object,
            OnError = (e, _, _) => reported.Add(e),
        };

        await new PulsarConsumer(consumer.Object, Failing(), options).ReceiveAsync();

        VerifyAcked(consumer, Times.Never());
        VerifyRedelivered(consumer, Times.Once());
        Assert.Equal(2, reported.Count);
        Assert.Contains("Dead-letter write failed", reported[0].Message);
        Assert.Equal("broker down", reported[0].InnerException!.Message);
        Assert.IsType<InvalidOperationException>(reported[1]);
    }

    [Fact]
    public async Task CancelledDeadLetterWritePropagatesAndLeavesTheMessageUnacknowledged()
    {
        var consumer = ConsumerWith(Message(Body(), 2).Object);
        var options = new PulsarConsumerOptions
        {
            DeadLetterProducer = Dlq(new List<Sent>(), new OperationCanceledException()).Object,
        };

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => new PulsarConsumer(consumer.Object, Failing(), options).ReceiveAsync());

        VerifyAcked(consumer, Times.Never());
        VerifyRedelivered(consumer, Times.Never());
    }

    [Fact]
    public async Task HandlerCancelledWhileShuttingDownLeavesTheMessageUnsettled()
    {
        using var cts = new CancellationTokenSource();
        var consumer = ConsumerWith(Message(Body()).Object);
        var handlers = new Dictionary<string, BabelHandler>
        {
            [Urn] = (_, _, ct) =>
            {
                cts.Cancel();
                ct.ThrowIfCancellationRequested();
                return Task.CompletedTask;
            },
        };

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => new PulsarConsumer(consumer.Object, handlers).ReceiveAsync(cts.Token));

        VerifyAcked(consumer, Times.Never());
        VerifyRedelivered(consumer, Times.Never());
    }

    [Fact]
    public async Task ThrowingOnErrorHookIsSwallowedAfterTheMessageIsSettled()
    {
        var consumer = ConsumerWith(Message(Body(), 2).Object);
        var options = new PulsarConsumerOptions { OnError = (_, _, _) => throw new InvalidOperationException("hook") };

        await new PulsarConsumer(consumer.Object, Failing(), options).ReceiveAsync();

        VerifyAcked(consumer, Times.Once());
    }

    [Theory]
    [InlineData("persistent://public/default/orders-partition-3", "orders")]
    [InlineData("persistent://public/default/orders-partition-x", "orders-partition-x")]
    [InlineData("orders", "orders")]
    public async Task OriginalQueueIsTheTopicNameWithoutPartitionSuffix(string topic, string expected)
    {
        var sent = new List<Sent>();
        var consumer = ConsumerWith(Message(Body(), 2, topic: topic).Object);
        var options = new PulsarConsumerOptions { DeadLetterProducer = Dlq(sent).Object };

        await new PulsarConsumer(consumer.Object, Failing(), options).ReceiveAsync();

        Assert.Equal(expected, Assert.Single(sent).DeadLetter.GetProperty("original_queue").GetString());
    }

    [Fact]
    public async Task OriginalQueueFallsBackToTheConsumerTopicThenDefault()
    {
        var sent = new List<Sent>();
        var message = Message(Body(), 2);
        message.SetupGet(m => m.MessageId).Returns((MessageId)null!);
        var consumer = ConsumerWith(message.Object);
        consumer.SetupGet(c => c.Topic).Returns("persistent://public/default/billing");
        var options = new PulsarConsumerOptions { DeadLetterProducer = Dlq(sent).Object };
        await new PulsarConsumer(consumer.Object, Failing(), options).ReceiveAsync();
        Assert.Equal("billing", sent[0].DeadLetter.GetProperty("original_queue").GetString());

        consumer.SetupGet(c => c.Topic).Returns((string)null!);
        await new PulsarConsumer(consumer.Object, Failing(), options).ReceiveAsync();
        Assert.Equal("default", sent[1].DeadLetter.GetProperty("original_queue").GetString());

        consumer.SetupGet(c => c.Topic).Returns("persistent://public/default/");
        await new PulsarConsumer(consumer.Object, Failing(), options).ReceiveAsync();
        Assert.Equal("default", sent[2].DeadLetter.GetProperty("original_queue").GetString());
    }

    // -- decisions 2 and 4: the poison route ---------------------------------------------------------------------

    [Theory]
    [InlineData("not json at all")]
    [InlineData("{}")]
    [InlineData("[]")]
    [InlineData("null")]
    [InlineData("{\"job\":5}")]
    [InlineData("{\"job\":\"  \"}")]
    [InlineData("{\"urn\":5,\"data\":{}}")]
    [InlineData("")]
    public async Task PoisonBodyWithDeadLetteringOnIsRecordedAndAcknowledgedWithoutRetryOrHandler(string body)
    {
        var sent = new List<Sent>();
        var consumer = ConsumerWith(Message(Encoding.UTF8.GetBytes(body), 0, new Dictionary<string, string>
        {
            ["bq-trace-id"] = "t-1",
        }).Object);
        var handled = 0;
        Exception? reported = null;
        var options = new PulsarConsumerOptions
        {
            DeadLetterProducer = Dlq(sent).Object,
            OnError = (e, _, _) => reported = e,
        };

        await new PulsarConsumer(consumer.Object, Counting(() => handled++), options).ReceiveAsync();

        Assert.Equal(0, handled);
        var record = Assert.Single(sent);
        Assert.Equal("poison", record.DeadLetter.GetProperty("reason").GetString());
        Assert.Equal("orders", record.DeadLetter.GetProperty("original_queue").GetString());
        Assert.Equal("t-1", record.Metadata["bq-trace-id"]);
        VerifyAcked(consumer, Times.Once());
        VerifyRedelivered(consumer, Times.Never());
        Assert.IsType<BabelQueueException>(reported);
    }

    [Fact]
    public async Task NonObjectPoisonBodyIsWrappedWithItsOriginalText()
    {
        var sent = new List<Sent>();
        var consumer = ConsumerWith(Message(Encoding.UTF8.GetBytes("not-json")).Object);

        await new PulsarConsumer(
            consumer.Object,
            new Dictionary<string, BabelHandler>(),
            new PulsarConsumerOptions { DeadLetterProducer = Dlq(sent).Object }).ReceiveAsync();

        var json = Assert.Single(sent).Json;
        Assert.Equal("not-json", json.GetProperty("raw").GetString());
        Assert.False(json.TryGetProperty("raw_encoding", out _));
    }

    [Fact]
    public async Task InvalidUtf8BodyIsPoisonAndCarriedAsBase64()
    {
        var bytes = new byte[] { 0x7B, 0xFF, 0xFE, 0x7D };
        var sent = new List<Sent>();
        var consumer = ConsumerWith(Message(bytes).Object);
        Exception? reported = null;

        await new PulsarConsumer(
            consumer.Object,
            new Dictionary<string, BabelHandler>(),
            new PulsarConsumerOptions { DeadLetterProducer = Dlq(sent).Object, OnError = (e, _, _) => reported = e }).ReceiveAsync();

        var json = Assert.Single(sent).Json;
        Assert.Equal("base64", json.GetProperty("raw_encoding").GetString());
        Assert.Equal(bytes, Convert.FromBase64String(json.GetProperty("raw").GetString()!));
        Assert.Contains("UTF-8", reported!.Message);
        VerifyAcked(consumer, Times.Once());
    }

    [Fact]
    public async Task IdentityLessJsonObjectIsSplicedByteForByteIntoThePoisonRecord()
    {
        const string body = "{\"trace_id\":\"t\",\"data\":{\"n\":1e400},\"x\":1}";
        var sent = new List<Sent>();
        var consumer = ConsumerWith(Message(Encoding.UTF8.GetBytes(body)).Object);

        await new PulsarConsumer(
            consumer.Object,
            new Dictionary<string, BabelHandler>(),
            new PulsarConsumerOptions { DeadLetterProducer = Dlq(sent).Object }).ReceiveAsync();

        var text = Encoding.UTF8.GetString(Assert.Single(sent).Payload);
        Assert.StartsWith(body[..^1] + ",\"dead_letter\":", text, StringComparison.Ordinal);
        Assert.Contains("1e400", text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task NonConformantButIdentifiedEnvelopeIsPoisonToo()
    {
        // A job but no meta / data: Accepts fails, so it can never be handled however often it is redelivered.
        var sent = new List<Sent>();
        var consumer = ConsumerWith(Message(Encoding.UTF8.GetBytes("{\"job\":\"" + Urn + "\"}")).Object);
        var handled = 0;

        await new PulsarConsumer(
            consumer.Object,
            Counting(() => handled++),
            new PulsarConsumerOptions { DeadLetterProducer = Dlq(sent).Object }).ReceiveAsync();

        Assert.Equal(0, handled);
        Assert.Equal("poison", Assert.Single(sent).DeadLetter.GetProperty("reason").GetString());
    }

    [Fact]
    public async Task UnsupportedSchemaVersionPropertyIsWrittenRawWithoutADeadLetterBlock()
    {
        // ADR-0037 decision 1 rule 4: the incoming body exactly as received -- no ADR-0035 record, no dead_letter block.
        var body = Body();
        var sent = new List<Sent>();
        var consumer = ConsumerWith(Message(body, 0, new Dictionary<string, string>
        {
            ["bq-job"] = Urn,
            ["bq-schema-version"] = "2",
        }).Object);
        var handled = 0;

        await new PulsarConsumer(
            consumer.Object,
            Counting(() => handled++),
            new PulsarConsumerOptions { DeadLetterProducer = Dlq(sent).Object }).ReceiveAsync();

        Assert.Equal(0, handled);
        var record = Assert.Single(sent);
        Assert.Equal(body, record.Payload);
        Assert.False(record.Json.TryGetProperty("dead_letter", out _));
        Assert.Equal("2", record.Metadata["bq-schema-version"]);
        VerifyAcked(consumer, Times.Once());
    }

    [Fact]
    public async Task OnUnknownUrnHookNeverAffectsAPoisonBody()
    {
        // The hook is for an unknown URN only (ADR-0037 "Configured strategy only"): with the DLQ on a poison record is
        // written, with it off the poison takes the bounded retry -- the hook is never called and nothing is dropped as "handled".
        var hook = 0;
        var sent = new List<Sent>();
        var on = ConsumerWith(Message(Encoding.UTF8.GetBytes("junk")).Object);
        await new PulsarConsumer(
            on.Object,
            new Dictionary<string, BabelHandler>(),
            new PulsarConsumerOptions
            {
                DeadLetterProducer = Dlq(sent).Object,
                OnUnknownUrn = (_, _, _) => { hook++; return Task.CompletedTask; },
            }).ReceiveAsync();
        Assert.Equal("poison", Assert.Single(sent).DeadLetter.GetProperty("reason").GetString());
        VerifyAcked(on, Times.Once());

        var off = ConsumerWith(Message(Encoding.UTF8.GetBytes("junk")).Object);
        await new PulsarConsumer(
            off.Object,
            new Dictionary<string, BabelHandler>(),
            new PulsarConsumerOptions { OnUnknownUrn = (_, _, _) => { hook++; return Task.CompletedTask; } }).ReceiveAsync();
        VerifyRedelivered(off, Times.Once());
        VerifyAcked(off, Times.Never());

        Assert.Equal(0, hook);
    }

    [Fact]
    public async Task PoisonRecordCarriesTheReconciledAttemptsAsInformation()
    {
        var sent = new List<Sent>();
        var consumer = ConsumerWith(Message(Encoding.UTF8.GetBytes("nope"), 4).Object);

        await new PulsarConsumer(
            consumer.Object,
            new Dictionary<string, BabelHandler>(),
            new PulsarConsumerOptions { DeadLetterProducer = Dlq(sent).Object }).ReceiveAsync();

        Assert.Equal(4, Assert.Single(sent).DeadLetter.GetProperty("attempts").GetInt32());
    }

    [Fact]
    public async Task PoisonWithDeadLetteringOffIsRetriedThenDropped()
    {
        var first = ConsumerWith(Message(Encoding.UTF8.GetBytes("{}"), 0).Object);
        await new PulsarConsumer(first.Object, new Dictionary<string, BabelHandler>()).ReceiveAsync();
        VerifyRedelivered(first, Times.Once());
        VerifyAcked(first, Times.Never());

        var last = ConsumerWith(Message(Encoding.UTF8.GetBytes("{}"), 2).Object);
        await new PulsarConsumer(last.Object, new Dictionary<string, BabelHandler>()).ReceiveAsync();
        VerifyRedelivered(last, Times.Never());
        VerifyAcked(last, Times.Once());
    }

    [Fact]
    public async Task VersionGatedDeliveryWithDeadLetteringOffIsDroppedOnTheFirstDeliveryWithAttemptsLeft()
    {
        // ADR-0037 decision 1 rule 3 (Pulsar): ack and drop, on the first delivery, with MaxAttempts nowhere near exhausted.
        var message = Message(Encoding.UTF8.GetBytes("{ nope"), 0, new Dictionary<string, string> { ["bq-schema-version"] = "9" });
        var consumer = ConsumerWith(message.Object);

        await new PulsarConsumer(consumer.Object, new Dictionary<string, BabelHandler>()).ReceiveAsync();

        message.Verify(m => m.Value(), Times.Never);
        VerifyAcked(consumer, Times.Once());
        VerifyRedelivered(consumer, Times.Never());
    }

    [Fact]
    public async Task FailedPoisonDeadLetterWriteRedeliversTheOriginal()
    {
        var consumer = ConsumerWith(Message(Encoding.UTF8.GetBytes("junk")).Object);
        var reported = new List<Exception>();
        var options = new PulsarConsumerOptions
        {
            DeadLetterProducer = Dlq(new List<Sent>(), new InvalidOperationException("down")).Object,
            OnError = (e, _, _) => reported.Add(e),
        };

        await new PulsarConsumer(consumer.Object, new Dictionary<string, BabelHandler>(), options).ReceiveAsync();

        VerifyAcked(consumer, Times.Never());
        VerifyRedelivered(consumer, Times.Once());
        Assert.Equal(2, reported.Count);
    }

    [Fact]
    public async Task PoisonWithoutPropertiesStillWritesARecord()
    {
        var sent = new List<Sent>();
        var message = new Mock<IMessage<byte[]>>();
        message.Setup(m => m.Value()).Returns(Encoding.UTF8.GetBytes("junk"));
        message.SetupGet(m => m.MessageId).Returns(new MessageId(0, 0, 0, 0, "orders"));
        message.SetupGet(m => m.Properties).Returns((IReadOnlyDictionary<string, string>)null!);
        var consumer = ConsumerWith(message.Object);

        await new PulsarConsumer(
            consumer.Object,
            new Dictionary<string, BabelHandler>(),
            new PulsarConsumerOptions { DeadLetterProducer = Dlq(sent).Object }).ReceiveAsync();

        Assert.Single(sent);
        VerifyAcked(consumer, Times.Once());
    }

    [Fact]
    public async Task NullBodyIsPoison()
    {
        var sent = new List<Sent>();
        var message = new Mock<IMessage<byte[]>>();
        message.Setup(m => m.Value()).Returns((byte[])null!);
        message.SetupGet(m => m.MessageId).Returns(new MessageId(0, 0, 0, 0, "orders"));
        var consumer = ConsumerWith(message.Object);

        await new PulsarConsumer(
            consumer.Object,
            new Dictionary<string, BabelHandler>(),
            new PulsarConsumerOptions { DeadLetterProducer = Dlq(sent).Object }).ReceiveAsync();

        Assert.Equal("poison", Assert.Single(sent).DeadLetter.GetProperty("reason").GetString());
    }
}
