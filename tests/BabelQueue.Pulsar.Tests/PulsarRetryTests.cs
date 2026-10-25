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
/// The Go-model retry (broker-bindings §5.5): with a <see cref="PulsarConsumerOptions.RetryProducer"/> a failed message is
/// re-published with <c>bq-attempts</c> + 1 and acknowledged, so the bound holds on every subscription type, including
/// Exclusive and Failover where a negative acknowledgement does not advance <c>RedeliveryCount</c>.
/// </summary>
public sealed class PulsarRetryTests
{
    private const string Urn = "urn:babel:orders:created";

    private sealed record Wire(byte[] Body, Dictionary<string, string> Properties, uint RedeliveryCount);

    /// <summary>A one-queue broker on an Exclusive/Failover subscription: a nack redelivers with RedeliveryCount unchanged (0).</summary>
    private sealed class ExclusiveBroker
    {
        public Queue<Wire> Queue { get; } = new();

        public List<Wire> Published { get; } = new();

        public int Acks { get; private set; }

        public int Nacks { get; private set; }

        private Wire? _inflight;

        public Mock<IConsumer<byte[]>> Consumer()
        {
            var consumer = new Mock<IConsumer<byte[]>>();
            consumer.SetupGet(c => c.SubscriptionType).Returns(SubscriptionType.Exclusive);
            consumer.Setup(c => c.Receive(It.IsAny<CancellationToken>())).Returns(() =>
            {
                _inflight = Queue.Dequeue();
                var message = new Mock<IMessage<byte[]>>();
                message.Setup(m => m.Value()).Returns(_inflight.Body);
                message.SetupGet(m => m.Properties).Returns(_inflight.Properties);
                message.SetupGet(m => m.RedeliveryCount).Returns(_inflight.RedeliveryCount);
                message.SetupGet(m => m.MessageId).Returns(new MessageId(0, 0, 0, 0, "orders"));
                return new ValueTask<IMessage<byte[]>>(message.Object);
            });
            consumer.Setup(c => c.Acknowledge(It.IsAny<MessageId>(), It.IsAny<CancellationToken>()))
                .Returns(() => { Acks++; return ValueTask.CompletedTask; });
            consumer.Setup(c => c.RedeliverUnacknowledgedMessages(It.IsAny<IEnumerable<MessageId>>(), It.IsAny<CancellationToken>()))
                .Returns(() => { Nacks++; Queue.Enqueue(_inflight!); return ValueTask.CompletedTask; });
            return consumer;
        }

        public Mock<IProducer<byte[]>> Producer(Exception? failWith = null)
        {
            var producer = new Mock<IProducer<byte[]>>();
            var setup = producer.Setup(p => p.Send(It.IsAny<MessageMetadata>(), It.IsAny<byte[]>(), It.IsAny<CancellationToken>()));
            if (failWith is not null)
            {
                setup.Throws(failWith);
                return producer;
            }

            setup.Callback<MessageMetadata, byte[], CancellationToken>((meta, body, _) =>
            {
                var props = new Dictionary<string, string>();
                foreach (var key in new[] { "bq-job", "bq-attempts", "bq-trace-id", "bq-schema-version", "bq-message-id", "bq-source-lang", "x-custom" })
                {
                    var value = meta[key];
                    if (value is not null)
                    {
                        props[key] = value;
                    }
                }

                var wire = new Wire(body, props, 0);
                Published.Add(wire);
                Queue.Enqueue(wire);
            }).Returns(new ValueTask<MessageId>(new MessageId(0, 0, 0, 0, "orders")));
            return producer;
        }
    }

    private static Wire First(string? body = null, Dictionary<string, string>? props = null)
    {
        var env = EnvelopeCodec.Make(Urn, new Dictionary<string, object?> { ["order_id"] = 1 }, "orders", null);
        return new Wire(
            Encoding.UTF8.GetBytes(body ?? EnvelopeCodec.Encode(env)),
            props ?? PulsarProperties.Of(env).ToDictionary(p => p.Key, p => p.Value),
            0);
    }

    private static async Task Drain(ExclusiveBroker broker, PulsarConsumer consumer, int cap = 12)
    {
        for (var i = 0; i < cap && broker.Queue.Count > 0; i++)
        {
            await consumer.ReceiveAsync();
        }
    }

    [Fact]
    public async Task ExclusiveSubscriptionFailingHandlerIsBoundedAndDeadLettered()
    {
        var broker = new ExclusiveBroker();
        broker.Queue.Enqueue(First(props: null));
        var dlq = new List<byte[]>();
        var dlqProducer = new Mock<IProducer<byte[]>>();
        dlqProducer.Setup(p => p.Send(It.IsAny<MessageMetadata>(), It.IsAny<byte[]>(), It.IsAny<CancellationToken>()))
            .Callback<MessageMetadata, byte[], CancellationToken>((_, b, _) => dlq.Add(b))
            .Returns(new ValueTask<MessageId>(new MessageId(0, 0, 0, 0, "orders.dlq")));
        var deliveries = 0;
        var handlers = new Dictionary<string, BabelHandler>
        {
            [Urn] = (_, _, _) => { deliveries++; throw new InvalidOperationException("boom"); },
        };
        var consumer = new PulsarConsumer(
            broker.Consumer().Object,
            handlers,
            new PulsarConsumerOptions { RetryProducer = broker.Producer().Object, DeadLetterProducer = dlqProducer.Object });

        await Drain(broker, consumer);

        Assert.Equal(3, deliveries);
        Assert.Equal(0, broker.Nacks);
        Assert.Equal(2, broker.Published.Count);
        Assert.Equal("1", broker.Published[0].Properties["bq-attempts"]);
        Assert.Equal("2", broker.Published[1].Properties["bq-attempts"]);
        Assert.Equal(1, EnvelopeCodec.Decode(Encoding.UTF8.GetString(broker.Published[0].Body)).Attempts);
        Assert.Equal(2, EnvelopeCodec.Decode(Encoding.UTF8.GetString(broker.Published[1].Body)).Attempts);
        var record = JsonDocument.Parse(Assert.Single(dlq)).RootElement;
        Assert.Equal(3, record.GetProperty("dead_letter").GetProperty("attempts").GetInt32());
        Assert.Equal("failed", record.GetProperty("dead_letter").GetProperty("reason").GetString());
        Assert.Equal(3, broker.Acks);
        Assert.Empty(broker.Queue);
    }

    [Fact]
    public async Task ExclusiveSubscriptionWithDeadLetteringOffIsDroppedAfterTheThirdDelivery()
    {
        var broker = new ExclusiveBroker();
        broker.Queue.Enqueue(First());
        var deliveries = 0;
        var consumer = new PulsarConsumer(
            broker.Consumer().Object,
            new Dictionary<string, BabelHandler> { [Urn] = (_, _, _) => { deliveries++; throw new InvalidOperationException("boom"); } },
            new PulsarConsumerOptions { RetryProducer = broker.Producer().Object });

        await Drain(broker, consumer);

        Assert.Equal(3, deliveries);
        Assert.Empty(broker.Queue);
        Assert.Equal(3, broker.Acks);
    }

    [Fact]
    public async Task ExclusiveSubscriptionUnknownUrnIsBoundedToo()
    {
        var broker = new ExclusiveBroker();
        broker.Queue.Enqueue(First());
        var consumer = new PulsarConsumer(
            broker.Consumer().Object,
            new Dictionary<string, BabelHandler>(),
            new PulsarConsumerOptions { RetryProducer = broker.Producer().Object });

        await Drain(broker, consumer);

        Assert.Empty(broker.Queue);
        Assert.Equal(2, broker.Published.Count);
    }

    [Fact]
    public async Task ExclusivePoisonWithDeadLetteringOffRepublishesTheReEncodedEnvelopeThenDrops()
    {
        var broker = new ExclusiveBroker();
        var wire = new Wire(Encoding.UTF8.GetBytes("not-json"), new Dictionary<string, string> { ["x-custom"] = "keep" }, 0);
        broker.Queue.Enqueue(wire);
        var consumer = new PulsarConsumer(
            broker.Consumer().Object,
            new Dictionary<string, BabelHandler>(),
            new PulsarConsumerOptions { RetryProducer = broker.Producer().Object });

        await Drain(broker, consumer);

        Assert.Equal(2, broker.Published.Count);
        // ADR-0036 decision 1 (Go parity): the copy is the body re-encoded by the codec with attempts = counted, not the
        // original bytes; the count also lives in bq-attempts, so the decision terminates.
        Assert.DoesNotContain(broker.Published, p => Encoding.UTF8.GetString(p.Body) == "not-json");
        Assert.Equal(1, EnvelopeCodec.Decode(Encoding.UTF8.GetString(broker.Published[0].Body)).Attempts);
        Assert.Equal(2, EnvelopeCodec.Decode(Encoding.UTF8.GetString(broker.Published[1].Body)).Attempts);
        Assert.Equal("keep", broker.Published[0].Properties["x-custom"]);
        Assert.Equal("1", broker.Published[0].Properties["bq-attempts"]);
        Assert.Equal("2", broker.Published[1].Properties["bq-attempts"]);
        Assert.Empty(broker.Queue);
        Assert.Equal(0, broker.Nacks);
    }

    [Theory]
    [InlineData(SubscriptionType.Exclusive)]
    [InlineData(SubscriptionType.Failover)]
    public void ExclusiveOrFailoverWithoutAClientOrRetryProducerFailsAtSetup(SubscriptionType type)
    {
        // A bare nack does not advance RedeliveryCount there, so MaxAttempts could never be reached.
        var consumer = new Mock<IConsumer<byte[]>>();
        consumer.SetupGet(c => c.SubscriptionType).Returns(type);

        var error = Assert.Throws<InvalidOperationException>(
            () => new PulsarConsumer(consumer.Object, new Dictionary<string, BabelHandler>()));

        Assert.Contains(type.ToString(), error.Message, StringComparison.Ordinal);
        Assert.Contains("RetryProducer", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AnUndeclaredSubscriptionTypeIsTreatedAsExclusiveAndFailsClosed()
    {
        // No SubscriptionType reported (the enum default is Exclusive) and neither a client nor a retry producer.
        var consumer = new Mock<IConsumer<byte[]>>();

        var error = Assert.Throws<InvalidOperationException>(
            () => new PulsarConsumer(consumer.Object, new Dictionary<string, BabelHandler>()));

        Assert.Contains("Exclusive", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AnUndeclaredSubscriptionTypeIsFineWithARetryProducerOrAClient()
    {
        var broker = new ExclusiveBroker();
        var withProducer = new Mock<IConsumer<byte[]>>();
        _ = new PulsarConsumer(
            withProducer.Object, new Dictionary<string, BabelHandler>(), new PulsarConsumerOptions { RetryProducer = broker.Producer().Object });

        var (client, topics, _) = ClientWith(broker);
        var withClient = new Mock<IConsumer<byte[]>>();
        withClient.SetupGet(c => c.Topic).Returns("orders");
        _ = new PulsarConsumer(
            withClient.Object, new Dictionary<string, BabelHandler>(), new PulsarConsumerOptions { Client = client.Object });

        Assert.Equal(new[] { "orders" }, topics);
    }

    [Theory]
    [InlineData(SubscriptionType.Shared)]
    [InlineData(SubscriptionType.KeyShared)]
    public async Task SharedAndKeySharedAcceptTheNackPathWithoutAProducer(SubscriptionType type)
    {
        var broker = new ExclusiveBroker();
        broker.Queue.Enqueue(First());
        var mock = broker.Consumer();
        mock.SetupGet(c => c.SubscriptionType).Returns(type);
        var consumer = new PulsarConsumer(
            mock.Object,
            new Dictionary<string, BabelHandler> { [Urn] = (_, _, _) => throw new InvalidOperationException("boom") });

        await consumer.ReceiveAsync();

        Assert.Equal(1, broker.Nacks);
        Assert.Equal(0, broker.Acks);
    }

    private static (Mock<IPulsarClient> Client, List<string> Topics, Mock<IProducer<byte[]>> Producer) ClientWith(ExclusiveBroker broker)
    {
        var topics = new List<string>();
        var producer = broker.Producer();
        var client = new Mock<IPulsarClient>();
        client.Setup(c => c.CreateProducer(It.IsAny<ProducerOptions<byte[]>>()))
            .Callback<ProducerOptions<byte[]>>(o => topics.Add(o.Topic))
            .Returns(producer.Object);
        return (client, topics, producer);
    }

    [Theory]
    [InlineData(SubscriptionType.Exclusive)]
    [InlineData(SubscriptionType.Failover)]
    public async Task ConsumerCreatesItsRetryProducerFromTheClientAndBoundsAnExclusiveSubscription(SubscriptionType type)
    {
        var broker = new ExclusiveBroker();
        broker.Queue.Enqueue(First());
        var (client, topics, _) = ClientWith(broker);
        var mock = broker.Consumer();
        mock.SetupGet(c => c.SubscriptionType).Returns(type);
        mock.SetupGet(c => c.Topic).Returns("persistent://public/default/orders");
        var deliveries = 0;

        await using var consumer = new PulsarConsumer(
            mock.Object,
            new Dictionary<string, BabelHandler> { [Urn] = (_, _, _) => { deliveries++; throw new InvalidOperationException("boom"); } },
            new PulsarConsumerOptions { Client = client.Object });
        await Drain(broker, consumer);

        Assert.Equal(new[] { "persistent://public/default/orders" }, topics);
        Assert.Equal(3, deliveries);
        Assert.Equal(0, broker.Nacks);
        Assert.Equal(2, broker.Published.Count);
        Assert.Empty(broker.Queue);
    }

    [Fact]
    public async Task ClientCreatedRetryProducerIsDisposedWithTheConsumer()
    {
        var broker = new ExclusiveBroker();
        var (client, _, producer) = ClientWith(broker);
        var mock = broker.Consumer();
        mock.SetupGet(c => c.Topic).Returns("orders");

        var consumer = new PulsarConsumer(
            mock.Object, new Dictionary<string, BabelHandler>(), new PulsarConsumerOptions { Client = client.Object });
        await consumer.DisposeAsync();

        producer.Verify(p => p.DisposeAsync(), Times.Once);
    }

    [Fact]
    public async Task ASuppliedRetryProducerWinsOverTheClientAndIsNotDisposedByTheConsumer()
    {
        var broker = new ExclusiveBroker();
        var (client, topics, _) = ClientWith(broker);
        var supplied = broker.Producer();
        var mock = broker.Consumer();
        mock.SetupGet(c => c.Topic).Returns("orders");

        var consumer = new PulsarConsumer(
            mock.Object,
            new Dictionary<string, BabelHandler>(),
            new PulsarConsumerOptions { Client = client.Object, RetryProducer = supplied.Object });
        await consumer.DisposeAsync();

        Assert.Empty(topics);
        supplied.Verify(p => p.DisposeAsync(), Times.Never);
    }

    [Fact]
    public void ClientWithoutAConsumerTopicFailsAtSetup()
    {
        var broker = new ExclusiveBroker();
        var (client, topics, _) = ClientWith(broker);

        var error = Assert.Throws<InvalidOperationException>(() => new PulsarConsumer(
            broker.Consumer().Object, new Dictionary<string, BabelHandler>(), new PulsarConsumerOptions { Client = client.Object }));

        Assert.Contains("no topic", error.Message, StringComparison.Ordinal);
        Assert.Empty(topics);
    }

    [Fact]
    public async Task DisposeWithoutAnOwnedProducerIsANoOp()
    {
        await TestConsumersConsumer().DisposeAsync();
    }

    private static PulsarConsumer TestConsumersConsumer() =>
        new(TestConsumers.Shared().Object, new Dictionary<string, BabelHandler>());

    [Fact]
    public async Task RepublishKeepsRoutingPropertiesAndTheCarriedBqAttemptsWins()
    {
        var broker = new ExclusiveBroker();
        var env = EnvelopeCodec.Make(Urn, new Dictionary<string, object?> { ["order_id"] = 1 }, "orders", "trace-9");
        broker.Queue.Enqueue(First(props: PulsarProperties.Of(env).ToDictionary(p => p.Key, p => p.Value)));
        var consumer = new PulsarConsumer(
            broker.Consumer().Object,
            new Dictionary<string, BabelHandler> { [Urn] = (_, _, _) => throw new InvalidOperationException("boom") },
            new PulsarConsumerOptions { RetryProducer = broker.Producer().Object, MaxAttempts = 5 });

        await consumer.ReceiveAsync();

        var published = Assert.Single(broker.Published);
        Assert.Equal(Urn, published.Properties["bq-job"]);
        Assert.Equal("1", published.Properties["bq-attempts"]);
        Assert.Equal("1", published.Properties["bq-schema-version"]);
    }

    [Fact]
    public async Task FailedRepublishLeavesTheOriginalForRedeliveryAndReports()
    {
        var broker = new ExclusiveBroker();
        broker.Queue.Enqueue(First());
        var reported = new List<Exception>();
        var consumer = new PulsarConsumer(
            broker.Consumer().Object,
            new Dictionary<string, BabelHandler> { [Urn] = (_, _, _) => throw new InvalidOperationException("boom") },
            new PulsarConsumerOptions
            {
                RetryProducer = broker.Producer(new InvalidOperationException("down")).Object,
                OnError = (e, _, _) => reported.Add(e),
            });

        await consumer.ReceiveAsync();

        Assert.Equal(0, broker.Acks);
        Assert.Equal(1, broker.Nacks);
        Assert.Contains("Retry re-publish failed", reported[0].Message);
    }

    [Fact]
    public async Task CancelledRepublishPropagates()
    {
        var broker = new ExclusiveBroker();
        broker.Queue.Enqueue(First());
        var consumer = new PulsarConsumer(
            broker.Consumer().Object,
            new Dictionary<string, BabelHandler> { [Urn] = (_, _, _) => throw new InvalidOperationException("boom") },
            new PulsarConsumerOptions { RetryProducer = broker.Producer(new OperationCanceledException()).Object });

        await Assert.ThrowsAsync<OperationCanceledException>(() => consumer.ReceiveAsync());

        Assert.Equal(0, broker.Acks);
        Assert.Equal(0, broker.Nacks);
    }

    [Fact]
    public async Task RepublishWithNoPropertiesOnTheOriginalStillCarriesBqAttempts()
    {
        var broker = new ExclusiveBroker();
        var wire = First(props: new Dictionary<string, string>());
        broker.Queue.Enqueue(wire);
        var consumer = new PulsarConsumer(
            broker.Consumer().Object,
            new Dictionary<string, BabelHandler> { [Urn] = (_, _, _) => throw new InvalidOperationException("boom") },
            new PulsarConsumerOptions { RetryProducer = broker.Producer().Object });

        await consumer.ReceiveAsync();

        Assert.Equal("1", Assert.Single(broker.Published).Properties["bq-attempts"]);
    }
}
