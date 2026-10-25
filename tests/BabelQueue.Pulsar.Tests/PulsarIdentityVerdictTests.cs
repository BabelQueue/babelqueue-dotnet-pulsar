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
/// ADR-0036 decision 4 at the consumer: every body of the vendored <c>identity_verdict</c> section (core scope, so the
/// verdict itself is pinned by the BabelQueue.Core runner) is fed to the Pulsar consumer. A <c>poison</c> body is never
/// handed to a handler and takes the poison route; the only <c>ok</c> body that is also a complete, acceptable envelope
/// reaches its handler (the other <c>ok</c> bodies have a valid identity but are not whole envelopes, which the consumer
/// rejects through <c>Accepts</c>, also as poison).
/// </summary>
public sealed class PulsarIdentityVerdictTests
{
    private static readonly string Dir = Path.Combine(AppContext.BaseDirectory, "conformance");

    private static IReadOnlyList<(string Name, string Body, string Expect)> Cases()
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(Path.Combine(Dir, "manifest.json")));
        return doc.RootElement.GetProperty("identity_verdict").GetProperty("cases").EnumerateArray()
            .Select(c => (c.GetProperty("name").GetString()!, c.GetProperty("body").GetString()!, c.GetProperty("expect").GetString()!))
            .ToList();
    }

    [Fact]
    public async Task EveryPoisonVerdictBodyTakesThePoisonRouteAndNoHandlerSeesIt()
    {
        var cases = Cases();
        Assert.True(cases.Count >= 97);
        var failures = new List<string>();
        foreach (var (name, body, expect) in cases.Where(c => c.Expect == "poison"))
        {
            var (consumer, sent, handled) = await Run(body);
            if (handled != 0 || sent.Count != 1 || Reason(sent[0]) != "poison"
                || consumer.Invocations.Count(i => i.Method.Name == nameof(IConsumer<byte[]>.Acknowledge)) != 1
                || consumer.Invocations.Count(i => i.Method.Name == nameof(IConsumer<byte[]>.RedeliverUnacknowledgedMessages)) != 0)
            {
                failures.Add(name);
            }
        }

        Assert.Empty(failures);
    }

    [Fact]
    public async Task TheCompleteOkEnvelopeReachesItsHandler()
    {
        var full = Cases().Single(c => c.Name == "full valid");
        var (_, sent, handled) = await Run(full.Body, handlerUrn: EnvelopeCodec.Urn(EnvelopeCodec.Decode(full.Body)));

        Assert.Equal(1, handled);
        Assert.Empty(sent);
    }

    [Fact]
    public async Task EveryOkVerdictBodyIsEitherHandledOrParkedNeverRedeliveredForever()
    {
        foreach (var (name, body, _) in Cases().Where(c => c.Expect == "ok"))
        {
            var urn = EnvelopeCodec.Urn(EnvelopeCodec.Decode(body));
            var (consumer, sent, handled) = await Run(body, handlerUrn: urn);
            var redelivered = consumer.Invocations.Count(i => i.Method.Name == nameof(IConsumer<byte[]>.RedeliverUnacknowledgedMessages));
            Assert.True(redelivered == 0 && (handled == 1 || sent.Count == 1), name);
        }
    }

    private static string? Reason(byte[] record) =>
        JsonDocument.Parse(record).RootElement.GetProperty("dead_letter").GetProperty("reason").GetString();

    private static async Task<(Mock<IConsumer<byte[]>> Consumer, List<byte[]> Sent, int Handled)> Run(string body, string handlerUrn = "urn:never")
    {
        var message = new Mock<IMessage<byte[]>>();
        message.Setup(m => m.Value()).Returns(Encoding.UTF8.GetBytes(body));
        message.SetupGet(m => m.RedeliveryCount).Returns(0u);
        message.SetupGet(m => m.MessageId).Returns(new MessageId(0, 0, 0, 0, "orders"));
        message.SetupGet(m => m.Properties).Returns(new Dictionary<string, string>());
        var consumer = TestConsumers.Shared();
        consumer.Setup(c => c.Receive(It.IsAny<CancellationToken>())).Returns(new ValueTask<IMessage<byte[]>>(message.Object));

        var sent = new List<byte[]>();
        var producer = new Mock<IProducer<byte[]>>();
        producer.Setup(p => p.Send(It.IsAny<MessageMetadata>(), It.IsAny<byte[]>(), It.IsAny<CancellationToken>()))
            .Callback<MessageMetadata, byte[], CancellationToken>((_, b, _) => sent.Add(b))
            .Returns(new ValueTask<MessageId>(new MessageId(0, 0, 0, 0, "orders.dlq")));

        var handled = 0;
        var handlers = new Dictionary<string, BabelHandler>
        {
            [handlerUrn] = (_, _, _) => { handled++; return Task.CompletedTask; },
        };
        await new PulsarConsumer(consumer.Object, handlers, new PulsarConsumerOptions { DeadLetterProducer = producer.Object }).ReceiveAsync();
        return (consumer, sent, handled);
    }
}
