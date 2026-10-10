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
/// Apache Pulsar binding conformance against the vendored canonical suite's <c>pulsar</c>
/// block: the §5 property projection (bq-* string→string) and the
/// <c>attempts = max(body, RedeliveryCount)</c> reconciliation (no −1; the redelivery count is
/// 0-based). No Pulsar, no network.
/// </summary>
public sealed class PulsarConformanceTests
{
    private static readonly string Dir = Path.Combine(AppContext.BaseDirectory, "conformance");

    private static JsonElement Pulsar()
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(Path.Combine(Dir, "manifest.json")));
        return doc.RootElement.GetProperty("pulsar").Clone();
    }

    [Fact]
    public void PropertyProjectionMatchesGolden()
    {
        var projection = Pulsar().GetProperty("property_projection");
        var body = File.ReadAllText(Path.Combine(Dir, projection.GetProperty("envelope_file").GetString()!));
        var got = PulsarProperties.Of(EnvelopeCodec.Decode(body));

        var want = projection.GetProperty("properties");
        Assert.Equal(want.EnumerateObject().Count(), got.Count);
        foreach (var prop in want.EnumerateObject())
        {
            Assert.True(got.ContainsKey(prop.Name), prop.Name);
            Assert.Equal(prop.Value.GetString(), got[prop.Name]);
        }
    }

    [Fact]
    public async Task AttemptsReconciliationMatchesGolden()
    {
        foreach (var testCase in Pulsar().GetProperty("attempts_reconciliation").GetProperty("cases").EnumerateArray())
        {
            var bodyAttempts = testCase.GetProperty("body_attempts").GetInt32();
            var redeliveryCount = (uint)testCase.GetProperty("redelivery_count").GetInt32();
            var expected = testCase.GetProperty("expected_attempts").GetInt32();

            var env = EnvelopeCodec.Make(
                "urn:babel:orders:created", new Dictionary<string, object?> { ["x"] = 1 }, "orders")
                with { Attempts = bodyAttempts };

            var message = new Mock<IMessage<byte[]>>();
            message.Setup(m => m.Value()).Returns(Encoding.UTF8.GetBytes(EnvelopeCodec.Encode(env)));
            message.SetupGet(m => m.RedeliveryCount).Returns(redeliveryCount);
            message.SetupGet(m => m.MessageId).Returns(new MessageId(0, 0, 0, 0, "orders"));

            var consumer = new Mock<IConsumer<byte[]>>();
            consumer.Setup(c => c.Receive(It.IsAny<CancellationToken>()))
                .Returns(new ValueTask<IMessage<byte[]>>(message.Object));

            var seen = -1;
            var handlers = new Dictionary<string, BabelHandler>
            {
                ["urn:babel:orders:created"] = (e, _, _) => { seen = e.Attempts; return Task.CompletedTask; },
            };
            await new PulsarConsumer(consumer.Object, handlers).ReceiveAsync();

            Assert.Equal(expected, seen);
        }
    }

    [Fact]
    public async Task SchemaVersionGateMatchesGolden()
    {
        var gate = Pulsar().GetProperty("schema_version_gate");
        var property = gate.GetProperty("property").GetString()!;
        var body = File.ReadAllText(Path.Combine(Dir, gate.GetProperty("fixture").GetString()!));
        var job = EnvelopeCodec.Decode(body).Job!;

        var cases = gate.GetProperty("cases").EnumerateArray().ToList();
        Assert.NotEmpty(cases);
        foreach (var testCase in cases)
        {
            var absent = testCase.TryGetProperty("absent", out var a) && a.GetBoolean();
            var value = absent ? null : testCase.GetProperty("value").GetString();
            var expect = testCase.GetProperty("expect").GetString();

            var properties = new Dictionary<string, string>();
            if (!absent)
            {
                properties[property] = value!;
            }

            var message = new Mock<IMessage<byte[]>>();
            message.Setup(m => m.Value()).Returns(Encoding.UTF8.GetBytes(body));
            message.SetupGet(m => m.Properties).Returns(properties);
            message.SetupGet(m => m.RedeliveryCount).Returns(0u);
            message.SetupGet(m => m.MessageId).Returns(new MessageId(0, 0, 0, 0, "orders"));

            var consumer = new Mock<IConsumer<byte[]>>();
            consumer.Setup(c => c.Receive(It.IsAny<CancellationToken>()))
                .Returns(new ValueTask<IMessage<byte[]>>(message.Object));

            var seen = 0;
            var handlers = new Dictionary<string, BabelHandler>
            {
                [job] = (_, _, _) => { seen++; return Task.CompletedTask; },
            };
            await new PulsarConsumer(consumer.Object, handlers).ReceiveAsync();

            var label = absent ? "<absent>" : $"'{value}'";
            Assert.True(
                (expect == "decode" ? 1 : 0) == seen,
                $"{label}: expected {expect} but handler ran {seen} time(s)");
            consumer.Verify(
                c => c.Acknowledge(It.IsAny<MessageId>(), It.IsAny<CancellationToken>()),
                expect == "decode" ? Times.Once() : Times.Never());
            consumer.Verify(
                c => c.RedeliverUnacknowledgedMessages(It.IsAny<IEnumerable<MessageId>>(), It.IsAny<CancellationToken>()),
                expect == "decode" ? Times.Never() : Times.Once());
        }
    }
}
