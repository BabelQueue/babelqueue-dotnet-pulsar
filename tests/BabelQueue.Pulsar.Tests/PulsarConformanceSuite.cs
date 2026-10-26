using System.Globalization;
using System.Text;
using System.Text.Json;
using BabelQueue;
using BabelQueue.Pulsar;
using DotPulsar;
using DotPulsar.Abstractions;
using Moq;

namespace BabelQueue.Pulsar.Tests;

/// <summary>One manifest case of a section this package carries.</summary>
internal sealed record SuiteCase(string Section, string Id, Func<Task> Run);

internal sealed class ConformanceFailure : Exception
{
    public ConformanceFailure(string message) : base(message)
    {
    }
}

/// <summary>A case that does not apply to this package; the reason cites the authorising ADR (results.schema.json).</summary>
internal sealed class ConformanceSkip : Exception
{
    public ConformanceSkip(string reason) : base(reason)
    {
    }
}

/// <summary>
/// Every case of the Pulsar-scoped sections of the vendored conformance manifest (<c>pulsar</c>,
/// <c>pulsar_exhaustion</c>, <c>version_gate_pulsar</c>, plus the core-scoped <c>poison_dlq</c> whose writer lives here), each runnable on its own,
/// plus the <c>results.json</c> writer of ADR-0032. Case ids follow <c>conformance/schema/results.schema.json</c>: a
/// top-level <c>cases</c> array gives <c>&lt;name&gt;</c> (or <c>&lt;section&gt;#&lt;idx&gt;</c>), a sub-key with its own
/// <c>cases</c> gives <c>&lt;sub-key&gt;/&lt;name&gt;</c> (or <c>&lt;sub-key&gt;/&lt;idx&gt;</c>), any other object sub-key is
/// one case named after it. A case with no runner here is reported as a failure, never silently skipped, so a manifest
/// that grows cannot pass unnoticed. The core-scoped <c>identity_verdict</c> section belongs to the BabelQueue.Core
/// runner (it feeds the core decoder); this package's consumer is tested against its bodies in
/// <c>PulsarIdentityVerdictTests</c>.
/// </summary>
internal static class PulsarConformanceSuite
{
    private const string Urn = "urn:babel:orders:created";
    // Scope "core" in the manifest, but the writer of the poison DLQ record (ADR-0035) is this package's consumer.
    private const string PoisonDlqSection = "poison_dlq";
    private static readonly string Dir = Path.Combine(AppContext.BaseDirectory, "conformance");

    private static readonly Lazy<JsonElement> Manifest = new(() =>
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(Path.Combine(Dir, "manifest.json")));
        return doc.RootElement.Clone();
    });

    internal static string ConformanceVersion => File.ReadAllText(Path.Combine(Dir, "CONFORMANCE_VERSION")).Trim();

    /// <summary>The manifest sections whose declared scope is <c>pulsar</c> (the in-scope sections of this package).</summary>
    internal static IReadOnlyList<string> Sections() =>
        Manifest.Value.GetProperty("sections").EnumerateObject()
            .Where(s => s.Value.GetProperty("scope").GetString() == "pulsar" || s.Name == PoisonDlqSection)
            .Select(s => s.Name)
            .ToList();

    internal static IReadOnlyList<SuiteCase> All()
    {
        var cases = new List<SuiteCase>();
        foreach (var section in Sections())
        {
            var body = Manifest.Value.GetProperty(section);
            if (body.TryGetProperty("cases", out var top) && top.ValueKind == JsonValueKind.Array)
            {
                var index = 0;
                foreach (var c in top.EnumerateArray())
                {
                    var id = Name(c) ?? section + "#" + index;
                    cases.Add(Case(section, id, null, c));
                    index++;
                }
            }

            foreach (var sub in body.EnumerateObject())
            {
                if (sub.Name is "cases" or "description" or "schema" || sub.Value.ValueKind != JsonValueKind.Object)
                {
                    continue;
                }

                if (sub.Value.TryGetProperty("cases", out var nested) && nested.ValueKind == JsonValueKind.Array)
                {
                    var index = 0;
                    foreach (var c in nested.EnumerateArray())
                    {
                        var id = sub.Name + "/" + (Name(c) ?? index.ToString(CultureInfo.InvariantCulture));
                        cases.Add(Case(section, id, sub, c));
                        index++;
                    }
                }
                else
                {
                    cases.Add(Case(section, sub.Name, sub, sub.Value));
                }
            }
        }

        return cases;
    }

    private static string? Name(JsonElement c) =>
        c.ValueKind == JsonValueKind.Object && c.TryGetProperty("name", out var n) && n.ValueKind == JsonValueKind.String ? n.GetString() : null;

    private static SuiteCase Case(string section, string id, JsonProperty? sub, JsonElement c)
    {
        Func<Task>? run = (section, sub?.Name) switch
        {
            ("pulsar", "property_projection") => () => PropertyProjection(c),
            ("pulsar", "attempts_reconciliation") => () => AttemptsReconciliation(c),
            ("pulsar", "schema_version_gate") => () => SchemaVersionGate(sub!.Value.Value, c),
            ("pulsar_exhaustion", null) => () => Exhaustion(c),
            ("version_gate_pulsar", null) => () => VersionGateSettle(body: Manifest.Value.GetProperty(section), c),
            ("poison_dlq", null) => () => PoisonDlq(c),
            _ => null,
        };
        return new SuiteCase(section, id, run ?? (() => throw new ConformanceFailure($"no runner for {section}/{id}")));
    }

    // -- pulsar -----------------------------------------------------------------------------------------------------

    private static Task PropertyProjection(JsonElement projection)
    {
        var body = File.ReadAllText(Path.Combine(Dir, projection.GetProperty("envelope_file").GetString()!));
        var got = PulsarProperties.Of(EnvelopeCodec.Decode(body));
        var want = projection.GetProperty("properties");
        Require(want.EnumerateObject().Count() == got.Count, $"expected {want.EnumerateObject().Count()} properties, got {got.Count}");
        foreach (var prop in want.EnumerateObject())
        {
            Require(got.TryGetValue(prop.Name, out var value) && value == prop.Value.GetString(), $"property {prop.Name} differs from the golden value");
        }

        return Task.CompletedTask;
    }

    private static async Task AttemptsReconciliation(JsonElement testCase)
    {
        var env = EnvelopeCodec.Make(Urn, new Dictionary<string, object?> { ["x"] = 1 }, "orders")
            with { Attempts = testCase.GetProperty("body_attempts").GetInt32() };
        var message = MessageOf(Encoding.UTF8.GetBytes(EnvelopeCodec.Encode(env)), (uint)testCase.GetProperty("redelivery_count").GetInt32(), null);
        var consumer = ConsumerOf(message);

        var seen = -1;
        var handlers = new Dictionary<string, BabelHandler> { [Urn] = (e, _, _) => { seen = e.Attempts; return Task.CompletedTask; } };
        await new PulsarConsumer(consumer.Object, handlers).ReceiveAsync();

        var expected = testCase.GetProperty("expected_attempts").GetInt32();
        Require(seen == expected, $"handler saw attempts {seen}, expected {expected}");
    }

    private static async Task SchemaVersionGate(JsonElement gate, JsonElement testCase)
    {
        var property = gate.GetProperty("property").GetString()!;
        var body = File.ReadAllText(Path.Combine(Dir, gate.GetProperty("fixture").GetString()!));
        var job = EnvelopeCodec.Decode(body).Job!;
        var absent = testCase.TryGetProperty("absent", out var a) && a.GetBoolean();
        var value = absent ? null : testCase.GetProperty("value").GetString();
        var expect = testCase.GetProperty("expect").GetString();

        var properties = new Dictionary<string, string>();
        if (!absent)
        {
            properties[property] = value!;
        }

        var consumer = ConsumerOf(MessageOf(Encoding.UTF8.GetBytes(body), 0, properties));
        var seen = 0;
        var handlers = new Dictionary<string, BabelHandler> { [job] = (_, _, _) => { seen++; return Task.CompletedTask; } };
        await new PulsarConsumer(consumer.Object, handlers).ReceiveAsync();

        Require((expect == "decode" ? 1 : 0) == seen, $"expected {expect} but the handler ran {seen} time(s)");
        // A version-gated delivery (anything but "decode") is acknowledged and dropped on its first delivery (ADR-0037, no
        // DLQ producer here); the exact version_gate_pulsar settle outcomes are asserted in VersionGateSettle.
        consumer.Verify(c => c.Acknowledge(It.IsAny<MessageId>(), It.IsAny<CancellationToken>()), Times.Once());
        consumer.Verify(
            c => c.RedeliverUnacknowledgedMessages(It.IsAny<IEnumerable<MessageId>>(), It.IsAny<CancellationToken>()),
            Times.Never());
    }

    // -- pulsar_exhaustion (ADR-0036 decisions 1 and 2) -------------------------------------------------------------

    private static async Task Exhaustion(JsonElement testCase)
    {
        if (testCase.TryGetProperty("on_unknown_urn", out _))
        {
            // Java, .NET and Node have no unknown-URN strategy option: their strategy is always `fail` (ADR-0036 decision 2).
            throw new ConformanceSkip("ADR-0036: rows with an on_unknown_urn value apply only to an SDK with the strategy option (Go, Python, PHP Dispatcher); the .NET consumer has none");
        }

        var kind = testCase.GetProperty("kind").GetString()!;
        var redelivery = (uint)testCase.GetProperty("redelivery_count").GetInt32();
        var maxAttempts = testCase.TryGetProperty("max_attempts", out var m) ? m.GetInt32() : 3;
        var deadLetter = testCase.GetProperty("dead_letter").GetBoolean();
        var properties = new Dictionary<string, string>();
        if (testCase.TryGetProperty("bq_attempts", out var bq))
        {
            properties["bq-attempts"] = bq.GetString()!;
        }

        byte[] body;
        if (kind == "poison")
        {
            body = Encoding.UTF8.GetBytes("not-json");
        }
        else
        {
            var env = EnvelopeCodec.Make(Urn, new Dictionary<string, object?> { ["x"] = 1 }, "orders")
                with { Attempts = testCase.TryGetProperty("body_attempts", out var ba) ? ba.GetInt32() : 0 };
            body = Encoding.UTF8.GetBytes(EnvelopeCodec.Encode(env));
        }

        var consumer = ConsumerOf(MessageOf(body, redelivery, properties));
        var sent = new List<(MessageMetadata Metadata, byte[] Payload)>();
        var handled = 0;
        var handlers = kind == "failed"
            ? new Dictionary<string, BabelHandler> { [Urn] = (_, _, _) => { handled++; throw new InvalidOperationException("boom"); } }
            : new Dictionary<string, BabelHandler>();
        var options = new PulsarConsumerOptions
        {
            MaxAttempts = maxAttempts,
            DeadLetterProducer = deadLetter ? ProducerInto(sent).Object : null,
        };
        await new PulsarConsumer(consumer.Object, handlers, options).ReceiveAsync();

        var expect = testCase.GetProperty("expect");
        var outcome = expect.GetProperty("outcome").GetString();
        var acks = Invocations(consumer, nameof(IConsumer<byte[]>.Acknowledge));
        var redeliveries = Invocations(consumer, nameof(IConsumer<byte[]>.RedeliverUnacknowledgedMessages));
        switch (outcome)
        {
            case "retry":
                Require(redeliveries == 1 && acks == 0 && sent.Count == 0, $"retry: redelivered {redeliveries}, acked {acks}, dead-lettered {sent.Count}");
                break;
            case "drop":
                Require(redeliveries == 0 && acks == 1 && sent.Count == 0, $"drop: redelivered {redeliveries}, acked {acks}, dead-lettered {sent.Count}");
                break;
            case "dlq":
                Require(redeliveries == 0 && acks == 1 && sent.Count == 1, $"dlq: redelivered {redeliveries}, acked {acks}, dead-lettered {sent.Count}");
                using (var record = JsonDocument.Parse(sent[0].Payload))
                {
                    var block = record.RootElement.GetProperty("dead_letter");
                    var reason = expect.GetProperty("reason").GetString();
                    Require(block.GetProperty("reason").GetString() == reason, $"dead_letter.reason is {block.GetProperty("reason").GetString()}, expected {reason}");
                    if (expect.TryGetProperty("dead_letter_attempts", out var attempts))
                    {
                        Require(block.GetProperty("attempts").GetInt32() == attempts.GetInt32(), $"dead_letter.attempts is {block.GetProperty("attempts").GetInt32()}, expected {attempts.GetInt32()}");
                    }
                }

                break;
            default:
                throw new ConformanceFailure($"unknown outcome {outcome}");
        }

        Require(kind != "poison" || handled == 0, "a poison body reached a handler");
    }

    // -- version_gate_pulsar (ADR-0037 decision 1) -----------------------------------------------------------------

    private static async Task VersionGateSettle(JsonElement body, JsonElement testCase)
    {
        if (testCase.TryGetProperty("on_unknown_urn", out var strategy) && strategy.GetString() != "fail")
        {
            // The .NET Pulsar consumer has no unknown-URN strategy option: it behaves as `fail` (ADR-0037 "Strategy").
            throw new ConformanceSkip("ADR-0037: rows with an on_unknown_urn value other than fail apply only to an SDK whose Pulsar consumer has the strategy option; the .NET consumer has none (it behaves as fail)");
        }

        var deadLetterOn = testCase.GetProperty("dead_letter").GetString() == "on";
        var writeFails = testCase.TryGetProperty("dlq_write", out var w) && w.GetString() == "fails";
        var expect = testCase.GetProperty("expect");
        // This consumer's `off` means no DLQ writer exists, so outcome_no_writer (where present) is the expectation there.
        var outcome = !deadLetterOn && expect.TryGetProperty("outcome_no_writer", out var nw)
            ? nw.GetString()!
            : expect.GetProperty("outcome").GetString()!;
        var properties = new Dictionary<string, string> { ["bq-job"] = Urn, ["bq-schema-version"] = "2" };
        if (testCase.TryGetProperty("bq_attempts", out var bq))
        {
            properties["bq-attempts"] = bq.GetString()!;
        }

        foreach (var incoming in body.GetProperty("incoming_bodies").EnumerateArray())
        {
            var name = incoming.GetProperty("name").GetString();
            var bytes = Encoding.UTF8.GetBytes(incoming.GetProperty("body").GetString()!);
            var consumer = ConsumerOf(MessageOf(bytes, 0, properties));
            var sent = new List<(MessageMetadata Metadata, byte[] Payload)>();
            var dlq = ProducerInto(sent, writeFails ? new InvalidOperationException("dlq down") : null);
            var retry = ProducerInto(new List<(MessageMetadata Metadata, byte[] Payload)>(), null);
            var handled = 0;
            var unknown = 0;
            var handlers = new Dictionary<string, BabelHandler> { [Urn] = (_, _, _) => { handled++; return Task.CompletedTask; } };
            var options = new PulsarConsumerOptions
            {
                DeadLetterProducer = deadLetterOn ? dlq.Object : null,
                RetryProducer = retry.Object,
                OnUnknownUrn = (_, _, _) => { unknown++; return Task.CompletedTask; },
            };
            await new PulsarConsumer(consumer.Object, handlers, options).ReceiveAsync();

            var acks = Invocations(consumer, nameof(IConsumer<byte[]>.Acknowledge));
            var nacks = Invocations(consumer, nameof(IConsumer<byte[]>.RedeliverUnacknowledgedMessages));
            Require(handled == 0 && unknown == 0, $"{name}: a version-gated delivery reached a handler or OnUnknownUrn");
            Require(retry.Invocations.Count == 0, $"{name}: a version-gated delivery was retried");
            switch (outcome)
            {
                case "ack":
                    Require(acks == 1 && nacks == 0 && sent.Count == 0, $"{name}: ack: acked {acks}, nacked {nacks}, written {sent.Count}");
                    break;
                case "dlq_raw":
                    Require(acks == 1 && nacks == 0 && sent.Count == 1, $"{name}: dlq_raw: acked {acks}, nacked {nacks}, written {sent.Count}");
                    Require(sent[0].Payload.SequenceEqual(bytes), $"{name}: the DLQ record is not the incoming body byte for byte");
                    break;
                case "unsettled":
                    Require(acks == 0 && nacks == 1, $"{name}: unsettled: acked {acks}, nacked {nacks}");
                    break;
                default:
                    throw new ConformanceFailure($"unknown outcome {outcome}");
            }
        }
    }

    private static int Invocations(Mock<IConsumer<byte[]>> consumer, string method) =>
        consumer.Invocations.Count(i => i.Method.Name == method);

    // -- poison_dlq (ADR-0035) --------------------------------------------------------------------------------------

    private static readonly JsonDocumentOptions DeepJson = new() { MaxDepth = 10_100 };

    private static Task PoisonDlq(JsonElement testCase)
    {
        var bytes = testCase.TryGetProperty("body_base64", out var b64)
            ? Convert.FromBase64String(b64.GetString()!)
            : Encoding.UTF8.GetBytes(testCase.GetProperty("body").GetString()!);
        var originalQueue = Manifest.Value.GetProperty(PoisonDlqSection).GetProperty("original_queue").GetString()!;
        var expect = testCase.GetProperty("expect");
        var shape = expect.GetProperty("shape").GetString()!;

        var record = PulsarConsumer.PoisonBody(bytes, "poison", "System.Exception", 1700000000000L, originalQueue, 0);
        var text = Encoding.UTF8.GetString(record);
        Require(!text.Contains('�', StringComparison.Ordinal), "the record holds U+FFFD");

        using var doc = JsonDocument.Parse(record, DeepJson);
        Require(doc.RootElement.ValueKind == JsonValueKind.Object, "the record is not a JSON object");
        var deadLetter = doc.RootElement.EnumerateObject().Where(p => p.Name == "dead_letter").ToList();
        Require(deadLetter.Count == 1, "the record must carry exactly one top-level dead_letter");
        Require(
            deadLetter[0].Value.GetProperty("reason").GetString() == "poison"
            && deadLetter[0].Value.GetProperty("original_queue").GetString() == originalQueue,
            "dead_letter must have reason poison and the original queue");
        var withoutDeadLetter = doc.RootElement.EnumerateObject().Where(p => p.Name != "dead_letter").ToList();

        var want = expect.GetProperty("record");
        if (shape == "splice_or_wrapper" && !SameMembers(withoutDeadLetter, want))
        {
            foreach (var literal in expect.GetProperty("splice_literals").EnumerateArray())
            {
                Require(text.Contains(literal.GetString()!, StringComparison.Ordinal), $"the record does not contain {literal.GetString()} verbatim");
            }

            Require(!withoutDeadLetter.Any(p => p.Name is "raw" or "raw_encoding"), "a spliced record has a top-level raw / raw_encoding member");
            var keys = withoutDeadLetter.Select(p => p.Name).Distinct(StringComparer.Ordinal).OrderBy(n => n, StringComparer.Ordinal).ToList();
            var wantKeys = expect.GetProperty("splice_keys").EnumerateArray().Select(k => k.GetString()!).OrderBy(n => n, StringComparer.Ordinal).ToList();
            Require(keys.SequenceEqual(wantKeys), $"top-level members {string.Join(",", keys)}, expected {string.Join(",", wantKeys)}");
            return Task.CompletedTask;
        }

        Require(SameMembers(withoutDeadLetter, want), $"the record without dead_letter differs from the expected record: {text}");
        return Task.CompletedTask;
    }

    private static bool SameMembers(List<JsonProperty> members, JsonElement want)
    {
        var wantMembers = want.EnumerateObject().ToList();
        return members.Count == wantMembers.Count
            && members.All(m => wantMembers.Count(w => w.Name == m.Name && SameJson(m.Value, w.Value)) == 1);
    }

    private static bool SameJson(JsonElement a, JsonElement b)
    {
        if (a.ValueKind != b.ValueKind)
        {
            return false;
        }

        return a.ValueKind switch
        {
            JsonValueKind.Object => SameMembers(a.EnumerateObject().ToList(), b),
            JsonValueKind.Array => a.GetArrayLength() == b.GetArrayLength()
                && a.EnumerateArray().Zip(b.EnumerateArray(), SameJson).All(x => x),
            JsonValueKind.String => a.GetString() == b.GetString(),
            JsonValueKind.Number => a.GetRawText() == b.GetRawText(),
            _ => true,
        };
    }

    // -- helpers ----------------------------------------------------------------------------------------------------

    private static void Require(bool condition, string message)
    {
        if (!condition)
        {
            throw new ConformanceFailure(message);
        }
    }

    private static IMessage<byte[]> MessageOf(byte[] body, uint redeliveryCount, IReadOnlyDictionary<string, string>? properties)
    {
        var message = new Mock<IMessage<byte[]>>();
        message.Setup(m => m.Value()).Returns(body);
        message.SetupGet(m => m.RedeliveryCount).Returns(redeliveryCount);
        message.SetupGet(m => m.MessageId).Returns(new MessageId(0, 0, 0, 0, "orders"));
        message.SetupGet(m => m.Properties).Returns(properties ?? new Dictionary<string, string>());
        return message.Object;
    }

    private static Mock<IConsumer<byte[]>> ConsumerOf(IMessage<byte[]> message)
    {
        var consumer = TestConsumers.Shared();
        consumer.Setup(c => c.Receive(It.IsAny<CancellationToken>())).Returns(new ValueTask<IMessage<byte[]>>(message));
        return consumer;
    }

    private static Mock<IProducer<byte[]>> ProducerInto(List<(MessageMetadata Metadata, byte[] Payload)> sent, Exception? failure = null)
    {
        var producer = new Mock<IProducer<byte[]>>();
        var setup = producer.Setup(p => p.Send(It.IsAny<MessageMetadata>(), It.IsAny<byte[]>(), It.IsAny<CancellationToken>()));
        if (failure is not null)
        {
            setup.Callback<MessageMetadata, byte[], CancellationToken>((meta, payload, _) => sent.Add((meta, payload))).Throws(failure);
            return producer;
        }

        setup
            .Callback<MessageMetadata, byte[], CancellationToken>((meta, payload, _) => sent.Add((meta, payload)))
            .Returns(new ValueTask<MessageId>(new MessageId(0, 0, 0, 0, "orders.dlq")));
        return producer;
    }

    // -- results.json (ADR-0032) --------------------------------------------------------------------------------------

    /// <summary>Run every case, build the <c>results.json</c> document, and write it to <paramref name="path"/> when given.</summary>
    internal static async Task<string> WriteResultsAsync(string? path)
    {
        var sections = new SortedDictionary<string, SortedDictionary<string, (string Status, string? Reason)>>(StringComparer.Ordinal);
        foreach (var c in All())
        {
            (string, string?) outcome;
            try
            {
                await c.Run();
                outcome = ("pass", null);
            }
            catch (ConformanceSkip skip)
            {
                outcome = ("skip", skip.Message);
            }
#pragma warning disable CA1031 // Any failure of a case is that case's recorded result, not a crash of the writer.
            catch (Exception e)
#pragma warning restore CA1031
            {
                outcome = ("fail", e.Message);
            }

            if (!sections.TryGetValue(c.Section, out var bucket))
            {
                bucket = sections[c.Section] = new SortedDictionary<string, (string, string?)>(StringComparer.Ordinal);
            }

            bucket[c.Id] = outcome;
        }

        using var stream = new MemoryStream();
        using (var w = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = true }))
        {
            w.WriteStartObject();
            w.WriteString("sdk", "babelqueue-dotnet-pulsar");
            w.WriteString("version", PackageVersion());
            w.WriteString("conformance_version", ConformanceVersion);
            w.WriteString("generated_at", DateTimeOffset.UtcNow.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture));
            w.WriteStartArray("bindings");
            w.WriteStringValue("pulsar");
            w.WriteEndArray();
            w.WriteStartObject("sections");
            foreach (var (section, cases) in sections)
            {
                w.WriteStartObject(section);
                foreach (var (id, (status, reason)) in cases)
                {
                    w.WriteStartObject(id);
                    w.WriteString("status", status);
                    if (reason is not null)
                    {
                        w.WriteString("reason", reason);
                    }

                    w.WriteEndObject();
                }

                w.WriteEndObject();
            }

            w.WriteEndObject();
            w.WriteEndObject();
        }

        var json = Encoding.UTF8.GetString(stream.ToArray()) + "\n";
        if (!string.IsNullOrWhiteSpace(path))
        {
            await File.WriteAllTextAsync(path, json);
        }

        return json;
    }

    private static string PackageVersion()
    {
        var info = typeof(PulsarPublisher).Assembly
            .GetCustomAttributes(typeof(System.Reflection.AssemblyInformationalVersionAttribute), false)
            .Cast<System.Reflection.AssemblyInformationalVersionAttribute>()
            .Single().InformationalVersion;
        var plus = info.IndexOf('+', StringComparison.Ordinal);
        return plus < 0 ? info : info[..plus];
    }
}
