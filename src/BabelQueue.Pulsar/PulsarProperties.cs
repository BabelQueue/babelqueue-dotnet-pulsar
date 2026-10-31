using System.Globalization;

namespace BabelQueue.Pulsar;

/// <summary>
/// Projects the envelope's contract fields onto native Pulsar message properties
/// (string→string): <c>bq-job</c> = URN, <c>bq-trace-id</c> = <c>trace_id</c>,
/// <c>bq-message-id</c> = <c>meta.id</c>, plus <c>bq-schema-version</c> /
/// <c>bq-source-lang</c> / <c>bq-attempts</c>. The body stays authoritative (Contract §5.2);
/// all values are strings (Pulsar properties are string-typed).
/// </summary>
internal static class PulsarProperties
{
    /// <summary>
    /// The properties the transport owns: the §5.2 projection plus <c>bq-delay</c>. They are never accepted from, nor
    /// surfaced as, out-of-band transport headers (parity with the Go reference).
    /// </summary>
    private static readonly HashSet<string> Contract = new(StringComparer.Ordinal)
    {
        "bq-job", "bq-trace-id", "bq-message-id", "bq-schema-version", "bq-source-lang", "bq-attempts", "bq-delay",
    };

    /// <summary>True when <paramref name="key"/> is a contract property the transport owns.</summary>
    public static bool IsContract(string key) => Contract.Contains(key);

    public static IReadOnlyDictionary<string, string> Of(Envelope envelope)
    {
        var props = new Dictionary<string, string>(StringComparer.Ordinal);
        Put(props, "bq-job", envelope.Job);
        Put(props, "bq-trace-id", envelope.TraceId);

        var meta = envelope.Meta;
        if (meta is not null)
        {
            Put(props, "bq-message-id", meta.Id);
            props["bq-schema-version"] = meta.SchemaVersion.ToString(CultureInfo.InvariantCulture);
            Put(props, "bq-source-lang", meta.Lang);
        }

        props["bq-attempts"] = envelope.Attempts.ToString(CultureInfo.InvariantCulture);
        return props;
    }

    /// <summary>A copy of a received message's properties (empty when the message has none); used for a poison dead-letter record, whose body is not an envelope to project.</summary>
    public static IReadOnlyDictionary<string, string> Copy(IReadOnlyDictionary<string, string>? properties)
        => properties is null
            ? new Dictionary<string, string>(StringComparer.Ordinal)
            : new Dictionary<string, string>(properties, StringComparer.Ordinal);

    /// <summary>
    /// Overlays the out-of-band string <paramref name="headers"/> (e.g. a W3C <c>traceparent</c>, ADR-0028) onto an
    /// outgoing message's properties, beside the <c>bq-</c> contract projection. An existing property is never
    /// overwritten, a contract property name (<see cref="IsContract"/>, including <c>bq-delay</c>) is always dropped,
    /// and empty keys/values are skipped. Keys are applied in
    /// ordinal order so the result is deterministic. A <c>null</c>/empty map is a no-op.
    /// </summary>
    public static void Merge(IDictionary<string, string> properties, IReadOnlyDictionary<string, string>? headers)
    {
        ArgumentNullException.ThrowIfNull(properties);
        if (headers is null || headers.Count == 0)
        {
            return;
        }

        foreach (var key in headers.Keys.OrderBy(static k => k, StringComparer.Ordinal))
        {
            var value = headers[key];
            if (string.IsNullOrEmpty(key) || string.IsNullOrEmpty(value) || IsContract(key) || properties.ContainsKey(key))
            {
                continue;
            }

            properties[key] = value;
        }
    }

    private static void Put(Dictionary<string, string> props, string key, string? value)
    {
        if (!string.IsNullOrEmpty(value))
        {
            props[key] = value;
        }
    }
}

/// <summary>
/// Out-of-band transport-header carrier for Pulsar (ADR-0028). A W3C <c>traceparent</c> (and
/// <c>tracestate</c>) rides as a message property <b>beside</b> the frozen envelope — never inside it — next to the
/// §5 <c>bq-*</c> projection. This is the consume-side half: it surfaces a received message's properties as a flat
/// map a handler hands to <c>Telemetry.Wrap(handler, headers)</c>, so the consumer span becomes a true child of the
/// producer span. With no <c>traceparent</c> the core falls back to the v0.1 <c>trace_id</c> mapping.
/// </summary>
public static class PulsarTraceHeaders
{
    /// <summary>
    /// The properties of a received message as a flat map (empty when it has none). Only
    /// out-of-band headers (e.g. <c>traceparent</c>) surface: the <c>bq-</c> contract properties the transport owns
    /// are left out, so handing the map back to <c>PublishWithHeadersAsync</c> cannot re-inject them. Empty values
    /// are skipped.
    /// </summary>
    public static Dictionary<string, string> Extract(DotPulsar.Abstractions.IMessage<byte[]> message)
    {
        ArgumentNullException.ThrowIfNull(message);
        return Extract(message.Properties);
    }

    /// <summary>The same flat map, from a raw property dictionary (or <c>null</c>).</summary>
    public static Dictionary<string, string> Extract(IReadOnlyDictionary<string, string>? properties)
    {
        var headers = new Dictionary<string, string>(StringComparer.Ordinal);
        if (properties is null)
        {
            return headers;
        }

        foreach (var (key, value) in properties)
        {
            if (!string.IsNullOrEmpty(value) && !PulsarProperties.IsContract(key))
            {
                headers[key] = value;
            }
        }

        return headers;
    }
}
