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

    private static void Put(Dictionary<string, string> props, string key, string? value)
    {
        if (!string.IsNullOrEmpty(value))
        {
            props[key] = value;
        }
    }
}
