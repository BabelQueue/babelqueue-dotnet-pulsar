using BabelQueue;
using BabelQueue.Pulsar;
using Xunit;

namespace BabelQueue.Pulsar.Tests;

/// <summary>
/// §5 property projection (no broker): bq-job = URN, bq-trace-id = trace_id,
/// bq-message-id = meta.id, plus bq-schema-version / bq-source-lang / bq-attempts.
/// </summary>
public sealed class PulsarPropertiesTests
{
    private static Envelope Sample() =>
        EnvelopeCodec.Make(
            "urn:babel:orders:created",
            new Dictionary<string, object?> { ["order_id"] = 1042 },
            "orders",
            "trace-xyz");

    [Fact]
    public void ProjectsContractProperties()
    {
        var env = Sample();
        var props = PulsarProperties.Of(env);

        Assert.Equal("urn:babel:orders:created", props["bq-job"]);
        Assert.Equal("trace-xyz", props["bq-trace-id"]);
        Assert.Equal(env.Meta!.Id, props["bq-message-id"]);
        Assert.Equal(env.Meta.SchemaVersion.ToString(System.Globalization.CultureInfo.InvariantCulture), props["bq-schema-version"]);
        Assert.Equal(env.Meta.Lang, props["bq-source-lang"]);
        Assert.Equal("0", props["bq-attempts"]);
    }

    [Fact]
    public void AllValuesAreStrings()
    {
        var props = PulsarProperties.Of(Sample());
        Assert.All(props.Values, v => Assert.IsType<string>(v));
    }
}
