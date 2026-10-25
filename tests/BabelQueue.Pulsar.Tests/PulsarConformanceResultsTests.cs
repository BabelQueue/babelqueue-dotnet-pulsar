using System.Text.Json;
using Xunit;

namespace BabelQueue.Pulsar.Tests;

/// <summary>
/// Runs every case of the Pulsar-scoped manifest sections through <see cref="PulsarConformanceSuite"/> and, when
/// <c>BQ_CONFORMANCE_RESULTS</c> names a path, writes the <c>results.json</c> release asset (ADR-0032).
/// </summary>
public sealed class PulsarConformanceResultsTests
{
    private static readonly string[] ExpectedSections = { "poison_dlq", "pulsar", "pulsar_exhaustion" };

    [Fact]
    public async Task ResultsHaveNoFailuresAndAreWrittenOnRequest()
    {
        var json = await PulsarConformanceSuite.WriteResultsAsync(Environment.GetEnvironmentVariable("BQ_CONFORMANCE_RESULTS"));
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.Equal("babelqueue-dotnet-pulsar", root.GetProperty("sdk").GetString());
        Assert.Equal(new[] { "pulsar" }, root.GetProperty("bindings").EnumerateArray().Select(b => b.GetString()).ToArray());
        Assert.Equal(PulsarConformanceSuite.ConformanceVersion, root.GetProperty("conformance_version").GetString());

        var sections = root.GetProperty("sections");
        Assert.Equal(ExpectedSections, sections.EnumerateObject().Select(s => s.Name).OrderBy(n => n, StringComparer.Ordinal).ToArray());

        var failures = new List<string>();
        foreach (var section in sections.EnumerateObject())
        {
            foreach (var c in section.Value.EnumerateObject())
            {
                var status = c.Value.GetProperty("status").GetString();
                var reason = c.Value.TryGetProperty("reason", out var r) ? r.GetString() : null;
                if (status == "skip" && reason is not null && reason.Contains("ADR-", StringComparison.Ordinal))
                {
                    continue; // a skip is legal only for an individual case and must cite the authorising ADR
                }

                if (status != "pass")
                {
                    failures.Add($"{section.Name}/{c.Name}: {reason}");
                }
            }
        }

        Assert.True(failures.Count == 0, string.Join("; ", failures));
    }

    [Fact]
    public async Task OnlyTheStrategyRowsOfPulsarExhaustionAreSkipped()
    {
        using var doc = JsonDocument.Parse(await PulsarConformanceSuite.WriteResultsAsync(null));
        var exhaustion = doc.RootElement.GetProperty("sections").GetProperty("pulsar_exhaustion");

        var skipped = exhaustion.EnumerateObject().Where(c => c.Value.GetProperty("status").GetString() == "skip").ToList();
        Assert.Equal(10, skipped.Count);
        Assert.All(skipped, c => Assert.Contains("ADR-0036", c.Value.GetProperty("reason").GetString(), StringComparison.Ordinal));
        Assert.Equal(19, exhaustion.EnumerateObject().Count(c => c.Value.GetProperty("status").GetString() == "pass"));
    }

    [Fact]
    public void EveryInScopeSectionIsEnumeratedWithUniqueIds()
    {
        var cases = PulsarConformanceSuite.All();
        Assert.Equal(ExpectedSections, PulsarConformanceSuite.Sections().OrderBy(s => s, StringComparer.Ordinal).ToArray());
        Assert.Equal(cases.Count, cases.Select(c => (c.Section, c.Id)).Distinct().Count());
        Assert.Contains(cases, c => c is { Section: "pulsar", Id: "property_projection" });
        Assert.Contains(cases, c => c is { Section: "pulsar", Id: "schema_version_gate/0" });
        Assert.Contains(cases, c => c is { Section: "pulsar", Id: "attempts_reconciliation/first-delivery" });
        Assert.Equal(29, cases.Count(c => c.Section == "pulsar_exhaustion"));
        Assert.Equal(43, cases.Count(c => c.Section == "poison_dlq"));
        Assert.Contains(cases, c => c is { Section: "poison_dlq", Id: "invalid-utf8-one-byte-two-pad" });
    }
}
