using System.Reflection;
using System.Text.Json;
using MBrokerBench.Components;
using MBrokerBench.Models;
using MBrokerBench.Strategies;

static (ConsumerGroup, CostCentricModifiedWorstFitAssignment) Setup(
    double[] rates, long[] lags, int[] owners, ConsumerProfile[] profiles)
{
    var strategy = new CostCentricModifiedWorstFitAssignment();
    var partitions = rates.Select((rate, i) => new Partition($"P{i}")
        { ProductionRate = rate, CurrentLag = lags[i] }).ToList();
    var group = new ConsumerGroup("audit", partitions, ConsumerProfiles.AllProfiles,
        ConsumerProfiles.Large, strategy);
    for (int i = 0; i < profiles.Length; i++)
    {
        var c = new Consumer($"C{i}", profiles[i])
            { State = ConsumerState.Running, StartupTimeRemaining = 0 };
        c.Tick(20); // Fully warm before the decision: efficiency becomes one.
        group.Consumers.Add(c);
    }
    for (int i = 0; i < partitions.Count; i++)
    {
        var c = group.Consumers[owners[i]];
        partitions[i].AssignedConsumer = c;
        c.AssignedPartitions.Add(partitions[i]);
    }
    return (group, strategy);
}

static object Snapshot(ConsumerGroup group) => new
{
    fleet = group.Consumers.Select(c => new
    {
        c.Id, profile = c.ConsumerProfile.Name, capacity = c.MaxCapacity,
        rate = c.GetCurrentWorkloadRate(), lag = c.AssignedPartitions.Sum(p => p.CurrentLag),
        required = c.GetCurrentAndFutureWorkloadRate(group.LatencySLASeconds),
        partitions = c.AssignedPartitions.Select(p => p.Id).ToArray()
    }).ToArray(),
    cost_per_second = group.TotalCostPerSecond
};

static string[] PlannedFleet(CostCentricModifiedWorstFitAssignment strategy, double demand)
{
    var method = strategy.GetType().GetMethod("GetOptimalFleetCombination",
        BindingFlags.NonPublic | BindingFlags.Instance)!;
    return ((List<ConsumerProfile>)method.Invoke(strategy, new object[] { demand, 0.0 })!)
        .Select(p => p.Name).ToArray();
}

// Case 1: aggregate capacity covers input, but no selected worker can own P0.
var (packing, packingStrategy) = Setup(new double[] { 1100, 50, 50 }, new long[3],
    new[] { 0, 1, 1 }, new[] { ConsumerProfiles.Medium, ConsumerProfiles.Small });
var packingPlan = PlannedFleet(packingStrategy, 1200);
packingStrategy.Assign(packing.AllPartitions, packing.Consumers);
await packingStrategy.AutoScale();
var packingDecision = Snapshot(packing);
for (int t = 0; t < 10; t++)
{
    packing.AllPartitions.ForEach(p => p.Produce(1));
    packing.Consumers.ForEach(c => c.Consume(1));
}
if (!packingPlan.SequenceEqual(new[] { "Small", "Medium" }) &&
    !packingPlan.SequenceEqual(new[] { "Medium", "Small" }))
    throw new Exception("Unexpected upstream fleet selection.");
if (packing.AllPartitions[0].CurrentLag != 1000)
    throw new Exception("Packing counterexample did not reproduce.");

// Case 2: lag drives provisioning, but production-only stickiness keeps it on C0.
var (sticky, stickyStrategy) = Setup(new double[] { 790, 790, 790, 250, 250 },
    new long[] { 10000, 10000, 10000, 0, 0 }, new[] { 0, 0, 0, 1, 1 },
    new[] { ConsumerProfiles.Large, ConsumerProfiles.Large, ConsumerProfiles.Large });
var stickyPlan = PlannedFleet(stickyStrategy, 2870 + 30000 / 10.0);
stickyStrategy.Assign(sticky.AllPartitions, sticky.Consumers);
await stickyStrategy.AutoScale();
var stickyDecision = Snapshot(sticky);
for (int t = 0; t < 10; t++)
{
    sticky.AllPartitions.ForEach(p => p.Produce(1));
    sticky.Consumers.ForEach(c => c.Consume(1));
}
if (sticky.Consumers.Count != 3 || sticky.AllPartitions.Take(3).Any(p => p.AssignedConsumer!.Id != "C0"))
    throw new Exception("Sticky counterexample did not reproduce.");
if (sticky.AllPartitions.Sum(p => p.CurrentLag) != 28700)
    throw new Exception("Unexpected queue evolution.");

Console.WriteLine(JsonSerializer.Serialize(new
{
    upstream_revision = "bed4c83dea8b68cb665cc222e080ea7736818606",
    scope = "Original C# decision methods and ten fixed-assignment simulation steps; not a Kafka trial or complete campaign.",
    packing = new { planned_fleet = packingPlan, decision = packingDecision,
        backlog_after_10_seconds = packing.AllPartitions.Sum(p => p.CurrentLag) },
    stickiness = new { planned_fleet = stickyPlan, decision = stickyDecision,
        backlog_after_10_seconds = sticky.AllPartitions.Sum(p => p.CurrentLag) }
}, new JsonSerializerOptions { WriteIndented = true }));
