// Logging and telemetry are not needed for the isolated decision-rule audit.
// No scheduling, queueing, assignment or capacity logic is replaced here.
namespace Spectre.Console { }
namespace MBrokerBench
{
    public static class Logger { public static void Log(string message) { } }
    public static class MetricsExporter
    {
        public static void SetConsumers(int n) { }
        public static void IncScaleUp() { }
        public static void IncScaleDown() { }
        public static void SetPartition(string id, long lag, double rate) { }
        public static void SetPartitionAssignment(string id, string? owner) { }
        public static void SetConsumerMetrics(string id, double utilization, int count) { }
    }
}
