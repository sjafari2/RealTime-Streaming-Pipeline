# Local execution checkout

The original development repository remains the final source. Four accepted trials were retained after preflight source reads and Kubernetes requests timed out before the next workload started. Once API access returned, the original configuration, replica count, StatefulSet template and placement anchors were restored and verified.

The remaining trials execute from an identical checkout of the pushed source outside cloud-synchronized storage. Runtime source hashes and the original commit are recorded. Scientific settings, runtime software, workload seeds and placement are unchanged. The execution checkout uses the original results directory and command lock. This avoids repeated cloud-file loading delays without removing source or deployment checks.
