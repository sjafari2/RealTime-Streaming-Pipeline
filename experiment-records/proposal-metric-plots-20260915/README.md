# Proposal plot selection — 15 September 2026

These plots describe the 24 existing completed trials. No new experiment is implied.

The advisor proposal already includes completion p99, unfinished percentage,
throughput, consumer-position lag B, B growth and lag skew. Retain the p99 and
unfinished plots together, with both run numbers visible. Add Q (processing
backlog) and Q growth to explain whether unfinished processing is accumulating.
Keep B and Q explicitly named rather than silently replacing one with the other.

Recommended main-result figures:

1. All-outcome summary: conditional completion p99 beside unfinished percentage.
2. Matched 80/20 comparisons, both runs: throughput and Q illustrate the repeatability
   and time course of the observed scaling benefit.
3. Matched single-partition comparisons, both runs: show favorable and unfavorable
   outcomes together. Do not select only the successful scaling run.
4. Sustained balanced comparisons as supporting context; starting ownership was
   recorded but matching was not required.

CPU/RSS and skew are explanatory diagnostics or appendix figures. CPU/RSS are
individual application processes, not pod totals, broker usage, requested resources
or proof of hardware capacity. Historical resource scrape timestamps are absent,
so cached or stale values cannot be fully excluded. Do not use these traces to
claim that one host's CPU capacity caused an observed treatment difference.

Lag skew = maximum partition B / mean partition B, with zero when all B values are
zero. With 60 partitions, concentration can approach 60; the ratio is not an input
percentage. A balanced input may still yield nonuniform lag. Low absolute lag may
also produce a high ratio, so show skew alongside absolute backlog.

Each dashboard includes throughput, B, Q, Q window growth, producer/consumer
process CPU and RSS, lag skew and maximum partition lag. Curves cover evaluation
only. The intervention marker is scheduled scaling, not completion of the handover.
Growth uses 15 contiguous intervals (30 seconds at the export grid). Missing, stale,
invalid or ownership-discontinuous lag observations remain gaps. The reported lag
coverage does not certify growth-window or resource coverage.

Throughput uses distinct IDs in nonoverlapping 30-second evaluation bins and is
checked against the saved run-average useful throughput. When local event evidence
cannot reproduce that total, show the saved run mean as a labelled horizontal line,
not a reconstructed curve. Producer acknowledgment curves require the producer
archive count to match the final-record count. Raw historical records are preserved.

Generate PNG and editable SVG figures with:

    python python-scripts/draw_all_results.py --output results/proposal-metric-plots-20260915

Dependencies: matplotlib and numpy. Generated figures and numeric plot data are in
that results directory, outside Git. The small source hash index distinguishes
hashes of JSON file bytes from hashes of decompressed event text (`decoded_text:`).
These hashes do not replace separate raw-data backups.

Validation: generated 12 ten-panel dashboards and one outcome overview in both
PNG and SVG (26 figures). All 24 reconstructed useful-throughput means match their
saved outcome summaries on the completed generation pass; all four process-resource
metric families are present in every run. Transient local reads initially timed out,
but the final pass completed. A synthetic duplicate-ID check verified bin counting.
Figures were visually inspected; outcome unfinished percentages share a 0–100 scale.
No live monitoring freshness or new experiment validation is claimed.
