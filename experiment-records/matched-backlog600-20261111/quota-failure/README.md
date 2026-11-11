# Larger-backlog attempt excluded after storage exhaustion

Run `run-20261111-095307` was the first larger-backlog Keep 3 attempt. All three producers reported `[Errno 122] Disk quota exceeded` in their final records. Per-message acknowledgment evidence ended before production ended. The existing integrity checks rejected this run. Its measured performance was not used to decide whether to repeat it.

The complete failed raw evidence remains in the campaign evidence root, with a separately verified local copy under `backup/matched-backlog600-20261111/quota-failure/run-20261111-095307`. Both local copies are on the same physical device.

The 10 GiB producer evidence volume was full. Closed producer evidence for the 24 admitted traffic-concentration runs was copied into `traffic-24-runs-v2.tar.gz` under `/app/consumer-merge-data/archived-producer-evidence-20261111` on the separate consumer PVC. All 216 archived files were verified against source sizes and SHA-256 hashes. Sources were checked again before removing only those archived duplicates from the producer evidence directory. Local traffic results and backups remain unchanged. The first empty transfer artifact is retained separately and is not the verified archive.

This freed about 2.73 GB. A fresh write and fsync succeeded. Stopped run control, the original shared configuration, consumer StatefulSet template and update strategy, and 3 starting consumers were verified before retry. The frozen runtime revision remains `8321430676485ac349bbed0328fde8f0f4635b1d`. Scientific settings, pressure gates and validation rules were unchanged. The accepted smaller Keep 3 result is retained.

The archive hash, complete file manifest, and storage verification are recorded alongside this note. These small records do not replace the raw evidence archive.
