import pandas as pd
import numpy as np
import argparse
import time
import os

def simulate_processing_delay():
    # Simulate normally-distributed delay (mean=1.0s, std=0.2s)
    return max(0.0, np.random.normal(loc=1.0, scale=0.2))

def main(input_file, output_dir, pod_index, proc_index):
    print(f"[Application] Reading: {input_file}")

    try:
        df = pd.read_csv(input_file)
    except Exception as e:
        print(f"[ERROR] Failed to read {input_file}: {e}")
        return

    # Debug: show column info and sample
    #print(f"[DEBUG] Columns detected: {df.columns.tolist()}")
    #print(f"[DEBUG] Sample rows:\n{df.head()}")

    # Handle missing or unnamed columns (fallback header)
    if df.shape[1] == 1 and df.columns[0].startswith("Unnamed"):
        print(f"[WARNING] Detected single-column CSV. Attempting to reload with header=None")
        df = pd.read_csv(input_file, header=None)
        print(f"[DEBUG] Data with header=None:\n{df.head()}")

        # Fallback column names – update if needed
        expected_cols = [
            "index",
            "topic",
            "size_bytes",
            "producer_timestamp",
            "consumer_timestamp",
            "consumer_delay_sec"
        ]
        if df.shape[1] == len(expected_cols):
            df.columns = expected_cols
        else:
            print(f"[ERROR] Unexpected column count in {input_file}. Skipping file.")
            return

    if df.shape[0] == 0 or 'consumer_timestamp' not in df.columns:
        print(f"[WARNING] Empty or invalid format in {input_file}")
        return

    # Simulate delay before application starts processing
    delay = simulate_processing_delay()
    time.sleep(delay)

    # Compute application delay relative to consumer timestamp
    now = time.time()
    df['application_timestamp'] = now
    df['application_delay_sec'] = df['consumer_timestamp'].apply(
        lambda t: now - t if pd.notna(t) else None
    )

    # Rename and write the processed file
    output_file = f"consumer_app_pod{pod_index}_proc{proc_index}.csv"
    output_path = os.path.join(output_dir, output_file)

    df.to_csv(output_path, index=False, header=True)
    print(f"[Application] Processed file saved to: {output_path}")

    # Remove the original file to prevent reprocessing
    try:
        os.remove(input_file)
        print(f"[Application] Deleted original file: {input_file}")
    except Exception as e:
        print(f"[WARNING] Could not delete {input_file}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Input CSV file path")
    parser.add_argument("--output", required=True, help="Output directory path")
    parser.add_argument("--pod_index", required=True, type=int)
    parser.add_argument("--proc_index", required=True, type=int)

    args = parser.parse_args()

    main(args.input, args.output, args.pod_index, args.proc_index)

