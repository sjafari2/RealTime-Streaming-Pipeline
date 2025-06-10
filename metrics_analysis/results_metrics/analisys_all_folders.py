import pandas as pd
import glob
import os
import matplotlib.pyplot as plt

base_dir = 'result/consumer/Pod_0/2025-06-02'
output_combined_csv = 'combined_kafka_metrics.csv'

csv_paths = glob.glob(os.path.join(base_dir, '*/', '*.csv'))

df_list = []
for csv_file in csv_paths:
    df = pd.read_csv(csv_file, header=None)
    df.columns = ['index', 'delay_sec', 'size_bytes', 'recv_timestamp', 'sent_timestamp', 'topic']
    df_list.append(df)

if not df_list:
    raise ValueError("No CSV files found.")

combined_df = pd.concat(df_list, ignore_index=True)

combined_df['recv_time'] = pd.to_datetime(combined_df['recv_timestamp'], unit='s')
combined_df['sent_time'] = pd.to_datetime(combined_df['sent_timestamp'], unit='s')

combined_df.to_csv(output_combined_csv, index=False)
print(f"Saved combined data to {output_combined_csv}")

print("Overall Delay Stats:")
print(combined_df['delay_sec'].describe())

print("\nMessage Size Stats:")
print(combined_df['size_bytes'].describe())

duration = combined_df['recv_timestamp'].max() - combined_df['recv_timestamp'].min()
total_bytes = combined_df['size_bytes'].sum()
throughput_mbps = (total_bytes / 1024 / 1024) / duration
print(f"\nThroughput: {throughput_mbps:.4f} MB/s")

grouped = combined_df.groupby('topic').agg({
    'delay_sec': ['mean', 'std', 'max'],
    'size_bytes': ['mean', 'std']
}).round(3)

print("\nPer-topic Summary:")
print(grouped)


# Delay Over Time
plt.figure(figsize=(12, 4))
plt.plot(combined_df['recv_time'], combined_df['delay_sec'], marker='.', linestyle='-', alpha=0.6)
plt.title("Delay Over Time")
plt.ylabel("Delay (s)")
plt.xlabel("Receive Time")
plt.grid(True)
plt.tight_layout()
plt.show()

# Boxplot per Topic
plt.figure(figsize=(10, 5))
combined_df.boxplot(column='delay_sec', by='topic', rot=90)
plt.title("Delay Distribution per Topic")
plt.suptitle("")
plt.ylabel("Delay (s)")
plt.tight_layout()
plt.show()

