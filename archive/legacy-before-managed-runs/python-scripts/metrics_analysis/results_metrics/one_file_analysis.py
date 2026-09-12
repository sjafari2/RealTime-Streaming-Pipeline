import pandas as pd
import matplotlib.pyplot as plt

csv_path = './result_consumer_pod0_proc0.csv'

df = pd.read_csv(csv_path)

df.columns = ['index', 'delay_sec', 'size_bytes', 'recv_timestamp', 'sent_timestamp', 'topic']

df['recv_time'] = pd.to_datetime(df['recv_timestamp'], unit='s')
df['sent_time'] = pd.to_datetime(df['sent_timestamp'], unit='s')

print("=== Delay Statistics ===")
print(df['delay_sec'].describe())

print("\n=== Message Size Statistics ===")
print(df['size_bytes'].describe())

duration_sec = df['recv_timestamp'].max() - df['recv_timestamp'].min()
total_bytes = df['size_bytes'].sum()
throughput_mbps = (total_bytes / 1024 / 1024) / duration_sec
print(f"\nTotal duration: {duration_sec:.2f} sec")
print(f"Total size: {total_bytes / 1024:.2f} KB")
print(f"Estimated throughput: {throughput_mbps:.6f} MB/s")

plt.figure(figsize=(12, 6))
plt.plot(df['recv_time'], df['delay_sec'], marker='o', linestyle='-', markersize=3)
plt.title('Kafka Message Delay Over Time')
plt.xlabel('Receive Time')
plt.ylabel('Delay (seconds)')
plt.grid(True)
plt.tight_layout()
plt.show()

