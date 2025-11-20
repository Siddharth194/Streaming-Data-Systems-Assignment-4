import pandas as pd
import matplotlib.pyplot as plt

# Read CSV file
df = pd.read_csv("Streaming-Data-Systems-Assignment-4/latency_records_flink")

# Sort by throughput (optional, but makes line plot cleaner)
df = df.sort_values(by="throughput")

# Plot line chart
plt.figure(figsize=(8, 5))
plt.plot(df["throughput"], df["latency"], marker="o")

# Labels and title
plt.xlabel("Throughput (events/sec)")
plt.ylabel("Latency (ms)")
plt.title("Throughput vs Latency")
plt.grid(True)

# Show the plot
plt.show()
