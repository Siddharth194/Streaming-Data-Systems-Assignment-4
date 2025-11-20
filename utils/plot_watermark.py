import re
from datetime import datetime
import matplotlib.pyplot as plt

# Path to your log file
log_file = "/home/siddharth/StreamingDataSystems/assn_4/Streaming-Data-Systems-Assignment-4/watermark_progression.txt"

pattern = re.compile(
    r"wm=([\d\-T:\.Z]+)\s+processing_time=([\d\-T:\.:\.Z]+)"
)

event_times = []
processing_times = []

with open(log_file, "r") as f:
    for line in f:
        print(line)
        match = pattern.search(line)
        print(match)
        if match:
            wm_str = match.group(1)
            pt_str = match.group(2)

            # Parse timestamps
            try:
                event_dt = datetime.fromisoformat(wm_str.replace("Z", "+00:00"))
                proc_dt = datetime.fromisoformat(pt_str)
            except Exception:
                continue

            event_times.append(event_dt)
            processing_times.append(proc_dt)

# --- Plot ---
plt.figure(figsize=(10, 5))
plt.plot(event_times, processing_times, marker='o')
plt.xlabel("Event Time (Watermark)")
plt.ylabel("Processing Time")
plt.title("Processing Time vs Event Time")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
