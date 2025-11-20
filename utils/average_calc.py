# filename containing the numbers
file_path = "average_latency_spark"

# read numbers and calculate average
with open(file_path, "r") as f:
    numbers = [float(line.strip()) for line in f if line.strip()]  # ignore empty lines

if numbers:
    average = sum(numbers) / len(numbers)
    print(f"Average: {average}")
else:
    print("No numbers found in the file.")
