import re
from collections import defaultdict

# regex patterns
value_pattern = re.compile(r'(\d+.\d+)\s+ms$')
task_patterns = ["Trace Generation", "IR Generation", "MIR backend generation"]

def extract_info(file_name):
    task_times = defaultdict(list)
    with open(file_name, "r") as f:
        for line in f.readlines():
            for task in task_patterns:
                if task in line:
                    value_match = value_pattern.search(line)
                    if value_match:
                        value_in_ms = float(value_match.group(1))
                        task_times[task].append(value_in_ms)

    for task, times in task_times.items():
        average_time = sum(times) / len(times)
        print(f"Task: {task}")
        print(f"Average time in milliseconds: {average_time}")

extract_info("test.log")