import re
from collections import defaultdict

# regex patterns
value_pattern = re.compile(r'(\d+.\d+)\s+ms$')
task_patterns = ["Trace Generation", "IR Generation", "MIR_MIRGen", "MIR_MIRComp", "Flounder_FlounderGen", "Flounder_FlounderComp", "MLIR_MLIRGen", "[23:27", "CPP_CPPGen", "CPP_CPPComp", "BC_BCGen"]

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
        average_time = sum(times) / 15
        print(f"{task},{average_time}")

extract_info("test.log")