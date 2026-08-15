import os
import glob
import re

directory = r"C:\Users\Shubh\.gemini\antigravity\scratch\nebulastream\nes-logical-operators"

# Find all .cpp and .hpp files
files = glob.glob(os.path.join(directory, "**", "*.cpp"), recursive=True)
files.extend(glob.glob(os.path.join(directory, "**", "*.hpp"), recursive=True))

count = 0
for file_path in files:
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()
    
    if "CannotDeserialize" in content:
        lines = content.split('\n')
        new_lines = []
        for line in lines:
            if "CannotDeserialize" in line:
                # only skip if there is another mention of deserialize like "Failed to deserialize TypedLogicalOperator"
                lower_line = line.lower()
                # count how many times "deserialize" appears. "cannotdeserialize" has 1.
                if lower_line.count("deserialize") == 1 and "deserialized" not in lower_line:
                    line = line.replace("CannotDeserialize", "InvalidLogicalFunctionArgument")
            new_lines.append(line)
        
        new_content = '\n'.join(new_lines)
        if new_content != content:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(new_content)
            print(f"Updated {file_path}")
            count += 1

print(f"Total files updated: {count}")
