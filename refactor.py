import os
import glob
import re

def refactor_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    original_content = content
    
    # regex to match std::make_shared<PhysicalOperatorWrapper>( ... ) and remove the two schema args
    # PhysicalOperatorWrapper constructor calls in nes-query-compiler generally have:
    # 1. physicalOperator
    # 2. inputSchema (or leftInputSchema etc)
    # 3. outputSchema (or std::nullopt)
    # 4. inputMemoryLayout
    # 5. outputMemoryLayout
    # [optional] PipelineLocation, etc.
    
    # Let's match PhysicalOperatorWrapper( and then process arguments
    
    # We can just remove the 2nd and 3rd args if we find PhysicalOperatorWrapper
    
    # A safer regex replacement for specific known patterns:
    content = re.sub(
        r'std::make_shared<PhysicalOperatorWrapper>\(([^,]+),\s*[^,]+,\s*[^,]+,\s*([^,]+),\s*([^,\)]+)',
        r'std::make_shared<PhysicalOperatorWrapper>(\1, \2, \3',
        content
    )
    
    content = re.sub(
        r'PhysicalOperatorWrapper\(([^,]+),\s*[^,]+,\s*[^,]+,\s*([^,]+),\s*([^,\)]+)',
        r'PhysicalOperatorWrapper(\1, \2, \3',
        content
    )

    if content != original_content:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"Updated {file_path}")

if __name__ == '__main__':
    for file_path in glob.glob('nes-query-compiler/**/*.cpp', recursive=True):
        refactor_file(file_path)
    for file_path in glob.glob('nes-physical-operators/tests/**/*.cpp', recursive=True):
        refactor_file(file_path)
    for file_path in glob.glob('nes-physical-operators/src/**/*.cpp', recursive=True):
        refactor_file(file_path)
    for file_path in glob.glob('nes-physical-operators/include/**/*.hpp', recursive=True):
        refactor_file(file_path)
