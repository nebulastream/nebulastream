import os
import glob
import re

for root, dirs, files in os.walk('nes-nautilus'):
    for file in files:
        if file.endswith(('.hpp', '.cpp')):
            path = os.path.join(root, file)
            with open(path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # replace Schema includes
            content = re.sub(r'#include <Schema/Schema\.hpp>\n?', '', content)
            content = re.sub(r'#include <Schema/SchemaFwd\.hpp>\n?', '', content)
            
            # Replace Schema<QualifiedUnboundField, Ordered> with std::vector<PhysicalField>
            content = re.sub(r'const Schema<QualifiedUnboundField, Ordered>& (\w+)', r'const std::vector<PhysicalField>& \1', content)
            content = re.sub(r'Schema<QualifiedUnboundField, Ordered>', r'std::vector<PhysicalField>', content)
            
            # Add include PhysicalField.hpp
            if 'PhysicalField' in content and 'PhysicalField.hpp' not in content:
                content = '#include <Interface/PhysicalField.hpp>\n' + content
                
            with open(path, 'w', encoding='utf-8') as f:
                f.write(content)
