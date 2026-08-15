import os
import glob
import re

for root, dirs, files in os.walk('nes-physical-operators'):
    for file in files:
        if file.endswith(('.hpp', '.cpp')):
            path = os.path.join(root, file)
            with open(path, 'r', encoding='utf-8') as f:
                content = f.read()

            original_content = content
            
            # replace Schema includes
            content = re.sub(r'#include <Schema/Schema\.hpp>\n?', '', content)
            content = re.sub(r'#include <Schema/SchemaFwd\.hpp>\n?', '', content)
            content = re.sub(r'#include <Schema/Binder\.hpp>\n?', '', content)
            content = re.sub(r'#include <DataTypes/UnboundSchema\.hpp>(?: /// NOLINT[^\n]*)?\n?', '', content)
            
            # Replace QualifiedUnboundField with PhysicalField
            content = re.sub(r'std::vector<QualifiedUnboundField>', r'std::vector<NES::PhysicalField>', content)

            if content != original_content:
                with open(path, 'w', encoding='utf-8') as f:
                    f.write(content)
