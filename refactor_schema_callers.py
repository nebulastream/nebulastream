import os
import glob
import re

for root, dirs, files in os.walk('.'):
    for file in files:
        if file.endswith(('.hpp', '.cpp')):
            path = os.path.join(root, file)
            # Skip nes-nautilus as it doesn't have PhysicalFieldHelper nor does it call lowerSchema much (except tests)
            with open(path, 'r', encoding='utf-8') as f:
                content = f.read()

            original_content = content
            
            # replace JoinSchema
            if 'JoinSchema' in content:
                content = re.sub(r'#include <Schema/Schema\.hpp>\n?', '', content)
                content = re.sub(r'#include <Schema/SchemaFwd\.hpp>\n?', '', content)
                content = re.sub(r'const Schema<QualifiedUnboundField, Ordered>& (\w+)', r'const std::vector<PhysicalField>& \1', content)
                content = re.sub(r'Schema<QualifiedUnboundField, Ordered>', r'std::vector<PhysicalField>', content)
                
                # if inside JoinSchema construction we pass schema directly, we need to wrap it
                # JoinSchema(leftSchema, rightSchema, joinSchema) -> JoinSchema(PhysicalFieldHelper::createPhysicalFields(leftSchema), ...)
                content = re.sub(r'JoinSchema\(([^,]+),([^,]+),([^)]+)\)', 
                                 r'JoinSchema(NES::PhysicalFieldHelper::createPhysicalFields(\1), NES::PhysicalFieldHelper::createPhysicalFields(\2), NES::PhysicalFieldHelper::createPhysicalFields(\3))', 
                                 content)

            # replace LowerSchemaProvider::lowerSchema
            if 'LowerSchemaProvider::lowerSchema' in content:
                # LowerSchemaProvider::lowerSchema(a, b, c) -> LowerSchemaProvider::lowerSchema(a, NES::PhysicalFieldHelper::createPhysicalFields(b), c)
                content = re.sub(r'LowerSchemaProvider::lowerSchema\(([^,]+),\s*([^,]+),\s*([^)]+)\)',
                                 r'LowerSchemaProvider::lowerSchema(\1, NES::PhysicalFieldHelper::createPhysicalFields(\2), \3)',
                                 content)
                                 
                # LowerSchemaProvider::lowerSchemaWithOutputFormat(a, b, c, d)
                content = re.sub(r'LowerSchemaProvider::lowerSchemaWithOutputFormat\(([^,]+),\s*([^,]+),\s*([^,]+),\s*([^)]+)\)',
                                 r'LowerSchemaProvider::lowerSchemaWithOutputFormat(\1, NES::PhysicalFieldHelper::createPhysicalFields(\2), \3, \4)',
                                 content)

            if content != original_content:
                if 'PhysicalFieldHelper' in content and 'PhysicalFieldHelper.hpp' not in content:
                    content = '#include <LoweringRules/LowerToPhysical/PhysicalFieldHelper.hpp>\n' + content
                if 'PhysicalField' in content and 'PhysicalField.hpp' not in content:
                    content = '#include <Interface/PhysicalField.hpp>\n' + content

                with open(path, 'w', encoding='utf-8') as f:
                    f.write(content)
