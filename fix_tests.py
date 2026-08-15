import os
import re

files = [
    'nes-physical-operators/tests/EmitPhysicalOperatorTest.cpp',
    'nes-physical-operators/tests/InferModelPhysicalOperatorTest.cpp'
]

for path in files:
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Replace Schema with std::vector<PhysicalField>
        content = re.sub(r'#include <Schema/Schema\.hpp>\n?', '', content)
        content = re.sub(r'#include <Schema/SchemaFwd\.hpp>\n?', '', content)
        content = re.sub(r'#include <DataTypes/UnboundSchema\.hpp>\n?', '', content)
        
        if 'EmitPhysicalOperatorTest.cpp' in path:
            content = content.replace(
                'auto schema = Schema<QualifiedUnboundField, Ordered>{QualifiedUnboundField{Identifier::parse("A_FIELD"), DataType::Type::UINT32}};',
                'auto schema = std::vector<PhysicalField>{{Identifier::parse("A_FIELD"), DataType::Type::UINT32}};'
            )
        
        if 'InferModelPhysicalOperatorTest.cpp' in path:
            content = content.replace(
                'using TestSchema = Schema<UnqualifiedUnboundField, Ordered>;',
                'using TestSchema = std::vector<PhysicalField>;'
            )
            # Fix usages of TestSchema
            content = content.replace(
                'TestSchema{UnqualifiedUnboundField{Identifier::parse("id"), DataType::Type::UINT32},',
                'TestSchema{{Identifier::parse("id"), DataType::Type::UINT32},'
            )
            content = content.replace(
                'UnqualifiedUnboundField{Identifier::parse("value"), DataType::Type::UINT32}}',
                '{Identifier::parse("value"), DataType::Type::UINT32}}'
            )
            
        with open(path, 'w', encoding='utf-8') as f:
            f.write(content)
