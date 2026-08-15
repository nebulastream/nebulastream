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
            if 'PhysicalField' in content:
                # Replace accesses
                # field.getFullyQualifiedName() -> field.identifier
                # schema[i]->getFullyQualifiedName() -> schema[i].identifier
                # However, schema[i] is now PhysicalField, not std::optional.
                # So schema[i]-> -> schema[i].
                # Wait, schema[i] used to be an optional. 
                # Let's just fix it manually using regex.
                
                content = re.sub(r'(\w+(?:\[[^\]]+\])?)(?:->|\.)getFullyQualifiedName\(\)', r'\1.identifier', content)
                content = re.sub(r'(\w+(?:\[[^\]]+\])?)(?:->|\.)getDataType\(\)', r'\1.dataType', content)
                
                # fieldOpt.has_value() is now true since we have vector of structs.
                # fieldOpt->... -> fieldOpt.
                # Let's just remove INVARIANT(fieldOpt.has_value()...) in PagedVectorRef.cpp
                # or replace -> with . for identifier and dataType.
                content = re.sub(r'->identifier', r'.identifier', content)
                content = re.sub(r'->dataType', r'.dataType', content)
                content = re.sub(r'\bfieldOpt\.has_value\(\)', r'true', content)

            with open(path, 'w', encoding='utf-8') as f:
                f.write(content)
