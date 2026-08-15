/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#pragma once

#include <vector>
#include <Schema/Schema.hpp>
#include <Interface/PhysicalField.hpp>

namespace NES
{

class PhysicalFieldHelper
{
public:
    static std::vector<PhysicalField> createPhysicalFields(const Schema<QualifiedUnboundField, Ordered>& schema)
    {
        std::vector<PhysicalField> fields;
        fields.reserve(schema.getSize());
        for (const auto& field : schema)
        {
            fields.push_back(PhysicalField{field.getFullyQualifiedName(), field.getDataType()});
        }
        return fields;
    }
};

} // namespace NES
