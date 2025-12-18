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

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Serialization/SerializedData.hpp>
#include "Functions/LogicalFunction.hpp"
#include "Sources/SourceDescriptor.hpp"

namespace NES
{
SerializedDataType serializeDataType(const DataType& dataType);
DataType deserializeDataType(const SerializedDataType& dataType);

SerializedSchema serializeSchema(const Schema& schema);
Schema deserializeSchema(const SerializedSchema& schema);
std::vector<Schema> deserializeSchemas(const std::vector<SerializedSchema>& schemas);

Schema::MemoryLayoutType deserializeMemoryLayout(const SerializedMemoryLayout& serializedMemoryLayout);

LogicalFunction deserializeFunction(const SerializedFunction& serializedFunction);

// SourceDescriptor deserializeSourceDescriptor(const SerializedSourceDescriptor& serializedSourceDescriptor);

rfl::Generic serializeDescriptorConfig(const DescriptorConfig::Config& config);
// DescriptorConfig::Config deserializeDescriptorConfig(const SerializedDescriptorConfig serializedConfig);

}