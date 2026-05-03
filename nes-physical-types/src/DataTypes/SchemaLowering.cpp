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

#include <DataTypes/SchemaLowering.hpp>

#include <string>
#include <utility>

#include <DataTypes/DataType.hpp>
#include <DataTypes/LogicalType.hpp>
#include <DataTypes/Nullable.hpp>
#include <DataTypes/PhysicalLayout.hpp>
#include <DataTypes/Schema.hpp>
#include <ErrorHandling.hpp>
#include <LogicalTypeLoweringRegistry.hpp>

namespace NES
{

namespace
{
/// Map a primitive physical DataType back into the prototype's reduced
/// LogicalType vocabulary (INTEGER / FLOAT / BOOL / TEXT). Lowered Schema
/// fields use these user-facing names so the result round-trips through
/// `toPhysical` and `SchemaFormatter` with the names users see in DDL.
LogicalType primitiveLogicalType(const DataType& dt)
{
    const auto nullable = dt.nullable ? Nullable::IS_NULLABLE : Nullable::NOT_NULLABLE;
    using Type = DataType::Type;
    switch (dt.type)
    {
        case Type::INT8:
        case Type::INT16:
        case Type::INT32:
        case Type::INT64:
        case Type::UINT8:
        case Type::UINT16:
        case Type::UINT32:
        case Type::UINT64:
            return LogicalType{"INTEGER", {}, nullable};
        case Type::FLOAT32:
        case Type::FLOAT64:
            return LogicalType{"FLOAT", {}, nullable};
        case Type::BOOLEAN:
            return LogicalType{"BOOL", {}, nullable};
        case Type::CHAR:
        case Type::VARSIZED:
            return LogicalType{"TEXT", {}, nullable};
        case Type::UNDEFINED:
            return LogicalType{"UNDEFINED", {}, nullable};
    }
    std::unreachable();
}
}

Schema lowerSchema(const Schema& logical)
{
    Schema lowered;
    for (const auto& field : logical.getFields())
    {
        auto layoutOpt = LogicalTypeLoweringRegistry::instance().create(
            std::string(field.logicalType.getName()), LogicalTypeLoweringRegistryArguments{.logicalType = field.logicalType});
        INVARIANT(
            layoutOpt.has_value(),
            "No physical layout registered for logical type '{}' (field '{}'). Add an entry to LogicalTypeLoweringRegistry.",
            field.logicalType.getName(),
            field.name);
        for (const auto& component : layoutOpt->components)
        {
            lowered.addField(field.name + component.suffix, primitiveLogicalType(component.physicalType));
        }
    }
    return lowered;
}

}
