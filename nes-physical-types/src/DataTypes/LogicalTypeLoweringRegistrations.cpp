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

#include <DataTypes/DataType.hpp>
#include <DataTypes/Nullable.hpp>
#include <DataTypes/PhysicalLayout.hpp>
#include <LogicalTypeLoweringRegistry.hpp>

namespace NES
{

namespace
{
PhysicalType scalarLayout(const DataType::Type type, const LogicalTypeLoweringRegistryArguments& args)
{
    return PhysicalType{.components = {{.suffix = "", .type = type}}, .nullable = args.logicalType.isNullable()};
}
}

LogicalTypeLoweringRegistryReturnType
LogicalTypeLoweringGeneratedRegistrar::RegisterINTEGERLogicalTypeLowering(LogicalTypeLoweringRegistryArguments args)
{
    return scalarLayout(DataType::Type::INT64, args);
}

LogicalTypeLoweringRegistryReturnType
LogicalTypeLoweringGeneratedRegistrar::RegisterFLOATLogicalTypeLowering(LogicalTypeLoweringRegistryArguments args)
{
    return scalarLayout(DataType::Type::FLOAT64, args);
}

LogicalTypeLoweringRegistryReturnType
LogicalTypeLoweringGeneratedRegistrar::RegisterTEXTLogicalTypeLowering(LogicalTypeLoweringRegistryArguments args)
{
    return scalarLayout(DataType::Type::VARSIZED, args);
}

LogicalTypeLoweringRegistryReturnType
LogicalTypeLoweringGeneratedRegistrar::RegisterBOOLLogicalTypeLowering(LogicalTypeLoweringRegistryArguments args)
{
    return scalarLayout(DataType::Type::BOOLEAN, args);
}

}
