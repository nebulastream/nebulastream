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
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/PhysicalLayout.hpp>
#include <LogicalTypeLoweringRegistry.hpp>

namespace NES
{

namespace
{
PhysicalLayout scalarLayout(const DataType::Type physicalType, const LogicalTypeLoweringRegistryArguments& args)
{
    const auto nullable = args.logicalType.getNullable();
    return PhysicalLayout{.components = {{.suffix = "", .physicalType = DataTypeProvider::provideDataType(physicalType, nullable)}}};
}
}

LogicalTypeLoweringRegistryReturnType
LogicalTypeLoweringGeneratedRegistrar::RegisterBOOLEANLogicalTypeLowering(LogicalTypeLoweringRegistryArguments args)
{
    return scalarLayout(DataType::Type::BOOLEAN, args);
}

LogicalTypeLoweringRegistryReturnType
LogicalTypeLoweringGeneratedRegistrar::RegisterCHARLogicalTypeLowering(LogicalTypeLoweringRegistryArguments args)
{
    return scalarLayout(DataType::Type::CHAR, args);
}

LogicalTypeLoweringRegistryReturnType
LogicalTypeLoweringGeneratedRegistrar::RegisterFLOAT32LogicalTypeLowering(LogicalTypeLoweringRegistryArguments args)
{
    return scalarLayout(DataType::Type::FLOAT32, args);
}

LogicalTypeLoweringRegistryReturnType
LogicalTypeLoweringGeneratedRegistrar::RegisterFLOAT64LogicalTypeLowering(LogicalTypeLoweringRegistryArguments args)
{
    return scalarLayout(DataType::Type::FLOAT64, args);
}

LogicalTypeLoweringRegistryReturnType
LogicalTypeLoweringGeneratedRegistrar::RegisterINT8LogicalTypeLowering(LogicalTypeLoweringRegistryArguments args)
{
    return scalarLayout(DataType::Type::INT8, args);
}

LogicalTypeLoweringRegistryReturnType
LogicalTypeLoweringGeneratedRegistrar::RegisterINT16LogicalTypeLowering(LogicalTypeLoweringRegistryArguments args)
{
    return scalarLayout(DataType::Type::INT16, args);
}

LogicalTypeLoweringRegistryReturnType
LogicalTypeLoweringGeneratedRegistrar::RegisterINT32LogicalTypeLowering(LogicalTypeLoweringRegistryArguments args)
{
    return scalarLayout(DataType::Type::INT32, args);
}

LogicalTypeLoweringRegistryReturnType
LogicalTypeLoweringGeneratedRegistrar::RegisterINT64LogicalTypeLowering(LogicalTypeLoweringRegistryArguments args)
{
    return scalarLayout(DataType::Type::INT64, args);
}

LogicalTypeLoweringRegistryReturnType
LogicalTypeLoweringGeneratedRegistrar::RegisterUINT8LogicalTypeLowering(LogicalTypeLoweringRegistryArguments args)
{
    return scalarLayout(DataType::Type::UINT8, args);
}

LogicalTypeLoweringRegistryReturnType
LogicalTypeLoweringGeneratedRegistrar::RegisterUINT16LogicalTypeLowering(LogicalTypeLoweringRegistryArguments args)
{
    return scalarLayout(DataType::Type::UINT16, args);
}

LogicalTypeLoweringRegistryReturnType
LogicalTypeLoweringGeneratedRegistrar::RegisterUINT32LogicalTypeLowering(LogicalTypeLoweringRegistryArguments args)
{
    return scalarLayout(DataType::Type::UINT32, args);
}

LogicalTypeLoweringRegistryReturnType
LogicalTypeLoweringGeneratedRegistrar::RegisterUINT64LogicalTypeLowering(LogicalTypeLoweringRegistryArguments args)
{
    return scalarLayout(DataType::Type::UINT64, args);
}

LogicalTypeLoweringRegistryReturnType
LogicalTypeLoweringGeneratedRegistrar::RegisterUNDEFINEDLogicalTypeLowering(LogicalTypeLoweringRegistryArguments args)
{
    return scalarLayout(DataType::Type::UNDEFINED, args);
}

LogicalTypeLoweringRegistryReturnType
LogicalTypeLoweringGeneratedRegistrar::RegisterVARSIZEDLogicalTypeLowering(LogicalTypeLoweringRegistryArguments args)
{
    return scalarLayout(DataType::Type::VARSIZED, args);
}

}
