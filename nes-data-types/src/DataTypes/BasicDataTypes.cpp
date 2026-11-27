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

#include <BasicDataTypes.hpp>

#include <optional>

#include <DataTypes/DataType.hpp>
#include <DataTypeRegistry.hpp>

namespace NES
{
DataTypeRegistryReturnType provideBOOLEANDataType(const DataTypeRegistryArguments arguments)
{
    return DataType{DataType::Type::BOOLEAN, arguments.nullable, std::nullopt};
}

DataTypeRegistryReturnType provideCHARDataType(const DataTypeRegistryArguments arguments)
{
    return DataType{DataType::Type::CHAR, arguments.nullable, std::nullopt};
}

DataTypeRegistryReturnType provideFLOAT32DataType(const DataTypeRegistryArguments arguments)
{
    return DataType{DataType::Type::FLOAT32, arguments.nullable, std::nullopt};
}

DataTypeRegistryReturnType provideFLOAT64DataType(const DataTypeRegistryArguments arguments)
{
    return DataType{DataType::Type::FLOAT64, arguments.nullable, std::nullopt};
}

DataTypeRegistryReturnType provideINT8DataType(const DataTypeRegistryArguments arguments)
{
    return DataType{DataType::Type::INT8, arguments.nullable, std::nullopt};
}

DataTypeRegistryReturnType provideINT16DataType(const DataTypeRegistryArguments arguments)
{
    return DataType{DataType::Type::INT16, arguments.nullable, std::nullopt};
}

DataTypeRegistryReturnType provideINT32DataType(const DataTypeRegistryArguments arguments)
{
    return DataType{DataType::Type::INT32, arguments.nullable, std::nullopt};
}

DataTypeRegistryReturnType provideINT64DataType(const DataTypeRegistryArguments arguments)
{
    return DataType{DataType::Type::INT64, arguments.nullable, std::nullopt};
}

DataTypeRegistryReturnType provideUINT8DataType(const DataTypeRegistryArguments arguments)
{
    return DataType{DataType::Type::UINT8, arguments.nullable, std::nullopt};
}

DataTypeRegistryReturnType provideUINT16DataType(const DataTypeRegistryArguments arguments)
{
    return DataType{DataType::Type::UINT16, arguments.nullable, std::nullopt};
}

DataTypeRegistryReturnType provideUINT32DataType(const DataTypeRegistryArguments arguments)
{
    return DataType{DataType::Type::UINT32, arguments.nullable, std::nullopt};
}

DataTypeRegistryReturnType provideUINT64DataType(const DataTypeRegistryArguments arguments)
{
    return DataType{DataType::Type::UINT64, arguments.nullable, std::nullopt};
}

DataTypeRegistryReturnType provideUNDEFINEDDataType(const DataTypeRegistryArguments arguments)
{
    return DataType{DataType::Type::UNDEFINED, arguments.nullable, std::nullopt};
}

DataTypeRegistryReturnType provideVARSIZEDDataType(const DataTypeRegistryArguments arguments)
{
    return DataType{DataType::Type::VARSIZED, arguments.nullable, std::nullopt};
}
}
