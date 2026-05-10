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

#include <DataTypes/BasicDataTypes.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypeRegistry.hpp>

namespace NES
{
DataTypeRegistryReturnType provideBOOLEANDataType(DataTypeRegistryArguments args)
{
    return DataType{DataType::Type::BOOLEAN, args.nullable};
}

DataTypeRegistryReturnType provideCHARDataType(DataTypeRegistryArguments args)
{
    return DataType{DataType::Type::CHAR, args.nullable};
}

DataTypeRegistryReturnType provideINT8DataType(DataTypeRegistryArguments args)
{
    return DataType{DataType::Type::INT8, args.nullable};
}

DataTypeRegistryReturnType provideINT16DataType(DataTypeRegistryArguments args)
{
    return DataType{DataType::Type::INT16, args.nullable};
}

DataTypeRegistryReturnType provideINT32DataType(DataTypeRegistryArguments args)
{
    return DataType{DataType::Type::INT32, args.nullable};
}

DataTypeRegistryReturnType provideINT64DataType(DataTypeRegistryArguments args)
{
    return DataType{DataType::Type::INT64, args.nullable};
}

DataTypeRegistryReturnType provideUINT8DataType(DataTypeRegistryArguments args)
{
    return DataType{DataType::Type::UINT8, args.nullable};
}

DataTypeRegistryReturnType provideUINT16DataType(DataTypeRegistryArguments args)
{
    return DataType{DataType::Type::UINT16, args.nullable};
}

DataTypeRegistryReturnType provideUINT32DataType(DataTypeRegistryArguments args)
{
    return DataType{DataType::Type::UINT32, args.nullable};
}

DataTypeRegistryReturnType provideUINT64DataType(DataTypeRegistryArguments args)
{
    return DataType{DataType::Type::UINT64, args.nullable};
}

DataTypeRegistryReturnType provideFLOAT32DataType(DataTypeRegistryArguments args)
{
    return DataType{DataType::Type::FLOAT32, args.nullable};
}

DataTypeRegistryReturnType provideFLOAT64DataType(DataTypeRegistryArguments args)
{
    return DataType{DataType::Type::FLOAT64, args.nullable};
}

DataTypeRegistryReturnType provideVARSIZEDDataType(DataTypeRegistryArguments args)
{
    return DataType{DataType::Type::VARSIZED, args.nullable};
}

DataTypeRegistryReturnType provideUNDEFINEDDataType(DataTypeRegistryArguments args)
{
    return DataType{DataType::Type::UNDEFINED, args.nullable};
}
}
