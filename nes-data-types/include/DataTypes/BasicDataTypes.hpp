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

#include <DataTypeRegistry.hpp>

namespace NES
{
/// Functions to register in the DataType registry for our "basic" types.
/// ARRAY types are not included here, they are created directly via the DataType constructor.
/// STRUCT is not included here, we only accept STRUCT datatypes as named and registered plugins, which use the corresponding DataType constructor
/// in their provide<PLUGIN>DataType function.
DataTypeRegistryReturnType provideBOOLEANDataType(DataTypeRegistryArguments args);
DataTypeRegistryReturnType provideCHARDataType(DataTypeRegistryArguments args);
DataTypeRegistryReturnType provideINT8DataType(DataTypeRegistryArguments args);
DataTypeRegistryReturnType provideINT16DataType(DataTypeRegistryArguments args);
DataTypeRegistryReturnType provideINT32DataType(DataTypeRegistryArguments args);
DataTypeRegistryReturnType provideINT64DataType(DataTypeRegistryArguments args);
DataTypeRegistryReturnType provideUINT8DataType(DataTypeRegistryArguments args);
DataTypeRegistryReturnType provideUINT16DataType(DataTypeRegistryArguments args);
DataTypeRegistryReturnType provideUINT32DataType(DataTypeRegistryArguments args);
DataTypeRegistryReturnType provideUINT64DataType(DataTypeRegistryArguments args);
DataTypeRegistryReturnType provideFLOAT32DataType(DataTypeRegistryArguments args);
DataTypeRegistryReturnType provideFLOAT64DataType(DataTypeRegistryArguments args);
DataTypeRegistryReturnType provideVARSIZEDDataType(DataTypeRegistryArguments args);
DataTypeRegistryReturnType provideUNDEFINEDDataType(DataTypeRegistryArguments args);
}
