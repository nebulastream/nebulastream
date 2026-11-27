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

/// Factory Functions for the built-in, basic data types.
namespace NES
{
DataTypeRegistryReturnType provideBOOLEANDataType(DataTypeRegistryArguments arguments);
DataTypeRegistryReturnType provideCHARDataType(DataTypeRegistryArguments arguments);
DataTypeRegistryReturnType provideFLOAT32DataType(DataTypeRegistryArguments arguments);
DataTypeRegistryReturnType provideFLOAT64DataType(DataTypeRegistryArguments arguments);
DataTypeRegistryReturnType provideINT8DataType(DataTypeRegistryArguments arguments);
DataTypeRegistryReturnType provideINT16DataType(DataTypeRegistryArguments arguments);
DataTypeRegistryReturnType provideINT32DataType(DataTypeRegistryArguments arguments);
DataTypeRegistryReturnType provideINT64DataType(DataTypeRegistryArguments arguments);
DataTypeRegistryReturnType provideUINT8DataType(DataTypeRegistryArguments arguments);
DataTypeRegistryReturnType provideUINT16DataType(DataTypeRegistryArguments arguments);
DataTypeRegistryReturnType provideUINT32DataType(DataTypeRegistryArguments arguments);
DataTypeRegistryReturnType provideUINT64DataType(DataTypeRegistryArguments arguments);
DataTypeRegistryReturnType provideUNDEFINEDDataType(DataTypeRegistryArguments arguments);
DataTypeRegistryReturnType provideVARSIZEDDataType(DataTypeRegistryArguments arguments);
}
