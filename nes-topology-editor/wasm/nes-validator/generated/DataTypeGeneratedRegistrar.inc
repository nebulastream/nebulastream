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

/// NOLINT(clang-diagnostic-error)
#ifndef INCLUDED_FROM_DATA_TYPE_REGISTRY
#    error "This file should not be included directly! Include instead include DataTypeRegistry.hpp"
#endif

#include <string>
#include <Util/Registry.hpp>
#include <DataTypes/DataType.hpp>

namespace NES::DataTypeGeneratedRegistrar
{

/// declaration of all functions for 'DataTypes'
DataTypeRegistryReturnType RegisterBOOLEANDataType(DataTypeRegistryArguments);
DataTypeRegistryReturnType RegisterCHARDataType(DataTypeRegistryArguments);
DataTypeRegistryReturnType RegisterFLOAT32DataType(DataTypeRegistryArguments);
DataTypeRegistryReturnType RegisterFLOAT64DataType(DataTypeRegistryArguments);
DataTypeRegistryReturnType RegisterINT8DataType(DataTypeRegistryArguments);
DataTypeRegistryReturnType RegisterINT16DataType(DataTypeRegistryArguments);
DataTypeRegistryReturnType RegisterINT32DataType(DataTypeRegistryArguments);
DataTypeRegistryReturnType RegisterINT64DataType(DataTypeRegistryArguments);
DataTypeRegistryReturnType RegisterUINT8DataType(DataTypeRegistryArguments);
DataTypeRegistryReturnType RegisterUINT16DataType(DataTypeRegistryArguments);
DataTypeRegistryReturnType RegisterUINT32DataType(DataTypeRegistryArguments);
DataTypeRegistryReturnType RegisterUINT64DataType(DataTypeRegistryArguments);
DataTypeRegistryReturnType RegisterUNDEFINEDDataType(DataTypeRegistryArguments);
DataTypeRegistryReturnType RegisterVARSIZEDDataType(DataTypeRegistryArguments);

}

namespace NES
{
template <>
inline void Registrar<DataTypeRegistry, std::string, DataTypeRegistryReturnType, DataTypeRegistryArguments>::registerAll(
    [[maybe_unused]] Registry<Registrar>& registry)
{
    using namespace NES::DataTypeGeneratedRegistrar;
    registry.addEntry("BOOLEAN", RegisterBOOLEANDataType);
	registry.addEntry("CHAR", RegisterCHARDataType);
	registry.addEntry("FLOAT32", RegisterFLOAT32DataType);
	registry.addEntry("FLOAT64", RegisterFLOAT64DataType);
	registry.addEntry("INT8", RegisterINT8DataType);
	registry.addEntry("INT16", RegisterINT16DataType);
	registry.addEntry("INT32", RegisterINT32DataType);
	registry.addEntry("INT64", RegisterINT64DataType);
	registry.addEntry("UINT8", RegisterUINT8DataType);
	registry.addEntry("UINT16", RegisterUINT16DataType);
	registry.addEntry("UINT32", RegisterUINT32DataType);
	registry.addEntry("UINT64", RegisterUINT64DataType);
	registry.addEntry("UNDEFINED", RegisterUNDEFINEDDataType);
	registry.addEntry("VARSIZED", RegisterVARSIZEDDataType);
	
}
}
