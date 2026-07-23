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

#include <functional>
#include <string>
#include <DataTypes/DataType.hpp>
#include <Util/RuntimeRegistry.hpp>

namespace NES
{

using DataTypeRegistryReturnType = DataType;

struct DataTypeRegistryArguments
{
    DataType::NULLABLE nullable{DataType::NULLABLE::IS_NULLABLE};
};

using DataTypeFn = std::function<DataTypeRegistryReturnType(DataTypeRegistryArguments)>;

/// Filled by loadBuiltinPlugins() / plugin registration (see cmake/RuntimeRegistrationUtil.cmake).
/// Entries are keyed by the DataType::Type enumerator name; the entry expression constructs the
/// DataType directly from the enumerator matching the key.
/// Case-insensitive to mirror the retired BaseRegistry.
class DataTypeRegistry : public RuntimeRegistry<DataTypeRegistry, std::string, DataTypeFn, /*CaseSensitive*/ false>
{
public:
    /// Defined out-of-line (DataTypeRegistry.cpp) so exactly one instance exists process-wide
    /// even with plugins loaded as shared objects.
    static DataTypeRegistry& instance();
};

}
