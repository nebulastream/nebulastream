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
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Util/RuntimeRegistry.hpp>

namespace NES
{

using PhysicalFunctionRegistryReturnType = PhysicalFunction;

struct PhysicalFunctionRegistryArguments
{
    std::vector<PhysicalFunction> childFunctions;
    std::vector<DataType> inputTypes;
    DataType outputType;
};

using PhysicalFunctionFn = std::function<PhysicalFunctionRegistryReturnType(PhysicalFunctionRegistryArguments)>;

/// Filled by loadBuiltinPlugins() / plugin registration (see cmake/RuntimeRegistrationUtil.cmake).
/// Entries are static create<Key> members on the function classes (construction differs per
/// function, and a single class can register under several keys).
/// Case-insensitive to mirror the retired BaseRegistry.
class PhysicalFunctionRegistry : public RuntimeRegistry<PhysicalFunctionRegistry, std::string, PhysicalFunctionFn, /*CaseSensitive*/ false>
{
public:
    /// Defined out-of-line (PhysicalFunctionRegistry.cpp) so exactly one instance exists
    /// process-wide even with plugins loaded as shared objects.
    static PhysicalFunctionRegistry& instance();
};

}
