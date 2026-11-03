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
#include <UdfDescriptor.hpp>

namespace NES
{

using UdfLoweringRegistryReturnType = PhysicalFunction;

struct UdfLoweringRegistryArguments
{
    /// The UDF a call was resolved to.
    UdfDescriptor udf;
    std::vector<PhysicalFunction> childFunctions;
    std::vector<DataType> inputTypes;
    DataType outputType;
    /// The query's python_udf_import_paths.
    std::vector<std::string> importPaths;
};

using UdfLoweringFn = std::function<UdfLoweringRegistryReturnType(UdfLoweringRegistryArguments)>;

/// Lowers Inline UDF calls to physical functions, keyed by the compiler that executes them: Codon comes with nes-python-udf.
/// A backend that is missing from the registry was not part of this build. Other UDFs are not functions at this point:
/// UDFLiftingRule moved them into a UDF operator.
class UdfLoweringRegistry : public RuntimeRegistry<UdfLoweringRegistry, std::string, UdfLoweringFn, /*CaseSensitive*/ false>
{
public:
    static UdfLoweringRegistry& instance();
};

}
