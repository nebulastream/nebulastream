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

#include <memory>
#include <string>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Util/Registry.hpp>

namespace NES
{

using LogicalFunctionRegistryReturnType = LogicalFunction;
struct LogicalFunctionRegistryArguments
{
    DescriptorConfig::Config config;
    std::vector<LogicalFunction> children;
    DataType dataType;
};

class LogicalFunctionRegistry
    : public BaseRegistry<LogicalFunctionRegistry, std::string, LogicalFunctionRegistryReturnType, LogicalFunctionRegistryArguments>
{
};
}

#define INCLUDED_FROM_REGISTRY_LOGICAL_FUNCTION
#include <LogicalFunctionGeneratedRegistrar.inc>
#undef INCLUDED_FROM_REGISTRY_LOGICAL_FUNCTION
