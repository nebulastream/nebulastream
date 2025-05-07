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
#include <Configurations/Descriptor.hpp>
#include <LogicalFunctions/Function.hpp>
#include <Util/Registry.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES::Logical
{

using FunctionRegistryReturnType = Logical::Function;
struct FunctionRegistryArguments
{
    NES::Configurations::DescriptorConfig::Config config;
    std::vector<Logical::Function> children;
    std::shared_ptr<DataType> stamp;
};

class FunctionRegistry
    : public BaseRegistry<FunctionRegistry, std::string, FunctionRegistryReturnType, FunctionRegistryArguments>
{
};
}

#define INCLUDED_FROM_REGISTRY_FUNCTION
#include <FunctionGeneratedRegistrar.inc>
#undef INCLUDED_FROM_REGISTRY_FUNCTION
