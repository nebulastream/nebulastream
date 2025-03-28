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
#include <Util/Registry.hpp>
#include <Abstract/LogicalFunction.hpp>
#include <Configurations/Descriptor.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{


using UnaryLogicalFunctionRegistryReturnType = std::unique_ptr<UnaryLogicalFunction>;
struct UnaryLogicalFunctionRegistryArguments
{
    NES::Configurations::DescriptorConfig::Config config;
    std::unique_ptr<LogicalFunction> child;
    std::unique_ptr<DataType> stamp;
};

class UnaryLogicalFunctionRegistry : public BaseRegistry<
                                         UnaryLogicalFunctionRegistry,
                                         std::string,
                                         UnaryLogicalFunctionRegistryReturnType,
                                         UnaryLogicalFunctionRegistryArguments>
{
};

}
#define INCLUDED_FROM_REGISTRY_UNARY_LOGICAL_FUNCTION
#include <UnaryLogicalFunctionGeneratedRegistrar.inc>
#undef INCLUDED_FROM_REGISTRY_UNARY_LOGICAL_FUNCTION