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

#include <string>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Util/PluginRegistry.hpp>
#include <SerializableOperator.pb.h>

namespace NES
{

using RegistrySignatureLogicalOperator = RegistrySignature<std::string, LogicalOperator>;
class RegistryLogicalOperator : public BaseRegistry<RegistryLogicalOperator, RegistrySignatureLogicalOperator>
{
};

using RegistrySignatureDeserializeLogicalOperator = RegistrySignature<std::string, LogicalOperator, SerializableOperator>;
class RegistryDeserializeLogicalOperator
    : public BaseRegistry<RegistryDeserializeLogicalOperator, RegistrySignatureDeserializeLogicalOperator>
{
};
}

#define INCLUDED_FROM_REGISTRY_LOGICAL_OPERATOR
#include <Operators/LogicalOperatorGeneratedRegistrar.inc>
#undef INCLUDED_FROM_REGISTRY_LOGICAL_OPERATOR
