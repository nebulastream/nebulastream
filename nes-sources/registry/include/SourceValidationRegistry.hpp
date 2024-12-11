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
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <Util/PluginRegistry.hpp>

namespace NES::Sources
{

using SourceValidationRegistryReturnType = NES::Configurations::DescriptorConfig::Config;
struct SourceValidationRegistryArguments
{
    std::unordered_map<std::string, std::string> config;
};

using SourceValidationRegistrySignature
    = RegistrySignature<std::string, SourceValidationRegistryReturnType, SourceValidationRegistryArguments>;
class SourceValidationRegistry final : public BaseRegistry<SourceValidationRegistry, SourceValidationRegistrySignature>
{
};

}

#define INCLUDED_FROM_SOURCE_VALIDATION_REGISTRY
#include <SourceValidationGeneratedRegistrar.inc>
#undef INCLUDED_FROM_SOURCE_VALIDATION_REGISTRY
