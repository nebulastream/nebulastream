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

#include <Util/Registry.hpp>
#include <Util/VersionPluginList.hpp>

namespace NES
{

using InputFormatterValidationRegistryReturnType = DescriptorConfig::Config;

struct InputFormatterValidationRegistryArguments
{
    std::unordered_map<std::string, std::string> config;
};

class InputFormatterValidationRegistry final : public BaseRegistry<
                                                   InputFormatterValidationRegistry,
                                                   std::string,
                                                   InputFormatterValidationRegistryReturnType,
                                                   InputFormatterValidationRegistryArguments>
{
};
}

#define INCLUDED_FROM_INPUT_FORMATTER_VALIDATION_REGISTRY
#include <InputFormatterValidationGeneratedRegistrar.inc>
#undef INCLUDED_FROM_INPUT_FORMATTER_VALIDATION_REGISTRY

namespace NES
{
/// Lists this registry's plugins under `--version` (see Util/VersionPluginList.hpp).
inline const VersionPluginListEntry inputFormatterValidationRegistryVersionPlugins{
    "InputFormatters", [] { return InputFormatterValidationRegistry::instance().getRegisteredNames(); }};
}
