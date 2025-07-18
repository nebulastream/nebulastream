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

namespace NES::Sinks
{

using SinkValidationRegistryReturnType = DescriptorConfig::Config;
struct SinkValidationRegistryArguments
{
    std::unordered_map<std::string, std::string> config;
};

class SinkValidationRegistry final
    : public BaseRegistry<SinkValidationRegistry, std::string, SinkValidationRegistryReturnType, SinkValidationRegistryArguments>
{
};

}

#define INCLUDED_FROM_SINK_VALIDATION_REGISTRY
#include <SinkValidationGeneratedRegistrar.inc>
#undef INCLUDED_FROM_SINK_VALIDATION_REGISTRY
