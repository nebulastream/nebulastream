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

#ifndef INCLUDED_FROM_SINK_VALIDATION_REGISTRY
#    error "This file should not be included directly! Include instead include SinkRegistry.hpp"
#endif

#include <string>
#include <Util/PluginRegistry.hpp>
#include <Configurations/Descriptor.hpp>

namespace NES::Sinks::SinkValidationGeneratedRegistrar
{

std::unique_ptr<NES::Configurations::DescriptorConfig::Config> RegisterSinkValidationPrint(std::unordered_map<std::string, std::string>&&);
std::unique_ptr<NES::Configurations::DescriptorConfig::Config> RegisterSinkValidationFile(std::unordered_map<std::string, std::string>&&);

}

namespace NES
{
template <>
inline void Registrar<Sinks::SinkValidationRegistry, Sinks::SinkValidationRegistrySignature>::registerAll(
    [[maybe_unused]] Registry<Registrar>& registry)
{
    using namespace NES::Sinks::SinkValidationGeneratedRegistrar;
    registry.registerPlugin("Print", RegisterSinkValidationPrint);
    registry.registerPlugin("File", RegisterSinkValidationFile);
}
}
