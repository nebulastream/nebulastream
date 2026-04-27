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

// Validation-only translation unit for the TCP source plugin. It owns the
// RegisterTCPSourceValidation symbol consumed by nes-sources-validation's
// generated registrar. The runtime source class and Register factory live in
// TCPSource.cpp.

#include <string>
#include <unordered_map>
#include <utility>

#include <Configurations/Descriptor.hpp>
#include <SourceValidationRegistry.hpp>
#include <TCPSourceConfig.hpp>

namespace NES
{

SourceValidationRegistryReturnType RegisterTCPSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersTCP>(std::move(sourceConfig.config), TCPSourceName::name());
}

}
