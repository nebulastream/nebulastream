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

// Validation-only TU for the File sink. Mirrors FileSourceValidation.cpp on
// the sources side — owns the RegisterFileSinkValidation symbol consumed by
// nes-sinks-validation's generated registrar.

#include <string>
#include <unordered_map>
#include <utility>

#include <Configurations/Descriptor.hpp>
#include <Sinks/FileSinkConfig.hpp>
#include <SinkValidationRegistry.hpp>

namespace NES
{

SinkValidationRegistryReturnType RegisterFileSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersFile>(std::move(sinkConfig.config), std::string(FileSinkName::NAME));
}

}
