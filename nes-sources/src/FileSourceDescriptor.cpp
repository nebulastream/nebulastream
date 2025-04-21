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

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <format>
#include <ios>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <ErrorHandling.hpp>
#include <FileSourceDescriptor.hpp>
#include <SourceValidationRegistry.hpp>

namespace NES::Sources
{

static NES::Configurations::DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return Configurations::DescriptorConfig::validateAndFormat<ConfigParametersCSV>(std::move(config), ConfigParametersCSV::Name);
}


SourceValidationRegistryReturnType
SourceValidationGeneratedRegistrar::RegisterFileSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return validateAndFormat(std::move(sourceConfig.config));
}

}
