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

// Validation-only translation unit for the File source plugin. It owns the
// RegisterFileSourceValidation symbol consumed by nes-sources-validation's
// generated registrar. The companion FileSource.cpp holds the runtime source
// class and its Register factory, and compiles into nes-sources.

#include <string>
#include <unordered_map>
#include <utility>

#include <Configurations/Descriptor.hpp>
#include <FileSourceConfig.hpp>
#include <SourceValidationRegistry.hpp>

namespace NES
{

SourceValidationRegistryReturnType RegisterFileSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersCSV>(std::move(sourceConfig.config), std::string(FileSourceName::NAME));
}

}
