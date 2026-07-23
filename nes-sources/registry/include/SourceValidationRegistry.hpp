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

#include <functional>
#include <string>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Util/RuntimeRegistry.hpp>

namespace NES
{

using SourceValidationRegistryReturnType = DescriptorConfig::Config;

struct SourceValidationRegistryArguments
{
    std::unordered_map<std::string, std::string> config;
};

using SourceValidationFn = std::function<SourceValidationRegistryReturnType(SourceValidationRegistryArguments)>;

/// Creates the registry entry for a source implementation: validation delegates to the source
/// class's static validateAndFormat. The entry expression in cmake/RuntimeRegistrationUtil.cmake
/// instantiates this per plugin type.
template <typename SourceImpl>
SourceValidationFn makeSourceValidator()
{
    return [](SourceValidationRegistryArguments arguments) -> SourceValidationRegistryReturnType
    { return SourceImpl::validateAndFormat(std::move(arguments.config)); };
}

class SourceValidationRegistry final
    : public RuntimeRegistry<SourceValidationRegistry, std::string, SourceValidationFn, /*CaseSensitive*/ false>
{
public:
    static SourceValidationRegistry& instance();
};

}
