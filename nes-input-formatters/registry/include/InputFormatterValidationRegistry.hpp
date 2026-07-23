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

using InputFormatterValidationRegistryReturnType = DescriptorConfig::Config;

struct InputFormatterValidationRegistryArguments
{
    std::unordered_map<std::string, std::string> config;
};

using InputFormatterValidationFn = std::function<InputFormatterValidationRegistryReturnType(InputFormatterValidationRegistryArguments)>;

/// Creates the registry entry for an indexer implementation: validation delegates to the indexer
/// class's static validateAndFormat.
template <typename IndexerImpl>
InputFormatterValidationFn makeInputFormatterValidator()
{
    return [](InputFormatterValidationRegistryArguments arguments) -> InputFormatterValidationRegistryReturnType
    { return IndexerImpl::validateAndFormat(std::move(arguments.config)); };
}

/// Filled by loadBuiltinPlugins() / plugin registration (see cmake/RuntimeRegistrationUtil.cmake).
/// Case-insensitive to mirror the retired BaseRegistry.
class InputFormatterValidationRegistry final : public RuntimeRegistry<
                                                   InputFormatterValidationRegistry,
                                                   std::string,
                                                   InputFormatterValidationFn,
                                                   /*CaseSensitive*/ false>
{
public:
    /// Defined out-of-line (InputFormatterValidationRegistry.cpp) so exactly one instance
    /// exists process-wide even with plugins loaded as shared objects.
    static InputFormatterValidationRegistry& instance();
};

}
