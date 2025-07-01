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

#include <Sources/SourceValidationProvider.hpp>

#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <ErrorHandling.hpp>
#include <SourceValidationRegistry.hpp>

namespace NES::Sources::SourceValidationProvider
{

std::optional<NES::Configurations::DescriptorConfig::Config>
provide(const std::string_view sourceType, std::unordered_map<std::string, std::string> stringConfig)
{
    auto sourceValidationRegistryArguments = SourceValidationRegistryArguments(std::move(stringConfig));
    return SourceValidationRegistry::instance().create(std::string{sourceType}, std::move(sourceValidationRegistryArguments));
}
}
