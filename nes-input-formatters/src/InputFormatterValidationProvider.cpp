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

#include <InputFormatterValidationProvider.hpp>

#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <InputFormatterDescriptor.hpp>
#include <InputFormatterValidationRegistry.hpp>

namespace NES::InputFormatterValidationProvider
{
std::optional<DescriptorConfig::Config>
provide(const std::string_view inputFormat, std::unordered_map<std::string, std::string> stringConfig)
{
    /// The input formatter type is passed explicitly, but the validation expects it as part of the config (see InputFormatterDescriptor::TYPE).
    stringConfig.insert_or_assign(InputFormatterDescriptor::getTypeString(), std::string{inputFormat});
    auto inputFormatValidationArgs = InputFormatterValidationRegistryArguments(std::move(stringConfig));
    return InputFormatterValidationRegistry::instance().create(std::string{inputFormat}, std::move(inputFormatValidationArgs));
}
}
