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

#include <any>
#include <expected>
#include <string>
#include <string_view>
#include <utility>

#include <Configurations/ConfigResolution.hpp>
#include <Configurations/ConfigValue.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatterConfigRegistry.hpp>
#include <InputFormatterConfigSchemaRegistry.hpp>
#include <InputFormatterDescriptor.hpp>

namespace NES::InputFormatterValidationProvider
{

std::expected<InputFormatterDescriptor, Exception>
provide(const std::string_view inputFormatterType, const Schema<LiteralConfigValue, Ordered>& config)
{
    const auto type = std::string{inputFormatterType};

    /// The NATIVE format requires no input formatting and carries no config.
    if (toUpperCase(inputFormatterType) == NATIVE_FORMAT)
    {
        if (config.size() != 0)
        {
            return std::unexpected{
                InvalidConfigParameter("The NATIVE format accepts no config parameters, but {} were passed", config.size())};
        }
        return InputFormatterDescriptor{std::string{NATIVE_FORMAT}, std::any{}};
    }

    const auto declaredSchema = InputFormatterConfigSchemaRegistry::instance().getSchema(type);
    if (not declaredSchema.has_value())
    {
        return std::unexpected{UnknownInputFormatterType(
            "The input formatter type '{}' is not registered. If it is a plugin, make sure you activated it.", inputFormatterType)};
    }
    auto resolvedConfig = resolveConfig(config, *declaredSchema);
    if (not resolvedConfig.has_value())
    {
        return std::unexpected{
            InvalidConfigParameter("Invalid config for input formatter type '{}': {}", inputFormatterType, resolvedConfig.error())};
    }
    const auto* configEntry = InputFormatterConfigRegistry::instance().find(type);
    if (configEntry == nullptr)
    {
        return std::unexpected{UnknownInputFormatterType(
            "The input formatter type '{}' declares a config schema but no InputFormatterConfig registry entry.", inputFormatterType)};
    }

    return InputFormatterDescriptor{type, configEntry->instantiate(InstantiatedConfig{std::move(resolvedConfig).value()})};
}

}
