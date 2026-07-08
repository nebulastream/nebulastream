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

#include <expected>
#include <string_view>

#include <Configurations/ConfigResolution.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatterDescriptor.hpp>

namespace NES::InputFormatterValidationProvider
{

/// Name of the format that requires no input formatting (and therefore carries no config).
constexpr std::string_view NATIVE_FORMAT = "NATIVE";

/// Resolves the passed literal config values against the config schema the input formatter
/// declares (via the InputFormatterConfigSchema registry), instantiates the formatter-defined
/// config struct (via the InputFormatterConfig registry), and wraps it in a descriptor.
/// The NATIVE format accepts no config parameters and yields a descriptor with an empty config.
[[nodiscard]] std::expected<InputFormatterDescriptor, Exception>
provide(std::string_view inputFormatterType, const Schema<LiteralConfigValue, Ordered>& config);

}
