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
#include <optional>
#include <string>

#include <Configurations/ConfigField.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/RuntimeRegistry.hpp>

namespace NES
{

/// Maps an input formatter type name to a provider of the config schema the formatter declares
/// (see e.g. CSVInputFormatIndexer::getConfigSchema). Frontends resolve user-passed configs
/// against this schema (see Configurations/ConfigResolution.hpp).
/// Entries self-register at static initialization time via generated glue TUs
/// (see create_runtime_registry(InputFormatterConfigSchema ...) in nes-input-formatters/CMakeLists.txt).
using InputFormatterConfigSchemaProviderFn = std::function<Schema<QualifiedErasedConfigField, Ordered>()>;

class InputFormatterConfigSchemaRegistry
    : public RuntimeRegistry<InputFormatterConfigSchemaRegistry, std::string, InputFormatterConfigSchemaProviderFn, /*CaseSensitive*/ false>
{
public:
    [[nodiscard]] std::optional<Schema<QualifiedErasedConfigField, Ordered>> getSchema(const std::string& inputFormatterType) const
    {
        if (const auto* provider = find(inputFormatterType))
        {
            return (*provider)();
        }
        return std::nullopt;
    }
};

}
