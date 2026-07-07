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

/// Maps a source type name to a provider of the config schema the source declares
/// (see e.g. GeneratorSource::getConfigSchema). Frontends resolve user-passed configs against
/// this schema (see Configurations/ConfigResolution.hpp) to obtain an InstantiatedConfig.
/// Entries self-register at static initialization time via generated glue TUs
/// (see create_runtime_registry(SourceConfigSchema ...) in nes-sources/CMakeLists.txt).
/// The entry is a provider function rather than the schema itself so that schema construction
/// runs lazily at first use instead of during static initialization.
using SourceConfigSchemaProviderFn = std::function<Schema<QualifiedErasedConfigField, Ordered>()>;

class SourceConfigSchemaRegistry
    : public RuntimeRegistry<SourceConfigSchemaRegistry, std::string, SourceConfigSchemaProviderFn, /*CaseSensitive*/ false>
{
public:
    [[nodiscard]] std::optional<Schema<QualifiedErasedConfigField, Ordered>> getSchema(const std::string& sourceType) const
    {
        if (const auto* provider = find(sourceType))
        {
            return (*provider)();
        }
        return std::nullopt;
    }
};

}
