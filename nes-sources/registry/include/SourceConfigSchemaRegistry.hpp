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
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigLiteral.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Types/NESType.hpp>
#include <Util/RuntimeRegistry.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatterDescriptor.hpp>

namespace NES
{

using SourceConfigSchemaProviderFn = std::function<Schema<QualifiedErasedConfigField, Ordered>()>;

struct SourceTypeTrait
{
    Identifier sourceType;
};

/// Value representation produced by SQL anonymous source constructors (e.g. `FileSource(...)`).
/// Carries the source type and config literal schema without depending on `LogicalOperator`,
/// avoiding circular dependencies between `nes-sources` and `nes-logical-operators`.
struct BoundAnonymousSource
{
    Identifier sourceType;
    Schema<LiteralConfigValue, Ordered> configOptions;

    [[nodiscard]] std::expected<
        std::tuple<GeneralSourceConfig, PluginSourceConfiguration, InputFormatterDescriptor, Schema<UnqualifiedUnboundField, Ordered>>,
        Exception>
    resolve(
        const Schema<ConfigFieldDefault, Ordered>& configDefaults = {},
        const Schema<ConfigFieldTransformation, Unordered>& configTransformations = {}) const;
};

class SourceConfigSchemaRegistry
    : public RuntimeRegistry<SourceConfigSchemaRegistry, std::string, SourceConfigSchemaProviderFn, /*CaseSensitive*/ false>
{
public:
    static constexpr bool EXPOSE_TO_SQL_SCOPE = true;

    static SourceConfigSchemaRegistry& instance();
    static NESTypeClass sourceTypeClass();
    static std::vector<NamedNESParametrizedTypeClass> getParametrizedTypeClasses();

    [[nodiscard]] std::optional<Schema<QualifiedErasedConfigField, Ordered>> getSchema(const std::string& sourceType) const
    {
        if (const auto provider = find(sourceType))
        {
            return (*provider)();
        }
        return std::nullopt;
    }
};

NESFunctor makeSourceConfigSchemaFunctor(std::string_view sourceTypeName, SourceConfigSchemaProviderFn provider);

}
