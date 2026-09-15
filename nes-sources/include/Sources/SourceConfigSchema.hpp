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

#include <cstddef>
#include <expected>
#include <optional>
#include <ostream>
#include <tuple>

#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigLiteral.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Formatter.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatterDescriptor.hpp>

namespace NES
{

struct GeneralSourceConfig
{
    Host host;
    std::optional<size_t> maxInflightBuffers;
    bool operator==(const GeneralSourceConfig&) const = default;

    friend std::ostream& operator<<(std::ostream& os, const GeneralSourceConfig& config);
};

class PhysicalSourceBuilder
{
public:
    PhysicalSourceBuilder(
        GeneralSourceConfig generalSourceConfig,
        PluginSourceConfiguration sourcePluginConfig,
        InputFormatterDescriptor inputFormatterPluginConfig);
    std::expected<SourceDescriptor, Exception> build(
        PhysicalSourceId physicalSourceId,
        Schema<UnqualifiedUnboundField, Ordered> schema,
        std::optional<Identifier> logicalSourceName = std::nullopt) &&; /// NOLINT(fuchsia-default-arguments-declarations)
    PhysicalSourceBuilder(const PhysicalSourceBuilder& other) = delete;
    PhysicalSourceBuilder(PhysicalSourceBuilder&& other) noexcept = default;
    PhysicalSourceBuilder& operator=(const PhysicalSourceBuilder& other) = delete;
    PhysicalSourceBuilder& operator=(PhysicalSourceBuilder&& other) noexcept = default;
    ~PhysicalSourceBuilder() = default;

private:
    GeneralSourceConfig generalSourceConfig;
    PluginSourceConfiguration sourcePluginConfig;
    InputFormatterDescriptor inputFormatterPluginConfig;
    bool wasCalled = false;
};

class SourceConfigSchema
{
public:
    [[nodiscard]] static std::expected<SourceConfigSchema, Exception>
    create(const Identifier& sourceType, const Identifier& inputFormatterType);

    [[nodiscard]] std::expected<
        std::tuple<
            GeneralSourceConfig,
            PluginSourceConfiguration,
            InputFormatterDescriptor,
            std::optional<Schema<UnqualifiedUnboundField, Ordered>>>,
        Exception>
    resolveConfigs(const Schema<LiteralConfigValue, Ordered>& values) const;

    [[nodiscard]] const Schema<QualifiedErasedConfigField, Ordered>& getConfigFields() const { return configSchema; }

    [[nodiscard]] SourceConfigSchema withConfigDefaults(Schema<ConfigFieldDefault, Ordered> configDefaults) const;
    [[nodiscard]] SourceConfigSchema withConfigTransformations(Schema<ConfigFieldTransformation, Unordered> configTransformations) const;

private:
    SourceConfigSchema(Identifier sourceType, Identifier inputFormatterType, Schema<QualifiedErasedConfigField, Ordered> configSchema);
    Identifier sourceType;
    Identifier inputFormatterType;
    Schema<QualifiedErasedConfigField, Ordered> configSchema;
    Schema<ConfigFieldDefault, Ordered> configDefaults;
    Schema<ConfigFieldTransformation, Unordered> configTransformations;
};

}

FMT_OSTREAM(NES::GeneralSourceConfig);
