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

#include <any>
#include <atomic>
#include <cstdint>
#include <expected>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

#include <Configurations/ConfigResolution.hpp>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include "Util/Pointers.hpp"

namespace NES
{

struct GeneralSourceConfig
{
    Host host;
    std::optional<size_t> maxInflightBuffers;
    bool operator==(const GeneralSourceConfig&) const = default;

    friend std::ostream& operator<<(std::ostream& os, const GeneralSourceConfig& config);
};

class SourceCatalog;

class PhysicalSourceBuilder
{
public:
    PhysicalSourceBuilder(
        GeneralSourceConfig generalSourceConfig,
        PluginSourceConfiguration sourcePluginConfig,
        InputFormatterDescriptor inputFormatterPluginConfig,
        std::shared_ptr<const SourceCatalog> catalog);
    std::expected<SourceDescriptor, Exception> build(Schema<UnqualifiedUnboundField, Ordered> schema) &&;
    PhysicalSourceBuilder(const PhysicalSourceBuilder& other) = delete;
    PhysicalSourceBuilder(PhysicalSourceBuilder&& other) noexcept = default;
    PhysicalSourceBuilder& operator=(const PhysicalSourceBuilder& other) = delete;
    PhysicalSourceBuilder& operator=(PhysicalSourceBuilder&& other) noexcept = default;

private:
    GeneralSourceConfig generalSourceConfig;
    PluginSourceConfiguration sourcePluginConfig;
    InputFormatterDescriptor inputFormatterPluginConfig;
    std::shared_ptr<const SourceCatalog> catalog;
    bool wasCalled = false;
};

class SourceConfigSchema
{
public:
    std::expected<std::tuple<GeneralSourceConfig, PluginSourceConfiguration, InputFormatterDescriptor, std::optional<Schema<UnqualifiedUnboundField, Ordered>>>, Exception>
    resolveConfigs(const Schema<LiteralConfigValue, Ordered>& values) const;
    SourceConfigSchema withConfigDefaults(Schema<ConfigFieldDefault, Ordered> configDefaults) const;
    SourceConfigSchema withConfigTransformations(Schema<ConfigFieldTransformation, Unordered> configTransformations) const;

private:
    SourceConfigSchema(Identifier sourceType, Identifier inputFormatterType, Schema<QualifiedErasedConfigField, Ordered> configSchema);
    Identifier sourceType;
    Identifier inputFormatterType;
    Schema<QualifiedErasedConfigField, Ordered> configSchema;
    Schema<ConfigFieldDefault, Ordered> configDefaults;
    Schema<ConfigFieldTransformation, Unordered> configTransformations;
    friend class SourceCatalog;
};

/// @brief The source catalog handles the mapping of logical to physical sources.
/// We expect the class to be used behind frontends that permit concurrent read-write access (like a REST server),
/// so all individual operations in this class are thread safe and atomic.
class SourceCatalog : std::enable_shared_from_this<SourceCatalog>
{
    struct Private
    {
        explicit Private() = default;
    };

public:
    explicit SourceCatalog(Private);
    static SharedPtr<SourceCatalog> create();
    ~SourceCatalog() = default;

    SourceCatalog(const SourceCatalog&) = delete;
    SourceCatalog(SourceCatalog&&) = delete;
    SourceCatalog& operator=(const SourceCatalog&) = delete;
    SourceCatalog& operator=(SourceCatalog&&) = delete;

    /// @param schema the schema of fields without the logical source name as a prefix
    /// @return the created logical source if successful with a schema containing the logical source name as a prefix,
    /// nullopt if a logical source with that name already existed
    [[nodiscard]] std::optional<NES::LogicalSource>
    addLogicalSource(const Identifier& logicalSourceName, const Schema<UnqualifiedUnboundField, Ordered>& schema);

    /// @brief method to delete a logical source and any associated physical source.
    /// @return bool indicating if this logical source was registered by name and removed
    [[nodiscard]] bool removeLogicalSource(const LogicalSource& logicalSource);

    [[nodiscard]] static std::expected<SourceConfigSchema, Exception>
    getConfigSchema(const Identifier& sourceType, const Identifier& inputFormatterType);

    std::expected<SourceDescriptor, Exception> registerWithLogicalSource(PhysicalSourceBuilder builder, const Identifier& logicalSourceName);

    /// @brief removes a physical source
    /// @return true if there is a source descriptor with that id registered and it was removed
    [[nodiscard]] bool removePhysicalSource(const SourceDescriptor& physicalSource);

    [[nodiscard]] std::optional<LogicalSource> getLogicalSource(const Identifier& logicalSourceName) const;

    [[nodiscard]] bool containsLogicalSource(const LogicalSource& logicalSource) const;
    [[nodiscard]] bool containsLogicalSource(const Identifier& logicalSourceName) const;

    [[nodiscard]] std::optional<SourceDescriptor> getPhysicalSource(PhysicalSourceId physicalSourceId) const;

    /// @brief retrieves physical sources for a logical source
    /// @returns nullopt if the logical source is not registered anymore, else the set of source descriptors associated with it
    [[nodiscard]] std::optional<std::unordered_set<SourceDescriptor>> getPhysicalSources(const LogicalSource& logicalSource) const;

    [[nodiscard]] std::unordered_set<LogicalSource> getAllLogicalSources() const;
    [[nodiscard]] std::unordered_map<LogicalSource, std::unordered_set<SourceDescriptor>> getLogicalToPhysicalSourceMapping() const;


private:
    mutable std::recursive_mutex catalogMutex;
    mutable std::atomic<PhysicalSourceId::Underlying> nextPhysicalSourceId{INITIAL_PHYSICAL_SOURCE_ID.getRawValue()};
    std::unordered_map<Identifier, LogicalSource> namesToLogicalSourceMapping;
    std::unordered_map<PhysicalSourceId, SourceDescriptor> idsToPhysicalSources;
    std::unordered_map<LogicalSource, std::unordered_set<SourceDescriptor>> logicalToPhysicalSourceMapping;

    friend PhysicalSourceBuilder;
};
}

FMT_OSTREAM(NES::GeneralSourceConfig);
