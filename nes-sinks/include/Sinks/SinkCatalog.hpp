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
#include <atomic>
#include <expected>
#include <optional>
#include <ostream>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>
#include <Configurations/ConfigResolution.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <OutputFormatters/OutputFormatterDescriptor.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Formatter.hpp>
#include <folly/Synchronized.h>
#include <ErrorHandling.hpp>

namespace NES
{

struct GeneralSinkConfig
{
    Host host;
    //Schema<UnqualifiedUnboundField, Ordered> schema;
    bool addTimestamp = false;
    size_t backpressureUpperThreshold = 1000;
    size_t backpressureLowerThreshold = 200;

    friend std::ostream& operator<<(std::ostream& os, const GeneralSinkConfig& config);
};

class SinkConfigSchema
{
public:
    std::expected<std::tuple<GeneralSinkConfig, PluginSinkConfiguration, OutputFormatterDescriptor>, Exception>
    resolveConfigs(const Schema<LiteralConfigValue, Ordered>& values) const;
    SinkConfigSchema withConfigDefaults(Schema<ConfigFieldDefault, Ordered> configDefaults) const;
    SinkConfigSchema withConfigTransformations(Schema<ConfigFieldTransformation, Unordered> configTransformations) const;

private:
    SinkConfigSchema(Identifier sinkType, Identifier outputFormatterType, Schema<QualifiedErasedConfigField, Ordered> configSchema);
    Identifier sinkType;
    Identifier outputFormatterType;
    Schema<QualifiedErasedConfigField, Ordered> configSchema;
    Schema<ConfigFieldDefault, Ordered> configDefaults;
    Schema<ConfigFieldTransformation, Unordered> configTransformations;
    friend class SinkCatalog;
};

class SinkCatalog
{
public:
    /// Combines the general sink fields, the sink-declared schema, and (unless NATIVE) the output
    /// formatter fields into the schema user-passed configs are resolved against.
    [[nodiscard]] static std::expected<SinkConfigSchema, Exception>
    getConfigSchema(const Identifier& sinkType, const Identifier& outputFormatterType);

    /// Peek into the passed values (or the caller defaults) for OUTPUT_FORMATTER.TYPE — the config
    /// schema to resolve against depends on it. An absent type means NATIVE (no formatting).
    [[nodiscard]] static std::expected<Identifier, Exception>
    peekOutputFormatterType(const Schema<LiteralConfigValue, Ordered>& values, const Schema<ConfigFieldDefault, Ordered>& configDefaults);

    /// Convenience for the full resolution pipeline: peek the formatter type, build the combined
    /// config schema, and resolve the passed values against it.
    [[nodiscard]] static std::expected<std::tuple<GeneralSinkConfig, AnonymousSinkSchema, PluginSinkConfiguration, OutputFormatterDescriptor>, Exception>
        resolveAnonymousSinkConfig(const Identifier& sinkType, const Schema<LiteralConfigValue, Ordered>& values);

    [[nodiscard]] static std::expected<std::tuple<GeneralSinkConfig, PluginSinkConfiguration, OutputFormatterDescriptor>, Exception>
    resolveNamedSinkConfig(const Identifier& sinkType, const Schema<LiteralConfigValue, Ordered>& values);


    /// The host and schema in generalSinkConfig are ignored — the explicit host (frontends apply
    /// their host policy first) and the sink-declared schema are authoritative.
    [[nodiscard]] std::expected<SinkDescriptor, Exception> addSinkDescriptor(
        Identifier sinkName,
        const Schema<UnqualifiedUnboundField, Ordered>& schema,
        GeneralSinkConfig generalSinkConfig,
        PluginSinkConfiguration pluginSinkConfig,
        OutputFormatterDescriptor outputFormatterDescriptor);

    std::optional<SinkDescriptor> getSinkDescriptor(const Identifier& sinkName) const;

    [[nodiscard]] SinkDescriptor createAnonymousSinkDescriptor(
        AnonymousSinkSchema sinkSchema,
        GeneralSinkConfig generalSinkConfig,
        PluginSinkConfiguration pluginSinkConfig,
        OutputFormatterDescriptor outputFormatterDescriptor) const;

    bool removeSinkDescriptor(const Identifier& sinkName);
    bool removeSinkDescriptor(const SinkDescriptor& sinkDescriptor);

    bool containsSinkDescriptor(const Identifier& sinkName) const;
    bool containsSinkDescriptor(const SinkDescriptor& sinkDescriptor) const;

    std::vector<SinkDescriptor> getAllSinkDescriptors() const;

private:
    mutable std::atomic<AnonymousSinkId::Underlying> nextAnonymousSinkId{INITIAL_ANONYMOUS_SINK_ID.getRawValue()};
    folly::Synchronized<std::unordered_map<Identifier, SinkDescriptor>> sinks;
};
}

FMT_OSTREAM(NES::GeneralSinkConfig);
