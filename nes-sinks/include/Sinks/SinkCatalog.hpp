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
#include <cstddef>
#include <expected>
#include <optional>
#include <ostream>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>
#include <Configurations/ConfigField.hpp>
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
    /// Host determines worker placement. Resolution leaves the INVALID sentinel if SINK.HOST was
    /// not passed; frontends then apply their host policy (or reject) before creating a descriptor.
    Host host;
    bool addTimestamp = false;
    size_t backpressureUpperThreshold = 1000;
    size_t backpressureLowerThreshold = 200;

    friend std::ostream& operator<<(std::ostream& os, const GeneralSinkConfig& config);
};

class SinkConfigSchema
{
public:
    std::expected<std::tuple<GeneralSinkConfig, AnonymousSinkSchema, PluginSinkConfiguration, OutputFormatterDescriptor>, Exception>
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

    [[nodiscard]] std::expected<SinkDescriptor, Exception> addSinkDescriptor(
        Identifier sinkName,
        const Schema<UnqualifiedUnboundField, Ordered>& schema,
        GeneralSinkConfig generalSinkConfig,
        PluginSinkConfiguration pluginSinkConfig,
        OutputFormatterDescriptor outputFormatterDescriptor);

    [[nodiscard]] SinkDescriptor createAnonymousSinkDescriptor(
        AnonymousSinkSchema sinkSchema,
        GeneralSinkConfig generalSinkConfig,
        PluginSinkConfiguration pluginSinkConfig,
        OutputFormatterDescriptor outputFormatterDescriptor) const;

    std::optional<SinkDescriptor> getSinkDescriptor(const Identifier& sinkName) const;


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
