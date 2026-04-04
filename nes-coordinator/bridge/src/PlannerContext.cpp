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

#include <PlannerContext.hpp>

#include <string>
#include <DataTypes/Schema.hpp>
#include <Sources/SourceValidationProvider.hpp>
#include <Util/Reflection.hpp>
#include <coordinator/lib.h>
#include <rfl/json/read.hpp>
#include <rfl/json/write.hpp>
#include <rust/cxx.h>

#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
Schema schemaFromJson(const std::string& json)
{
    auto result = rfl::json::read<detail::ReflectedSchema>(json);
    if (!result)
    {
        throw CannotDeserialize("Failed to deserialize Schema from JSON: {}", result.error().what());
    }
    return unreflect<Schema>(reflect(*result));
}

std::unordered_map<std::string, std::string> configFromJson(const std::string& json)
{
    auto result = rfl::json::read<std::unordered_map<std::string, std::string>>(json);
    if (!result)
    {
        throw CannotDeserialize("Failed to deserialize config from JSON: {}", result.error().what());
    }
    return *result;
}
}

std::optional<LogicalSource> getLogicalSource(const PlannerContext& ctx, std::string_view name)
{
    try
    {
        auto ffi = get_logical_source(ctx, rust::Str(name.data(), name.size()));
        auto schema = schemaFromJson(std::string(ffi.schema_json));
        return LogicalSource{std::string(ffi.name), schema};
    }
    catch (const rust::Error&)
    {
        return std::nullopt;
    }
}

std::optional<std::unordered_set<SourceDescriptor>> getSourceDescriptors(const PlannerContext& ctx, std::string_view logicalSourceName)
{
    try
    {
        auto ffiSources = get_source_descriptors(ctx, rust::Str(logicalSourceName.data(), logicalSourceName.size()));
        if (ffiSources.empty())
        {
            return std::nullopt;
        }

        std::unordered_set<SourceDescriptor> result;
        for (const auto& ffi : ffiSources)
        {
            auto logicalSource = getLogicalSource(ctx, std::string_view(ffi.logical_source_name));
            if (!logicalSource)
            {
                continue;
            }

            auto parserConfig = ParserConfig::create(configFromJson(std::string(ffi.parser_config_json)));
            auto sourceConfig = configFromJson(std::string(ffi.source_config_json));
            auto descriptorConfig = SourceValidationProvider::provide(std::string(ffi.source_type), std::move(sourceConfig));
            if (!descriptorConfig)
            {
                continue;
            }

            result.emplace(
                PhysicalSourceId{static_cast<PhysicalSourceId::Underlying>(ffi.id)},
                *logicalSource,
                std::string(ffi.source_type),
                Host(std::string(ffi.host_addr)),
                *descriptorConfig,
                parserConfig);
        }
        return result;
    }
    catch (const rust::Error&)
    {
        return std::nullopt;
    }
}

std::optional<SinkDescriptor> getSinkDescriptor(const PlannerContext& ctx, std::string_view name)
{
    try
    {
        auto ffi = get_sink_descriptor(ctx, rust::Str(name.data(), name.size()));
        auto schema = schemaFromJson(std::string(ffi.schema_json));
        auto config = configFromJson(std::string(ffi.config_json));
        auto descriptorConfig = SinkDescriptor::validateAndFormatConfig(std::string(ffi.sink_type), std::move(config));
        if (!descriptorConfig)
        {
            return std::nullopt;
        }
        return SinkDescriptor{
            SinkId{static_cast<uint64_t>(ffi.id)},
            std::string(ffi.name),
            schema,
            std::string(ffi.sink_type),
            Host(std::string(ffi.host_addr)),
            {},
            *descriptorConfig};
    }
    catch (const rust::Error&)
    {
        return std::nullopt;
    }
}

std::optional<WorkerInfo> getWorker(const PlannerContext& ctx, const Host& host)
{
    try
    {
        auto ffi = get_worker(ctx, rust::Str(host.getRawValue().data(), host.getRawValue().size()));
        Capacity cap = ffi.max_operators < 0
            ? Capacity{CapacityKind::Unlimited{}}
            : Capacity{CapacityKind::Limited{static_cast<size_t>(ffi.max_operators)}};
        return WorkerInfo{
            .host = Host(std::string(ffi.host_addr)),
            .data = std::string(ffi.data_addr),
            .maxOperators = cap};
    }
    catch (const rust::Error&)
    {
        return std::nullopt;
    }
}

NetworkTopology getTopology(const PlannerContext& ctx)
{
    auto ffi = get_topology(ctx);
    std::vector<Host> nodes;
    for (const auto& n : ffi.nodes) { nodes.emplace_back(std::string(n)); }
    std::vector<std::pair<Host, Host>> edges;
    for (const auto& l : ffi.links) { edges.emplace_back(Host(std::string(l.src_addr)), Host(std::string(l.dst_addr))); }
    return NetworkTopology::fromEdges(nodes, edges);
}

std::optional<SourceDescriptor> createInlineSource(
    const PlannerContext& ctx,
    const ConnectorKind kind,
    const std::string& sourceType,
    const Schema& schema,
    std::unordered_map<std::string, std::string> parserConfigMap,
    std::unordered_map<std::string, std::string> sourceConfigMap)
{
    Host host{""};
    if (auto it = sourceConfigMap.find("host"); it != sourceConfigMap.end())
    {
        host = Host(it->second);
        sourceConfigMap.erase(it);
    }

    auto schemaJson = rfl::json::write(reflect(schema));
    auto sourceConfigJson = rfl::json::write(sourceConfigMap);
    auto parserConfigJson = rfl::json::write(parserConfigMap);

    auto descriptorConfig = SourceValidationProvider::provide(sourceType, std::move(sourceConfigMap));
    if (!descriptorConfig)
    {
        return std::nullopt;
    }

    auto parserConfig = ParserConfig::create(std::move(parserConfigMap));
    auto id = PhysicalSourceId{static_cast<uint64_t>(
        create_inline_source(ctx, static_cast<bool>(kind), sourceType, schemaJson, sourceConfigJson, parserConfigJson, host.getRawValue()))};
    return SourceDescriptor{id, LogicalSource{id.toString(), schema}, sourceType, host, std::move(*descriptorConfig), parserConfig};
}

std::optional<SinkDescriptor> createInlineSink(
    const PlannerContext& ctx,
    const ConnectorKind kind,
    const Schema& schema,
    const std::string_view sinkType,
    std::unordered_map<std::string, std::string> config,
    const std::unordered_map<std::string, std::string>& formatConfig)
{
    Host host{""};
    if (const auto it = config.find("host"); it != config.end())
    {
        host = Host(it->second);
        config.erase(it);
    }

    auto schemaJson = rfl::json::write(reflect(schema));
    auto configJson = rfl::json::write(config);

    auto descriptorConfig = SinkDescriptor::validateAndFormatConfig(std::string{sinkType}, std::move(config));
    if (!descriptorConfig)
    {
        return std::nullopt;
    }
    auto id = static_cast<uint64_t>(
        create_inline_sink(ctx, static_cast<bool>(kind), std::string(sinkType), schemaJson, configJson, host.getRawValue()));
    return SinkDescriptor{SinkId{id}, "", schema, std::string(sinkType), host, formatConfig, std::move(*descriptorConfig)};
}

}
