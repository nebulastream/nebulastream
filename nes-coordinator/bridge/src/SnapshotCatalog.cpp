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

#include <SnapshotCatalog.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <nes-coordinator-bridge/planner.h>
#include <rust/cxx.h>
#include <Catalog.hpp>
#include <ErrorHandling.hpp>
#include <HostPolicy.hpp>
#include <Model.hpp>
#include <NetworkTopology.hpp>

#include <CatalogConfig.hpp>

namespace NES
{
namespace
{
std::string_view view(const rust::String& text)
{
    return std::string_view{text.data(), text.size()};
}

/// The host of an anonymous source or sink, taken out of its config.
/// It is removed before the rest is stored, because a source or sink type rejects a key that it does not define.
Host takeHost(std::unordered_map<Identifier, std::string>& config, const std::optional<Host>& defaultHost, const std::string_view clause)
{
    std::optional<Host> declared;
    if (const auto it = config.find(Identifier::parse("host")); it != config.end())
    {
        declared = Host{it->second};
        config.erase(it);
    }
    return resolveHost(declared, defaultHost, clause);
}
}

SnapshotCatalog::SnapshotCatalog(const Bridge::CatalogSnapshot& snapshot, std::optional<Host> defaultHost)
    : snapshot{snapshot}, defaultHost{std::move(defaultHost)}, nextSourceId{snapshot.next_source_id}, nextSinkId{snapshot.next_sink_id}
{
}

LogicalSource SnapshotCatalog::getLogicalSource(const std::string_view name) const
{
    const auto found = std::ranges::find_if(snapshot.logical_sources, [&](const auto& source) { return view(source.name) == name; });
    if (found == snapshot.logical_sources.end())
    {
        throw UnknownSourceName("logical source '{}' not found", name);
    }
    /// The catalog stores canonical names; parsing would fold them a second time.
    return LogicalSource{
        Identifier::fromCanonical(std::string{found->name}),
        CatalogConfig::fromJson<Schema<UnqualifiedUnboundField, Ordered>>(std::string{found->schema_json})};
}

std::unordered_set<SourceDescriptor> SnapshotCatalog::getPhysicalSources(const LogicalSource& logicalSource) const
{
    const auto logicalName = logicalSource.getLogicalSourceName().asCanonicalString();
    std::unordered_set<SourceDescriptor> result;
    for (const auto& source :
         snapshot.physical_sources | std::views::filter([&](const auto& source) { return view(source.logical_source) == logicalName; }))
    {
        auto descriptor = SourceDescriptor::create(
            PhysicalSourceId{source.id},
            logicalSource,
            Identifier::fromCanonical(std::string{source.source_type}),
            Host{std::string{source.host_addr}},
            CatalogConfig::fromJson<CatalogConfig::Options>(std::string{source.source_config_json}).values,
            CatalogConfig::fromJson<CatalogConfig::Options>(std::string{source.parser_config_json}).values,
            false);
        if (!descriptor)
        {
            throw std::move(descriptor).error();
        }
        result.emplace(std::move(*descriptor));
    }
    return result;
}

SourceDescriptor SnapshotCatalog::createAnonymousSource(
    const ConnectorKind kind,
    const Identifier& sourceType,
    const Schema<UnqualifiedUnboundField, Ordered>& schema,
    const std::unordered_map<Identifier, std::string>& sourceConfig,
    const std::unordered_map<Identifier, std::string>& formatConfig)
{
    auto config = sourceConfig;
    const auto host = takeHost(config, defaultHost, "SOURCE");
    const auto id = PhysicalSourceId{nextSourceId++};

    auto descriptor = SourceDescriptor::create(
        id, LogicalSource{Identifier::fromCanonical(id.toString()), schema}, sourceType, host, config, formatConfig, true);
    if (!descriptor)
    {
        throw std::move(descriptor).error();
    }
    createdSources.push_back(
        Bridge::PhysicalSource{
            .id = id.getRawValue(),
            .logical_source = {},
            .host_addr = rust::String{host.getRawValue()},
            .source_type = rust::String{sourceType.asCanonicalString()},
            .source_config_json = rust::String{CatalogConfig::toJson(CatalogConfig::Options{config})},
            .parser_config_json = rust::String{CatalogConfig::toJson(CatalogConfig::Options{formatConfig})},
            .internal = kind == ConnectorKind::Internal,
        });
    return std::move(*descriptor);
}

SinkDescriptor SnapshotCatalog::getSinkDescriptor(const std::string_view sinkName) const
{
    const auto found = std::ranges::find_if(snapshot.sinks, [&](const auto& sink) { return view(sink.name) == sinkName; });
    if (found == snapshot.sinks.end())
    {
        throw UnknownSinkName("sink '{}' not found", sinkName);
    }
    const auto [sinkConfig, formatConfig] = CatalogConfig::fromJson<CatalogConfig::SinkOptions>(std::string{found->config_json});
    return SinkDescriptor::createNamed(
        SinkId{found->id},
        Identifier::fromCanonical(std::string{found->name}),
        Identifier::fromCanonical(std::string{found->sink_type}),
        CatalogConfig::fromJson<Schema<UnqualifiedUnboundField, Ordered>>(std::string{found->schema_json}),
        Host{std::string{found->host_addr}},
        sinkConfig,
        formatConfig);
}

SinkDescriptor SnapshotCatalog::createAnonymousSink(
    const ConnectorKind kind,
    const Identifier& sinkType,
    const std::optional<Schema<UnqualifiedUnboundField, Ordered>>& schema,
    const std::unordered_map<Identifier, std::string>& sinkConfig,
    const std::unordered_map<Identifier, std::string>& formatConfig)
{
    auto config = sinkConfig;
    const auto host = takeHost(config, defaultHost, "SINK");
    const auto id = SinkId{nextSinkId++};

    /// A schema-less anonymous sink (for example INTO Void() with no target schema) is stored with a null schema.
    /// Its schema is inferred later from the query output at the sink operator.
    createdSinks.push_back(
        Bridge::Sink{
            .id = id.getRawValue(),
            .name = {},
            .host_addr = rust::String{host.getRawValue()},
            .sink_type = rust::String{sinkType.asCanonicalString()},
            .schema_json = rust::String{schema.has_value() ? CatalogConfig::toJson(*schema) : std::string{"null"}},
            .config_json
            = rust::String{CatalogConfig::toJson(CatalogConfig::SinkOptions{.sinkConfig = config, .formatConfig = formatConfig})},
            .internal = kind == ConnectorKind::Internal,
        });
    return SinkDescriptor::createAnonymous(id, sinkType, schema, host, config, formatConfig);
}

WorkerInfo SnapshotCatalog::getWorker(const Host& host) const
{
    const auto found
        = std::ranges::find_if(snapshot.workers, [&](const auto& worker) { return view(worker.host_addr) == host.getRawValue(); });
    if (found == snapshot.workers.end())
    {
        throw UnknownWorker("worker '{}' not found", host.getRawValue());
    }
    const Capacity capacity = found->max_operators < 0 ? Capacity{CapacityKind::Unlimited{}}
                                                       : Capacity{CapacityKind::Limited{static_cast<size_t>(found->max_operators)}};
    return WorkerInfo{.host = host, .dataAddress = std::string{found->data_addr}, .maxOperators = capacity};
}

NetworkTopology SnapshotCatalog::getTopology() const
{
    const auto nodes = snapshot.workers
        | std::views::transform([](const Bridge::Worker& worker) { return Host{std::string{worker.host_addr}}; })
        | std::ranges::to<std::vector>();
    const auto edges = snapshot.links
        | std::views::transform([](const Bridge::NetworkLink& link)
                                { return std::pair{Host{std::string{link.src_addr}}, Host{std::string{link.dst_addr}}}; })
        | std::ranges::to<std::vector>();
    return NetworkTopology::fromEdges(nodes, edges);
}

RegisteredModel SnapshotCatalog::getModel(const std::string_view name) const
{
    const auto found = std::ranges::find_if(snapshot.models, [&](const auto& model) { return view(model.name) == name; });
    if (found == snapshot.models.end())
    {
        throw UnknownModelName("ml model '{}' not found", name);
    }
    /// The catalog sends the model as one JSON object with the keys of the reflected model type.
    return CatalogConfig::fromJson<RegisteredModel>(std::string{found->model_json});
}

const rust::Vec<Bridge::PhysicalSource>& SnapshotCatalog::getCreatedSources() const
{
    return createdSources;
}

const rust::Vec<Bridge::Sink>& SnapshotCatalog::getCreatedSinks() const
{
    return createdSinks;
}

}
