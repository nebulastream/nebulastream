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

#include <CatalogBridge.hpp>

#include <cstddef>
#include <optional>
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
#include <Util/Reflection.hpp>
#include <Catalog.hpp>
#include <ErrorHandling.hpp>
#include <HostPolicy.hpp>
#include <Model.hpp>
#include <NetworkTopology.hpp>

#include <nes-coordinator-bridge/catalog.h>
#include <BridgeError.hpp>
#include <CatalogConfig.hpp>

namespace NES
{
namespace
{
rust::Str toRustStr(const std::string_view text)
{
    return rust::Str{text.data(), text.size()};
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

CatalogBridge::CatalogBridge(const Bridge::PlanningTransaction& ctx, std::optional<Host> defaultHost)
    : ctx{ctx}, defaultHost{std::move(defaultHost)}
{
}

LogicalSource CatalogBridge::getLogicalSource(const std::string_view name) const
{
    const auto [sourceName, schemaJson, error] = Bridge::get_logical_source(ctx, toRustStr(name));
    Bridge::throwIfError(error);
    /// The catalog stores canonical names; parsing would fold them a second time.
    return LogicalSource{
        Identifier::fromCanonical(std::string{sourceName}),
        CatalogConfig::fromJson<Schema<UnqualifiedUnboundField, Ordered>>(std::string{schemaJson})};
}

std::unordered_set<SourceDescriptor> CatalogBridge::getPhysicalSources(const LogicalSource& logicalSource) const
{
    const auto [error, sources] = Bridge::get_source_descriptors(ctx, toRustStr(logicalSource.getLogicalSourceName().asCanonicalString()));
    Bridge::throwIfError(error);

    std::unordered_set<SourceDescriptor> result;
    for (const auto& source : sources)
    {
        auto descriptor = SourceDescriptor::create(
            PhysicalSourceId{source.id},
            logicalSource,
            Identifier::fromCanonical(std::string{source.source_type}),
            Host{std::string{source.host_addr}},
            CatalogConfig::fromJson<CatalogConfig::Options>(std::string{source.source_config_json}).values,
            CatalogConfig::fromJson<CatalogConfig::Options>(std::string{source.parser_config_json}).values,
            source.is_anonymous);
        if (!descriptor)
        {
            throw std::move(descriptor).error();
        }
        result.emplace(std::move(*descriptor));
    }
    return result;
}

SourceDescriptor CatalogBridge::createAnonymousSource(
    const ConnectorKind kind,
    const Identifier& sourceType,
    const Schema<UnqualifiedUnboundField, Ordered>& schema,
    const std::unordered_map<Identifier, std::string>& sourceConfig,
    const std::unordered_map<Identifier, std::string>& formatConfig)
{
    auto config = sourceConfig;
    const auto host = takeHost(config, defaultHost, "SOURCE");

    const auto [error, rawId] = Bridge::create_anonymous_source(
        ctx,
        kind == ConnectorKind::Internal,
        rust::Str{sourceType.asCanonicalString()},
        CatalogConfig::toJson(CatalogConfig::Options{config}),
        CatalogConfig::toJson(CatalogConfig::Options{formatConfig}),
        host.getRawValue());
    Bridge::throwIfError(error);
    const auto id = PhysicalSourceId{rawId};

    auto descriptor = SourceDescriptor::create(
        id, LogicalSource{Identifier::fromCanonical(id.toString()), schema}, sourceType, host, config, formatConfig, true);
    if (!descriptor)
    {
        throw std::move(descriptor).error();
    }
    return std::move(*descriptor);
}

SinkDescriptor CatalogBridge::getSinkDescriptor(const std::string_view sinkName) const
{
    const auto [id, name, hostAddr, sinkType, schemaJson, configJson, error] = Bridge::get_sink_descriptor(ctx, toRustStr(sinkName));
    Bridge::throwIfError(error);

    const auto [sinkConfig, formatConfig] = CatalogConfig::fromJson<CatalogConfig::SinkOptions>(std::string{configJson});
    return SinkDescriptor::createNamed(
        SinkId{id},
        Identifier::fromCanonical(std::string{name}),
        Identifier::fromCanonical(std::string{sinkType}),
        CatalogConfig::fromJson<Schema<UnqualifiedUnboundField, Ordered>>(std::string{schemaJson}),
        Host{std::string{hostAddr}},
        sinkConfig,
        formatConfig);
}

SinkDescriptor CatalogBridge::createAnonymousSink(
    const ConnectorKind kind,
    const Identifier& sinkType,
    const std::optional<Schema<UnqualifiedUnboundField, Ordered>>& schema,
    const std::unordered_map<Identifier, std::string>& sinkConfig,
    const std::unordered_map<Identifier, std::string>& formatConfig)
{
    auto config = sinkConfig;
    const auto host = takeHost(config, defaultHost, "SINK");

    /// A schema-less anonymous sink (for example INTO Void() with no target schema) is stored with a null schema.
    /// Its schema is inferred later from the query output at the sink operator.
    const auto schemaJson = schema.has_value() ? CatalogConfig::toJson(*schema) : std::string{"null"};

    const auto [error, id] = Bridge::create_anonymous_sink(
        ctx,
        kind == ConnectorKind::Internal,
        rust::Str{sinkType.asCanonicalString()},
        schemaJson,
        CatalogConfig::toJson(CatalogConfig::SinkOptions{.sinkConfig = config, .formatConfig = formatConfig}),
        host.getRawValue());
    Bridge::throwIfError(error);

    return SinkDescriptor::createAnonymous(SinkId{id}, sinkType, schema, host, config, formatConfig);
}

WorkerInfo CatalogBridge::getWorker(const Host& host) const
{
    const auto [hostAddr, dataAddr, maxOperators, error] = Bridge::get_worker(ctx, toRustStr(host.getRawValue()));
    Bridge::throwIfError(error);
    const Capacity capacity
        = maxOperators < 0 ? Capacity{CapacityKind::Unlimited{}} : Capacity{CapacityKind::Limited{static_cast<size_t>(maxOperators)}};
    return WorkerInfo{.host = Host{std::string{hostAddr}}, .dataAddress = std::string{dataAddr}, .maxOperators = capacity};
}

NetworkTopology CatalogBridge::getTopology() const
{
    const auto [error, hosts, links] = Bridge::get_topology(ctx);
    Bridge::throwIfError(error);
    std::vector<Host> nodes;
    for (const auto& node : hosts)
    {
        nodes.emplace_back(std::string{node});
    }
    std::vector<std::pair<Host, Host>> edges;
    for (const auto& [source, target] : links)
    {
        edges.emplace_back(Host{std::string{source}}, Host{std::string{target}});
    }
    return NetworkTopology::fromEdges(nodes, edges);
}

RegisteredModel CatalogBridge::getModel(const std::string_view name) const
{
    const auto [modelJson, error] = Bridge::get_ml_model(ctx, toRustStr(name));
    Bridge::throwIfError(error);
    /// The catalog sends the model as one JSON object with the keys of the reflected model type.
    return CatalogConfig::fromJson<RegisteredModel>(std::string{modelJson});
}

}
