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

#include <string>

#include <Schema/Schema.hpp>
#include <Sources/SourceValidationProvider.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>

#include <nes-coordinator-bridge/catalog.h>
#include <rfl/json/read.hpp>
#include <rfl/json/write.hpp>
#include <rust/cxx.h>
#include <BridgeError.hpp>
#include <CatalogConfig.hpp>

namespace NES
{
namespace
{
Schema<UnqualifiedUnboundField, Ordered> schemaFromJson(const std::string& json)
{
    auto parsed = rfl::json::read<rfl::Generic>(json);
    if (!parsed)
    {
        throw CannotDeserialize("Failed to deserialize Schema from JSON: {}", parsed.error().what());
    }
    return ReflectionContext{}.unreflect<Schema<UnqualifiedUnboundField, Ordered>>(*parsed);
}

/// A config object as the catalog stores it: string values under canonical keys.
std::unordered_map<Identifier, std::string> configFromGeneric(const rfl::Generic::Object& object)
{
    std::unordered_map<std::string, std::string> config;
    for (const auto& [key, value] : object)
    {
        auto text = value.to_string();
        if (!text)
        {
            throw CannotDeserialize("config option '{}' is not a string", key);
        }
        config.emplace(key, *text);
    }
    return CatalogConfig::toIdentifierKeys(config);
}

rfl::Generic::Object objectFromJson(const std::string& json)
{
    auto parsed = rfl::json::read<rfl::Generic>(json);
    if (!parsed)
    {
        throw CannotDeserialize("Failed to deserialize config from JSON: {}", parsed.error().what());
    }
    auto object = parsed->to_object();
    if (!object)
    {
        throw CannotDeserialize("config is not a JSON object");
    }
    return *object;
}

std::unordered_map<Identifier, std::string> configFromJson(const std::string& json)
{
    return configFromGeneric(objectFromJson(json));
}

/// rfl cannot write a JSON object with identifier keys, so the keys are written in the canonical spelling that the read above parses.
std::string configToJson(const std::unordered_map<Identifier, std::string>& config)
{
    return rfl::json::write(CatalogConfig::toStringKeys(config));
}

/// The host of an anonymous source or sink, taken out of its config.
/// It is removed before the rest is stored, because a source or sink type rejects a key that it does not define.
Host takeHost(std::unordered_map<Identifier, std::string>& config, const HostPolicy& hostPolicy, const std::string_view subject)
{
    std::optional<Host> declared;
    if (const auto it = config.find(Identifier::parse("host")); it != config.end())
    {
        declared = Host{it->second};
        config.erase(it);
    }

    auto resolved = resolveHost(declared, hostPolicy, subject);
    if (!resolved)
    {
        throw std::move(resolved).error();
    }
    return Host{std::move(*resolved)};
}
}

CatalogBridge::CatalogBridge(const Bridge::PlanningTransaction& ctx, HostPolicy hostPolicy) : ctx{ctx}, hostPolicy{std::move(hostPolicy)}
{
}

LogicalSource CatalogBridge::getLogicalSource(const std::string_view name) const
{
    const auto [source_name, schema_json, error] = Bridge::get_logical_source(ctx, rust::Str{name.data(), name.size()});
    Bridge::raiseReported(error);
    const auto schema = schemaFromJson(std::string{schema_json});
    /// The catalog stores canonical names; parsing would fold them a second time.
    return LogicalSource{Identifier::fromCanonical(std::string{source_name}), schema};
}

std::unordered_set<SourceDescriptor> CatalogBridge::getPhysicalSources(const LogicalSource& logicalSource) const
{
    const auto logicalSourceName = logicalSource.getLogicalSourceName().asCanonicalString();
    const auto [error, sources] = Bridge::get_source_descriptors(ctx, rust::Str{logicalSourceName.data(), logicalSourceName.size()});
    Bridge::raiseReported(error);

    std::unordered_set<SourceDescriptor> result;
    for (const auto& source : sources)
    {
        auto sourceConfig = configFromJson(std::string{source.source_config_json});
        auto formatterConfig = configFromJson(std::string{source.parser_config_json});
        auto descriptor = SourceDescriptor::create(
            PhysicalSourceId{source.id},
            logicalSource,
            Identifier::fromCanonical(std::string{source.source_type}),
            Host{std::string{source.host_addr}},
            std::move(sourceConfig),
            formatterConfig,
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
    std::unordered_map<Identifier, std::string> config,
    std::unordered_map<Identifier, std::string> formatConfig)
{
    const auto host = takeHost(config, hostPolicy, "SOURCE");

    const auto configJson = configToJson(config);
    const auto formatJson = configToJson(formatConfig);

    const auto [error, rawId] = Bridge::create_anonymous_source(
        ctx, static_cast<bool>(kind), rust::Str{sourceType.asCanonicalString()}, configJson, formatJson, host.getRawValue());
    Bridge::raiseReported(error);
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
    const auto [id, name, host_addr, sink_type, schema_json, config_json, error]
        = Bridge::get_sink_descriptor(ctx, rust::Str{sinkName.data(), sinkName.size()});
    Bridge::raiseReported(error);

    /// The catalog stores the formatter options nested inside the sink's options; the descriptor takes them as a separate map.
    std::unordered_map<Identifier, std::string> formatConfig;
    rfl::Generic::Object sinkOptions;
    for (const auto& [key, value] : objectFromJson(std::string{config_json}))
    {
        if (key != CatalogConfig::OUTPUT_FORMATTER_KEY)
        {
            sinkOptions.insert(key, value);
            continue;
        }
        auto nested = value.to_object();
        if (!nested)
        {
            throw CannotDeserialize("the formatter options of sink '{}' are not a JSON object", std::string{name});
        }
        formatConfig = configFromGeneric(*nested);
    }
    const auto sinkConfig = configFromGeneric(sinkOptions);
    const auto schema = schemaFromJson(std::string{schema_json});

    auto descriptor = SinkDescriptor::createNamed(
        SinkId{id},
        Identifier::fromCanonical(std::string{name}),
        Identifier::fromCanonical(std::string{sink_type}),
        schema,
        Host{std::string{host_addr}},
        sinkConfig,
        formatConfig);
    if (!descriptor)
    {
        throw InvalidConfigParameter("sink \"{}\" of type \"{}\"", std::string{name}, std::string{sink_type});
    }
    return std::move(*descriptor);
}

SinkDescriptor CatalogBridge::createAnonymousSink(
    const ConnectorKind kind,
    const Identifier& sinkType,
    const std::optional<Schema<UnqualifiedUnboundField, Ordered>>& schema,
    std::unordered_map<Identifier, std::string> config,
    const std::unordered_map<Identifier, std::string>& formatConfig)
{
    const auto host = takeHost(config, hostPolicy, "SINK");

    /// A schema-less anonymous sink (for example INTO Void() with no target schema) is stored with a null schema.
    /// Its schema is inferred later from the query output at the sink operator.
    const auto schemaJson = schema.has_value() ? rfl::json::write(rfl::Generic(ReflectionContext{}.reflect(*schema))) : std::string{"null"};
    /// Stored in the same shape as a declared sink, with the formatter options nested inside.
    const auto configJson = rfl::json::write(
        CatalogConfig::mergeFormatConfig(CatalogConfig::toStringKeys(config), CatalogConfig::toStringKeys(formatConfig)));

    const auto [error, id] = Bridge::create_anonymous_sink(
        ctx, static_cast<bool>(kind), rust::Str{sinkType.asCanonicalString()}, schemaJson, configJson, host.getRawValue());
    Bridge::raiseReported(error);
    auto descriptor = SinkDescriptor::createAnonymous(SinkId{id}, sinkType, schema, host, config, formatConfig);
    if (!descriptor)
    {
        throw InvalidConfigParameter("anonymous sink of type \"{}\"", sinkType.asCanonicalString());
    }
    return std::move(*descriptor);
}

WorkerInfo CatalogBridge::getWorker(const Host& host) const
{
    const auto [host_addr, data_addr, max_operators, error]
        = Bridge::get_worker(ctx, rust::Str{host.getRawValue().data(), host.getRawValue().size()});
    Bridge::raiseReported(error);
    const Capacity cap
        = max_operators < 0 ? Capacity{CapacityKind::Unlimited{}} : Capacity{CapacityKind::Limited{static_cast<size_t>(max_operators)}};
    return WorkerInfo{.host = Host{std::string{host_addr}}, .data = std::string{data_addr}, .maxOperators = cap};
}

NetworkTopology CatalogBridge::getTopology() const
{
    const auto [error, hosts, links] = Bridge::get_topology(ctx);
    Bridge::raiseReported(error);
    std::vector<Host> nodes;
    for (const auto& host : hosts)
    {
        nodes.emplace_back(std::string{host});
    }
    std::vector<std::pair<Host, Host>> edges;
    for (const auto& [src_addr, dst_addr] : links)
    {
        edges.emplace_back(Host{std::string{src_addr}}, Host{std::string{dst_addr}});
    }
    return NetworkTopology::fromEdges(nodes, edges);
}

RegisteredModel CatalogBridge::getModel(std::string_view name) const
{
    const auto [model_name, model_path, input_schema_json, output_schema_json, imported_json, error]
        = Bridge::get_ml_model(ctx, rust::Str{name.data(), name.size()});
    Bridge::raiseReported(error);
    const auto inputs = rfl::json::read<rfl::Generic>(std::string{input_schema_json});
    const auto outputs = rfl::json::read<rfl::Generic>(std::string{output_schema_json});
    const auto imported = rfl::json::read<rfl::Generic>(std::string{imported_json});
    if (!inputs || !outputs || !imported)
    {
        throw CannotDeserialize("Failed to parse ml model '{}' returned by the catalog", name);
    }
    rfl::Generic::Object object;
    object[std::string{"name"}] = rfl::Generic(std::string{model_name});
    object[std::string{"path"}] = rfl::Generic(std::string{model_path});
    object[std::string{"imported"}] = *imported;
    object[std::string{"inputs"}] = *inputs;
    object[std::string{"outputs"}] = *outputs;
    return ReflectionContext{}.unreflect<RegisteredModel>(Reflected{rfl::Generic(object)});
}

}
