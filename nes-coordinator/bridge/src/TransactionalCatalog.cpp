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

#include <TransactionalCatalog.hpp>

#include <string>

#include <Schema/Schema.hpp>
#include <Sources/SourceValidationProvider.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>

#include <coordinator/lib.h>
#include <rfl/json/read.hpp>
#include <rfl/json/write.hpp>
#include <rust/cxx.h>
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

std::unordered_map<Identifier, std::string> configFromJson(const std::string& json)
{
    auto result = rfl::json::read<std::unordered_map<std::string, std::string>>(json);
    if (!result)
    {
        throw CannotDeserialize("Failed to deserialize config from JSON: {}", result.error().what());
    }
    return CatalogConfig::toIdentifierKeys(*result);
}

/// Inverse of `configFromJson`: `rfl` cannot emit a JSON object with `Identifier` keys, so fold them back to their
/// canonical string form (matching what `configFromJson` reads) before serializing.
std::string configToJson(const std::unordered_map<Identifier, std::string>& config)
{
    return rfl::json::write(CatalogConfig::toStringKeys(config));
}

/// Returns the address an entity written into a query is placed on.
/// Such an entity stores its host among its own config options instead of in a HOST clause.
/// The host is removed from the config before the rest is stored, because a source or sink type rejects a key it does not define.
/// An omitted host is resolved by the policy, the same way as for a declared entity.
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

/// Raises the failure a lookup reported alongside its payload.
/// A lookup answers with a struct rather than an error, because cxx renders a returned error with `Display` and the code
/// would not survive the crossing.
/// Raising it here as the exception that code names lets a test assert on it.
void raiseReported(const BridgeError& error)
{
    if (error.code != 0)
    {
        throw Exception(std::string{error.msg}, error.code);
    }
}
}

TransactionalCatalog::TransactionalCatalog(const TransactionContext& ctx, HostPolicy hostPolicy)
    : ctx{ctx}, hostPolicy{std::move(hostPolicy)}
{
}

LogicalSource TransactionalCatalog::getLogicalSource(const std::string_view name) const
{
    const auto [source_name, schema_json, error] = get_logical_source(ctx, rust::Str{name.data(), name.size()});
    raiseReported(error);
    const auto schema = schemaFromJson(std::string{schema_json});
    /// The catalog stores canonical names; `Identifier::parse` would fold them a second time.
    return LogicalSource{Identifier::fromCanonical(std::string{source_name}), schema};
}

std::unordered_set<SourceDescriptor> TransactionalCatalog::getPhysicalSources(const std::string_view logicalSourceName) const
{
    const auto sources = get_source_descriptors(ctx, rust::Str{logicalSourceName.data(), logicalSourceName.size()});
    const auto logicalSource = getLogicalSource(logicalSourceName);

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

SourceDescriptor TransactionalCatalog::createAnonymousSource(
    const ConnectorKind kind,
    const Identifier& sourceType,
    const Schema<UnqualifiedUnboundField, Ordered>& schema,
    std::unordered_map<Identifier, std::string> config,
    std::unordered_map<Identifier, std::string> formatConfig)
{
    const auto host = takeHost(config, hostPolicy, "SOURCE");

    const auto schemaJson = rfl::json::write(rfl::Generic(ReflectionContext{}.reflect(schema)));
    const auto configJson = configToJson(config);
    const auto formatJson = configToJson(formatConfig);

    const auto id = PhysicalSourceId{create_anonymous_source(
        ctx, static_cast<bool>(kind), rust::Str{sourceType.asCanonicalString()}, schemaJson, configJson, formatJson, host.getRawValue())};

    auto descriptor = SourceDescriptor::create(
        id, LogicalSource{Identifier::fromCanonical(id.toString()), schema}, sourceType, host, config, formatConfig, true);
    if (!descriptor)
    {
        throw std::move(descriptor).error();
    }
    return std::move(*descriptor);
}

SinkDescriptor TransactionalCatalog::getSinkDescriptor(const std::string_view sinkName) const
{
    const auto [id, name, host_addr, sink_type, schema_json, config_json, error]
        = get_sink_descriptor(ctx, rust::Str{sinkName.data(), sinkName.size()});
    raiseReported(error);

    auto parsed = rfl::json::read<std::unordered_map<std::string, rfl::Generic>>(std::string{config_json});
    if (!parsed)
    {
        throw CannotDeserialize("Failed to deserialize sink config from JSON: {}", parsed.error().what());
    }
    std::unordered_map<Identifier, std::string> formatConfig;
    if (const auto it = parsed->find(std::string{CatalogConfig::OUTPUT_FORMATTER_KEY}); it != parsed->end())
    {
        formatConfig = configFromJson(rfl::json::write(it->second));
        parsed->erase(it);
    }
    const auto sinkConfig = configFromJson(rfl::json::write(*parsed));
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

SinkDescriptor TransactionalCatalog::createAnonymousSink(
    const ConnectorKind kind,
    const Identifier& sinkType,
    const std::optional<Schema<UnqualifiedUnboundField, Ordered>>& schema,
    std::unordered_map<Identifier, std::string> config,
    const std::unordered_map<Identifier, std::string>& formatConfig)
{
    const auto host = takeHost(config, hostPolicy, "SINK");

    /// A schema-less anonymous sink (e.g. INTO Void() with no target schema) is stored with a null schema
    /// in the catalog and later inferred from the query output on the SinkLogicalOperator.
    const auto schemaJson = schema.has_value() ? rfl::json::write(rfl::Generic(ReflectionContext{}.reflect(*schema))) : std::string{"null"};
    const auto configJson = configToJson(config);

    const auto id = create_anonymous_sink(
        ctx, static_cast<bool>(kind), rust::Str{sinkType.asCanonicalString()}, schemaJson, configJson, host.getRawValue());
    auto descriptor = SinkDescriptor::createAnonymous(SinkId{id}, sinkType, schema, host, config, formatConfig);
    if (!descriptor)
    {
        throw InvalidConfigParameter("anonymous sink of type \"{}\"", sinkType.asCanonicalString());
    }
    return std::move(*descriptor);
}

WorkerInfo TransactionalCatalog::getWorker(const Host& host) const
{
    const auto [host_addr, data_addr, max_operators, error]
        = get_worker(ctx, rust::Str{host.getRawValue().data(), host.getRawValue().size()});
    raiseReported(error);
    const Capacity cap
        = max_operators < 0 ? Capacity{CapacityKind::Unlimited{}} : Capacity{CapacityKind::Limited{static_cast<size_t>(max_operators)}};
    return WorkerInfo{.host = Host{std::string{host_addr}}, .data = std::string{data_addr}, .maxOperators = cap};
}

NetworkTopology TransactionalCatalog::getTopology() const
{
    const auto [hosts, links] = get_topology(ctx);
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

RegisteredModel TransactionalCatalog::getModel(std::string_view name) const
{
    const auto [model_name, model_path, input_schema_json, output_schema_json, imported_json, error]
        = get_ml_model(ctx, rust::Str{name.data(), name.size()});
    raiseReported(error);
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
