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

#include <ranges>
#include <string>

#include <DataTypes/Schema.hpp>
#include <Sources/SourceValidationProvider.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>

#include <coordinator/lib.h>
#include <rfl/json/read.hpp>
#include <rfl/json/write.hpp>
#include <rust/cxx.h>

namespace NES {
    namespace {
        Schema<UnqualifiedUnboundField, Ordered> schemaFromJson(const std::string &json) {
            auto parsed = rfl::json::read<rfl::Generic>(json);
            if (!parsed) {
                throw CannotDeserialize("Failed to deserialize Schema from JSON: {}", parsed.error().what());
            }
            return ReflectionContext{}.unreflect<Schema<UnqualifiedUnboundField, Ordered> >(*parsed);
        }

        std::unordered_map<Identifier, std::string> configFromJson(const std::string &json) {
            auto result = rfl::json::read<std::unordered_map<std::string, std::string> >(json);
            if (!result) {
                throw CannotDeserialize("Failed to deserialize config from JSON: {}", result.error().what());
            }
            return *result | std::views::transform([](const auto &kv) {
                       return std::make_pair(Identifier::fromCanonical(kv.first), kv.second);
                   })
                   | std::ranges::to<std::unordered_map<Identifier, std::string> >();
        }

        /// Inverse of `configFromJson`: `rfl` cannot emit a JSON object with `Identifier` keys, so fold them back to their
        /// canonical string form (matching what `configFromJson` reads) before serializing.
        std::string configToJson(const std::unordered_map<Identifier, std::string> &config) {
            return rfl::json::write(
                config | std::views::transform([](const auto &kv) {
                    return std::make_pair(kv.first.asCanonicalString(), kv.second);
                })
                | std::ranges::to<std::unordered_map<std::string, std::string> >());
        }
    }

    namespace SourceCatalog {
        LogicalSource getLogicalSource(const PlannerContext &ctx, const std::string_view name) {
            const auto [source_name, schema_json] = get_logical_source(ctx, rust::Str{name.data(), name.size()});
            const auto schema = schemaFromJson(std::string{schema_json});
            /// The catalog stores canonical names; `Identifier::parse` would fold them a second time.
            return LogicalSource{Identifier::fromCanonical(std::string{source_name}), schema};
        }

        std::unordered_set<SourceDescriptor> getPhysicalSources(const PlannerContext &ctx,
                                                                const std::string_view logicalSourceName) {
            const auto sources = get_source_descriptors(ctx, rust::Str{
                                                            logicalSourceName.data(), logicalSourceName.size()
                                                        });
            const auto logicalSource = getLogicalSource(ctx, logicalSourceName);

            std::unordered_set<SourceDescriptor> result;
            for (const auto &source: sources) {
                auto sourceConfig = configFromJson(std::string{source.source_config_json});
                auto formatterConfig = configFromJson(std::string{source.parser_config_json});
                auto descriptor = SourceDescriptor::create(
                    PhysicalSourceId{source.id},
                    logicalSource,
                    Identifier::fromCanonical(std::string{source.source_type}),
                    Host{std::string{source.host_addr}},
                    std::move(sourceConfig),
                    formatterConfig);
                if (!descriptor) {
                    throw std::move(descriptor).error();
                }
                result.emplace(std::move(*descriptor));
            }
            return result;
        }

        SourceDescriptor createInlineSource(
            const PlannerContext &ctx,
            const ConnectorKind kind,
            const Identifier &sourceType,
            const Schema<UnqualifiedUnboundField, Ordered> &schema,
            std::unordered_map<Identifier, std::string> config,
            std::unordered_map<Identifier, std::string> formatConfig) {
            Host host{""};
            if (const auto it = config.find(Identifier::parse("host")); it != config.end()) {
                host = Host{it->second};
                config.erase(it);
            }

            const auto schemaJson = rfl::json::write(rfl::Generic(reflect(schema)));
            const auto configJson = configToJson(config);
            const auto formatJson = configToJson(formatConfig);

            const auto id = PhysicalSourceId{
                create_inline_source(
                    ctx, static_cast<bool>(kind), rust::Str{sourceType.asCanonicalString()}, schemaJson, configJson,
                    formatJson, host.getRawValue())
            };

            auto descriptor = SourceDescriptor::create(
                id, LogicalSource{Identifier::fromCanonical(id.toString()), schema}, sourceType, host, config,
                formatConfig);
            if (!descriptor) {
                throw std::move(descriptor).error();
            }
            return std::move(*descriptor);
        }
    }

    namespace SinkCatalog {
        SinkDescriptor getSinkDescriptor(const PlannerContext &ctx, const std::string_view sinkName) {
            const auto [id, name, host_addr, sink_type, schema_json, config_json]
                    = get_sink_descriptor(ctx, rust::Str{sinkName.data(), sinkName.size()});

            auto parsed = rfl::json::read<std::unordered_map<std::string, rfl::Generic> >(std::string{config_json});
            if (!parsed) {
                throw CannotDeserialize("Failed to deserialize sink config from JSON: {}", parsed.error().what());
            }
            std::unordered_map<Identifier, std::string> formatConfig;
            if (const auto it = parsed->find("OUTPUT_FORMATTER"); it != parsed->end()) {
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
            if (!descriptor) {
                throw InvalidConfigParameter("sink \"{}\" of type \"{}\"", std::string{name}, std::string{sink_type});
            }
            return std::move(*descriptor);
        }

        SinkDescriptor createInlineSink(
            const PlannerContext &ctx,
            const ConnectorKind kind,
            const Identifier &sinkType,
            const Schema<UnqualifiedUnboundField, Ordered> &schema,
            std::unordered_map<Identifier, std::string> config,
            const std::unordered_map<Identifier, std::string> &formatConfig) {
            Host host{""};
            if (const auto it = config.find(Identifier::parse("host")); it != config.end()) {
                host = Host{it->second};
                config.erase(it);
            }

            const auto schemaJson = rfl::json::write(rfl::Generic(reflect(schema)));
            const auto configJson = configToJson(config);

            const auto id = create_inline_sink(
                ctx, static_cast<bool>(kind), rust::Str{sinkType.asCanonicalString()}, schemaJson, configJson,
                host.getRawValue());
            auto descriptor = SinkDescriptor::createInline(SinkId{id}, sinkType, schema, host, config, formatConfig);
            if (!descriptor) {
                throw InvalidConfigParameter("inline sink of type \"{}\"", sinkType.asCanonicalString());
            }
            return std::move(*descriptor);
        }
    }

    namespace WorkerCatalog {
        WorkerInfo getWorker(const PlannerContext &ctx, const Host &host) {
            const auto [host_addr, data_addr, max_operators] = get_worker(ctx, rust::Str{
                                                                              host.getRawValue().data(),
                                                                              host.getRawValue().size()
                                                                          });
            const Capacity cap
                    = max_operators < 0
                          ? Capacity{CapacityKind::Unlimited{}}
                          : Capacity{CapacityKind::Limited{static_cast<size_t>(max_operators)}};
            return WorkerInfo{
                .host = Host{std::string{host_addr}}, .data = std::string{data_addr}, .maxOperators = cap
            };
        }

        NetworkTopology getTopology(const PlannerContext &ctx) {
            const auto [hosts, links] = get_topology(ctx);
            std::vector<Host> nodes;
            for (const auto &host: hosts) {
                nodes.emplace_back(std::string{host});
            }
            std::vector<std::pair<Host, Host> > edges;
            for (const auto &[src_addr, dst_addr]: links) {
                edges.emplace_back(Host{std::string{src_addr}}, Host{std::string{dst_addr}});
    }
    return NetworkTopology::fromEdges(nodes, edges);
}
}

namespace ModelCatalog
{
RegisteredModel getModel(const PlannerContext& ctx, std::string_view name)
{
    const auto [model_name, model_path, input_schema_json, output_schema_json, imported_json]
        = get_ml_model(ctx, rust::Str{name.data(), name.size()});
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
}
