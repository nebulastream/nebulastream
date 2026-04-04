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
#include <ErrorHandling.hpp>

#include <coordinator/lib.h>
#include <rfl/json/read.hpp>
#include <rfl/json/write.hpp>
#include <rust/cxx.h>

namespace NES {
    namespace {
        Schema schemaFromJson(const std::string &json) {
            auto result = rfl::json::read<detail::ReflectedSchema>(json);
            if (!result) {
                throw CannotDeserialize("Failed to deserialize Schema from JSON: {}", result.error().what());
            }
            return unreflect<Schema>(reflect(*result));
        }

        std::unordered_map<std::string, std::string> configFromJson(const std::string &json) {
            auto result = rfl::json::read<std::unordered_map<std::string, std::string> >(json);
            if (!result) {
                throw CannotDeserialize("Failed to deserialize config from JSON: {}", result.error().what());
            }
            return *result;
        }
    }

    namespace SourceCatalog {
        LogicalSource getLogicalSource(const PlannerContext &ctx, const std::string_view name) {
            const auto [source_name, schema_json] = get_logical_source(ctx, rust::Str{name.data(), name.size()});
            const auto schema = schemaFromJson(std::string{schema_json});
            return LogicalSource{std::string{source_name}, schema};
        }

        std::unordered_set<SourceDescriptor> getSourceDescriptors(
            const PlannerContext &ctx, const std::string_view logicalSourceName) {
            const auto sources =
                    get_source_descriptors(ctx, rust::Str{logicalSourceName.data(), logicalSourceName.size()});
            const auto logicalSource = SourceCatalog::getLogicalSource(ctx, logicalSourceName);

            std::unordered_set<SourceDescriptor> result;
            for (const auto &source: sources) {
                const auto parserConfig = ParserConfig::create(configFromJson(std::string{source.parser_config_json}));
                auto sourceConfig = configFromJson(std::string{source.source_config_json});
                auto descriptorConfig = SourceValidationProvider::provide(
                    std::string{source.source_type}, std::move(sourceConfig));
                if (!descriptorConfig) {
                    throw InvalidConfigParameter("source type \"{}\"", std::string{source.source_type});
                }

                result.emplace(
                    PhysicalSourceId{static_cast<PhysicalSourceId::Underlying>(source.id)},
                    logicalSource,
                    std::string{source.source_type},
                    Host{std::string{source.host_addr}},
                    *descriptorConfig,
                    parserConfig);
            }
            return result;
        }

        SourceDescriptor createInlineSource(
            const PlannerContext &ctx,
            const ConnectorKind kind,
            const std::string &sourceType,
            const Schema &schema,
            std::unordered_map<std::string, std::string> parserConfigMap,
            std::unordered_map<std::string, std::string> sourceConfigMap) {
            Host host{""};
            if (auto it = sourceConfigMap.find("host"); it != sourceConfigMap.end()) {
                host = Host{it->second};
                sourceConfigMap.erase(it);
            }

            auto schemaJson = rfl::json::write(reflect(schema));
            auto sourceConfigJson = rfl::json::write(sourceConfigMap);
            auto parserConfigJson = rfl::json::write(parserConfigMap);

            auto descriptorConfig = SourceValidationProvider::provide(sourceType, std::move(sourceConfigMap));
            if (!descriptorConfig) {
                throw InvalidConfigParameter("source type \"{}\"", sourceType);
            }

            auto parserConfig = ParserConfig::create(std::move(parserConfigMap));
            auto id = PhysicalSourceId{
                static_cast<uint64_t>(
                    create_inline_source(ctx, static_cast<bool>(kind), sourceType, schemaJson, sourceConfigJson,
                                         parserConfigJson, host.getRawValue()))
            };
            return SourceDescriptor{
                id, LogicalSource{id.toString(), schema}, sourceType, host, std::move(*descriptorConfig), parserConfig
            };
        }
    }

    namespace SinkCatalog {
        SinkDescriptor getSinkDescriptor(const PlannerContext &ctx, const std::string_view sinkName) {
            const auto [id, name, host_addr, sink_type, schema_json, config_json] = get_sink_descriptor(
                ctx, rust::Str{sinkName.data(), sinkName.size()});
            const auto schema = schemaFromJson(std::string{schema_json});
            auto config = configFromJson(std::string{config_json});
            const auto descriptorConfig = SinkDescriptor::validateAndFormatConfig(
                std::string{sink_type}, std::move(config));
            if (!descriptorConfig) {
                throw InvalidConfigParameter("sink type \"{}\"", std::string{sink_type});
            }
            return SinkDescriptor{
                SinkId{static_cast<uint64_t>(id)},
                std::string{name},
                schema,
                std::string{sink_type},
                Host{std::string{host_addr}},
                {},
                *descriptorConfig
            };
        }

        SinkDescriptor createInlineSink(
            const PlannerContext &ctx,
            const ConnectorKind kind,
            const Schema &schema,
            const std::string_view sinkType,
            std::unordered_map<std::string, std::string> config,
            const std::unordered_map<std::string, std::string> &formatConfig) {
            Host host{""};
            if (const auto it = config.find("host"); it != config.end()) {
                host = Host{it->second};
                config.erase(it);
            }

            const auto schemaJson = rfl::json::write(reflect(schema));
            const auto configJson = rfl::json::write(config);

            auto descriptorConfig = SinkDescriptor::validateAndFormatConfig(std::string{sinkType}, std::move(config));
            if (!descriptorConfig) {
                throw InvalidConfigParameter("sink type \"{}\"", std::string{sinkType});
            }
            const auto id = static_cast<uint64_t>(
                create_inline_sink(ctx, static_cast<bool>(kind), std::string{sinkType}, schemaJson, configJson,
                                   host.getRawValue()));
            return SinkDescriptor{
                SinkId{id}, "", schema, std::string{sinkType}, host, formatConfig, std::move(*descriptorConfig)
            };
        }
    }

    namespace WorkerCatalog {
        WorkerInfo getWorker(const PlannerContext &ctx, const Host &host) {
            const auto [host_addr, data_addr, max_operators] = get_worker(
                ctx, rust::Str{host.getRawValue().data(), host.getRawValue().size()});
            const Capacity cap = max_operators < 0
                                     ? Capacity{CapacityKind::Unlimited{}}
                                     : Capacity{CapacityKind::Limited{static_cast<size_t>(max_operators)}};
            return WorkerInfo{
                .host = Host{std::string{host_addr}},
                .data = std::string{data_addr},
                .maxOperators = cap
            };
        }

        NetworkTopology getTopology(const PlannerContext &ctx) {
            const auto [hosts, links] = get_topology(ctx);
            std::vector<Host> nodes;
            for (const auto &host: hosts) { nodes.emplace_back(std::string{host}); }
            std::vector<std::pair<Host, Host> > edges;
            for (const auto &[src_addr, dst_addr]: links) {
                edges.emplace_back(Host{std::string{src_addr}}, Host{std::string{dst_addr}});
            }
            return NetworkTopology::fromEdges(nodes, edges);
        }
    }
}
