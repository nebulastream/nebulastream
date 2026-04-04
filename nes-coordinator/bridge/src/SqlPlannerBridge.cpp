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

#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <Util/Reflection.hpp>
#include <coordinator/lib.h>
#include <rfl/TaggedUnion.hpp>
#include <rfl/json/read.hpp>
#include <rfl/json/write.hpp>
#include <SqlPlanner.hpp>

namespace NES {
    namespace {
        struct CreateLogicalSource {
            std::string name;
            std::string schema_json;
        };

        struct CreatePhysicalSource {
            std::string logical_source;
            std::string source_type;
            std::string source_config_json;
            std::string parser_config_json;
        };

        struct CreateSink {
            std::string name;
            std::string sink_type;
            std::string schema_json;
            std::string sink_config_json;
            std::string format_config_json;
        };

        struct CreateWorker {
            std::string host_addr;
            std::string data_addr;
            int32_t max_operators;
            std::vector<std::string> peers;
            std::string config_json;
        };

        struct DropLogicalSource {
            std::string name;
        };

        struct DropPhysicalSource {
            uint64_t id;
        };

        struct DropSink {
            std::string name;
        };

        struct DropQuery {
            std::string name;
            int64_t id;
            int32_t stop_mode;
        };

        struct DropWorker {
            std::string host;
        };

        struct CreateQuery {
            std::optional<std::string> name;
            std::string sql;
        };

        struct ExplainQuery {
            std::string explanation;
        };

        struct GetLogicalSource {
            std::optional<std::string> name;
        };

        struct GetPhysicalSource {
            std::optional<int64_t> id;
            std::optional<std::string> logical_source;
        };

        struct GetSink {
            std::optional<std::string> name;
        };

        struct GetQuery {
            std::optional<std::vector<int64_t>> ids;
            std::optional<std::string> name;
        };

        struct GetWorker {
            std::optional<std::string> host_addr;
        };

        using JsonStatement = rfl::TaggedUnion<
            "tag",
            CreateLogicalSource,
            CreatePhysicalSource,
            CreateSink,
            CreateWorker,
            DropLogicalSource,
            DropPhysicalSource,
            DropSink,
            DropQuery,
            DropWorker,
            CreateQuery,
            ExplainQuery,
            GetLogicalSource,
            GetPhysicalSource,
            GetSink,
            GetQuery,
            GetWorker>;

        JsonStatement convert(const CreateLogicalSourceResult &result) {
            return CreateLogicalSource{result.name, rfl::json::write(reflect(result.schema))};
        }

        JsonStatement convert(const CreatePhysicalSourceResult &result) {
            return CreatePhysicalSource{
                result.logicalSourceName, result.sourceType, rfl::json::write(result.sourceConfig),
                rfl::json::write(result.parserConfig)
            };
        }

        JsonStatement convert(const CreateSinkResult &result) {
            return CreateSink{
                result.name,
                result.sinkType,
                rfl::json::write(reflect(result.schema)),
                rfl::json::write(result.sinkConfig),
                rfl::json::write(result.formatConfig)
            };
        }

        JsonStatement convert(const CreateWorkerResult &result) {
            return CreateWorker{
                result.hostAddr,
                result.dataAddr,
                result.capacity.has_value() ? static_cast<int32_t>(*result.capacity) : -1,
                result.peers,
                rfl::json::write(result.config)
            };
        }

        JsonStatement convert(const DropLogicalSourceResult &result) {
            return DropLogicalSource{result.name};
        }

        JsonStatement convert(const DropPhysicalSourceResult &result) {
            return DropPhysicalSource{result.id};
        }

        JsonStatement convert(const DropSinkResult &result) {
            return DropSink{result.name};
        }

        JsonStatement convert(const DropQueryResult &result) {
            return DropQuery{result.name.value_or(""), result.id.value_or(-1), static_cast<int32_t>(result.stopMode)};
        }

        JsonStatement convert(const DropWorkerResult &result) {
            return DropWorker{result.host};
        }

        JsonStatement convert(const QueryPlanResult &result, std::string sql) {
            return CreateQuery{.name = result.name, .sql = std::move(sql)};
        }

        JsonStatement convert(const ExplainQueryResult &result) {
            return ExplainQuery{result.explanation};
        }

        JsonStatement convert(const ShowLogicalSourcesResult &result) {
            return GetLogicalSource{.name = result.name};
        }

        JsonStatement convert(const ShowPhysicalSourcesResult &result) {
            return GetPhysicalSource{
                .id = result.id.has_value() ? std::optional(static_cast<int64_t>(*result.id)) : std::nullopt,
                .logical_source = result.logicalSourceName,
            };
        }

        JsonStatement convert(const ShowSinksResult &result) {
            return GetSink{.name = result.name};
        }

        JsonStatement convert(const ShowQueriesResult &result) {
            return GetQuery{
                .ids = result.id.has_value() ? std::optional(std::vector{*result.id}) : std::nullopt,
                .name = result.name,
            };
        }

        JsonStatement convert(const ShowWorkersResult &result) {
            return GetWorker{.host_addr = result.host};
        }

        JsonStatement convert(const WorkerStatusResult &result) {
            // WorkerStatus has no Rust equivalent yet — map to GetWorker with no filters.
            return GetWorker{.host_addr = std::nullopt};
        }

        JsonStatement convert(const StatementResult &result, std::string sql = {}) {
            return std::visit([&sql](const auto &res) -> JsonStatement {
                if constexpr (std::is_same_v<std::decay_t<decltype(res)>, QueryPlanResult>) {
                    return convert(res, std::move(sql));
                } else {
                    return convert(res);
                }
            }, result);
        }
    }

    PlannedStatement plan_sql(const PlannerContext &catalog, rust::Str sql, rust::Str optimizerConfig) {
        auto sqlStr = std::string(sql.data(), sql.size());
        auto config = QueryOptimizerConfiguration{};
        if (!optimizerConfig.empty()) {
            if (auto parsed = rfl::json::read<std::unordered_map<std::string, std::string>>(std::string{optimizerConfig})) {
                config.overwriteConfigWithCommandLineInput(*parsed);
            }
        }
        auto executed = SqlPlanner{catalog, std::move(config)}.execute(sqlStr);
        if (!executed) {
            throw std::runtime_error(executed.error().what());
        }

        auto json = rfl::json::write(convert(*executed, sqlStr));

        rust::Vec<PlannedFragment> serialized;
        rust::Vec<int64_t> sourceIds;
        rust::Vec<int64_t> sinkIds;
        if (auto *result = std::get_if<QueryPlanResult>(&*executed)) {
            for (const auto &[host, fragments]: result->decomposedPlan) {
                for (const auto &fragment: fragments) {
                    const auto proto = QueryPlanSerializationUtil::serializeQueryPlan(fragment);
                    const auto bytes = proto.SerializeAsString();
                    const auto allOps = flatten(fragment);
                    const auto sinks = getOperatorByType<SinkLogicalOperator>(fragment);
                    const auto sources = getOperatorByType<SourceDescriptorLogicalOperator>(fragment);
                    const auto numOps = static_cast<int32_t>(allOps.size());
                    const auto numSinks = static_cast<int32_t>(sinks.size());
                    const auto numSources = static_cast<int32_t>(sources.size());

                    for (const auto &source : sources) {
                        sourceIds.push_back(static_cast<int64_t>(source->getSourceDescriptor().getPhysicalSourceId().getRawValue()));
                    }

                    for (const auto &sink : sinks) {
                        const auto &desc = sink->getSinkDescriptor();
                        if (desc.has_value()) {
                            sinkIds.push_back(static_cast<int64_t>(desc->getSinkId().getRawValue()));
                        }
                    }

                    rust::Vec<uint8_t> planBytes;
                    planBytes.reserve(bytes.size());
                    for (const auto byte: bytes) {
                        planBytes.push_back(static_cast<uint8_t>(byte));
                    }

                    serialized.push_back(
                        PlannedFragment{
                            .host_addr = rust::String(host.getRawValue()),
                            .plan = std::move(planBytes),
                            .num_operators = numOps - numSinks - numSources,
                            .has_source = numSources > 0,
                        });
                }
            }
        }

        return PlannedStatement{
            .json = rust::String(json),
            .fragments = std::move(serialized),
            .source_ids = std::move(sourceIds),
            .sink_ids = std::move(sinkIds),
        };
    }
}
