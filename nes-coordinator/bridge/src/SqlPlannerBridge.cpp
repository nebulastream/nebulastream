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

#include <SqlPlanner.hpp>
#include <NetworkTopology.hpp>
#include <coordinator/lib.h>
#include <Util/Reflection.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <rfl/TaggedUnion.hpp>
#include <rfl/json/write.hpp>

namespace NES
{

namespace
{

/// JSON-serializable mirror types for the C++ StatementResult variants.
/// Schema/config fields are pre-serialized to JSON strings.

struct CreateLogicalSource
{
    std::string name;
    std::string schema_json;
};

struct CreatePhysicalSource
{
    std::string logical_source;
    std::string source_type;
    std::string source_config_json;
    std::string parser_config_json;
};

struct CreateSink
{
    std::string name;
    std::string sink_type;
    std::string schema_json;
    std::string sink_config_json;
    std::string format_config_json;
};

struct CreateWorker
{
    std::string host_addr;
    std::string data_addr;
    int32_t capacity;
    std::vector<std::string> peers;
    std::string config_json;
};

struct DropLogicalSource
{
    std::string name;
};

struct DropPhysicalSource
{
    uint64_t id;
};

struct DropSink
{
    std::string name;
};

struct DropQuery
{
    std::string name;
    int64_t id;
    int32_t stop_mode;
};

struct DropWorker
{
    std::string host;
};

struct CreateQuery
{
    std::optional<std::string> name;
    std::string sql_statement;
};

struct ExplainQuery
{
    std::string explanation;
};

struct ShowLogicalSources
{
    std::string name;
};

struct ShowPhysicalSources
{
    std::string logical_source;
    int64_t id;
};

struct ShowSinks
{
    std::string name;
};

struct ShowQueries
{
    std::string name;
    int64_t id;
};

struct ShowWorkers
{
    std::string host;
};

struct WorkerStatus
{
    std::vector<std::string> hosts;
};

using PlannedStatement = rfl::TaggedUnion<
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
    ShowLogicalSources,
    ShowPhysicalSources,
    ShowSinks,
    ShowQueries,
    ShowWorkers,
    WorkerStatus>;

PlannedStatement convert(const CreateLogicalSourceResult& r) { return CreateLogicalSource{r.name, rfl::json::write(reflect(r.schema))}; }
PlannedStatement convert(const CreatePhysicalSourceResult& r) { return CreatePhysicalSource{r.logicalSourceName, r.sourceType, rfl::json::write(r.sourceConfig), rfl::json::write(r.parserConfig)}; }
PlannedStatement convert(const CreateSinkResult& r) { return CreateSink{r.name, r.sinkType, rfl::json::write(reflect(r.schema)), rfl::json::write(r.sinkConfig), rfl::json::write(r.formatConfig)}; }
PlannedStatement convert(const CreateWorkerResult& r) { return CreateWorker{r.hostAddr, r.dataAddr, r.capacity.has_value() ? static_cast<int32_t>(*r.capacity) : -1, r.peers, rfl::json::write(r.config)}; }
PlannedStatement convert(const DropLogicalSourceResult& r) { return DropLogicalSource{r.name}; }
PlannedStatement convert(const DropPhysicalSourceResult& r) { return DropPhysicalSource{r.id}; }
PlannedStatement convert(const DropSinkResult& r) { return DropSink{r.name}; }
PlannedStatement convert(const DropQueryResult& r) { return DropQuery{r.name.value_or(""), r.id.value_or(-1), static_cast<int32_t>(r.stopMode)}; }
PlannedStatement convert(const DropWorkerResult& r) { return DropWorker{r.host}; }
PlannedStatement convert(const QueryPlanResult& r)
{
    std::string sql;
    for (const auto& [host, plans] : r.plan)
        for (const auto& plan : plans)
            if (sql.empty()) { sql = plan.getOriginalSql(); }
    return CreateQuery{.name = r.name, .sql_statement = std::move(sql)};
}
PlannedStatement convert(const ExplainQueryResult& r) { return ExplainQuery{r.explanation}; }
PlannedStatement convert(const ShowLogicalSourcesResult& r) { return ShowLogicalSources{r.name.value_or("")}; }
PlannedStatement convert(const ShowPhysicalSourcesResult& r) { return ShowPhysicalSources{r.logicalSourceName.value_or(""), r.id.has_value() ? static_cast<int64_t>(*r.id) : -1}; }
PlannedStatement convert(const ShowSinksResult& r) { return ShowSinks{r.name.value_or("")}; }
PlannedStatement convert(const ShowQueriesResult& r) { return ShowQueries{r.name.value_or(""), r.id.value_or(-1)}; }
PlannedStatement convert(const ShowWorkersResult& r) { return ShowWorkers{r.host.value_or("")}; }
PlannedStatement convert(const WorkerStatusResult& r) { return WorkerStatus{r.host}; }

PlannedStatement convert(const StatementResult& result)
{
    return std::visit([](const auto& r) -> PlannedStatement { return convert(r); }, result);
}

}

::PlannedStatement plan_sql(const CatalogRef& ctx, rust::Str sql)
{
    SqlPlanner planner(ctx, NetworkTopology{});
    auto executed = planner.execute(std::string_view(sql.data(), sql.size()));
    if (!executed)
    {
        throw std::runtime_error(executed.error().what());
    }

    auto json = rfl::json::write(convert(*executed));

    // For query plans, extract fragments with serialized protobuf plans
    rust::Vec<::PlannedFragment> fragments;
    if (auto* qp = std::get_if<QueryPlanResult>(&*executed))
    {
        for (const auto& [host, plans] : qp->plan)
        {
            for (const auto& plan : plans)
            {
                auto proto = QueryPlanSerializationUtil::serializeQueryPlan(plan);
                auto bytes = proto.SerializeAsString();
                auto totalOps = static_cast<int32_t>(flatten(plan).size());
                auto sinks = static_cast<int32_t>(getOperatorByType<SinkLogicalOperator>(plan).size());
                auto sources = static_cast<int32_t>(getOperatorByType<SourceDescriptorLogicalOperator>(plan).size());

                rust::Vec<uint8_t> plan_bytes;
                plan_bytes.reserve(bytes.size());
                for (auto b : bytes) { plan_bytes.push_back(static_cast<uint8_t>(b)); }

                fragments.push_back(::PlannedFragment{
                    .host_addr = rust::String(host.getRawValue()),
                    .plan = std::move(plan_bytes),
                    .used_capacity = totalOps - sinks - sources,
                    .has_source = sources > 0,
                });
            }
        }
    }

    return ::PlannedStatement{
        .json = rust::String(std::move(json)),
        .fragments = std::move(fragments),
    };
}

}
