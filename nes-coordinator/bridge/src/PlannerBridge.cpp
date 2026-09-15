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

#include <PlannerBridge.hpp>

#include <expected>
#include <utility>

#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <nes-coordinator-bridge/catalog.h>
#include <nes-coordinator-bridge/planner.h>
#include <rfl/json/read.hpp>
#include <rfl/json/write.hpp>
#include <rust/cxx.h>
#include <BridgeError.hpp>
#include <CatalogBridge.hpp>
#include <SqlPlanner.hpp>

namespace NES::Bridge
{
namespace
{
std::expected<PlannedStatement, Exception>
planStatement(const PlanningTransaction& ctx, rust::Str sql, rust::Str optimizerConfig, rust::Str defaultHost)
{
    auto sqlStr = std::string(sql.data(), sql.size());
    auto config = QueryOptimizerConfiguration{};
    if (!optimizerConfig.empty())
    {
        auto parsed = rfl::json::read<std::unordered_map<std::string, std::string>>(std::string{optimizerConfig});
        if (!parsed)
        {
            throw CannotDeserialize("Failed to deserialize optimizer config from JSON: {}", parsed.error().what());
        }
        config.overwriteConfigWithCommandLineInput(*parsed);
    }
    /// An empty default host rejects statements that omit their HOST clause.
    /// A non-empty one is substituted (embedded deployments, where the only worker is the local one).
    HostPolicy hostPolicy = RequireHostConfig{};
    if (!defaultHost.empty())
    {
        hostPolicy = DefaultHost{std::string{defaultHost}};
    }
    auto executed = SqlPlanner{std::make_shared<CatalogBridge>(ctx, hostPolicy), std::move(config), std::move(hostPolicy)}.plan(sqlStr);
    if (!executed)
    {
        return std::unexpected{std::move(executed).error()};
    }

    /// The planner's statement is already shaped like the Rust model's, so it serializes without an intermediate conversion.
    auto json = rfl::json::write(executed->statement);

    rust::Vec<PlannedQueryFragment> serialized;
    for (const auto& fragment : executed->fragments)
    {
        const auto bytes = QueryPlanSerializationUtil::serializeQueryPlan(fragment.plan).SerializeAsString();
        rust::Vec<uint8_t> planBytes;
        planBytes.reserve(bytes.size());
        for (const auto byte : bytes)
        {
            planBytes.push_back(static_cast<uint8_t>(byte));
        }
        serialized.push_back(PlannedQueryFragment{
            .host_addr = rust::String(fragment.host.getRawValue()),
            .plan = std::move(planBytes),
            .num_operators = fragment.numOperators,
            .has_source = fragment.hasSource,
        });
    }
    rust::Vec<int64_t> sourceIds;
    for (const auto& id : executed->sourceIds)
    {
        sourceIds.push_back(static_cast<int64_t>(id.getRawValue()));
    }
    rust::Vec<int64_t> sinkIds;
    for (const auto& id : executed->sinkIds)
    {
        sinkIds.push_back(id.getRawValue());
    }

    return PlannedStatement{
        .error = {},
        .json = rust::String(json),
        .fragments = std::move(serialized),
        .source_ids = std::move(sourceIds),
        .sink_ids = std::move(sinkIds),
    };
}
}

PlannedStatement plan_sql(const PlanningTransaction& ctx, rust::Str sql, rust::Str optimizerConfig, rust::Str defaultHost)
{
    return guard([&] { return planStatement(ctx, sql, optimizerConfig, defaultHost); });
}
}
