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

#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include <Configurations/Util.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <nes-coordinator-bridge/planner.h>
#include <rfl/json/write.hpp>
#include <rust/cxx.h>
#include <BridgeConfig.hpp>
#include <BridgeError.hpp>
#include <ErrorHandling.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <SnapshotCatalog.hpp>
#include <SqlPlanner.hpp>

namespace NES::Bridge
{
namespace
{
/// The coordinator stores each plan and sends a fragment's plan to the worker unchanged.
/// It never reads a plan, so the plans are passed as the protobuf bytes that the worker takes.
rust::Vec<uint8_t> serialize(const LogicalPlan& plan)
{
    const auto bytes = QueryPlanSerializationUtil::serializeQueryPlan(plan).SerializeAsString();
    rust::Vec<uint8_t> planBytes;
    planBytes.reserve(bytes.size());
    for (const auto byte : bytes)
    {
        planBytes.push_back(static_cast<uint8_t>(byte));
    }
    return planBytes;
}

PlannedStatement planStatement(const Planner& planner, const CatalogSnapshot& snapshot, const rust::Str sql)
{
    const auto catalog = std::make_shared<SnapshotCatalog>(snapshot, planner.defaultHost);
    const auto [statement, globalPlan, fragments]
        = SqlPlanner{catalog, planner.optimizerConfig, planner.defaultHost}.plan(std::string{sql});

    rust::Vec<PlannedQueryFragment> serializedFragments;
    for (const auto& [host, plan, numOperators, hasSource] : fragments)
    {
        serializedFragments.push_back(
            PlannedQueryFragment{
                .host_addr = rust::String{host.getRawValue()},
                .plan = serialize(plan),
                .num_operators = numOperators,
                .has_source = hasSource,
            });
    }
    return PlannedStatement{
        /// Empty on success.
        /// A failure does not reach this line: it is thrown, and the FFI function puts it into this field.
        .error = {},
        .statement_json = rust::String{rfl::json::write(statement)},
        .global_plan = globalPlan.transform(serialize).value_or(rust::Vec<uint8_t>{}),
        .fragments = std::move(serializedFragments),
        .created_sources = catalog->getCreatedSources(),
        .created_sinks = catalog->getCreatedSinks(),
    };
}
}

std::unique_ptr<Planner> make_planner(const rust::Str optimizerConfig, const rust::Str defaultHost)
{
    auto config = resolveConfiguration<QueryOptimizerConfiguration>(literalsFromJson(std::string{optimizerConfig}));
    if (!config.has_value())
    {
        throw InvalidConfigParameter("{}", config.error());
    }
    /// The coordinator passes an empty string when it has no default host.
    return std::make_unique<Planner>(Planner{
        .optimizerConfig = std::move(*config),
        .defaultHost = defaultHost.empty() ? std::nullopt : std::optional{Host{std::string{defaultHost}}}});
}

PlannedStatement plan_sql(const Planner& planner, const CatalogSnapshot& snapshot, const rust::Str sql)
{
    return catchAsBridgeError([&] { return planStatement(planner, snapshot, sql); });
}
}
