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

#pragma once

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Configurations/Util.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <nes-coordinator-bridge/catalog.h>
#include <nes-coordinator-bridge/planner.h>
#include <nes-coordinator-bridge/test_context.h>
#include <rust/cxx.h>
#include <BridgeError.hpp>
#include <CatalogBridge.hpp>
#include <ErrorHandling.hpp>
#include <PlannerBridge.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <SqlPlanner.hpp>

namespace NES
{
/// A test catalog that was filled from SQL, and the query to plan against it.
/// It owns the transaction, so the catalog exists for as long as the test plans against it.
struct PlanningScenario
{
    rust::Box<Bridge::TestPlanningTransaction> context;
    std::string query;

    /// Plans the query and returns the plans of each worker.
    /// A planning failure is thrown.
    [[nodiscard]] std::unordered_map<Host, std::vector<LogicalPlan>> optimize() const
    {
        auto [statement, fragments]
            = SqlPlanner{std::make_shared<CatalogBridge>(context->context(), std::nullopt), defaultConfiguration<QueryOptimizerConfiguration>(), std::nullopt}
                  .plan(query);
        std::unordered_map<Host, std::vector<LogicalPlan>> plansByHost;
        for (auto& fragment : fragments)
        {
            plansByHost[fragment.host].push_back(std::move(fragment.plan));
        }
        return plansByHost;
    }
};

/// Creates an in-memory catalog, runs every statement of `setupSql` on it, and pairs it with `query`.
/// The setup statements go through the same planning call as in production.
inline PlanningScenario loadAndBind(const std::string_view setupSql, const std::string_view query)
{
    auto context = Bridge::create_test_planner_context();
    const auto statements = AntlrSQLQueryParser::splitStatements(setupSql);
    if (!statements.has_value())
    {
        throw Exception{statements.error()};
    }
    for (const auto& statement : *statements)
    {
        const auto [error, statementJson, fragments]
            = Bridge::plan_sql(context->context(), rust::Str{statement.data(), statement.size()}, rust::Str{}, rust::Str{});
        Bridge::throwIfError(error);
        context->execute_seed_statement(statementJson);
    }
    return PlanningScenario{.context = std::move(context), .query = std::string{query}};
}
}
