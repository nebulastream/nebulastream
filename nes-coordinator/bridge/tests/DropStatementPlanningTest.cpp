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

#include <QueryOptimizerConfiguration.hpp>
#include <SqlPlanner.hpp>

#include <memory>
#include <string_view>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <coordinator/lib.h>
#include <gtest/gtest.h>
#include <rfl/Variant.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <TransactionalCatalog.hpp>

namespace NES
{
/// A drop statement is a pure catalog request: the planner binds it and maps it onto the wire struct without
/// consulting the catalog, so an empty throwaway context is enough to plan one.
class DropStatementPlanningTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("DropStatementPlanning.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup DropStatementPlanning class.");
    }

    static void TearDownTestSuite() { NES_INFO("Tear down DropStatementPlanning class."); }

    static StatementResult plan(const std::string_view sql)
    {
        const auto context = create_test_planner_context();
        auto output
            = SqlPlanner{std::make_shared<TransactionalCatalog>(context->context(), RequireHostConfig{}), QueryOptimizerConfiguration{}, RequireHostConfig{}}
                  .plan(sql);
        if (!output)
        {
            throw std::move(output).error();
        }
        return std::move(output->statement);
    }
};

/// The filter-less form of every subject but the worker must reach the coordinator with all filter fields empty,
/// which is how the Rust side spells "drop every entity of this kind".
TEST_F(DropStatementPlanningTest, FilterlessDropClearsEveryFilter)
{
    const auto logicalSource = plan("DROP LOGICAL SOURCE");
    ASSERT_TRUE(rfl::holds_alternative<DropLogicalSourceResult>(logicalSource.variant()));
    EXPECT_FALSE(rfl::get<DropLogicalSourceResult>(logicalSource.variant()).name.has_value());

    const auto physicalSource = plan("DROP PHYSICAL SOURCE");
    ASSERT_TRUE(rfl::holds_alternative<DropPhysicalSourceResult>(physicalSource.variant()));
    EXPECT_FALSE(rfl::get<DropPhysicalSourceResult>(physicalSource.variant()).id.has_value());

    const auto sink = plan("DROP SINK");
    ASSERT_TRUE(rfl::holds_alternative<DropSinkResult>(sink.variant()));
    EXPECT_FALSE(rfl::get<DropSinkResult>(sink.variant()).name.has_value());

    const auto model = plan("DROP MODEL");
    ASSERT_TRUE(rfl::holds_alternative<DropModelResult>(model.variant()));
    EXPECT_FALSE(rfl::get<DropModelResult>(model.variant()).name.has_value());

    const auto query = plan("DROP QUERY");
    ASSERT_TRUE(rfl::holds_alternative<DropQueryResult>(query.variant()));
    const auto& filters = rfl::get<DropQueryResult>(query.variant()).filters;
    EXPECT_FALSE(filters.ids.has_value());
    EXPECT_FALSE(filters.name.has_value());
}

/// The filter value travels on as the canonical (SQL case-folded) spelling, because that is what the catalog keys on.
TEST_F(DropStatementPlanningTest, FilteredDropCarriesTheFilter)
{
    const auto logicalSource = plan("DROP LOGICAL SOURCE WHERE NAME = 'some_source'");
    ASSERT_TRUE(rfl::holds_alternative<DropLogicalSourceResult>(logicalSource.variant()));
    EXPECT_EQ(rfl::get<DropLogicalSourceResult>(logicalSource.variant()).name, "SOME_SOURCE");

    const auto physicalSource = plan("DROP PHYSICAL SOURCE WHERE ID = 7");
    ASSERT_TRUE(rfl::holds_alternative<DropPhysicalSourceResult>(physicalSource.variant()));
    EXPECT_EQ(rfl::get<DropPhysicalSourceResult>(physicalSource.variant()).id, 7);

    const auto sink = plan("DROP SINK WHERE NAME = 'some_sink'");
    ASSERT_TRUE(rfl::holds_alternative<DropSinkResult>(sink.variant()));
    EXPECT_EQ(rfl::get<DropSinkResult>(sink.variant()).name, "SOME_SINK");
}

/// Removing a worker needs a concrete address, so this is the one subject without a filter-less form.
TEST_F(DropStatementPlanningTest, DropWorkerRequiresAHostFilter)
{
    const auto worker = plan("DROP WORKER WHERE HOST = 'localhost:8080'");
    ASSERT_TRUE(rfl::holds_alternative<DropWorkerResult>(worker.variant()));
    EXPECT_EQ(rfl::get<DropWorkerResult>(worker.variant()).host.value(), "localhost:8080");

    EXPECT_THROW(plan("DROP WORKER"), Exception);
    EXPECT_THROW(plan("DROP WORKER WHERE NAME = 'localhost:8080'"), Exception);
}
}
