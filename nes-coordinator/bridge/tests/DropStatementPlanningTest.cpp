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
#include <optional>
#include <string_view>
#include <utility>
#include <variant>
#include <Configurations/Util.hpp>
#include <Identifiers/Identifier.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <nes-coordinator-bridge/test_context.h>
#include <BaseUnitTest.hpp>
#include <CatalogBridge.hpp>
#include <CoordinatorStatement.hpp>
#include <ErrorHandling.hpp>

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

    static CoordinatorStatement plan(const std::string_view sql)
    {
        const auto context = Bridge::create_test_planner_context();
        auto output
            = SqlPlanner{std::make_shared<CatalogBridge>(context->context(), std::nullopt), defaultConfiguration<QueryOptimizerConfiguration>(), std::nullopt}
                  .plan(sql);
        return std::move(output.statement);
    }
};

/// The ALL form of every subject but the worker must reach the coordinator with all filter fields empty,
/// which the Rust side reads as every entity of this kind.
TEST_F(DropStatementPlanningTest, DropAll)
{
    const auto logicalSource = plan("DROP LOGICAL SOURCE ALL");
    ASSERT_TRUE(std::holds_alternative<PlannedDropLogicalSource>(logicalSource));
    EXPECT_FALSE(std::get<PlannedDropLogicalSource>(logicalSource).name.has_value());

    const auto physicalSource = plan("DROP PHYSICAL SOURCE ALL");
    ASSERT_TRUE(std::holds_alternative<PlannedDropPhysicalSource>(physicalSource));
    EXPECT_FALSE(std::get<PlannedDropPhysicalSource>(physicalSource).id.has_value());

    const auto sink = plan("DROP SINK ALL");
    ASSERT_TRUE(std::holds_alternative<PlannedDropSink>(sink));
    EXPECT_FALSE(std::get<PlannedDropSink>(sink).name.has_value());

    const auto model = plan("DROP MODEL ALL");
    ASSERT_TRUE(std::holds_alternative<PlannedDropModel>(model));
    EXPECT_FALSE(std::get<PlannedDropModel>(model).name.has_value());

    const auto query = plan("DROP QUERY ALL");
    ASSERT_TRUE(std::holds_alternative<PlannedDropQuery>(query));
    const auto& filters = std::get<PlannedDropQuery>(query).filters;
    EXPECT_FALSE(filters.ids.has_value());
    EXPECT_FALSE(filters.name.has_value());

    const auto stop = plan("STOP QUERY ALL");
    ASSERT_TRUE(std::holds_alternative<PlannedStopQuery>(stop));
    const auto& stopFilters = std::get<PlannedStopQuery>(stop).filters;
    EXPECT_FALSE(stopFilters.ids.has_value());
    EXPECT_FALSE(stopFilters.name.has_value());
}

TEST_F(DropStatementPlanningTest, FilteredDropCarriesTheFilter)
{
    const auto logicalSource = plan("DROP LOGICAL SOURCE WHERE NAME = 'some_source'");
    ASSERT_TRUE(std::holds_alternative<PlannedDropLogicalSource>(logicalSource));
    EXPECT_EQ(std::get<PlannedDropLogicalSource>(logicalSource).name, Identifier::parse("SOME_SOURCE"));

    const auto physicalSource = plan("DROP PHYSICAL SOURCE WHERE ID = 7");
    ASSERT_TRUE(std::holds_alternative<PlannedDropPhysicalSource>(physicalSource));
    EXPECT_EQ(std::get<PlannedDropPhysicalSource>(physicalSource).id, PhysicalSourceId{7});

    const auto sink = plan("DROP SINK WHERE NAME = 'some_sink'");
    ASSERT_TRUE(std::holds_alternative<PlannedDropSink>(sink));
    EXPECT_EQ(std::get<PlannedDropSink>(sink).name, Identifier::parse("SOME_SINK"));
}

/// Removing a worker needs a concrete address, so this is the one subject without an ALL form.
TEST_F(DropStatementPlanningTest, DropWorkerRequiresAHostFilter)
{
    const auto worker = plan("DROP WORKER WHERE HOST = 'localhost:8080'");
    ASSERT_TRUE(std::holds_alternative<PlannedDropWorker>(worker));
    EXPECT_EQ(std::get<PlannedDropWorker>(worker).hostAddr, "localhost:8080");

    EXPECT_THROW(plan("DROP WORKER"), Exception);
    EXPECT_THROW(plan("DROP WORKER ALL"), Exception);
    EXPECT_THROW(plan("DROP WORKER WHERE NAME = 'localhost:8080'"), Exception);
}
}
