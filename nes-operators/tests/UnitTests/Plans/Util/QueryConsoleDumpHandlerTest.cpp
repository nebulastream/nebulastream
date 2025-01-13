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
#include <memory>
#include <Functions/ArithmeticalFunctions/NodeFunctionAdd.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionMod.hpp>
#include <Functions/NodeFunctionConstantValue.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <Operators/LogicalOperators/LogicalMapOperator.hpp>
#include <Operators/LogicalOperators/LogicalSelectionOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceNameLogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/AvgAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowOperator.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Types/TumblingWindow.hpp>
#include <gtest/gtest.h>
#include <sys/types.h>
#include <BaseUnitTest.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include "Measures/TimeCharacteristic.hpp"
#include "Measures/TimeMeasure.hpp"
#include "Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp"
#include "Operators/LogicalOperators/Windows/LogicalWindowDescriptor.hpp"
#include "Operators/Operator.hpp"
#include "Util/Logger/LogLevel.hpp"
#include "Util/Logger/Logger.hpp"
#include "Util/Logger/impl/NesLogger.hpp"

namespace NES
{
class QueryConsoleDumpHandlerTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        NES::Logger::setupLogging("QueryConsoleDumpHandlerTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup QueryConsoleDumpHandlerTest test class.");
    }

    void SetUp() override { Testing::BaseUnitTest::SetUp(); }

protected:
    u_int64_t milliseconds = 5000;
};

/// simple source filter map sink
TEST_F(QueryConsoleDumpHandlerTest, printQuerySourceFilterMapSink)
{
    auto source = std::make_shared<SourceNameLogicalOperator>("test_source_1", getNextOperatorId());
    auto queryPlan = QueryPlan::create(source);
    auto zero = NodeFunctionConstantValue::create(DataTypeFactory::createInt32(), "0");
    auto accessFilter = NodeFunctionFieldAccess::create(DataTypeFactory::createInt32(), "field1");
    auto modulo = NodeFunctionMod::create(accessFilter, zero);
    auto filter = std::make_shared<LogicalSelectionOperator>(modulo, getNextOperatorId());
    queryPlan->appendOperatorAsNewRoot(filter);
    auto one = NodeFunctionConstantValue::create(DataTypeFactory::createInt32(), "1");
    auto accessMap1 = NodeFunctionFieldAccess::create(DataTypeFactory::createInt32(), "field1");
    auto add = NodeFunctionAdd::create(accessMap1, one);
    auto accessMap2 = NodeFunctionFieldAccess::create(DataTypeFactory::createInt32(), "field1");
    auto assign = NodeFunctionFieldAssignment::create(std::static_pointer_cast<NodeFunctionFieldAccess>(accessMap2), add);
    auto map = std::make_shared<LogicalMapOperator>(assign, getNextOperatorId());
    queryPlan->appendOperatorAsNewRoot(map);
    auto sink = std::make_shared<SinkLogicalOperator>("print_sink", getNextOperatorId());
    queryPlan->appendOperatorAsNewRoot(sink);
    const auto* expected = "                        \n"
                           "    SINK(print_sink)    \n"
                           "            │           \n"
                           "MAP(field1 = field1 + 1)\n"
                           "            │           \n"
                           "   FILTER(field1 % 0)   \n"
                           "            │           \n"
                           " SOURCE(test_source_1)  \n";
    auto actual = queryPlan->toString();
    NES_DEBUG("Query Plan actual:\n{}", actual)
    EXPECT_EQ(actual, expected);
}

/// join to two streams: window agg and filter each into its own sink
/// sink1-map-\
///            +---filter---window agg---test_source_1
/// sink2-----/
TEST_F(QueryConsoleDumpHandlerTest, printQueryWindowAggFilterTwoSinks)
{
    auto source = std::make_shared<SourceNameLogicalOperator>("test_source_1", getNextOperatorId());
    auto queryPlan = QueryPlan::create(source);
    auto accessAgg = NodeFunctionFieldAccess::create(DataTypeFactory::createInt32(), "field1");
    auto asAgg = NodeFunctionFieldAccess::create(DataTypeFactory::createInt32(), "field1_avg");
    auto agg = Windowing::AvgAggregationDescriptor::create(
        std::static_pointer_cast<NodeFunctionFieldAccess>(accessAgg), std::static_pointer_cast<NodeFunctionFieldAccess>(asAgg));
    auto windowType = Windowing::TumblingWindow::of(
        std::make_shared<Windowing::TimeCharacteristic>(Windowing::TimeCharacteristic(Windowing::TimeCharacteristic::Type::IngestionTime)),
        Windowing::TimeMeasure(milliseconds));
    auto descriptor = Windowing::LogicalWindowDescriptor::create({agg}, windowType);
    auto window = std::make_shared<LogicalWindowOperator>(descriptor, getNextOperatorId());
    queryPlan->appendOperatorAsNewRoot(window);
    auto zero = NodeFunctionConstantValue::create(DataTypeFactory::createInt32(), "0");
    auto accessFilter = NodeFunctionFieldAccess::create(DataTypeFactory::createInt32(), "field1");
    auto modulo = NodeFunctionMod::create(accessFilter, zero);
    auto filter = std::make_shared<LogicalSelectionOperator>(modulo, getNextOperatorId());
    queryPlan->appendOperatorAsNewRoot(filter);
    auto sink1 = std::make_shared<SinkLogicalOperator>("sink1", getNextOperatorId());
    queryPlan->appendOperatorAsNewRoot(sink1);
    auto sink2 = std::make_shared<SinkLogicalOperator>("sink2", getNextOperatorId());
    queryPlan->addRootOperator(sink2);
    filter->addParent(sink2);
    const auto* expected = "                       \n"
                           "SINK(sink1) SINK(sink2)\n"
                           "     └─────┬─────┘     \n"
                           "  FILTER(field1 % 0)   \n"
                           "           │           \n"
                           "    WINDOW AGG(Avg)    \n"
                           "           │           \n"
                           " SOURCE(test_source_1) \n";
    auto actual = queryPlan->toString();
    NES_DEBUG("Query Plan actual:\n{}", actual)
    EXPECT_EQ(actual, expected);
}
/// TODO #685 Add a test that forces us to add dummy nodes for nodes that have to be moved to another layer while being drawn.
}
