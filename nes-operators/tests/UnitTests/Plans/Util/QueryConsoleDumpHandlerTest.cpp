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
#include <Functions/NodeFunctionConstantValue.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Operators/LogicalOperators/LogicalSelectionOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceNameLogicalOperator.hpp>
#include <gtest/gtest.h>
#include <BaseIntegrationTest.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

class QueryConsoleDumpHandlerTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        NES::Logger::setupLogging("QueryConsoleDumpHandlerTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup QueryConsoleDumpHandlerTest test class.");
    }

    void SetUp() override
    {
        Testing::BaseUnitTest::SetUp();

        pred1 = NodeFunctionConstantValue::create(DataTypeFactory::createInt8(), "1");
        pred2 = NodeFunctionConstantValue::create(DataTypeFactory::createInt8(), "2");
        pred3 = NodeFunctionConstantValue::create(DataTypeFactory::createInt8(), "3");
        pred4 = NodeFunctionConstantValue::create(DataTypeFactory::createInt8(), "4");
        pred5 = NodeFunctionConstantValue::create(DataTypeFactory::createInt8(), "5");
        pred6 = NodeFunctionConstantValue::create(DataTypeFactory::createInt8(), "6");
        pred7 = NodeFunctionConstantValue::create(DataTypeFactory::createInt8(), "7");

        sourceOp1 = std::make_shared<SourceNameLogicalOperator>("test_source_1", getNextOperatorId());
        sourceOp2 = std::make_shared<SourceNameLogicalOperator>("test_source_2", getNextOperatorId());
        filterOp1 = std::make_shared<LogicalSelectionOperator>(pred1, getNextOperatorId());
        filterOp2 = std::make_shared<LogicalSelectionOperator>(pred2, getNextOperatorId());
        filterOp3 = std::make_shared<LogicalSelectionOperator>(pred3, getNextOperatorId());
        filterOp4 = std::make_shared<LogicalSelectionOperator>(pred4, getNextOperatorId());
        sinkOp1 = std::make_shared<SinkLogicalOperator>("print_sink", getNextOperatorId());
        sinkOp2 = std::make_shared<SinkLogicalOperator>("print_sink", getNextOperatorId());
        sinkOp3 = std::make_shared<SinkLogicalOperator>("print_sink", getNextOperatorId());

        children.clear();
        parents.clear();
    }

protected:
    NodeFunctionPtr pred1, pred2, pred3, pred4, pred5, pred6, pred7;
    LogicalOperatorPtr sourceOp1, sourceOp2;

    LogicalOperatorPtr filterOp1, filterOp2, filterOp3, filterOp4;
    LogicalOperatorPtr sinkOp1, sinkOp2, sinkOp3;

    std::vector<NodePtr> children{};
    std::vector<NodePtr> parents{};
};

/// simple source filter map sink
TEST_F(QueryConsoleDumpHandlerTest, printQuerySourceFilterMapSink)
{
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(filterOp1);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);

    NES_DEBUG("{}", queryPlan->toString());
}

/// join to two streams: window agg and filter each into its own sink

}