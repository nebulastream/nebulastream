/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <API/Schema.hpp>
#include <API/Expressions/ArithmeticalExpressions.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Optimizer/Phases/QueryChooseMemLayoutPhase.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>

namespace NES {

class QueryChooseMemLayoutPhaseTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::setupLogging("QueryChooseMemLayoutPhaseTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup QueryChooseMemLayoutPhaseTest test case.");
    }

    /* Will be called before a  test is executed. */
    void SetUp() override { NES_INFO("Setup QueryChooseMemLayoutPhaseTest test case."); }

    /* Will be called before a test is executed. */
    void TearDown() override { NES_INFO("Tear down QueryChooseMemLayoutPhaseTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down QueryChooseMemLayoutPhaseTest test class."); }

};

TEST_F(QueryChooseMemLayoutPhaseTest, executeQueryChooseMemLayoutSimple) {
    auto inputSchema = Schema::create();
    inputSchema->addField("f1", BasicType::INT32);
    inputSchema->addField("f2", BasicType::INT8);

    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>(QueryParsingServicePtr());
    streamCatalog->removeLogicalStream("default_logical");
    streamCatalog->addLogicalStream("default_logical", inputSchema);

    auto source = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("default_logical"));
    auto map = LogicalOperatorFactory::createMapOperator(Attribute("f3") = Attribute("f1") * 42);
    auto sink = LogicalOperatorFactory::createSinkOperator(FileSinkDescriptor::create(""));

    auto plan = QueryPlan::create(source);
    plan->appendOperatorAsNewRoot(map);
    plan->appendOperatorAsNewRoot(sink);

    auto phase = Optimizer::QueryChooseMemLayoutPhase::create(Schema::COL_LAYOUT);
    phase->execute(plan);

    // TODO write here a simple test

}

}