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

#include <API/Query.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Catalogs/StreamCatalogEntry.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/QueryMerger/Signature/QueryPlanSignature.hpp>
#include <Phases/TypeInferencePhase.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>
#include <z3++.h>

namespace NES::Optimizer {

class SignatureInferencePhaseTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::setupLogging("Z3ExpressionInferencePhaseTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup Z3ExpressionInferencePhaseTest test case.");
    }

    /* Will be called before a  test is executed. */
    void SetUp() {}

    /* Will be called before a test is executed. */
    void TearDown() { NES_INFO("Tear down Z3ExpressionInferencePhaseTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down Z3ExpressionInferencePhaseTest test class."); }
};

/**
 * @brief In this test we execute query merger phase on a single invalid query plan.
 */
TEST_F(SignatureInferencePhaseTest, executeQueryMergerPhaseForSingleInvalidQueryPlan) {

    //Prepare
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create a new query without assigning it a query id.");

    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);

    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(streamConf, physicalNode);
    streamCatalog->addPhysicalStream("default_logical", sce);

    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    z3::ContextPtr context = std::make_shared<z3::context>();
    auto signatureInferencePhase = SignatureInferencePhase::create(context);

    auto query1 = Query::from("default_logical").map(Attribute("f3") = Attribute("id")++).sink(FileSinkDescriptor::create(""));
    auto plan1 = query1.getQueryPlan();

    typeInferencePhase->execute(plan1);
    signatureInferencePhase->execute(plan1);

    auto query2 = Query::from("default_logical").map(Attribute("f3") = Attribute("id")++).sink(FileSinkDescriptor::create(""));
    auto plan2 = query2.getQueryPlan();

    typeInferencePhase->execute(plan2);
    signatureInferencePhase->execute(plan2);

    auto mapOperators1 = plan1->getOperatorByType<MapLogicalOperatorNode>();
    auto mapOperators2 = plan2->getOperatorByType<MapLogicalOperatorNode>();

    ASSERT_EQ(mapOperators1.size(), 1);
    ASSERT_EQ(mapOperators2.size(), 1);

    ASSERT_TRUE(mapOperators1[0]->getSignature()->isEqual(mapOperators2[0]->getSignature()));

    auto srcOperators1 = plan1->getOperatorByType<SourceLogicalOperatorNode>();
    auto srcOperators2 = plan2->getOperatorByType<SourceLogicalOperatorNode>();

    ASSERT_EQ(srcOperators1.size(), 1);
    ASSERT_EQ(srcOperators2.size(), 1);

    ASSERT_TRUE(srcOperators1[0]->getSignature()->isEqual(srcOperators2[0]->getSignature()));
}
}// namespace NES::Optimizer
