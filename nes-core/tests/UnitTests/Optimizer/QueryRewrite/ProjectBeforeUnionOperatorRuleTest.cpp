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

// clang-format off
#include <gtest/gtest.h>
// clang-format on
#include <API/Query.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryRewrite/ProjectBeforeUnionOperatorRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <iostream>

using namespace NES;

class ProjectBeforeUnionOperatorRuleTest : public testing::Test {

  public:
    SchemaPtr schema;
    SourceCatalogPtr sourceCatalog;

    /* Will be called before all tests in this class are started. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ProjectBeforeUnionOperatorRuleTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup ProjectBeforeUnionOperatorRuleTest test case.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override { schema = Schema::create()->addField("a", BasicType::UINT32)->addField("b", BasicType::UINT32); }

    /* Will be called before a test is executed. */
    void TearDown() override { NES_INFO("Setup ProjectBeforeUnionOperatorRuleTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down ProjectBeforeUnionOperatorRuleTest test class."); }

    void setupSensorNodeAndSourceCatalog(const SourceCatalogPtr& sourceCatalog) const {
        NES_INFO("Setup FilterPushDownTest test case.");
        TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);
        LogicalSourcePtr logicalSource1 = LogicalSource::create("x", schema);
        LogicalSourcePtr logicalSource2 = LogicalSource::create("y", schema);
        PhysicalSourcePtr physicalSource1 = PhysicalSource::create("x", "x1");
        PhysicalSourcePtr physicalSource2 = PhysicalSource::create("y", "y1");
        SourceCatalogEntryPtr sce1 = std::make_shared<SourceCatalogEntry>(physicalSource1, logicalSource1, physicalNode);
        SourceCatalogEntryPtr sce2 = std::make_shared<SourceCatalogEntry>(physicalSource1, logicalSource2, physicalNode);
        sourceCatalog->addPhysicalSource("x", sce1);
        sourceCatalog->addPhysicalSource("y", sce2);
        sourceCatalog->addLogicalSource("x", schema);
        sourceCatalog->addLogicalSource("y", schema);
    }
};

TEST_F(ProjectBeforeUnionOperatorRuleTest, testAddingProjectForUnionWithDifferentSchemas) {

    // Prepare
    SourceCatalogPtr sourceCatalog = std::make_shared<SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalog(sourceCatalog);
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query subQuery = Query::from("x");
    Query query = Query::from("y").unionWith(subQuery).sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto projectionOperators = queryPlan->getOperatorByType<ProjectionLogicalOperatorNode>();
    EXPECT_TRUE(projectionOperators.empty());

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog);
    typeInferencePhase->execute(queryPlan);

    auto projectBeforeUnionOperatorRule = Optimizer::ProjectBeforeUnionOperatorRule::create();
    auto updatedQueryPlan = projectBeforeUnionOperatorRule->apply(queryPlan);

    typeInferencePhase->execute(updatedQueryPlan);

    projectionOperators = updatedQueryPlan->getOperatorByType<ProjectionLogicalOperatorNode>();
    EXPECT_TRUE(projectionOperators.size() == 1);
    auto projectOperator = projectionOperators[0];
    SchemaPtr projectOutputSchema = projectOperator->getOutputSchema();
    EXPECT_TRUE(projectOutputSchema->hasFieldName("y$a"));
    EXPECT_TRUE(projectOutputSchema->hasFieldName("y$b"));
}

TEST_F(ProjectBeforeUnionOperatorRuleTest, testAddingProjectForUnionWithSameSchemas) {

    // Prepare
    SourceCatalogPtr sourceCatalog = std::make_shared<SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalog(sourceCatalog);
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query subQuery = Query::from("x");
    Query query = Query::from("x").unionWith(subQuery).sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto projectionOperators = queryPlan->getOperatorByType<ProjectionLogicalOperatorNode>();
    EXPECT_TRUE(projectionOperators.empty());

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog);
    typeInferencePhase->execute(queryPlan);

    auto projectBeforeUnionOperatorRule = Optimizer::ProjectBeforeUnionOperatorRule::create();
    auto updatedQueryPlan = projectBeforeUnionOperatorRule->apply(queryPlan);

    typeInferencePhase->execute(updatedQueryPlan);

    projectionOperators = updatedQueryPlan->getOperatorByType<ProjectionLogicalOperatorNode>();
    EXPECT_TRUE(projectionOperators.empty());
}
