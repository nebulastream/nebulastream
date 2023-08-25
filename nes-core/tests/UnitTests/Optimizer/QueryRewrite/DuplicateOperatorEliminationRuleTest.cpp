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
#include <BaseIntegrationTest.hpp>
// clang-format on
#include <API/QueryAPI.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryRewrite/DuplicateOperatorEliminationRule.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Experimental/SpatialType.hpp>
#include <Util/Logger/Logger.hpp>
#include <iostream>

using namespace NES;

class DuplicateOperatorEliminationRuleTest : public Testing::BaseIntegrationTest {

  public:
    SchemaPtr schema;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("DuplicateOperatorEliminationRuleTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup DuplicateOperatorEliminationRuleTest test case.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    }
};

void setupSensorNodeAndSourceCatalog(const Catalogs::Source::SourceCatalogPtr& sourceCatalog) {
    NES_INFO("Setup FilterPushDownTest test case.");
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4, properties);
    auto csvSourceType = CSVSourceType::create();
    PhysicalSourcePtr physicalSource = PhysicalSource::create("default_logical", "test_stream", csvSourceType);
    LogicalSourcePtr logicalSource = LogicalSource::create("default_logical", Schema::create());
    Catalogs::Source::SourceCatalogEntryPtr sce1 =
        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, physicalNode);
    sourceCatalog->addPhysicalSource("default_logical", sce1);
}

bool isFilterAndAccessesCorrectFields(NodePtr filter, std::vector<std::string> accessedFields) {
    if (!filter->instanceOf<FilterLogicalOperatorNode>()) {
        return false;
    }

    auto count = accessedFields.size();

    DepthFirstNodeIterator depthFirstNodeIterator(filter->as<FilterLogicalOperatorNode>()->getPredicate());
    for (auto itr = depthFirstNodeIterator.begin(); itr != NES::DepthFirstNodeIterator::end(); ++itr) {
        if ((*itr)->instanceOf<FieldAccessExpressionNode>()) {
            const FieldAccessExpressionNodePtr accessExpressionNode = (*itr)->as<FieldAccessExpressionNode>();
            if (std::find(accessedFields.begin(), accessedFields.end(), accessExpressionNode->getFieldName())
                == accessedFields.end()) {
                return false;
            }
            count--;
        }
    }
    return count == 0;
}


/**
 *
 *       Sink               Sink
 *        |                  |
 *        F(id < 45)         F(id < 45)
 *        |            =>    |
 *        F(id < 45)         Src
 *        |
 *        Src
 *
 *       Sink
 *
 */

TEST_F(DuplicateOperatorEliminationRuleTest, testEliminatingDuplicateFilter) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query =
        Query::from("default_logical")
            .filter(Attribute("id") < 45)
            .filter(Attribute("id") < 45)
            .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr filterOperator = (*itr);
    ++itr;
    const NodePtr filterOperator2 = (*itr);
    ++itr;
    const NodePtr srcOperator = (*itr);

    // Execute
    auto duplicateOperatorEliminationRule = Optimizer::DuplicateOperatorEliminationRule::create();
    const QueryPlanPtr updatedPlan = duplicateOperatorEliminationRule->apply(queryPlan);

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}


/**
 *                      Sink
 *                      |
 *                      F(name = "dan")
 *                      |
 *        --------------J----------------
 *        |
 *        F (id > 9999)               F (id > 9999)
 *        |                             |
 *        Src1(id, name, age)         Src2(id, name, number)
 *
 */

TEST_F(DuplicateOperatorEliminationRuleTest, testEliminatingDuplicateFilterFromDifferentBranchesDoNotGetEliminated) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());

    //setup source 1
    NES::SchemaPtr schema = NES::Schema::create()
                                ->addField("id", NES::BasicType::UINT64)
                                ->addField("orderId", NES::BasicType::UINT64)
                                ->addField("ts", NES::BasicType::UINT64);
    sourceCatalog->addLogicalSource("src2", schema);

    //setup source two
    NES::SchemaPtr schema2 = NES::Schema::create()
                                 ->addField("id", NES::BasicType::UINT64)
                                 ->addField("price", NES::BasicType::UINT64)
                                 ->addField("ts", NES::BasicType::UINT64);

    sourceCatalog->addLogicalSource("src1", schema2);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query subQuery = Query::from("src2")
        .filter(Attribute("id") < 1000);

    Query query = Query::from("src1")
                      .filter(Attribute("id") < 1000)
                      .joinWith(subQuery)
                      .where(Attribute("id"))
                      .equalsTo(Attribute("orderId"))
                      .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    //type inference
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, Catalogs::UDF::UDFCatalog::create());
    typeInferencePhase->execute(queryPlan);

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();
    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr joinOperator = (*itr);
    ++itr;
    const NodePtr watermarkOperatorAboveSrc2 = (*itr);
    ++itr;
    const NodePtr filterOperatorAboveSrc2 = (*itr);
    ++itr;
    const NodePtr srcOperatorSrc2 = (*itr);
    ++itr;
    const NodePtr watermarkOperatorAboveSrc1 = (*itr);
    ++itr;
    const NodePtr filterOperatorAboveSrc1 = (*itr);
    ++itr;
    const NodePtr srcOperatorSrc1 = (*itr);

    auto duplicateOperatorEliminationRule = Optimizer::DuplicateOperatorEliminationRule::create();
    // Execute
    NES_DEBUG("Input Query Plan: {}", (queryPlan)->toString());
    const QueryPlanPtr updatedPlan = duplicateOperatorEliminationRule -> apply(queryPlan);
    NES_DEBUG("Updated Query Plan: {}", (updatedPlan)->toString());


    // TODO: method for checking equality of an QueryPlan
    // check if the query plan remains the same
    DepthFirstNodeIterator updatedQueryPlanIterator(updatedPlan->getRootOperators()[0]);
    auto updatedItr = updatedQueryPlanIterator.begin();
    EXPECT_TRUE(sinkOperator->equal(*updatedItr));
    ++updatedItr;
    EXPECT_TRUE(joinOperator->equal(*updatedItr));
    ++updatedItr;
    EXPECT_TRUE(watermarkOperatorAboveSrc2->equal(*updatedItr));
    ++updatedItr;
    EXPECT_TRUE(filterOperatorAboveSrc2->equal(*updatedItr));
    ++updatedItr;
    EXPECT_TRUE(srcOperatorSrc2->equal(*updatedItr));
    ++updatedItr;
    EXPECT_TRUE(watermarkOperatorAboveSrc1->equal(*updatedItr));
    ++updatedItr;
    EXPECT_TRUE(filterOperatorAboveSrc1->equal(*updatedItr));
    ++updatedItr;
    EXPECT_TRUE(srcOperatorSrc1->equal(*updatedItr));
}

TEST_F(DuplicateOperatorEliminationRuleTest, testEliminatingDuplicateProjections) {

        Catalogs::Source::SourceCatalogPtr sourceCatalog =
            std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());

        //setup source 1
        NES::SchemaPtr schema = NES::Schema::create()
                                    ->addField("id", NES::BasicType::UINT64)
                                    ->addField("orderId", NES::BasicType::UINT64)
                                    ->addField("ts", NES::BasicType::UINT64);
        sourceCatalog->addLogicalSource("src2", schema);

        //setup source two
        NES::SchemaPtr schema2 = NES::Schema::create()
                                     ->addField("id", NES::BasicType::UINT64)
                                     ->addField("price", NES::BasicType::UINT64)
                                     ->addField("ts", NES::BasicType::UINT64);

        sourceCatalog->addLogicalSource("src1", schema2);

        // Prepare
        SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
        Query subQuery = Query::from("src2")
                             .filter(Attribute("id") < 1000);

        Query query = Query::from("src1")
                          .filter(Attribute("id") < 1000)
                          .joinWith(subQuery)
                          .where(Attribute("id"))
                          .equalsTo(Attribute("orderId"))
                          .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                          .project(Attribute("id"), Attribute("orderId"))
                          .project(Attribute("id"), Attribute("orderId"))
                          .sink(printSinkDescriptor);
        const QueryPlanPtr queryPlan = query.getQueryPlan();

        //type inference
        auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, Catalogs::UDF::UDFCatalog::create());
        typeInferencePhase->execute(queryPlan);

        DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
        auto itr = queryPlanNodeIterator.begin();
        const NodePtr sinkOperator = (*itr);
        ++itr;
        const NodePtr projectOperator = (*itr);
        ++itr;
        const NodePtr joinOperator = (*itr);
        ++itr;
        const NodePtr watermarkOperatorAboveSrc2 = (*itr);
        ++itr;
        const NodePtr filterOperatorAboveSrc2 = (*itr);
        ++itr;
        const NodePtr srcOperatorSrc2 = (*itr);
        ++itr;
        const NodePtr watermarkOperatorAboveSrc1 = (*itr);
        ++itr;
        const NodePtr filterOperatorAboveSrc1 = (*itr);
        ++itr;
        const NodePtr srcOperatorSrc1 = (*itr);

        auto duplicateOperatorEliminationRule = Optimizer::DuplicateOperatorEliminationRule::create();
        // Execute
        NES_DEBUG("Input Query Plan: {}", (queryPlan)->toString());
        const QueryPlanPtr updatedPlan = duplicateOperatorEliminationRule -> apply(queryPlan);
        NES_DEBUG("Updated Query Plan: {}", (updatedPlan)->toString());
}