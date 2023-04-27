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
#include <NesBaseTest.hpp>
// clang-format on
#include <API/QueryAPI.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedQueryContainmentRule.hpp>
#include <Optimizer/QuerySignatures/SignatureContainmentUtil.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Util/Logger/Logger.hpp>
#include <Windowing/Watermark/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Windowing/Watermark/IngestionTimeWatermarkStrategyDescriptor.hpp>
#include <iostream>
#include <z3++.h>

using namespace NES;

class Z3SignatureBasedQueryContainmentRuleTest : public Testing::TestWithErrorHandling<testing::Test> {

  public:
    SchemaPtr schema;
    SchemaPtr schemaHouseholds;

    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    std::shared_ptr<Catalogs::UDF::UDFCatalog> udfCatalog;
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    //Vector with tuples containing two queries. The queries exhibit different containment relationships
    //We indicate the respective containment relationship above each tuple
    std::vector<std::tuple<Query, Query>> containmentCasesTrue = {
        //Equal
        std::tuple<Query, Query>(Query::from("windTurbines")
                                     .map(Attribute("value") = 40)
                                     .filter(Attribute("id") < 45)
                                     .filter(Attribute("id") < 45)
                                     .filter(Attribute("id") < 45)
                                     .filter(Attribute("id") < 45)
                                     .sink(printSinkDescriptor),
                                 Query::from("windTurbines")
                                     .map(Attribute("value") = 40)
                                     .filter(Attribute("id") < 45)
                                     .filter(Attribute("id") < 45)
                                     .filter(Attribute("id") < 45)
                                     .filter(Attribute("id") < 45)
                                     .sink(printSinkDescriptor)),
        //Sig2 contains Sig1
        std::tuple<Query, Query>(
            Query::from("windTurbines").map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(printSinkDescriptor),
            Query::from("windTurbines").map(Attribute("value") = 40).filter(Attribute("id") < 60).sink(printSinkDescriptor)),
        //Sig1 contains Sig2
        std::tuple<Query, Query>(
            Query::from("windTurbines").map(Attribute("value") = 40).filter(Attribute("id") < 60).sink(printSinkDescriptor),
            Query::from("windTurbines").map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(printSinkDescriptor)),
        //Sig2 contains Sig1
        std::tuple<Query, Query>(
            Query::from("windTurbines").map(Attribute("value") = 40).project(Attribute("value")).sink(printSinkDescriptor),
            Query::from("windTurbines").map(Attribute("value") = 40).sink(printSinkDescriptor))};

    //Vector with tuples containing two queries. The queries do not exhibit containment relationships but are for testing
    //that our algorithm correctly identifies these cases as NO_CONTAINMENT
    std::vector<std::tuple<Query, Query>> noContainmentCases = {
        std::tuple<Query, Query>(Query::from("windTurbines")
                                     .map(Attribute("value") = 40)
                                     .filter(Attribute("id") < 45)
                                     .filter(Attribute("id") < 45)
                                     .filter(Attribute("id") < 45)
                                     .filter(Attribute("id") < 45)
                                     .sink(printSinkDescriptor),
                                 Query::from("windTurbines")
                                     .map(Attribute("value") = 40)
                                     .filter(Attribute("id") > 45)
                                     .filter(Attribute("id") > 45)
                                     .filter(Attribute("id") > 45)
                                     .filter(Attribute("id") > 45)
                                     .sink(printSinkDescriptor)),
        std::tuple<Query, Query>(
            Query::from("windTurbines").map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(printSinkDescriptor),
            Query::from("windTurbines").map(Attribute("value") = 40).filter(Attribute("id") > 60).sink(printSinkDescriptor)),
        std::tuple<Query, Query>(
            Query::from("windTurbines").map(Attribute("value") = 40).filter(Attribute("id") > 60).sink(printSinkDescriptor),
            Query::from("windTurbines").map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(printSinkDescriptor))};

    /* Will be called before all tests in this class are started. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("Z3SignatureBasedCompleteQueryMergerRuleTest.log", NES::LogLevel::LOG_TRACE);
        NES_INFO("Setup Z3SignatureBasedCompleteQueryMergerRuleTest test case.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::TestWithErrorHandling<testing::Test>::SetUp();
        schema = Schema::create()
                     ->addField("ts", BasicType::UINT32)
                     ->addField("type", BasicType::UINT32)
                     ->addField("id", BasicType::UINT32)
                     ->addField("value", BasicType::UINT64)
                     ->addField("id1", BasicType::UINT32)
                     ->addField("value1", BasicType::UINT64)
                     ->addField("value2", BasicType::UINT64);

        schemaHouseholds = Schema::create()
                               ->addField("ts", BasicType::UINT32)
                               ->addField("type", DataTypeFactory::createFixedChar(8))
                               ->addField("id", BasicType::UINT32)
                               ->addField("value", BasicType::UINT64)
                               ->addField("id1", BasicType::UINT32)
                               ->addField("value1", BasicType::FLOAT32)
                               ->addField("value2", BasicType::FLOAT64)
                               ->addField("value3", BasicType::UINT64);

        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
        sourceCatalog->addLogicalSource("windTurbines", schema);
        sourceCatalog->addLogicalSource("solarPanels", schema);
        sourceCatalog->addLogicalSource("households", schemaHouseholds);
        udfCatalog = Catalogs::UDF::UDFCatalog::create();
    }
};

//Todo: #3493 more test cases needed: different schema
/**
 * @brief test if the apply function for the containment based complete query merger rule returns true for the given queries (identifies containment correctly)
 */
TEST_F(Z3SignatureBasedQueryContainmentRuleTest, testContainmentBasedCompleteQueryMergerRule) {
    for (auto queries : containmentCasesTrue) {
        QueryPlanPtr queryPlanSQPQuery = get<0>(queries).getQueryPlan();
        SinkLogicalOperatorNodePtr sinkOperatorSQPQuery = queryPlanSQPQuery->getSinkOperators()[0];
        QueryId queryIdSQPQuery = PlanIdGenerator::getNextQueryId();
        queryPlanSQPQuery->setQueryId(queryIdSQPQuery);

        QueryPlanPtr queryPlanNewQuery = get<1>(queries).getQueryPlan();
        SinkLogicalOperatorNodePtr sinkOperatorNewQuery = queryPlanSQPQuery->getSinkOperators()[0];
        QueryId queryIdNewQuery = PlanIdGenerator::getNextQueryId();
        queryPlanNewQuery->setQueryId(queryIdNewQuery);

        auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
        typeInferencePhase->execute(queryPlanSQPQuery);
        typeInferencePhase->execute(queryPlanNewQuery);

        z3::ContextPtr context = std::make_shared<z3::context>();
        auto z3InferencePhase =
            Optimizer::SignatureInferencePhase::create(context, Optimizer::QueryMergerRule::Z3SignatureBasedQueryContainmentRule);
        z3InferencePhase->execute(queryPlanSQPQuery);
        z3InferencePhase->execute(queryPlanNewQuery);

        auto globalQueryPlan = GlobalQueryPlan::create();
        globalQueryPlan->addQueryPlan(queryPlanSQPQuery);
        globalQueryPlan->addQueryPlan(queryPlanNewQuery);

        //execute
        auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedQueryContainmentRule::create(context);
        ASSERT_TRUE(signatureBasedEqualQueryMergerRule->apply(globalQueryPlan));
    }
}

/**
 * @brief test if the apply function for the containment based complete query merger rule returns false for the given queries (identifies no containment present correctly)
 */
TEST_F(Z3SignatureBasedQueryContainmentRuleTest, testContainmentBasedCompleteQueryMergerRuleReturnsFalse) {
    for (auto queries : noContainmentCases) {
        QueryPlanPtr queryPlanSQPQuery = get<0>(queries).getQueryPlan();
        SinkLogicalOperatorNodePtr sinkOperatorSQPQuery = queryPlanSQPQuery->getSinkOperators()[0];
        QueryId queryIdSQPQuery = PlanIdGenerator::getNextQueryId();
        queryPlanSQPQuery->setQueryId(queryIdSQPQuery);

        QueryPlanPtr queryPlanNewQuery = get<1>(queries).getQueryPlan();
        SinkLogicalOperatorNodePtr sinkOperatorNewQuery = queryPlanSQPQuery->getSinkOperators()[0];
        QueryId queryIdNewQuery = PlanIdGenerator::getNextQueryId();
        queryPlanNewQuery->setQueryId(queryIdNewQuery);

        auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
        typeInferencePhase->execute(queryPlanSQPQuery);
        typeInferencePhase->execute(queryPlanNewQuery);

        z3::ContextPtr context = std::make_shared<z3::context>();
        auto z3InferencePhase =
            Optimizer::SignatureInferencePhase::create(context, Optimizer::QueryMergerRule::Z3SignatureBasedQueryContainmentRule);
        z3InferencePhase->execute(queryPlanSQPQuery);
        z3InferencePhase->execute(queryPlanNewQuery);

        auto globalQueryPlan = GlobalQueryPlan::create();
        globalQueryPlan->addQueryPlan(queryPlanSQPQuery);
        globalQueryPlan->addQueryPlan(queryPlanNewQuery);

        //execute
        auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedQueryContainmentRule::create(context);
        ASSERT_FALSE(signatureBasedEqualQueryMergerRule->apply(globalQueryPlan));
    }
}
