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
#include <Catalogs/UDF/UdfCatalog.hpp>
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

class QueryContainmentIdentificationTest : public Testing::TestWithErrorHandling<testing::Test> {

  public:
    SchemaPtr schema;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    std::shared_ptr<Catalogs::UDF::UdfCatalog> udfCatalog;
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    std::vector<std::tuple<std::tuple<Query, Query>, NES::Optimizer::ContainmentDetected>> containmentCasesMixed = {
        //Equal
        {std::tuple<Query, Query>(Query::from("car")
                                      .map(Attribute("value") = 40)
                                      .filter(Attribute("id") < 45)
                                      .filter(Attribute("id") < 45)
                                      .filter(Attribute("id") < 45)
                                      .filter(Attribute("id") < 45)
                                      .sink(printSinkDescriptor),
                                  Query::from("car")
                                      .map(Attribute("value") = 40)
                                      .filter(Attribute("id") < 45)
                                      .filter(Attribute("id") < 45)
                                      .filter(Attribute("id") < 45)
                                      .filter(Attribute("id") < 45)
                                      .sink(printSinkDescriptor)),
         NES::Optimizer::EQUALITY},
        //Sig2 contains Sig1
        {std::tuple<Query, Query>(
             Query::from("car").map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(printSinkDescriptor),
             Query::from("car").map(Attribute("value") = 40).filter(Attribute("id") < 60).sink(printSinkDescriptor)),
         Optimizer::LEFT_SIG_CONTAINED},
        //Sig1 contains Sig2
        {std::tuple<Query, Query>(
             Query::from("car").map(Attribute("value") = 40).filter(Attribute("id") < 60).sink(printSinkDescriptor),
             Query::from("car").map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(printSinkDescriptor)),
         Optimizer::RIGHT_SIG_CONTAINED},
        //Sig2 contains Sig1 but containment cannot be detected
        {std::tuple<Query, Query>(
             Query::from("car").map(Attribute("value") = 40).project(Attribute("value").as("newValue")).sink(printSinkDescriptor),
             Query::from("car").map(Attribute("value") = 40).sink(printSinkDescriptor)),
         Optimizer::NO_CONTAINMENT},
        //Sig2 contains Sig1
        {std::tuple<Query, Query>(
             Query::from("car").map(Attribute("value") = 40).project(Attribute("value")).sink(printSinkDescriptor),
             Query::from("car").map(Attribute("value") = 40).sink(printSinkDescriptor)),
         Optimizer::LEFT_SIG_CONTAINED},
        //No containment due to different transformations
        {std::tuple<Query, Query>(Query::from("car")
                                      .map(Attribute("value") = 40)
                                      .map(Attribute("value") = Attribute("value") + 10)
                                      .sink(printSinkDescriptor),
                                  Query::from("car").map(Attribute("value") = 40).sink(printSinkDescriptor)),
         Optimizer::NO_CONTAINMENT}};

    /* Will be called before all tests in this class are started. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("Z3SignatureBasedCompleteQueryMergerRuleTest.log", NES::LogLevel::LOG_DEBUG);
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
                     ->addField("value1", BasicType::UINT64);
        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
        sourceCatalog->addLogicalSource("car", schema);
        sourceCatalog->addLogicalSource("bike", schema);
        sourceCatalog->addLogicalSource("truck", schema);
        udfCatalog = Catalogs::UDF::UdfCatalog::create();
    }
};

//Todo: #3493 more test cases needed: different schema
/**
 * @brief tests if the correct containment relationship is returned by the signature containment util
 */
TEST_F(QueryContainmentIdentificationTest, testContainmentIdentification) {
    std::vector<Optimizer::ContainmentDetected> resultList;
    for (auto entry : containmentCasesMixed) {
        auto queries = get<0>(entry);
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
        auto signatureContainmentUtil = Optimizer::SignatureContainmentUtil::create(context);
        std::vector<QueryPlanPtr> queryPlansToAdd = globalQueryPlan->getQueryPlansToAdd();

        NES_DEBUG(
            "Z3SignatureBasedContainmentBasedCompleteQueryMergerRule: Iterating over all Shared Query MetaData in the Global "
            "Query Plan");
        Optimizer::ContainmentDetected containment = NES::Optimizer::NO_CONTAINMENT;
        //Iterate over all shared query metadata to identify equal shared metadata
        for (const auto& targetQueryPlan : queryPlansToAdd) {
            bool matched = false;
            auto hostSharedQueryPlans =
                globalQueryPlan->getSharedQueryPlansConsumingSources(targetQueryPlan->getSourceConsumed());
            for (auto& hostSharedQueryPlan : hostSharedQueryPlans) {
                auto hostQueryPlan = hostSharedQueryPlan->getQueryPlan();
                // Prepare a map of matching address and target sink global query nodes
                // if there are no matching global query nodes then the shared query metadata are not matched
                std::map<OperatorNodePtr, OperatorNodePtr> targetToHostSinkOperatorMap;
                auto targetSink = targetQueryPlan->getSinkOperators()[0];
                auto hostSink = hostQueryPlan->getSinkOperators()[0];

                //Check if the host and target sink operator signatures match each other
                containment =
                    signatureContainmentUtil->checkContainment(hostSink->getZ3Signature(), targetSink->getZ3Signature());
                NES_TRACE("Z3SignatureBasedContainmentBasedCompleteQueryMergerRule: containment: " << containment);
                if (containment != NES::Optimizer::NO_CONTAINMENT) {
                    targetToHostSinkOperatorMap[targetSink] = hostSink;
                    matched = true;
                }
            }
            if (!matched) {
                NES_DEBUG("Z3SignatureBasedCompleteQueryMergerRule: computing a new Shared Query Plan");
                globalQueryPlan->createNewSharedQueryPlan(targetQueryPlan);
            }
        }
        ASSERT_EQ(containment, get<1>(entry));
    }
}
