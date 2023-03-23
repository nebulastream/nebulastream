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
#include <Util/magicenum/magic_enum.hpp>
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
    std::vector<std::tuple<std::tuple<Query, Query>, NES::Optimizer::ContainmentType>> containmentCasesMixed = {
        //Equal
        /*{std::tuple<Query, Query>(Query::from("car")
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
         Optimizer::ContainmentType::EQUALITY},
        //Sig2 contains Sig1
        {std::tuple<Query, Query>(
             Query::from("car").map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(printSinkDescriptor),
             Query::from("car").map(Attribute("value") = 40).filter(Attribute("id") < 60).sink(printSinkDescriptor)),
         Optimizer::ContainmentType::LEFT_SIG_CONTAINED},
        //Sig1 contains Sig2
        {std::tuple<Query, Query>(
             Query::from("car").map(Attribute("value") = 40).filter(Attribute("id") < 60).sink(printSinkDescriptor),
             Query::from("car").map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(printSinkDescriptor)),
         Optimizer::ContainmentType::RIGHT_SIG_CONTAINED},
        //Sig2 contains Sig1 but containment cannot be detected
        {std::tuple<Query, Query>(
             Query::from("car").map(Attribute("value") = 40).project(Attribute("value").as("newValue")).sink(printSinkDescriptor),
             Query::from("car").map(Attribute("value") = 40).sink(printSinkDescriptor)),
         Optimizer::NO_CONTAINMENT},
        //Sig2 contains Sig1 //todo: left sig contains right sig is wrong need to change signature creation for this to work properly
        {std::tuple<Query, Query>(
             Query::from("car").map(Attribute("value") = 40).project(Attribute("value")).sink(printSinkDescriptor),
             Query::from("car").map(Attribute("value") = 40).sink(printSinkDescriptor)),
         Optimizer::LEFT_SIG_CONTAINED},*/
        //Sig1 contains Sig2 //todo: left sig contains right sig is wrong need to change signature creation for this to work properly
        /*{std::tuple<Query, Query>(
             Query::from("car").map(Attribute("value") = 40).sink(printSinkDescriptor),
             Query::from("car").map(Attribute("value") = 40).project(Attribute("value")).sink(printSinkDescriptor)),
         Optimizer::ContainmentType::RIGHT_SIG_CONTAINED},*/
        //No containment due to different transformations
        /*{std::tuple<Query, Query>(Query::from("car")
                                      .map(Attribute("value") = 40)
                                      .map(Attribute("value") = Attribute("value") + 10)
                                      .sink(printSinkDescriptor),
                                  Query::from("car").map(Attribute("value") = 40).sink(printSinkDescriptor)),
         Optimizer::ContainmentType::NO_CONTAINMENT},
        //No containment due to different transformations
        {std::tuple<Query, Query>(Query::from("car")
                                      .window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10)))
                                      .apply(Sum(Attribute("value1")))
                                      .sink(printSinkDescriptor),
                                  Query::from("car")
                                      .window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10)))
                                      .apply(Sum(Attribute("value1")))
                                      .sink(printSinkDescriptor)),
         Optimizer::ContainmentType::EQUALITY},
        {std::tuple<Query, Query>(Query::from("car")
                                      .window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(20)))
                                      .apply(Sum(Attribute("value1")))
                                      .sink(printSinkDescriptor),
                                  Query::from("car")
                                      .window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10)))
                                      .apply(Sum(Attribute("value1")))
                                      .sink(printSinkDescriptor)),
         Optimizer::ContainmentType::RIGHT_SIG_CONTAINED},
        {std::tuple<Query, Query>(Query::from("car")
                                      .window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10)))
                                      .apply(Sum(Attribute("value1")))
                                      .sink(printSinkDescriptor),
                                  Query::from("car")
                                      .window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(20)))
                                      .apply(Sum(Attribute("value1")))
                                      .sink(printSinkDescriptor)),
         Optimizer::ContainmentType::LEFT_SIG_CONTAINED},
        {std::tuple<Query, Query>(Query::from("car")
                                      .filter(Attribute("value") < 40)
                                      .window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(20)))
                                      .apply(Sum(Attribute("value1")))
                                      .sink(printSinkDescriptor),
                                  Query::from("car")
                                      .window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(10)))
                                      .apply(Sum(Attribute("value1")))
                                      .sink(printSinkDescriptor)),
         Optimizer::ContainmentType::NO_CONTAINMENT},
        {std::tuple<Query, Query>(Query::from("car")
                                      .joinWith(Query::from("bike"))
                                      .where(Attribute("id1"))
                                      .equalsTo(Attribute("id"))
                                      .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                      .sink(printSinkDescriptor),
                                  Query::from("car")
                                      .joinWith(Query::from("bike"))
                                      .where(Attribute("id1"))
                                      .equalsTo(Attribute("id"))
                                      .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                      .sink(printSinkDescriptor)),
         Optimizer::ContainmentType::EQUALITY},
        {std::tuple<Query, Query>(Query::from("car")
                                      .joinWith(Query::from("bike"))
                                      .where(Attribute("id1"))
                                      .equalsTo(Attribute("id"))
                                      .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(10)))
                                      .sink(printSinkDescriptor),
                                  Query::from("car")
                                      .joinWith(Query::from("bike"))
                                      .where(Attribute("id1"))
                                      .equalsTo(Attribute("id"))
                                      .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                      .sink(printSinkDescriptor)),
         Optimizer::ContainmentType::LEFT_SIG_CONTAINED},
        {std::tuple<Query, Query>(Query::from("car")
                                      .joinWith(Query::from("bike"))
                                      .where(Attribute("id1"))
                                      .equalsTo(Attribute("id"))
                                      .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                      .sink(printSinkDescriptor),
                                  Query::from("car")
                                      .joinWith(Query::from("bike"))
                                      .where(Attribute("id1"))
                                      .equalsTo(Attribute("id"))
                                      .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(20)))
                                      .sink(printSinkDescriptor)),
         Optimizer::ContainmentType::RIGHT_SIG_CONTAINED},
        {std::tuple<Query, Query>(Query::from("car")
                                      .joinWith(Query::from("bike"))
                                      .where(Attribute("id1"))
                                      .equalsTo(Attribute("id"))
                                      .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                      .sink(printSinkDescriptor),
                                  Query::from("car")
                                      .joinWith(Query::from("bike"))
                                      .where(Attribute("id1"))
                                      .equalsTo(Attribute("id"))
                                      .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(20)))
                                      .filter(Attribute("value") < 10)
                                      .sink(printSinkDescriptor)),
         Optimizer::ContainmentType::RIGHT_SIG_CONTAINED},
        {std::tuple<Query, Query>(Query::from("car")
                                      .joinWith(Query::from("bike"))
                                      .where(Attribute("id1"))
                                      .equalsTo(Attribute("id"))
                                      .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                      .sink(printSinkDescriptor),
                                  Query::from("car")
                                      .joinWith(Query::from("bike"))
                                      .where(Attribute("id1"))
                                      .equalsTo(Attribute("id"))
                                      .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                      .filter(Attribute("value") < 5)
                                      .sink(printSinkDescriptor)),
         Optimizer::ContainmentType::RIGHT_SIG_CONTAINED},*/
        {std::tuple<Query, Query>(Query::from("car")
                                      .filter(Attribute("value") < 5)
                                      .joinWith(Query::from("bike"))
                                      .where(Attribute("id1"))
                                      .equalsTo(Attribute("id"))
                                      .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                      .sink(printSinkDescriptor),
                                  Query::from("car")
                                      .joinWith(Query::from("bike"))
                                      .where(Attribute("id1"))
                                      .equalsTo(Attribute("id"))
                                      .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(20)))
                                      .sink(printSinkDescriptor)),
         Optimizer::ContainmentType::NO_CONTAINMENT},
        {std::tuple<Query, Query>(Query::from("car")
                                      .filter(Attribute("value") < 5)
                                      .joinWith(Query::from("bike"))
                                      .where(Attribute("id1"))
                                      .equalsTo(Attribute("id"))
                                      .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                      .sink(printSinkDescriptor),
                                  Query::from("car")
                                      .joinWith(Query::from("bike"))
                                      .where(Attribute("id1"))
                                      .equalsTo(Attribute("id"))
                                      .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                      .sink(printSinkDescriptor)),
         Optimizer::LEFT_SIG_CONTAINED},
        //
        {std::tuple<Query, Query>(Query::from("car")
                                      .filter(Attribute("value") < 5)
                                      .joinWith(Query::from("bike"))
                                      .where(Attribute("id1"))
                                      .equalsTo(Attribute("id"))
                                      .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                      .sink(printSinkDescriptor),
                                  Query::from("car")
                                      .joinWith(Query::from("bike"))
                                      .where(Attribute("id1"))
                                      .equalsTo(Attribute("id"))
                                      .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                      .filter(Attribute("value") < 5)
                                      .sink(printSinkDescriptor)),
         Optimizer::NO_CONTAINMENT},
        //
        {std::tuple<Query, Query>(Query::from("car")
                                      .joinWith(Query::from("bike"))
                                      .where(Attribute("id1"))
                                      .equalsTo(Attribute("id"))
                                      .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000)))
                                      .sink(printSinkDescriptor),
                                  Query::from("car")
                                      .joinWith(Query::from("bike"))
                                      .where(Attribute("id1"))
                                      .equalsTo(Attribute("id"))
                                      .window(SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000), Milliseconds(1000)))
                                      .sink(printSinkDescriptor)),
         Optimizer::EQUALITY}*/
        {std::tuple<Query, Query>(Query::from("car")
                                      .window(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000))).apply(Sum(Attribute("value")))
                                      .sink(printSinkDescriptor),
                                  Query::from("car")
                                      .window(SlidingWindow::of(EventTime(Attribute("ts")), Milliseconds(1000), Milliseconds(10))).byKey(Attribute("id")).apply(Sum(Attribute("value")))
                                      .sink(printSinkDescriptor)),
         Optimizer::EQUALITY}};

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
    uint16_t counter = 0;
    for (auto entry : containmentCasesMixed) {
        auto queries = get<0>(entry);
        QueryPlanPtr queryPlanSQPQuery = get<0>(queries).getQueryPlan();
        QueryPlanPtr queryPlanNewQuery = get<1>(queries).getQueryPlan();
        //typ inference face
        auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
        typeInferencePhase->execute(queryPlanSQPQuery);
        typeInferencePhase->execute(queryPlanNewQuery);
        //obtain context and create signatures
        z3::ContextPtr context = std::make_shared<z3::context>();
        auto signatureInferencePhase =
            Optimizer::SignatureInferencePhase::create(context, Optimizer::QueryMergerRule::Z3SignatureBasedQueryContainmentRule);
        signatureInferencePhase->execute(queryPlanSQPQuery);
        signatureInferencePhase->execute(queryPlanNewQuery);
        SinkLogicalOperatorNodePtr sinkOperatorSQPQuery = queryPlanSQPQuery->getSinkOperators()[0];
        SinkLogicalOperatorNodePtr sinkOperatorNewQuery = queryPlanSQPQuery->getSinkOperators()[0];
        auto signatureContainmentUtil = Optimizer::SignatureContainmentUtil::create(context);
        std::map<OperatorNodePtr, OperatorNodePtr> targetToHostSinkOperatorMap;
        auto sqpSink = queryPlanSQPQuery->getSinkOperators()[0];
        auto newSink = queryPlanNewQuery->getSinkOperators()[0];
        //Check if the host and target sink operator signatures have a containment relationship
        Optimizer::ContainmentType containment =
            signatureContainmentUtil->checkContainment(sqpSink->getZ3Signature(), newSink->getZ3Signature());
        NES_TRACE("Z3SignatureBasedContainmentBasedCompleteQueryMergerRule: containment: " << magic_enum::enum_name(containment));
        NES_TRACE("Query pairing number: " << counter);
        ASSERT_EQ(containment, get<1>(entry));
        counter++;
    }
}
