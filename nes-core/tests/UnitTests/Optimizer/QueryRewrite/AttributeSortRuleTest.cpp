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

// clang-format off
#include <gtest/gtest.h>
// clang-format on
#include <API/QueryAPI.hpp>
#include <Catalogs/SourceCatalog.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/QueryRewrite/AttributeSortRule.hpp>
#include <Optimizer/QueryRewrite/FilterPushDownRule.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <iostream>

using namespace NES;

class AttributeSortRuleTest : public testing::Test {

  public:
    SchemaPtr schema;

    /* Will be called before all tests in this class are started. */
    static void SetUpTestCase() {
        NES::setupLogging("AttributeSortRuleTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup AttributeSortRuleTest test case.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override { schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64); }

    /* Will be called before a test is executed. */
    void TearDown() override { NES_INFO("Setup AttributeSortRuleTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down FilterPushDownTest test class."); }
};

TEST_F(AttributeSortRuleTest, testAttributeSortRuleForMapOperator1) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("src").map(Attribute("b") = Attribute("b") + Attribute("a")).sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto attributeSortRule = Optimizer::AttributeSortRule::create();

    attributeSortRule->apply(queryPlan);

    auto signatureInferencePhase =
        Optimizer::SignatureInferencePhase::create(/*Z3context*/ nullptr,
                                                   Optimizer::QueryMergerRule::HashSignatureBasedCompleteQueryMergerRule);
    signatureInferencePhase->execute(queryPlan);

    auto rootOperators = queryPlan->getRootOperators();
    EXPECT_TRUE(rootOperators.size() == 1);
    const auto* expectedSignature =
        "SINK().MAP(FieldAccessNode(b[Undefined])=FieldAccessNode(a[Undefined])+FieldAccessNode(b[Undefined])).SOURCE(src)";
    auto actualSignature = *rootOperators[0]->as<LogicalOperatorNode>()->getHashBasedSignature().begin()->second.begin();
    EXPECT_EQ(expectedSignature, actualSignature);
}

TEST_F(AttributeSortRuleTest, testAttributeSortRuleForMapOperator2) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query =
        Query::from("src").map(Attribute("b") = Attribute("c") + Attribute("b") + Attribute("a")).sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto attributeSortRule = Optimizer::AttributeSortRule::create();

    attributeSortRule->apply(queryPlan);

    auto signatureInferencePhase =
        Optimizer::SignatureInferencePhase::create(/*Z3context*/ nullptr,
                                                   Optimizer::QueryMergerRule::HashSignatureBasedCompleteQueryMergerRule);
    signatureInferencePhase->execute(queryPlan);

    auto rootOperators = queryPlan->getRootOperators();
    EXPECT_TRUE(rootOperators.size() == 1);
    const auto* expectedSignature =
        "SINK().MAP(FieldAccessNode(b[Undefined])=FieldAccessNode(a[Undefined])+FieldAccessNode(b[Undefined]"
        ")+FieldAccessNode(c[Undefined])).SOURCE(src)";
    auto actualSignature = *rootOperators[0]->as<LogicalOperatorNode>()->getHashBasedSignature().begin()->second.begin();
    EXPECT_EQ(expectedSignature, actualSignature);
}

TEST_F(AttributeSortRuleTest, testAttributeSortRuleForMapOperator3) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query =
        Query::from("src")
            .map(Attribute("b") = (Attribute("d") + Attribute("a")) + ((Attribute("c") + Attribute("b")) + Attribute("a")))
            .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto attributeSortRule = Optimizer::AttributeSortRule::create();

    attributeSortRule->apply(queryPlan);

    auto signatureInferencePhase =
        Optimizer::SignatureInferencePhase::create(/*Z3context*/ nullptr,
                                                   Optimizer::QueryMergerRule::HashSignatureBasedCompleteQueryMergerRule);
    signatureInferencePhase->execute(queryPlan);

    auto rootOperators = queryPlan->getRootOperators();
    EXPECT_TRUE(rootOperators.size() == 1);
    const auto* expectedSignature =
        "SINK().MAP(FieldAccessNode(b[Undefined])=FieldAccessNode(a[Undefined])+FieldAccessNode(a[Undefined]"
        ")+FieldAccessNode(b[Undefined])+FieldAccessNode(c[Undefined])+FieldAccessNode(d[Undefined])).SOURCE(src)";
    auto actualSignature = *rootOperators[0]->as<LogicalOperatorNode>()->getHashBasedSignature().begin()->second.begin();
    EXPECT_EQ(expectedSignature, actualSignature);
}

TEST_F(AttributeSortRuleTest, testAttributeSortRuleForMapOperator4) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("src")
                      .map(Attribute("b") = (Attribute("d") + Attribute("c")) * (Attribute("a") + Attribute("b")))
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto attributeSortRule = Optimizer::AttributeSortRule::create();

    attributeSortRule->apply(queryPlan);

    auto signatureInferencePhase =
        Optimizer::SignatureInferencePhase::create(/*Z3context*/ nullptr,
                                                   Optimizer::QueryMergerRule::HashSignatureBasedCompleteQueryMergerRule);
    signatureInferencePhase->execute(queryPlan);

    auto rootOperators = queryPlan->getRootOperators();
    EXPECT_TRUE(rootOperators.size() == 1);
    const auto* expectedSignature =
        "SINK().MAP(FieldAccessNode(b[Undefined])=FieldAccessNode(a[Undefined])+FieldAccessNode(b[Undefined]"
        ")*FieldAccessNode(c[Undefined])+FieldAccessNode(d[Undefined])).SOURCE(src)";
    auto actualSignature = *rootOperators[0]->as<LogicalOperatorNode>()->getHashBasedSignature().begin()->second.begin();
    EXPECT_EQ(expectedSignature, actualSignature);
}

TEST_F(AttributeSortRuleTest, testAttributeSortRuleForMapOperator5) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("src")
                      .map(Attribute("b") = Attribute("d") + Attribute("a") > Attribute("c") + Attribute("a"))
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto attributeSortRule = Optimizer::AttributeSortRule::create();

    attributeSortRule->apply(queryPlan);

    auto signatureInferencePhase =
        Optimizer::SignatureInferencePhase::create(/*Z3context*/ nullptr,
                                                   Optimizer::QueryMergerRule::HashSignatureBasedCompleteQueryMergerRule);
    signatureInferencePhase->execute(queryPlan);

    auto rootOperators = queryPlan->getRootOperators();
    EXPECT_TRUE(rootOperators.size() == 1);
    const auto* expectedSignature =
        "SINK().MAP(FieldAccessNode(b[Undefined])=FieldAccessNode(a[Undefined])+FieldAccessNode(d[Undefined]"
        ")>FieldAccessNode(a[Undefined])+FieldAccessNode(c[Undefined])).SOURCE(src)";
    auto actualSignature = *rootOperators[0]->as<LogicalOperatorNode>()->getHashBasedSignature().begin()->second.begin();
    EXPECT_EQ(expectedSignature, actualSignature);
}

TEST_F(AttributeSortRuleTest, testAttributeSortRuleForMapOperator6) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("src")
                      .map(Attribute("b") = Attribute("d") + Attribute("c") > Attribute("c") + Attribute("a"))
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto attributeSortRule = Optimizer::AttributeSortRule::create();

    attributeSortRule->apply(queryPlan);

    auto signatureInferencePhase =
        Optimizer::SignatureInferencePhase::create(/*Z3context*/ nullptr,
                                                   Optimizer::QueryMergerRule::HashSignatureBasedCompleteQueryMergerRule);
    signatureInferencePhase->execute(queryPlan);

    auto rootOperators = queryPlan->getRootOperators();
    EXPECT_TRUE(rootOperators.size() == 1);
    const auto* expectedSignature =
        "SINK().MAP(FieldAccessNode(b[Undefined])=FieldAccessNode(a[Undefined])+FieldAccessNode(c[Undefined]"
        ")<FieldAccessNode(c[Undefined])+FieldAccessNode(d[Undefined])).SOURCE(src)";
    auto actualSignature = *rootOperators[0]->as<LogicalOperatorNode>()->getHashBasedSignature().begin()->second.begin();
    EXPECT_EQ(expectedSignature, actualSignature);
}

TEST_F(AttributeSortRuleTest, testAttributeSortRuleForMapOperator7) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query =
        Query::from("src")
            .map(Attribute("b") = ((Attribute("d") + Attribute("c")) * Attribute("a")) > Attribute("c") + Attribute("a"))
            .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto attributeSortRule = Optimizer::AttributeSortRule::create();

    attributeSortRule->apply(queryPlan);

    auto signatureInferencePhase =
        Optimizer::SignatureInferencePhase::create(/*Z3context*/ nullptr,
                                                   Optimizer::QueryMergerRule::HashSignatureBasedCompleteQueryMergerRule);
    signatureInferencePhase->execute(queryPlan);

    auto rootOperators = queryPlan->getRootOperators();
    EXPECT_TRUE(rootOperators.size() == 1);
    const auto* expectedSignature =
        "SINK().MAP(FieldAccessNode(b[Undefined])=FieldAccessNode(a[Undefined])*FieldAccessNode(c[Undefined]"
        ")+FieldAccessNode(d[Undefined]"
        ")>FieldAccessNode(a[Undefined])+FieldAccessNode(c[Undefined])).SOURCE(src)";
    auto actualSignature = *rootOperators[0]->as<LogicalOperatorNode>()->getHashBasedSignature().begin()->second.begin();
    EXPECT_EQ(expectedSignature, actualSignature);
}

TEST_F(AttributeSortRuleTest, testAttributeSortRuleForMapOperator8) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query =
        Query::from("src")
            .map(Attribute("b") = ((Attribute("d") + Attribute("c")) * Attribute("d")) < Attribute("c") + Attribute("a"))
            .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto attributeSortRule = Optimizer::AttributeSortRule::create();
    attributeSortRule->apply(queryPlan);

    auto signatureInferencePhase =
        Optimizer::SignatureInferencePhase::create(/*Z3context*/ nullptr,
                                                   Optimizer::QueryMergerRule::HashSignatureBasedCompleteQueryMergerRule);
    signatureInferencePhase->execute(queryPlan);

    auto rootOperators = queryPlan->getRootOperators();
    EXPECT_TRUE(rootOperators.size() == 1);
    const auto* expectedSignature =
        "SINK().MAP(FieldAccessNode(b[Undefined])=FieldAccessNode(a[Undefined])+FieldAccessNode(c[Undefined])>"
        "FieldAccessNode(c[Undefined])+FieldAccessNode(d[Undefined])*FieldAccessNode(d[Undefined])).SOURCE(src)";
    auto actualSignature = *rootOperators[0]->as<LogicalOperatorNode>()->getHashBasedSignature().begin()->second.begin();
    EXPECT_EQ(expectedSignature, actualSignature);
}

TEST_F(AttributeSortRuleTest, testAttributeSortRuleForMapOperator9) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query =
        Query::from("src")
            .map(Attribute("b") = ((Attribute("d") + Attribute("c")) * Attribute("d")) <= Attribute("c") + Attribute("a"))
            .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto attributeSortRule = Optimizer::AttributeSortRule::create();
    attributeSortRule->apply(queryPlan);

    auto signatureInferencePhase =
        Optimizer::SignatureInferencePhase::create(/*Z3context*/ nullptr,
                                                   Optimizer::QueryMergerRule::HashSignatureBasedCompleteQueryMergerRule);
    signatureInferencePhase->execute(queryPlan);

    auto rootOperators = queryPlan->getRootOperators();
    EXPECT_TRUE(rootOperators.size() == 1);
    const auto* expectedSignature =
        "SINK().MAP(FieldAccessNode(b[Undefined])=FieldAccessNode(a[Undefined])+FieldAccessNode(c[Undefined])>="
        "FieldAccessNode(c[Undefined])+FieldAccessNode(d[Undefined])*FieldAccessNode(d[Undefined])).SOURCE(src)";
    auto actualSignature = *rootOperators[0]->as<LogicalOperatorNode>()->getHashBasedSignature().begin()->second.begin();
    EXPECT_EQ(expectedSignature, actualSignature);
}

TEST_F(AttributeSortRuleTest, testAttributeSortRuleForMapOperator10) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query =
        Query::from("src")
            .map(Attribute("b") = ((Attribute("d") + Attribute("c")) * Attribute("d")) == Attribute("c") + Attribute("a"))
            .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto attributeSortRule = Optimizer::AttributeSortRule::create();
    attributeSortRule->apply(queryPlan);

    auto signatureInferencePhase =
        Optimizer::SignatureInferencePhase::create(/*Z3context*/ nullptr,
                                                   Optimizer::QueryMergerRule::HashSignatureBasedCompleteQueryMergerRule);
    signatureInferencePhase->execute(queryPlan);

    auto rootOperators = queryPlan->getRootOperators();
    EXPECT_TRUE(rootOperators.size() == 1);
    const auto* expectedSignature =
        "SINK().MAP(FieldAccessNode(b[Undefined])=FieldAccessNode(a[Undefined])+FieldAccessNode(c[Undefined])=="
        "FieldAccessNode(c[Undefined])+FieldAccessNode(d[Undefined])*FieldAccessNode(d[Undefined])).SOURCE(src)";
    auto actualSignature = *rootOperators[0]->as<LogicalOperatorNode>()->getHashBasedSignature().begin()->second.begin();
    EXPECT_EQ(expectedSignature, actualSignature);
}

TEST_F(AttributeSortRuleTest, testAttributeSortRuleForMapOperator11) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query =
        Query::from("src")
            .map(Attribute("b") = ((Attribute("d") + Attribute("c")) * Attribute("d")) >= Attribute("c") + Attribute("a"))
            .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto attributeSortRule = Optimizer::AttributeSortRule::create();
    attributeSortRule->apply(queryPlan);

    auto signatureInferencePhase =
        Optimizer::SignatureInferencePhase::create(/*Z3context*/ nullptr,
                                                   Optimizer::QueryMergerRule::HashSignatureBasedCompleteQueryMergerRule);
    signatureInferencePhase->execute(queryPlan);

    auto rootOperators = queryPlan->getRootOperators();
    EXPECT_TRUE(rootOperators.size() == 1);
    const auto* expectedSignature =
        "SINK().MAP(FieldAccessNode(b[Undefined])=FieldAccessNode(a[Undefined])+FieldAccessNode(c[Undefined])<="
        "FieldAccessNode(c[Undefined])+FieldAccessNode(d[Undefined])*FieldAccessNode(d[Undefined])).SOURCE(src)";
    auto actualSignature = *rootOperators[0]->as<LogicalOperatorNode>()->getHashBasedSignature().begin()->second.begin();
    EXPECT_EQ(expectedSignature, actualSignature);
}

TEST_F(AttributeSortRuleTest, testAttributeSortRuleForMapOperator12) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query =
        Query::from("src")
            .map(Attribute("b") = ((Attribute("d") + Attribute("c")) * Attribute("d")) && Attribute("c") + Attribute("a"))
            .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto attributeSortRule = Optimizer::AttributeSortRule::create();
    attributeSortRule->apply(queryPlan);

    auto signatureInferencePhase =
        Optimizer::SignatureInferencePhase::create(/*Z3context*/ nullptr,
                                                   Optimizer::QueryMergerRule::HashSignatureBasedCompleteQueryMergerRule);
    signatureInferencePhase->execute(queryPlan);

    auto rootOperators = queryPlan->getRootOperators();
    EXPECT_TRUE(rootOperators.size() == 1);
    const auto* expectedSignature =
        "SINK().MAP(FieldAccessNode(b[Undefined])=FieldAccessNode(a[Undefined])+FieldAccessNode(c[Undefined])&&"
        "FieldAccessNode(c[Undefined])+FieldAccessNode(d[Undefined])*FieldAccessNode(d[Undefined])).SOURCE(src)";
    auto actualSignature = *rootOperators[0]->as<LogicalOperatorNode>()->getHashBasedSignature().begin()->second.begin();
    EXPECT_EQ(expectedSignature, actualSignature);
}

TEST_F(AttributeSortRuleTest, testAttributeSortRuleForMapOperator13) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query =
        Query::from("src")
            .map(Attribute("b") = ((Attribute("d") + Attribute("c")) * Attribute("d")) || Attribute("c") + Attribute("a"))
            .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto attributeSortRule = Optimizer::AttributeSortRule::create();
    attributeSortRule->apply(queryPlan);

    auto signatureInferencePhase =
        Optimizer::SignatureInferencePhase::create(/*Z3context*/ nullptr,
                                                   Optimizer::QueryMergerRule::HashSignatureBasedCompleteQueryMergerRule);
    signatureInferencePhase->execute(queryPlan);

    auto rootOperators = queryPlan->getRootOperators();
    EXPECT_TRUE(rootOperators.size() == 1);
    const auto* expectedSignature =
        "SINK().MAP(FieldAccessNode(b[Undefined])=FieldAccessNode(a[Undefined])+FieldAccessNode(c[Undefined])||"
        "FieldAccessNode(c[Undefined])+FieldAccessNode(d[Undefined])*FieldAccessNode(d[Undefined])).SOURCE(src)";
    auto actualSignature = *rootOperators[0]->as<LogicalOperatorNode>()->getHashBasedSignature().begin()->second.begin();
    EXPECT_EQ(expectedSignature, actualSignature);
}

TEST_F(AttributeSortRuleTest, testAttributeSortRuleForMapOperator14) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query =
        Query::from("src")
            .map(Attribute("b") = !(((Attribute("d") + Attribute("c")) * Attribute("d")) && Attribute("c") + Attribute("a")))
            .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto attributeSortRule = Optimizer::AttributeSortRule::create();
    attributeSortRule->apply(queryPlan);

    auto signatureInferencePhase =
        Optimizer::SignatureInferencePhase::create(/*Z3context*/ nullptr,
                                                   Optimizer::QueryMergerRule::HashSignatureBasedCompleteQueryMergerRule);
    signatureInferencePhase->execute(queryPlan);

    auto rootOperators = queryPlan->getRootOperators();
    EXPECT_TRUE(rootOperators.size() == 1);
    const auto* expectedSignature =
        "SINK().MAP(FieldAccessNode(b[Undefined])=!FieldAccessNode(a[Undefined])+FieldAccessNode(c[Undefined])&&"
        "FieldAccessNode(c[Undefined])+FieldAccessNode(d[Undefined])*FieldAccessNode(d[Undefined])).SOURCE(src)";
    auto actualSignature = *rootOperators[0]->as<LogicalOperatorNode>()->getHashBasedSignature().begin()->second.begin();
    EXPECT_EQ(expectedSignature, actualSignature);
}

TEST_F(AttributeSortRuleTest, testAttributeSortRuleForMapOperator15) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("src").map(Attribute("b") = 10 + Attribute("a")).sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto attributeSortRule = Optimizer::AttributeSortRule::create();
    attributeSortRule->apply(queryPlan);

    auto signatureInferencePhase =
        Optimizer::SignatureInferencePhase::create(/*Z3context*/ nullptr,
                                                   Optimizer::QueryMergerRule::HashSignatureBasedCompleteQueryMergerRule);
    signatureInferencePhase->execute(queryPlan);

    auto rootOperators = queryPlan->getRootOperators();
    EXPECT_TRUE(rootOperators.size() == 1);
    const auto* expectedSignature =
        "SINK().MAP(FieldAccessNode(b[Undefined])=ConstantValue(BasicValue(10))+FieldAccessNode(a[Undefined])).SOURCE(src)";
    auto actualSignature = *rootOperators[0]->as<LogicalOperatorNode>()->getHashBasedSignature().begin()->second.begin();
    EXPECT_EQ(expectedSignature, actualSignature);
}

TEST_F(AttributeSortRuleTest, testAttributeSortRuleForMapOperator16) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("src")
                      .map(Attribute("b") = (10 + Attribute("c")) * ((Attribute("a") + Attribute("b"))))
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto attributeSortRule = Optimizer::AttributeSortRule::create();
    attributeSortRule->apply(queryPlan);

    auto signatureInferencePhase =
        Optimizer::SignatureInferencePhase::create(/*Z3context*/ nullptr,
                                                   Optimizer::QueryMergerRule::HashSignatureBasedCompleteQueryMergerRule);
    signatureInferencePhase->execute(queryPlan);

    auto rootOperators = queryPlan->getRootOperators();
    EXPECT_TRUE(rootOperators.size() == 1);
    const auto* expectedSignature =
        "SINK().MAP(FieldAccessNode(b[Undefined])=ConstantValue(BasicValue(10))+FieldAccessNode(c[Undefined]"
        ")*FieldAccessNode(a[Undefined])+FieldAccessNode(b[Undefined])).SOURCE(src)";
    auto actualSignature = *rootOperators[0]->as<LogicalOperatorNode>()->getHashBasedSignature().begin()->second.begin();
    EXPECT_EQ(expectedSignature, actualSignature);
}

TEST_F(AttributeSortRuleTest, testAttributeSortRuleForMapOperator17) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("src")
                      .map(Attribute("b") = (100 + Attribute("c") + 10) + (Attribute("b") + Attribute("a")))
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto attributeSortRule = Optimizer::AttributeSortRule::create();
    attributeSortRule->apply(queryPlan);

    auto signatureInferencePhase =
        Optimizer::SignatureInferencePhase::create(/*Z3context*/ nullptr,
                                                   Optimizer::QueryMergerRule::HashSignatureBasedCompleteQueryMergerRule);
    signatureInferencePhase->execute(queryPlan);

    auto rootOperators = queryPlan->getRootOperators();
    EXPECT_TRUE(rootOperators.size() == 1);
    const auto* expectedSignature =
        "SINK().MAP(FieldAccessNode(b[Undefined])=ConstantValue(BasicValue(10))+ConstantValue(BasicValue(100))+FieldAccessNode(a["
        "Undefined])+FieldAccessNode(b[Undefined])+FieldAccessNode(c[Undefined])).SOURCE(src)";
    auto actualSignature = *rootOperators[0]->as<LogicalOperatorNode>()->getHashBasedSignature().begin()->second.begin();
    EXPECT_EQ(expectedSignature, actualSignature);
}

TEST_F(AttributeSortRuleTest, testAttributeSortRuleFilterOperator1) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("src")
                      .filter(((Attribute("d") + Attribute("c")) * Attribute("d")) > Attribute("c") + Attribute("a"))
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto attributeSortRule = Optimizer::AttributeSortRule::create();
    attributeSortRule->apply(queryPlan);

    auto signatureInferencePhase =
        Optimizer::SignatureInferencePhase::create(/*Z3context*/ nullptr,
                                                   Optimizer::QueryMergerRule::HashSignatureBasedCompleteQueryMergerRule);
    signatureInferencePhase->execute(queryPlan);

    auto rootOperators = queryPlan->getRootOperators();
    EXPECT_TRUE(rootOperators.size() == 1);
    const auto* expectedSignature =
        "SINK().FILTER(FieldAccessNode(a[Undefined])+FieldAccessNode(c[Undefined])<"
        "FieldAccessNode(c[Undefined])+FieldAccessNode(d[Undefined])*FieldAccessNode(d[Undefined])).SOURCE(src)";
    auto actualSignature = *rootOperators[0]->as<LogicalOperatorNode>()->getHashBasedSignature().begin()->second.begin();
    EXPECT_EQ(expectedSignature, actualSignature);
}

TEST_F(AttributeSortRuleTest, testAttributeSortRuleForFilterOperator2) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("src")
                      .filter(((Attribute("d") + Attribute("c")) * Attribute("d")) < Attribute("c") + Attribute("a"))
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto attributeSortRule = Optimizer::AttributeSortRule::create();
    attributeSortRule->apply(queryPlan);

    auto signatureInferencePhase =
        Optimizer::SignatureInferencePhase::create(/*Z3context*/ nullptr,
                                                   Optimizer::QueryMergerRule::HashSignatureBasedCompleteQueryMergerRule);
    signatureInferencePhase->execute(queryPlan);

    auto rootOperators = queryPlan->getRootOperators();
    EXPECT_TRUE(rootOperators.size() == 1);
    const auto* expectedSignature =
        "SINK().FILTER(FieldAccessNode(a[Undefined])+FieldAccessNode(c[Undefined])>"
        "FieldAccessNode(c[Undefined])+FieldAccessNode(d[Undefined])*FieldAccessNode(d[Undefined])).SOURCE(src)";
    auto actualSignature = *rootOperators[0]->as<LogicalOperatorNode>()->getHashBasedSignature().begin()->second.begin();
    EXPECT_EQ(expectedSignature, actualSignature);
}

TEST_F(AttributeSortRuleTest, testAttributeSortRuleForFilterOperator3) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("src")
                      .filter(((Attribute("d") + Attribute("c")) * Attribute("d")) <= Attribute("c") + Attribute("a"))
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto attributeSortRule = Optimizer::AttributeSortRule::create();
    attributeSortRule->apply(queryPlan);

    auto signatureInferencePhase =
        Optimizer::SignatureInferencePhase::create(/*Z3context*/ nullptr,
                                                   Optimizer::QueryMergerRule::HashSignatureBasedCompleteQueryMergerRule);
    signatureInferencePhase->execute(queryPlan);

    auto rootOperators = queryPlan->getRootOperators();
    EXPECT_TRUE(rootOperators.size() == 1);
    const auto* expectedSignature =
        "SINK().FILTER(FieldAccessNode(a[Undefined])+FieldAccessNode(c[Undefined])>="
        "FieldAccessNode(c[Undefined])+FieldAccessNode(d[Undefined])*FieldAccessNode(d[Undefined])).SOURCE(src)";
    auto actualSignature = *rootOperators[0]->as<LogicalOperatorNode>()->getHashBasedSignature().begin()->second.begin();
    EXPECT_EQ(expectedSignature, actualSignature);
}

TEST_F(AttributeSortRuleTest, testAttributeSortRuleForFilterOperator4) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("src")
                      .filter(((Attribute("d") + Attribute("c")) * Attribute("d")) >= Attribute("c") + Attribute("a"))
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto attributeSortRule = Optimizer::AttributeSortRule::create();
    attributeSortRule->apply(queryPlan);

    auto signatureInferencePhase =
        Optimizer::SignatureInferencePhase::create(/*Z3context*/ nullptr,
                                                   Optimizer::QueryMergerRule::HashSignatureBasedCompleteQueryMergerRule);
    signatureInferencePhase->execute(queryPlan);

    auto rootOperators = queryPlan->getRootOperators();
    EXPECT_TRUE(rootOperators.size() == 1);
    const auto* expectedSignature =
        "SINK().FILTER(FieldAccessNode(a[Undefined])+FieldAccessNode(c[Undefined])<="
        "FieldAccessNode(c[Undefined])+FieldAccessNode(d[Undefined])*FieldAccessNode(d[Undefined])).SOURCE(src)";
    auto actualSignature = *rootOperators[0]->as<LogicalOperatorNode>()->getHashBasedSignature().begin()->second.begin();
    EXPECT_EQ(expectedSignature, actualSignature);
}

TEST_F(AttributeSortRuleTest, testAttributeSortRuleForFilterOperator5) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("src")
                      .filter(((Attribute("d") + Attribute("c")) * Attribute("d")) != Attribute("c") + Attribute("a"))
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto attributeSortRule = Optimizer::AttributeSortRule::create();
    attributeSortRule->apply(queryPlan);

    auto signatureInferencePhase =
        Optimizer::SignatureInferencePhase::create(/*Z3context*/ nullptr,
                                                   Optimizer::QueryMergerRule::HashSignatureBasedCompleteQueryMergerRule);
    signatureInferencePhase->execute(queryPlan);

    auto rootOperators = queryPlan->getRootOperators();
    EXPECT_TRUE(rootOperators.size() == 1);
    const auto* expectedSignature =
        "SINK().FILTER(!FieldAccessNode(a[Undefined])+FieldAccessNode(c[Undefined])=="
        "FieldAccessNode(c[Undefined])+FieldAccessNode(d[Undefined])*FieldAccessNode(d[Undefined])).SOURCE(src)";
    auto actualSignature = *rootOperators[0]->as<LogicalOperatorNode>()->getHashBasedSignature().begin()->second.begin();
    EXPECT_EQ(expectedSignature, actualSignature);
}

TEST_F(AttributeSortRuleTest, testAttributeSortRuleForFilterOperator6) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("src")
                      .filter(((Attribute("d") + Attribute("c")) * Attribute("d")) == Attribute("c") + Attribute("a"))
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto attributeSortRule = Optimizer::AttributeSortRule::create();
    attributeSortRule->apply(queryPlan);

    auto signatureInferencePhase =
        Optimizer::SignatureInferencePhase::create(/*Z3context*/ nullptr,
                                                   Optimizer::QueryMergerRule::HashSignatureBasedCompleteQueryMergerRule);
    signatureInferencePhase->execute(queryPlan);

    auto rootOperators = queryPlan->getRootOperators();
    EXPECT_TRUE(rootOperators.size() == 1);
    const auto* expectedSignature =
        "SINK().FILTER(FieldAccessNode(a[Undefined])+FieldAccessNode(c[Undefined])=="
        "FieldAccessNode(c[Undefined])+FieldAccessNode(d[Undefined])*FieldAccessNode(d[Undefined])).SOURCE(src)";
    auto actualSignature = *rootOperators[0]->as<LogicalOperatorNode>()->getHashBasedSignature().begin()->second.begin();
    EXPECT_EQ(expectedSignature, actualSignature);
}
