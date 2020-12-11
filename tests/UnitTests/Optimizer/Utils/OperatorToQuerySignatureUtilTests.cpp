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
#include <Optimizer/Utils/OperatorToQuerySignatureUtil.hpp>
#include <API/Schema.hpp>
#include <Util/Logger.hpp>
#include <API/Expressions/Expressions.hpp>
#include <API/Expressions/ArithmeticalExpressions.hpp>
#include <API/Expressions/LogicalExpressions.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <iostream>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/QueryMerger/Signature/QuerySignature.hpp>
#include <z3++.h>
#include <Phases/TypeInferencePhase.hpp>

using namespace NES;

class OperatorToQuerySignatureUtilTests : public testing::Test {

  public:
    SchemaPtr schema;

    /* Will be called before a test is executed. */
    void SetUp() {
        NES::setupLogging("OperatorToFOLUtilTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup OperatorToFOLUtilTest test case.");
        schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    }

    /* Will be called before a test is executed. */
    void TearDown() { NES_INFO("Setup OperatorToFOLUtilTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down OperatorToFOLUtilTest test class."); }
};

TEST_F(OperatorToQuerySignatureUtilTests, testFiltersWithExactPredicates) {

    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();
    //Define Predicate
    ExpressionNodePtr predicate = Attribute("value") == 40;
    predicate->inferStamp(schema);

    //Create Source
    LogicalOperatorNodePtr sourceOperator =
        LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car", 1));

    //Create Filtersr

    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createFilterOperator(predicate);
    logicalOperator1->addChild(sourceOperator);
    logicalOperator1->inferSignature(context);
    auto sig1 = logicalOperator1->getSignature();

    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createFilterOperator(predicate);
    logicalOperator2->addChild(sourceOperator);
    logicalOperator2->inferSignature(context);
    auto sig2 = logicalOperator2->getSignature();

    //Assert
    ASSERT_TRUE(sig1->isEqual(sig2));
}

TEST_F(OperatorToQuerySignatureUtilTests, testFiltersWithEqualPredicates) {

    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();

    //Define Predicate
    ExpressionNodePtr predicate1 = Attribute("value") == 40;
    predicate1->inferStamp(schema);
    ExpressionNodePtr predicate2 = 40 == Attribute("value");
    predicate2->inferStamp(schema);

    //Create Source
    LogicalOperatorNodePtr sourceOperator =
        LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car", 1));

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createFilterOperator(predicate1);
    logicalOperator1->addChild(sourceOperator);
    logicalOperator1->inferSignature(context);
    auto sig1 = logicalOperator1->getSignature();

    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createFilterOperator(predicate2);
    logicalOperator2->addChild(sourceOperator);
    logicalOperator2->inferSignature(context);
    auto sig2 = logicalOperator2->getSignature();

    //Assert
    ASSERT_TRUE(sig1->isEqual(sig2));
}

TEST_F(OperatorToQuerySignatureUtilTests, testFiltersWithMultipleExactPredicates) {

    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();

    //Define Predicate
    ExpressionNodePtr predicate1 = Attribute("value") == 40 && Attribute("id") >= 40;
    predicate1->inferStamp(schema);

    //Create Source
    LogicalOperatorNodePtr sourceOperator =
        LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car", 1));

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createFilterOperator(predicate1);
    logicalOperator1->addChild(sourceOperator);
    logicalOperator1->inferSignature(context);
    auto sig1 = logicalOperator1->getSignature();

    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createFilterOperator(predicate1);
    logicalOperator2->addChild(sourceOperator);
    logicalOperator2->inferSignature(context);
    auto sig2 = logicalOperator2->getSignature();

    //Assert
    ASSERT_TRUE(sig1->isEqual(sig2));
}

TEST_F(OperatorToQuerySignatureUtilTests, testFiltersWithMultipleEqualPredicates1) {

    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();

    //Define Predicate
    ExpressionNodePtr predicate1 = Attribute("value") == 40 && Attribute("id") >= 40;
    predicate1->inferStamp(schema);
    ExpressionNodePtr predicate2 = Attribute("id") >= 40 && Attribute("value") == 40;
    predicate2->inferStamp(schema);

    //Create Source
    LogicalOperatorNodePtr sourceOperator =
        LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car", 1));

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createFilterOperator(predicate1);
    logicalOperator1->addChild(sourceOperator);
    logicalOperator1->inferSignature(context);
    auto sig1 = logicalOperator1->getSignature();

    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createFilterOperator(predicate2);
    logicalOperator2->addChild(sourceOperator);
    logicalOperator2->inferSignature(context);
    auto sig2 = logicalOperator2->getSignature();

    //Assert
    ASSERT_TRUE(sig1->isEqual(sig2));
}

TEST_F(OperatorToQuerySignatureUtilTests, testFiltersWithMultipleEqualPredicates2) {

    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();

    //Define Predicate
    ExpressionNodePtr predicate1 = Attribute("value") == 40 + 40 && Attribute("id") >= 40;
    predicate1->inferStamp(schema);
    ExpressionNodePtr predicate2 = Attribute("id") >= 40 && Attribute("value") == 80;
    predicate2->inferStamp(schema);

    //Create Source
    LogicalOperatorNodePtr sourceOperator =
        LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car", 1));

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createFilterOperator(predicate1);
    logicalOperator1->addChild(sourceOperator);
    logicalOperator1->inferSignature(context);
    auto sig1 = logicalOperator1->getSignature();

    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createFilterOperator(predicate2);
    logicalOperator2->addChild(sourceOperator);
    logicalOperator2->inferSignature(context);
    auto sig2 = logicalOperator2->getSignature();

    //Assert
    ASSERT_TRUE(sig1->isEqual(sig2));
}

TEST_F(OperatorToQuerySignatureUtilTests, testFiltersWithDifferentPredicates) {

    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();

    //Define Predicate
    ExpressionNodePtr predicate1 = Attribute("value") == 40;
    predicate1->inferStamp(schema);
    ExpressionNodePtr predicate2 = Attribute("id") == 40;
    predicate2->inferStamp(schema);

    //Create Source
    LogicalOperatorNodePtr sourceOperator =
        LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car", 1));

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createFilterOperator(predicate1);
    logicalOperator1->addChild(sourceOperator);
    logicalOperator1->inferSignature(context);
    auto sig1 = logicalOperator1->getSignature();

    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createFilterOperator(predicate2);
    logicalOperator2->addChild(sourceOperator);
    logicalOperator2->inferSignature(context);
    auto sig2 = logicalOperator2->getSignature();

    //Assert
    ASSERT_FALSE(sig1->isEqual(sig2));
}

TEST_F(OperatorToQuerySignatureUtilTests, testFiltersWithMultipleDifferentPredicates) {

    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();

    //Define Predicate
    ExpressionNodePtr predicate1 = Attribute("value") == 40 && Attribute("id") >= 40;
    predicate1->inferStamp(schema);
    ExpressionNodePtr predicate2 = Attribute("id") >= 40 or Attribute("value") == 40;
    predicate2->inferStamp(schema);

    //Create Source
    LogicalOperatorNodePtr sourceOperator =
        LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car", 1));

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createFilterOperator(predicate1);
    logicalOperator1->addChild(sourceOperator);
    logicalOperator1->inferSignature(context);
    auto sig1 = logicalOperator1->getSignature();
    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createFilterOperator(predicate2);
    logicalOperator2->addChild(sourceOperator);
    logicalOperator2->inferSignature(context);
    auto sig2 = logicalOperator2->getSignature();

    //Assert
    ASSERT_FALSE(sig1->isEqual(sig2));
}

TEST_F(OperatorToQuerySignatureUtilTests, testMapWithExactExpression) {

    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();
    //Define expression
    FieldAssignmentExpressionNodePtr expression = Attribute("value") = 40;
    expression->inferStamp(schema);

    //Create Source
    LogicalOperatorNodePtr sourceOperator =
        LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car", 1));

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createMapOperator(expression);
    logicalOperator1->addChild(sourceOperator);
    logicalOperator1->inferSignature(context);
    auto sig1 = logicalOperator1->getSignature();

    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createMapOperator(expression);
    logicalOperator2->addChild(sourceOperator);
    logicalOperator2->inferSignature(context);
    auto sig2 = logicalOperator2->getSignature();

    //Assert
    ASSERT_TRUE(sig1->isEqual(sig2));
}

TEST_F(OperatorToQuerySignatureUtilTests, testMapWithDifferentExpression) {

    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();
    //Define expression
    FieldAssignmentExpressionNodePtr expression1 = Attribute("value") = 40;
    expression1->inferStamp(schema);
    FieldAssignmentExpressionNodePtr expression2 = Attribute("id") = 40;
    expression2->inferStamp(schema);

    //Create Source
    LogicalOperatorNodePtr sourceOperator =
        LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car", 1));

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createMapOperator(expression1);
    logicalOperator1->addChild(sourceOperator);
    logicalOperator1->inferSignature(context);
    auto sig1 = logicalOperator1->getSignature();

    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createMapOperator(expression2);
    logicalOperator2->addChild(sourceOperator);
    logicalOperator2->inferSignature(context);
    auto sig2 = logicalOperator2->getSignature();

    //Assert
    ASSERT_FALSE(sig1->isEqual(sig2));
}

TEST_F(OperatorToQuerySignatureUtilTests, testMultipleMapsWithDifferentOrder) {

    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();
    //Define expression
    FieldAssignmentExpressionNodePtr expression1 = Attribute("id") = 40;
    expression1->inferStamp(schema);
    FieldAssignmentExpressionNodePtr expression2 = Attribute("value") = Attribute("id") + Attribute("value");
    expression2->inferStamp(schema);

    //Create Source
    LogicalOperatorNodePtr sourceOperator =
        LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car", 1));

    //Create map
    LogicalOperatorNodePtr logicalOperator11 = LogicalOperatorFactory::createMapOperator(expression1);
    logicalOperator11->addChild(sourceOperator);
    LogicalOperatorNodePtr logicalOperator12 = LogicalOperatorFactory::createMapOperator(expression2);
    logicalOperator12->addChild(logicalOperator11);
    logicalOperator12->inferSignature(context);
    auto sig1 = logicalOperator12->getSignature();

    LogicalOperatorNodePtr logicalOperator21 = LogicalOperatorFactory::createMapOperator(expression2);
    logicalOperator21->addChild(sourceOperator);
    LogicalOperatorNodePtr logicalOperator22 = LogicalOperatorFactory::createMapOperator(expression1);
    logicalOperator22->addChild(logicalOperator21);
    logicalOperator22->inferSignature(context);
    auto sig2 = logicalOperator22->getSignature();

    //Assert
    ASSERT_FALSE(sig1->isEqual(sig2));
}

TEST_F(OperatorToQuerySignatureUtilTests, testMultipleMapsWithSameOrder) {

    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();
    //Define expression
    FieldAssignmentExpressionNodePtr expression1 = Attribute("id") = 40;
    expression1->inferStamp(schema);
    FieldAssignmentExpressionNodePtr expression2 = Attribute("value") = Attribute("id") + Attribute("value");
    expression2->inferStamp(schema);

    //Create Source
    LogicalOperatorNodePtr sourceOperator =
        LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car", 1));

    //Create map
    LogicalOperatorNodePtr logicalOperator11 = LogicalOperatorFactory::createMapOperator(expression1);
    logicalOperator11->addChild(sourceOperator);
    LogicalOperatorNodePtr logicalOperator12 = LogicalOperatorFactory::createMapOperator(expression2);
    logicalOperator12->addChild(logicalOperator11);
    logicalOperator12->inferSignature(context);
    auto sig1 = logicalOperator12->getSignature();

    LogicalOperatorNodePtr logicalOperator21 = LogicalOperatorFactory::createMapOperator(expression1);
    logicalOperator21->addChild(sourceOperator);
    LogicalOperatorNodePtr logicalOperator22 = LogicalOperatorFactory::createMapOperator(expression2);
    logicalOperator22->addChild(logicalOperator21);
    logicalOperator22->inferSignature(context);
    auto sig2 = logicalOperator22->getSignature();

    //Assert
    ASSERT_TRUE(sig1->isEqual(sig2));
}

TEST_F(OperatorToQuerySignatureUtilTests, testMapWithDifferentExpressionOnSameField) {

    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();
    //Define expression
    FieldAssignmentExpressionNodePtr expression1 = Attribute("value") = 40;
    expression1->inferStamp(schema);
    FieldAssignmentExpressionNodePtr expression2 = Attribute("value") = 50;
    expression2->inferStamp(schema);

    //Create Source
    LogicalOperatorNodePtr sourceOperator =
        LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car", 1));

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createMapOperator(expression1);
    logicalOperator1->addChild(sourceOperator);
    logicalOperator1->inferSignature(context);
    auto sig1 = logicalOperator1->getSignature();

    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createMapOperator(expression2);
    logicalOperator2->addChild(sourceOperator);
    logicalOperator2->inferSignature(context);
    auto sig2 = logicalOperator2->getSignature();

    //Assert
    ASSERT_FALSE(sig1->isEqual(sig2));
}

TEST_F(OperatorToQuerySignatureUtilTests, testSourceWithSameStreamName) {

    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();
    //Define Predicate
    auto sourceDescriptor = LogicalStreamSourceDescriptor::create("Car", 1);

    //Create source operator
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createSourceOperator(sourceDescriptor);
    logicalOperator1->inferSignature(context);
    auto sig1 = logicalOperator1->getSignature();

    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createSourceOperator(sourceDescriptor);
    logicalOperator2->inferSignature(context);
    auto sig2 = logicalOperator2->getSignature();

    //Assert
    ASSERT_TRUE(sig1->isEqual(sig2));
}

TEST_F(OperatorToQuerySignatureUtilTests, testSourceWithDifferentStreamName) {

    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();
    //Define Predicate
    auto sourceDescriptor1 = LogicalStreamSourceDescriptor::create("Car", 1);
    auto sourceDescriptor2 = LogicalStreamSourceDescriptor::create("Truck", 2);

    //Create source
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createSourceOperator(sourceDescriptor1);
    logicalOperator1->inferSignature(context);
    auto sig1 = logicalOperator1->getSignature();

    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createSourceOperator(sourceDescriptor2);
    logicalOperator2->inferSignature(context);
    auto sig2 = logicalOperator2->getSignature();

    //Assert
    ASSERT_FALSE(sig1->isEqual(sig2));
}

//FIXME: fix this test in the next issue
//TEST_F(OperatorToQuerySignatureUtilTests, testEqualQueries) {
//    // Prepare
//    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
//    Query query1 = Query::from("car")
//        .map(Attribute("value") = Attribute("value") * 10)
//        .filter(Attribute("value") < 40 && Attribute("value") < 31)
//        .sink(printSinkDescriptor);
//    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
//    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
//    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
//    queryPlan1->setQueryId(queryId1);
//
//
//    //slover.add(!(value*40 < 40 && value*40*40 < 40 == value*80 < 40 && value*80*40 < 40 && value*80*40 == value * 40 *40))
//
//    Query query2 = Query::from("car")
//        .map(Attribute("value") = Attribute("value") * 10)
//        .filter(Attribute("value") < 40 && Attribute("value") < 40)
//        .sink(printSinkDescriptor);
//    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
//    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
//    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
//    queryPlan2->setQueryId(queryId2);
//
//    z3::ContextPtr context = std::make_shared<z3::context>();
//
//    auto typeInferencePhase = TypeInferencePhase::create(streamCatalog);
//    typeInferencePhase->execute(queryPlan1);
//    typeInferencePhase->execute(queryPlan2);
//
//    auto z3InferencePhase = Optimizer::SignatureInferencePhase::create(context);
//    z3InferencePhase->execute(queryPlan1);
//    z3InferencePhase->execute(queryPlan2);
//
//    auto equalQueryMergerRule = Optimizer::SignatureBasedEqualQueryMergerRule::create();
//    bool isEqual = equalQueryMergerRule->apply();
//
//    ASSERT_TRUE(isEqual);
//}