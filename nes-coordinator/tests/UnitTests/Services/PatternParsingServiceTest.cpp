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

#include <API/Query.hpp>
#include <API/QueryAPI.hpp>
#include <BaseIntegrationTest.hpp>
#include <Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperator.hpp>
#include <Operators/LogicalOperators/LogicalFilterOperator.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Services/QueryParsingService.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <iostream>
#include <regex>

using namespace NES;
/*
 * This test checks for the correctness of the pattern queries created by the NESPL Parsing Service.
 */
class PatternParsingServiceTest : public Testing::BaseUnitTest {

  public:
    /* Will be called before a test is executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("QueryPlanTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup QueryPlanTest test case.");
    }
};

std::string queryPlanToString(const QueryPlanPtr queryPlan) {
    std::regex r1("  ");
    std::regex r2("[0-9]");
    std::string queryPlanStr = std::regex_replace(queryPlan->toString(), r1, "");
    queryPlanStr = std::regex_replace(queryPlanStr, r2, "");
    return queryPlanStr;
}

TEST_F(PatternParsingServiceTest, simplePattern) {
    //pattern string as received from the NES UI and create query plan from parsing service
    std::string patternString = "PATTERN test:= A FROM defaultLogical AS A WHERE A.currentSpeed < A.allowedSpeed && A.value >= "
                                "30 INTO PRINT, FILE(fileName testSink2.csv), NULLOUTPUT ";
    std::shared_ptr<QueryParsingService> patternParsingService;
    QueryPlanPtr patternPlan = patternParsingService->createPatternFromCodeString(patternString);
    // expected result
    QueryPlanPtr queryPlan = QueryPlan::create();
    LogicalOperatorPtr source = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("defaultLogical"));
    queryPlan->appendOperatorAsNewRoot(source);
    LogicalOperatorPtr filter = LogicalOperatorFactory::createFilterOperator(
        ExpressionNodePtr(LessExpressionNode::create(NES::Attribute("defaultLogical$currentSpeed").getExpressionNode(),
                                                     NES::Attribute("defaultLogical$allowedSpeed").getExpressionNode())));
    LogicalOperatorPtr filter2 = LogicalOperatorFactory::createFilterOperator(
        ExpressionNodePtr(GreaterEqualsExpressionNode::create(NES::Attribute("defaultLogical$value").getExpressionNode(),
                                                              NES::ExpressionItem(30).getExpressionNode())));
    queryPlan->appendOperatorAsNewRoot(filter2);
    queryPlan->appendOperatorAsNewRoot(filter);
    LogicalOperatorPtr sinkPrint = LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    LogicalOperatorPtr sinkFile = LogicalOperatorFactory::createSinkOperator(NES::FileSinkDescriptor::create("testSink2.csv"));
    LogicalOperatorPtr sinkNull = LogicalOperatorFactory::createSinkOperator(NES::NullOutputSinkDescriptor::create());
    auto sinkOperators = {sinkPrint, sinkFile, sinkNull};
    const std::vector<NES::OperatorPtr>& rootOperators = queryPlan->getRootOperators();

    for (const auto& sinkOperator : sinkOperators) {
        for (const auto& rootOperator : rootOperators) {
            sinkOperator->addChild(rootOperator);
            queryPlan->removeAsRootOperator(rootOperator);
            queryPlan->addRootOperator(sinkOperator);
        }
    }
    //comparison of the expected and the actual generated query plan
    EXPECT_EQ(queryPlanToString(queryPlan), queryPlanToString(patternPlan));
}

TEST_F(PatternParsingServiceTest, DisjunctionPattern) {
    //pattern string as received from the NES UI and create query plan from parsing service
    std::string patternString = "PATTERN test:= (A OR B) FROM defaultLogical AS A, defaultLogicalB AS B WITHIN SLIDING "
                                "[timestamp, SIZE 3 MINUTE, SLIDE 1 MINUTE] INTO PRINT";
    std::shared_ptr<QueryParsingService> patternParsingService;
    QueryPlanPtr patternPlan = patternParsingService->createPatternFromCodeString(patternString);

    //expected result
    QueryPlanPtr queryPlan = QueryPlan::create();
    QueryPlanPtr subQueryPlan = QueryPlan::create();
    LogicalOperatorPtr source1 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("defaultLogical"));
    queryPlan->addRootOperator(source1);
    LogicalOperatorPtr source2 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("defaultLogicalB"));
    subQueryPlan->addRootOperator(source2);
    OperatorPtr unionOp = LogicalOperatorFactory::createUnionOperator();
    queryPlan->appendOperatorAsNewRoot(unionOp);
    unionOp->addChild(subQueryPlan->getRootOperators()[0]);
    LogicalOperatorPtr sink = LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(sink);

    //Comparison of the expected and the actual generated query plan
    EXPECT_EQ(queryPlanToString(queryPlan), queryPlanToString(patternPlan));
}

TEST_F(PatternParsingServiceTest, NestedDisjunctionPattern) {
    //pattern string as received from the NES UI and create query plan from parsing service
    std::string patternString = "PATTERN test:= ((A OR B) OR C) FROM defaultLogical AS A, defaultLogicalB AS B, "
                                "defaultLogicalC AS C INTO PRINT ";

    std::string patternString2 = "PATTERN test:= (C OR (A OR B)) FROM defaultLogical AS A, defaultLogicalB AS B, "
                                 "defaultLogicalC AS C INTO PRINT";
    std::shared_ptr<QueryParsingService> patternParsingService;

    QueryPlanPtr patternPlan = patternParsingService->createPatternFromCodeString(patternString);
    QueryPlanPtr patternPlan2 = patternParsingService->createPatternFromCodeString(patternString2);

    //expected result patternString
    QueryPlanPtr queryPlanA = QueryPlan::create();
    LogicalOperatorPtr source1 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("defaultLogical"));

    QueryPlanPtr queryPlanB = QueryPlan::create();
    LogicalOperatorPtr source2 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("defaultLogicalB"));

    QueryPlanPtr queryPlanC = QueryPlan::create();
    LogicalOperatorPtr source3 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("defaultLogicalC"));

    queryPlanA->addRootOperator(source1);
    queryPlanB->addRootOperator(source2);
    queryPlanC->addRootOperator(source3);
    Query query = Query(queryPlanA).unionWith(queryPlanB).unionWith(queryPlanC);
    queryPlanA = query.getQueryPlan();
    LogicalOperatorPtr sink = LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlanA->appendOperatorAsNewRoot(sink);

    //Comparison of the expected and the actual generated query plan
    EXPECT_EQ(queryPlanToString(queryPlanA), queryPlanToString(patternPlan));
    EXPECT_EQ(queryPlanToString(queryPlanA), queryPlanToString(patternPlan2));
}

TEST_F(PatternParsingServiceTest, ConjunctionPatternWithFilterTumblingWindow) {
    //pattern string as received from the NES UI
    std::string patternString =
        "PATTERN test:= A AND B FROM defaultLogical AS A, defaultLogicalB AS B  WHERE "
        "A.currentSpeed < B.allowedSpeed && B.value == 30 WITHIN TUMBLING [timestamp, SIZE 3 MINUTE] INTO PRINT";
    std::shared_ptr<QueryParsingService> patternParsingService;
    QueryPlanPtr patternPlan = patternParsingService->createPatternFromCodeString(patternString);

    //expected result
    QueryPlanPtr queryPlan = QueryPlan::create();
    QueryPlanPtr subQueryPlan = QueryPlan::create();
    LogicalOperatorPtr source1 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("defaultLogical"));
    queryPlan->addRootOperator(source1);
    LogicalOperatorPtr source2 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("defaultLogicalB"));
    subQueryPlan->addRootOperator(source2);
    NES::Query query = Query(queryPlan)
                           .andWith(Query(subQueryPlan))
                           .window(NES::Windowing::TumblingWindow::of(EventTime(Attribute("timestamp")), Minutes(3)));
    queryPlan = query.getQueryPlan();
    LogicalOperatorPtr filter = LogicalOperatorFactory::createFilterOperator(
        ExpressionNodePtr(LessExpressionNode::create(NES::Attribute("defaultLogical$currentSpeed").getExpressionNode(),
                                                     NES::Attribute("defaultLogicalB$allowedSpeed").getExpressionNode())));
    LogicalOperatorPtr filter2 = LogicalOperatorFactory::createFilterOperator(
        ExpressionNodePtr(EqualsExpressionNode::create(NES::Attribute("defaultLogicalB$value").getExpressionNode(),
                                                       NES::ExpressionItem(30).getExpressionNode())));
    queryPlan->appendOperatorAsNewRoot(filter2);
    queryPlan->appendOperatorAsNewRoot(filter);
    LogicalOperatorPtr sink = LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(sink);

    //Comparison of the expected and the actual generated query plan
    EXPECT_EQ(queryPlanToString(queryPlan), queryPlanToString(patternPlan));
}

TEST_F(PatternParsingServiceTest, ConjunctionPatternWithFilterIntervalJoin) {
    //pattern string as received from the NES UI
    std::string patternString =
        "PATTERN test:= A AND B FROM defaultLogical AS A, defaultLogicalB AS B  WHERE "
        "A.currentSpeed < B.allowedSpeed && B.value == 30 WITHIN INTERVAL [timestamp, INTERVAL 3 MINUTE] INTO PRINT";
    std::shared_ptr<QueryParsingService> patternParsingService;
    QueryPlanPtr patternPlan = patternParsingService->createPatternFromCodeString(patternString);

    //expected result
    QueryPlanPtr queryPlan = QueryPlan::create();
    QueryPlanPtr subQueryPlan = QueryPlan::create();
    LogicalOperatorPtr source1 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("defaultLogical"));
    queryPlan->addRootOperator(source1);
    LogicalOperatorPtr source2 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("defaultLogicalB"));
    subQueryPlan->addRootOperator(source2);
    NES::Query query = Query(queryPlan)
                           .map(Attribute("cep_leftKey") = 1)
                           .intervalJoinWith(Query(subQueryPlan).map(Attribute("cep_rightKey") = 1))
                           .where(Attribute("cep_leftKey") == Attribute("cep_rightKey"))
                           .lowerBound(EventTime(Attribute("timestamp")), 3, Minutes())
                           .upperBound(3, Minutes());

    queryPlan = query.getQueryPlan();
    LogicalOperatorPtr filter = LogicalOperatorFactory::createFilterOperator(
        ExpressionNodePtr(LessExpressionNode::create(NES::Attribute("defaultLogical$currentSpeed").getExpressionNode(),
                                                     NES::Attribute("defaultLogicalB$allowedSpeed").getExpressionNode())));
    LogicalOperatorPtr filter2 = LogicalOperatorFactory::createFilterOperator(
        ExpressionNodePtr(EqualsExpressionNode::create(NES::Attribute("defaultLogicalB$value").getExpressionNode(),
                                                       NES::ExpressionItem(30).getExpressionNode())));
    queryPlan->appendOperatorAsNewRoot(filter2);
    queryPlan->appendOperatorAsNewRoot(filter);
    LogicalOperatorPtr sink = LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(sink);

    //Comparison of the expected and the actual generated query plan
    EXPECT_EQ(queryPlanToString(queryPlan), queryPlanToString(patternPlan));
}

TEST_F(PatternParsingServiceTest, NestedConjunctionPattern) {
    //pattern string as received from the NES UI
    std::string patternString = "PATTERN test:= ((A AND B) AND C) FROM defaultLogical AS A, defaultLogicalB AS B, "
                                "defaultLogicalC AS C WITHIN SLIDING [timestamp, SIZE 3 MINUTE, SLIDE 1 MINUTE] INTO PRINT ";
    std::shared_ptr<QueryParsingService> patternParsingService;
    QueryPlanPtr patternPlan = patternParsingService->createPatternFromCodeString(patternString);

    //expected result
    QueryPlanPtr queryPlanA = QueryPlan::create();
    QueryPlanPtr queryPlanB = QueryPlan::create();
    QueryPlanPtr queryPlanC = QueryPlan::create();
    LogicalOperatorPtr source1 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("defaultLogical"));
    LogicalOperatorPtr source2 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("defaultLogicalB"));
    LogicalOperatorPtr source3 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("defaultLogicalC"));
    queryPlanA->addRootOperator(source1);
    queryPlanB->addRootOperator(source2);
    queryPlanC->addRootOperator(source3);
    NES::Query query = Query(queryPlanA)
                           .andWith(Query(queryPlanB))
                           .window(NES::Windowing::SlidingWindow::of(EventTime(Attribute("timestamp")), Minutes(3), Minutes(1)))
                           .andWith(Query(queryPlanC))
                           .window(NES::Windowing::SlidingWindow::of(EventTime(Attribute("timestamp")), Minutes(3), Minutes(1)));
    queryPlanA = query.getQueryPlan();
    LogicalOperatorPtr sink = LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlanA->appendOperatorAsNewRoot(sink);

    //Comparison of the expected and the actual generated query plan
    EXPECT_EQ(queryPlanToString(queryPlanA), queryPlanToString(patternPlan));
}

TEST_F(PatternParsingServiceTest, SequencePattern) {
    //pattern string as received from the NES UI
    std::string patternString = "PATTERN test:= A SEQ B FROM defaultLogical AS A, defaultLogicalB AS B WHERE A.id >= 400 && "
                                "B.value >= -30.5 WITHIN SLIDING [timestamp, SIZE 3 MINUTE, SLIDE 1 MINUTE] INTO PRINT";
    std::shared_ptr<QueryParsingService> patternParsingService;
    QueryPlanPtr patternPlan = patternParsingService->createPatternFromCodeString(patternString);

    //expected result
    NES::Query qSEQ = NES::Query::from("defaultLogical")
                          .seqWith(NES::Query::from("defaultLogicalB"))
                          .window(NES::Windowing::SlidingWindow::of(EventTime(Attribute("timestamp")), Minutes(3), Minutes(1)));
    auto queryPlan = qSEQ.getQueryPlan();
    LogicalOperatorPtr filter = LogicalOperatorFactory::createFilterOperator(
        ExpressionNodePtr(GreaterEqualsExpressionNode::create(NES::Attribute("defaultLogical$id").getExpressionNode(),
                                                              NES::ExpressionItem(400).getExpressionNode())));
    LogicalOperatorPtr filter2 = LogicalOperatorFactory::createFilterOperator(
        ExpressionNodePtr(GreaterEqualsExpressionNode::create(NES::Attribute("defaultLogicalB$value").getExpressionNode(),
                                                              NES::ExpressionItem(-30.5).getExpressionNode())));

    LogicalOperatorPtr sink4 = LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(filter2);
    queryPlan->appendOperatorAsNewRoot(filter);
    queryPlan->appendOperatorAsNewRoot(sink4);

    //Comparison of the expected and the actual generated query plan
    EXPECT_EQ(queryPlanToString(queryPlan), queryPlanToString(patternPlan));
}

TEST_F(PatternParsingServiceTest, NestedSequencePattern) {
    //pattern string as received from the NES UI
    std::string patternStringABC = "PATTERN test:= ((A SEQ B) SEQ C) FROM defaultLogical AS A, defaultLogicalB AS B, "
                                   "defaultLogicalC AS C WITHIN SLIDING [timestamp, SIZE 3 MINUTE, SLIDE 1 MINUTE] INTO PRINT ";

    std::shared_ptr<QueryParsingService> patternParsingService;
    QueryPlanPtr patternPlanABC = patternParsingService->createPatternFromCodeString(patternStringABC);

    //expected result ABC
    NES::Query queryABC =
        NES::Query::from("defaultLogical")
            .seqWith(NES::Query::from("defaultLogicalB"))
            .window(NES::Windowing::SlidingWindow::of(EventTime(Attribute("timestamp")), Minutes(3), Minutes(1)))
            .seqWith(NES::Query::from("defaultLogicalC"))
            .window(NES::Windowing::SlidingWindow::of(EventTime(Attribute("timestamp")), Minutes(3), Minutes(1)));
    auto queryPlan1 = queryABC.getQueryPlan();
    LogicalOperatorPtr sinkABC = LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan1->appendOperatorAsNewRoot(sinkABC);

    //Comparison of the expected and the actual generated query plan
    EXPECT_EQ(queryPlanToString(queryPlan1), queryPlanToString(patternPlanABC));
}

TEST_F(PatternParsingServiceTest, NestedSequencePatternWithIntervalJoin) {
    //pattern string as received from the NES UI
    std::string patternStringABC = "PATTERN test:= ((A SEQ B) SEQ C) FROM defaultLogical AS A, defaultLogicalB AS B, "
                                   "defaultLogicalC AS C WITHIN INTERVAL [timestamp, INTERVAL 3 MINUTE] INTO PRINT ";

    std::shared_ptr<QueryParsingService> patternParsingService;
    QueryPlanPtr patternPlanABC = patternParsingService->createPatternFromCodeString(patternStringABC);

    //expected result ABC
    NES::Query queryABC = NES::Query::from("defaultLogical")
                              .map(Attribute("cep_leftKey") = 1)
                              .intervalJoinWith(Query::from("defaultLogicalB").map(Attribute("cep_rightKey") = 1))
                              .where(Attribute("cep_leftKey") == Attribute("cep_rightKey"))
                              .lowerBound(EventTime(Attribute("timestamp")), 0, Minutes())
                              .upperBound(3, Minutes())
                              .map(Attribute("cep_leftKey") = 1)
                              .intervalJoinWith(Query::from("defaultLogicalC").map(Attribute("cep_rightKey") = 1))
                              .where(Attribute("cep_leftKey") == Attribute("cep_rightKey"))
                              .lowerBound(EventTime(Attribute("timestamp")), 0, Minutes())
                              .upperBound(3, Minutes());
    auto queryPlan1 = queryABC.getQueryPlan();
    LogicalOperatorPtr sinkABC = LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan1->appendOperatorAsNewRoot(sinkABC);

    //Comparison of the expected and the actual generated query plan
    EXPECT_EQ(queryPlanToString(queryPlan1), queryPlanToString(patternPlanABC));
}

TEST_F(PatternParsingServiceTest, MixedOperatorPattern) {
    //pattern string as received from the NES UI
    std::string patternStringBCA = "PATTERN test:= A AND (B SEQ C) FROM defaultLogicalA AS A, defaultLogicalB AS B, "
                                   "defaultLogicalC AS C WITHIN SLIDING [timestamp, SIZE 3 MINUTE, SLIDE 1 MINUTE] INTO PRINT ";

    std::shared_ptr<QueryParsingService> patternParsingService;
    QueryPlanPtr patternPlanBCA = patternParsingService->createPatternFromCodeString(patternStringBCA);

    NES::Query queryBCA =
        NES::Query::from("defaultLogicalB")
            .seqWith(NES::Query::from("defaultLogicalC"))
            .window(NES::Windowing::SlidingWindow::of(EventTime(Attribute("timestamp")), Minutes(3), Minutes(1)))
            .andWith(NES::Query::from("defaultLogicalA"))
            .window(NES::Windowing::SlidingWindow::of(EventTime(Attribute("timestamp")), Minutes(3), Minutes(1)));

    auto queryPlan2 = queryBCA.getQueryPlan();
    LogicalOperatorPtr sinkBCA = LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan2->appendOperatorAsNewRoot(sinkBCA);

    //Comparison of the expected and the actual generated query plan
    EXPECT_EQ(queryPlanToString(queryPlan2), queryPlanToString(patternPlanBCA));
}

// TODO see #5089
TEST_F(PatternParsingServiceTest, DISABLED_SequenceAndTimesPattern) {
    //pattern string as received from the NES UI
    std::string patternString = "PATTERN test := (((A SEQ B) AND C)[3:10]) FROM default_logical AS A, default_logical_b AS B, "
                                "default_logical_c AS C WITHIN SLIDING [timestamp, SIZE 3 MINUTE, SLIDE 1 MINUTE] INTO PRINT";
    // "PATTERN test:= (((A SEQ B) AND C)[3:10]) FROM default_logical AS A, default_logical_b AS B, default_logical_c AS C WITHIN [3 MINUTE] INTO Print :: testSink ";
    std::shared_ptr<QueryParsingService> patternParsingService;
    QueryPlanPtr patternPlan = patternParsingService->createPatternFromCodeString(patternString);

    //expected result
    QueryPlanPtr queryPlanA = QueryPlan::create();
    QueryPlanPtr queryPlanB = QueryPlan::create();
    QueryPlanPtr queryPlanC = QueryPlan::create();
    LogicalOperatorPtr source1 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"));
    LogicalOperatorPtr source2 =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical_b"));
    LogicalOperatorPtr source3 =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical_c"));
    queryPlanA->addRootOperator(source1);
    queryPlanB->addRootOperator(source2);
    queryPlanC->addRootOperator(source3);
    NES::Query query = Query(queryPlanA)
                           .seqWith(Query(queryPlanB))
                           .window(NES::Windowing::SlidingWindow::of(EventTime(Attribute("timestamp")), Minutes(3), Minutes(1)))
                           .andWith(Query(queryPlanC))
                           .window(NES::Windowing::SlidingWindow::of(EventTime(Attribute("timestamp")), Minutes(3), Minutes(1)))
                           .times(3, 10)
                           .window(NES::Windowing::SlidingWindow::of(EventTime(Attribute("timestamp")), Minutes(3), Minutes(1)));
    queryPlanA = query.getQueryPlan();
    LogicalOperatorPtr sink = LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlanA->appendOperatorAsNewRoot(sink);

    //Comparison of the expected and the actual generated query plan
    EXPECT_EQ(queryPlanToString(queryPlanA), queryPlanToString(patternPlan));
}

TEST_F(PatternParsingServiceTest, simplePatternWithSelect) {
    //pattern string as received from the NES UI
    std::string patternString = "PATTERN test:= (A) FROM defaultLogical AS A WITHIN SLIDING [timestamp, SIZE 3 MINUTE, SLIDE 1 "
                                "MINUTE] SELECT ce := [id=TU] INTO PRINT";
    //TODO fix Lexer, remove filter condition from output, i.e., no TU #2933
    std::shared_ptr<QueryParsingService> patternParsingService;
    QueryPlanPtr patternPlan = patternParsingService->createPatternFromCodeString(patternString);

    //expected result
    QueryPlanPtr queryPlan = QueryPlan::create();
    LogicalOperatorPtr source = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("defaultLogical"));
    queryPlan->addRootOperator(source);
    std::vector<ExpressionNodePtr> expression;
    expression.push_back(Attribute("id").getExpressionNode());
    LogicalOperatorPtr projection = LogicalOperatorFactory::createProjectionOperator(expression);
    queryPlan->appendOperatorAsNewRoot(projection);
    LogicalOperatorPtr sink = LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(sink);

    //Comparison of the expected and the actual generated query plan
    EXPECT_EQ(queryPlanToString(queryPlan), queryPlanToString(patternPlan));
}

TEST_F(PatternParsingServiceTest, simplePatternWithMultipleSelectStatements) {
    //pattern string as received from the NES UI
    std::string patternString = "PATTERN test:= (A) FROM defaultLogical AS A SELECT ce := [name=TU, type=random, "
                                "department=DIMA ] INTO PRINT";
    //TODO fix Lexer, remove filter condition from output, i.e., no TU #2933
    std::shared_ptr<QueryParsingService> patternParsingService;
    QueryPlanPtr patternPlan = patternParsingService->createPatternFromCodeString(patternString);

    //expected result
    QueryPlanPtr queryPlan = QueryPlan::create();
    LogicalOperatorPtr source = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("defaultLogical"));
    queryPlan->addRootOperator(source);
    std::vector<ExpressionNodePtr> expression;
    expression.push_back(Attribute("name").getExpressionNode());
    expression.push_back(Attribute("type").getExpressionNode());
    expression.push_back(Attribute("department").getExpressionNode());
    LogicalOperatorPtr projection = LogicalOperatorFactory::createProjectionOperator(expression);
    queryPlan->appendOperatorAsNewRoot(projection);
    LogicalOperatorPtr sink = LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(sink);

    //Comparison of the expected and the actual generated query plan
    EXPECT_EQ(queryPlanToString(queryPlan), queryPlanToString(patternPlan));
}

TEST_F(PatternParsingServiceTest, TimesOperator) {
    //pattern string as received from the NES UI
    std::string patternString =
        "PATTERN test:= (A[2:10]) FROM defaultLogical AS A WITHIN SLIDING [timestamp, SIZE 3 MINUTE, SLIDE 1 MINUTE] INTO PRINT";
    std::shared_ptr<QueryParsingService> patternParsingService;
    QueryPlanPtr patternPlan = patternParsingService->createPatternFromCodeString(patternString);

    QueryPlanPtr queryPlan = QueryPlan::create();
    LogicalOperatorPtr source = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("defaultLogical"));
    queryPlan->appendOperatorAsNewRoot(source);
    NES::Query qTimes = NES::Query(queryPlan).times(2, 10).window(
        NES::Windowing::SlidingWindow::of(EventTime(Attribute("timestamp")), Minutes(3), Minutes(1)));
    queryPlan = qTimes.getQueryPlan();
    LogicalOperatorPtr sink = LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(sink);

    //Comparison of the expected and the actual generated query plan
    EXPECT_EQ(queryPlanToString(queryPlan), queryPlanToString(patternPlan));
}

TEST_F(PatternParsingServiceTest, TimesOperatorExact) {
    //pattern string as received from the NES UI
    std::string patternString =
        "PATTERN test:= (A[2]) FROM defaultLogical AS A WITHIN SLIDING [timestamp, SIZE 3 MINUTE, SLIDE 1 MINUTE] INTO PRINT";
    std::shared_ptr<QueryParsingService> patternParsingService;
    QueryPlanPtr patternPlan = patternParsingService->createPatternFromCodeString(patternString);

    QueryPlanPtr queryPlan = QueryPlan::create();
    LogicalOperatorPtr source = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("defaultLogical"));
    queryPlan->appendOperatorAsNewRoot(source);
    NES::Query qTime = NES::Query(queryPlan).times(2).window(
        NES::Windowing::SlidingWindow::of(EventTime(Attribute("timestamp")), Minutes(3), Minutes(1)));
    queryPlan = qTime.getQueryPlan();
    LogicalOperatorPtr sink = LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(sink);

    //Comparison of the expected and the actual generated query plan
    EXPECT_EQ(queryPlanToString(queryPlan), queryPlanToString(patternPlan));
}

TEST_F(PatternParsingServiceTest, TimesOperatorUnbounded) {
    //pattern string as received from the NES UI
    std::string patternString =
        "PATTERN test:= (A+) FROM defaultLogical AS A WITHIN SLIDING [timestamp, SIZE 3 MINUTE, SLIDE 1 MINUTE] INTO PRINT";
    std::shared_ptr<QueryParsingService> patternParsingService;
    QueryPlanPtr patternPlan = patternParsingService->createPatternFromCodeString(patternString);

    QueryPlanPtr queryPlan = QueryPlan::create();
    LogicalOperatorPtr source = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("defaultLogical"));
    queryPlan->appendOperatorAsNewRoot(source);
    NES::Query qTimes = NES::Query(queryPlan).times().window(
        NES::Windowing::SlidingWindow::of(EventTime(Attribute("timestamp")), Minutes(3), Minutes(1)));
    queryPlan = qTimes.getQueryPlan();
    LogicalOperatorPtr sink = LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(sink);

    //Comparison of the expected and the actual generated query plan
    EXPECT_EQ(queryPlanToString(queryPlan), queryPlanToString(patternPlan));
}

TEST_F(PatternParsingServiceTest, failingPatternWrongSyntaxExpected) {
    //pattern string as received from the NES UI and create query plan from parsing service
    std::string patternString = "PATTERN test := (A AND B) FROM defaultLogical AS A, defaultLogicalB INTO PRINT";
    std::string patternString1 = "PATTERN test:= ";
    std::string patternString2 = "PATTERN test := (A SEQ B) FROM defaultLogical AS A, defaultLogicalB INTO PRINT";
    std::string patternString3 = "PATTERN test := (A[3:10]) FROM defaultLogical AS A, defaultLogicalB INTO PRINT";
    std::string patternString4 = "";
    std::shared_ptr<QueryParsingService> patternParsingService;
    // expected result
    EXPECT_THROW(QueryPlanPtr queryPlan = patternParsingService->createPatternFromCodeString(patternString),
                 Exceptions::RuntimeException);
    EXPECT_ANY_THROW(QueryPlanPtr queryPlan = patternParsingService->createPatternFromCodeString(patternString1));
    EXPECT_THROW(QueryPlanPtr queryPlan = patternParsingService->createPatternFromCodeString(patternString2),
                 Exceptions::RuntimeException);
    EXPECT_THROW(QueryPlanPtr queryPlan = patternParsingService->createPatternFromCodeString(patternString3),
                 Exceptions::RuntimeException);
    EXPECT_ANY_THROW(QueryPlanPtr queryPlan = patternParsingService->createPatternFromCodeString(patternString4));
}

TEST_F(PatternParsingServiceTest, SequencePatternChariteTest) {
    //pattern string as received from the NES UI
    std::string patternString1 =
        "PATTERN test:= A SEQ B FROM default/logical AS A, default/logicalB AS B "
        "WHERE A.currentSpeed == B.allowedSpeed && A.value > 0 && B.value >= 0 "
        "WITHIN SLIDING [timestamp, SIZE 3 MINUTE, SLIDE 1 MINUTE] INTO MQTT(address ws://mosquitto:9001, topic q1-results)";
    std::shared_ptr<QueryParsingService> patternParsingService;
    QueryPlanPtr patternPlan1 = patternParsingService->createPatternFromCodeString(patternString1);

    //pattern string as received from the NES UI
    std::string patternString2 =
        "PATTERN test:= default/logical SEQ default/logicalB FROM default/logical, default/logicalB "
        "WHERE default/logical.currentSpeed == default/logicalB.allowedSpeed && default/logical.value > "
        "0 && default/logicalB.value >= 0 "
        "WITHIN SLIDING [timestamp, SIZE 3 MINUTE, SLIDE 1 MINUTE] INTO MQTT(address ws://mosquitto:9001, topic q1-results)";
    QueryPlanPtr patternPlan2 = patternParsingService->createPatternFromCodeString(patternString2);

    //expected result
    NES::Query qSEQ = NES::Query::from("default/logical")
                          .seqWith(NES::Query::from("default/logicalB"))
                          .window(NES::Windowing::SlidingWindow::of(EventTime(Attribute("timestamp")), Minutes(3), Minutes(1)));
    auto queryPlan = qSEQ.getQueryPlan();
    // add filter for WHERE clause
    LogicalOperatorPtr filterA = LogicalOperatorFactory::createFilterOperator(
        ExpressionNodePtr(GreaterExpressionNode::create(NES::Attribute("default/logical$value").getExpressionNode(),
                                                        NES::ExpressionItem(0).getExpressionNode())));
    // add filter for WHERE clause
    LogicalOperatorPtr filterB = LogicalOperatorFactory::createFilterOperator(
        ExpressionNodePtr(GreaterEqualsExpressionNode::create(NES::Attribute("default/logicalB$value").getExpressionNode(),
                                                              NES::ExpressionItem(0).getExpressionNode())));
    LogicalOperatorPtr filter = LogicalOperatorFactory::createFilterOperator(
        ExpressionNodePtr(EqualsExpressionNode::create(NES::Attribute("default/logical$currentSpeed").getExpressionNode(),
                                                       NES::Attribute("default/logicalB$allowedSpeed").getExpressionNode())));
    queryPlan->appendOperatorAsNewRoot(filterB);
    queryPlan->appendOperatorAsNewRoot(filterA);
    queryPlan->appendOperatorAsNewRoot(filter);
    LogicalOperatorPtr sink4 =
        LogicalOperatorFactory::createSinkOperator(NES::MQTTSinkDescriptor::create("ws://mosquitto:9001", "q1-results"));
    queryPlan->appendOperatorAsNewRoot(sink4);

    //Comparison of the expected and the actual generated query plan
    EXPECT_EQ(queryPlanToString(queryPlan), queryPlanToString(patternPlan1));
    EXPECT_EQ(queryPlanToString(queryPlan), queryPlanToString(patternPlan2));
}
