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
#include <NesBaseTest.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/NullOutputSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Services/PatternParsingService.h>
#include <Util/Logger/Logger.hpp>
#include <climits>
#include <gtest/gtest.h>
#include <iostream>
#include <regex>

using namespace NES;
/*
 * This test checks for the correctness of the pattern queries created by the NESPL Parsing Service.
 */
class PatternParsingServiceTest : public Testing::TestWithErrorHandling<testing::Test> {

  public:
    /* Will be called before a test is executed. */
    void SetUp() override {
        NES::Logger::setupLogging("QueryPlanTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup QueryPlanTest test case.");
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_INFO("Setup QueryPlanTest test case.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        NES_INFO("Tear down QueryPlanTest test class.");
    }
};

TEST_F(PatternParsingServiceTest, simplePattern1HasSink) {
    //pattern string as received from the NES UI
    std::string patternString =
        "PATTERN test:= (A) FROM default_logical AS A INTO Print :: testSink ";
    std::shared_ptr<PatternParsingService> patternParsingService;
    QueryPtr query = patternParsingService->createPatternFromCodeString(patternString);
    const QueryPlanPtr patternPlan = query->getQueryPlan();
    // expected result
    QueryPlanPtr queryPlan = QueryPlan::create();
    LogicalOperatorNodePtr source =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"));
    queryPlan->appendOperatorAsNewRoot(source);
    LogicalOperatorNodePtr sink = LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(sink);

    //comparison of the expected and the actual generated query plan
    std::regex r2("[0-9]");
    std::string patternPlanStr = std::regex_replace(patternPlan->toString(), r2, "");
    std::string queryPlanStr = std::regex_replace(queryPlan->toString(), r2, "");
    EXPECT_EQ( queryPlanStr,patternPlanStr);
}

TEST_F(PatternParsingServiceTest, simplePattern1HasMultipleSinks) {
    //pattern string as received from the NES UI
    std::string patternString =
        "PATTERN test:= (A) FROM default_logical AS A INTO Print :: testSink, File :: testSink2, NullOutput :: testSink3  ";
    std::shared_ptr<PatternParsingService> patternParsingService;
    QueryPtr query = patternParsingService->createPatternFromCodeString(patternString);
    const QueryPlanPtr patternPlan = query->getQueryPlan();
    // expected results
    QueryPlanPtr queryPlan = QueryPlan::create();
    LogicalOperatorNodePtr op1 =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"));
    LogicalOperatorNodePtr op2 =
        LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    LogicalOperatorNodePtr op3 =
        LogicalOperatorFactory::createSinkOperator(NES::FileSinkDescriptor::create("testSink2"));
    LogicalOperatorNodePtr op4 =
        LogicalOperatorFactory::createSinkOperator(NES::NullOutputSinkDescriptor::create());
    op2->addChild(op1);
    op3->addChild(op1);
    op4->addChild(op1);
    queryPlan->addRootOperator(op2);
    queryPlan->addRootOperator(op3);
    queryPlan->addRootOperator(op4);

    //Comparison of the expected and the actual generated query plan
    std::regex r2("[0-9]");
    std::string patternPlanStr = std::regex_replace(patternPlan->toString(), r2, "");
    std::string queryPlanStr = std::regex_replace(queryPlan->toString(), r2, "");
    EXPECT_EQ( queryPlanStr,patternPlanStr);
}

//TODO Araine stopped here, no parsing of windows
TEST_F(PatternParsingServiceTest, simplePattern1HasTimes1) {
    //pattern string as received from the NES UI
    std::string patternString =
        "PATTERN test:= (A[2]) FROM default_logical AS A WITHIN [3 MINUTE] INTO Print :: testSink ";
    std::shared_ptr<PatternParsingService> patternParsingService;
    QueryPtr query = patternParsingService->createPatternFromCodeString(patternString);
    const QueryPlanPtr patternPlan = query->getQueryPlan();

    QueryPlanPtr queryPlan = QueryPlan::create();
    LogicalOperatorNodePtr op1 =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"));
    queryPlan->appendOperatorAsNewRoot(op1);

    op1 = LogicalOperatorFactory::createCEPIterationOperator(2, 2);
    queryPlan->appendOperatorAsNewRoot(op1);
    op1 = LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(op1);

    //Comparison of the expected and the actual generated query plan
    std::regex r2("[0-9]");
    NES_DEBUG(patternPlan->toString())
    std::string patternPlanStr = std::regex_replace(patternPlan->toString(), r2, "");
    NES_DEBUG("PatternParsingServiceTest:" + patternPlanStr);
    std::string queryPlanStr = std::regex_replace(queryPlan->toString(), r2, "");
    NES_DEBUG("PatternParsingServiceTest:" + queryPlanStr);
    EXPECT_EQ( queryPlanStr,patternPlanStr);
}

TEST_F(PatternParsingServiceTest, DISABLEDsimplePattern1HasTimes2) {
    //pattern string as received from the NES UI
    std::string patternString =
        "PATTERN test:= (A*) FROM default_logical AS A INTO Print :: testSink ";
    std::shared_ptr<PatternParsingService> patternParsingService;
    QueryPtr query = patternParsingService->createPatternFromCodeString(patternString);
    const QueryPlanPtr queryPlanPattern = query->getQueryPlan();

    QueryPlanPtr queryPlan = QueryPlan::create();
    LogicalOperatorNodePtr op1 =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"));
    queryPlan->appendOperatorAsNewRoot(op1);
    op1=LogicalOperatorFactory::createCEPIterationOperator(0, LLONG_MAX);
    queryPlan->appendOperatorAsNewRoot(op1);
    op1 = LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(op1);

    //Comparison of the expected and the actual generated query plan
    std::string queryPlanString=queryPlan->toString();
    std::string queryPlanPatternString=queryPlanPattern->toString();
    int current = 0;
    for (unsigned long i = 0; i <= queryPlanString.size() ; ++i) {
        if(!isdigit(queryPlanString[i])){
            queryPlanString[current] = queryPlanString[i];
            current++;
        }
    }
    queryPlanString=queryPlanString.substr(0,current);
    current=0;
    for (unsigned long i = 0; i <= queryPlanPatternString.size() ; ++i) {
        if(!isdigit(queryPlanPatternString[i])){
            queryPlanPatternString[current] = queryPlanPatternString[i];
            current++;
        }
    }
    queryPlanPatternString = queryPlanPatternString.substr(0,current);
    EXPECT_TRUE( queryPlanString == queryPlanPatternString);
}

TEST_F(PatternParsingServiceTest, DISABLEDsimplePattern1HasTimes3) {
    //pattern string as received from the NES UI
    std::string patternString =
        "PATTERN test:= (A+) FROM default_logical AS A INTO Print :: testSink ";
    std::shared_ptr<PatternParsingService> patternParsingService;
    QueryPtr query = patternParsingService->createPatternFromCodeString(patternString);
    const QueryPlanPtr queryPlanPattern = query->getQueryPlan();

    QueryPlanPtr queryPlan = QueryPlan::create();
    LogicalOperatorNodePtr op1 =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"));
    queryPlan->appendOperatorAsNewRoot(op1);
    op1 = LogicalOperatorFactory::createCEPIterationOperator(1, LLONG_MAX);
    queryPlan->appendOperatorAsNewRoot(op1);
    op1 = LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(op1);

    //Comparison of the expected and the actual generated query plan
    std::string queryPlanString=queryPlan->toString();
    std::string queryPlanPatternString=queryPlanPattern->toString();
    int current = 0;
    for (unsigned long i = 0; i <= queryPlanString.size() ; ++i) {
        if(!isdigit(queryPlanString[i])){
            queryPlanString[current] = queryPlanString[i];
            current++;
        }
    }
    queryPlanString=queryPlanString.substr(0,current);
    current=0;
    for (unsigned long i = 0; i <= queryPlanPatternString.size() ; ++i) {
        if(!isdigit(queryPlanPatternString[i])){
            queryPlanPatternString[current] = queryPlanPatternString[i];
            current++;
        }
    }
    queryPlanPatternString=queryPlanPatternString.substr(0,current);
    EXPECT_TRUE( queryPlanString == queryPlanPatternString);
}

TEST_F(PatternParsingServiceTest, DISABLEDsimplePattern1HasTimes4) {
    //pattern string as received from the NES UI
    std::string patternString =
        "PATTERN test:= (A[2+]) FROM default_logical AS A INTO Print :: testSink ";
    std::shared_ptr<PatternParsingService> patternParsingService;
    QueryPtr query = patternParsingService->createPatternFromCodeString(patternString);
    const QueryPlanPtr queryPlanPattern = query->getQueryPlan();

    QueryPlanPtr queryPlan = QueryPlan::create();
    LogicalOperatorNodePtr op1 =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"));
    queryPlan->appendOperatorAsNewRoot(op1);
    op1=LogicalOperatorFactory::createCEPIterationOperator(2, LLONG_MAX);
    queryPlan->appendOperatorAsNewRoot(op1);
    op1 = LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(op1);

    //Comparison of the expected and the actual generated query plan
    std::string queryPlanString=queryPlan->toString();
    std::string queryPlanPatternString=queryPlanPattern->toString();
    int current = 0;
    for (unsigned long i = 0; i <= queryPlanString.size() ; ++i) {
        if(!isdigit(queryPlanString[i])){
            queryPlanString[current] = queryPlanString[i];
            current++;
        }
    }
    queryPlanString=queryPlanString.substr(0,current);
    current=0;
    for (unsigned long i = 0; i <= queryPlanPatternString.size() ; ++i) {
        if(!isdigit(queryPlanPatternString[i])){
            queryPlanPatternString[current] = queryPlanPatternString[i];
            current++;
        }
    }
    queryPlanPatternString = queryPlanPatternString.substr(0,current);
    EXPECT_TRUE( queryPlanString == queryPlanPatternString);
}

TEST_F(PatternParsingServiceTest, DISABLEDsimplePattern1HasTimes5) {
    //pattern string as received from the NES UI
    std::string patternString =
        "PATTERN test:= (A[2:10]) FROM default_logical AS A INTO Print :: testSink ";
    std::shared_ptr<PatternParsingService> patternParsingService;
    QueryPtr query = patternParsingService->createPatternFromCodeString(patternString);
    const QueryPlanPtr queryPlanPattern = query->getQueryPlan();

    QueryPlanPtr queryPlan = QueryPlan::create();
    LogicalOperatorNodePtr op1 =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"));
    queryPlan->appendOperatorAsNewRoot(op1);
    op1=LogicalOperatorFactory::createCEPIterationOperator(2, 10);
    queryPlan->appendOperatorAsNewRoot(op1);
    op1 = LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(op1);

    //Comparison of the expected and the actual generated query plan
    std::string queryPlanString=queryPlan->toString();
    std::string queryPlanPatternString=queryPlanPattern->toString();
    int current = 0;
    for (unsigned long i = 0; i <= queryPlanString.size() ; ++i) {
        if(!isdigit(queryPlanString[i])){
            queryPlanString[current] = queryPlanString[i];
            current++;
        }
    }
    queryPlanString=queryPlanString.substr(0,current);
    current=0;
    for (unsigned long i = 0; i <= queryPlanPatternString.size() ; ++i) {
        if(!isdigit(queryPlanPatternString[i])){
            queryPlanPatternString[current] = queryPlanPatternString[i];
            current++;
        }
    }
    queryPlanPatternString=queryPlanPatternString.substr(0,current);
    EXPECT_TRUE( queryPlanString==queryPlanPatternString);
}

TEST_F(PatternParsingServiceTest, simplePattern1HasOr) {
    //pattern string as received from the NES UI
    std::string patternString =
        "PATTERN test:= (A OR B) FROM default_logical AS A, default_logical_b AS B  INTO Print :: testSink ";
    std::shared_ptr<PatternParsingService> patternParsingService;
    QueryPtr query = patternParsingService->createPatternFromCodeString(patternString);
    const QueryPlanPtr queryPlanPattern = query->getQueryPlan();

    //expected result
    QueryPlanPtr queryPlan = QueryPlan::create();
    QueryPlanPtr subQueryPlan = QueryPlan::create();
    LogicalOperatorNodePtr op1 =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"));
    queryPlan->addRootOperator(op1);
    LogicalOperatorNodePtr op2=LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical_b"));
    subQueryPlan->addRootOperator(op2);
    NES::Query query1=NES::Query(queryPlan);
    NES::Query query2=NES::Query(subQueryPlan);
    query1.orWith(query2);
    queryPlan=query1.getQueryPlan();
    LogicalOperatorNodePtr op4 =
        LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(op4);

    //Comparison of the expected and the actual generated query plan
    std::string queryPlanString=queryPlan->toString();
    std::string queryPlanPatternString=queryPlanPattern->toString();
    int current = 0;
    for (unsigned long i = 0; i <= queryPlanString.size() ; ++i) {
        if(!isdigit(queryPlanString[i])){
            queryPlanString[current] = queryPlanString[i];
            current++;
        }
    }
    queryPlanString=queryPlanString.substr(0,current);
    current=0;
    for (unsigned long i = 0; i <= queryPlanPatternString.size() ; ++i) {
        if(!isdigit(queryPlanPatternString[i])){
            queryPlanPatternString[current] = queryPlanPatternString[i];
            current++;
        }
    }
    queryPlanPatternString=queryPlanPatternString.substr(0,current);
    EXPECT_TRUE( queryPlanString==queryPlanPatternString);
}

TEST_F(PatternParsingServiceTest, simplePattern1HasSeq) {
    //pattern string as received from the NES UI
    std::string patternString =
        "PATTERN test:= (A SEQ B) FROM default_logical AS A, default_logical_b AS B  INTO Print :: testSink ";
    std::shared_ptr<PatternParsingService> patternParsingService;
    QueryPtr query = patternParsingService->createPatternFromCodeString(patternString);
    const QueryPlanPtr queryPlanPattern = query->getQueryPlan();

    //expected result
    QueryPlanPtr queryPlan = QueryPlan::create();
    QueryPlanPtr subQueryPlan = QueryPlan::create();
    LogicalOperatorNodePtr op1 =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"));
    queryPlan->addRootOperator(op1);
    LogicalOperatorNodePtr op2 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical_b"));
    subQueryPlan->addRootOperator(op2);
    NES::Query query1 = NES::Query(queryPlan);
    NES::Query query2 = NES::Query(subQueryPlan);
    query1.seqWith(query2);
    queryPlan = query1.getQueryPlan();
    LogicalOperatorNodePtr op4 =
        LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(op4);

    //Comparison of the expected and the actual generated query plan
    std::string queryPlanString=queryPlan->toString();
    std::string queryPlanPatternString=queryPlanPattern->toString();
    int current = 0;
    for (unsigned long i = 0; i <= queryPlanString.size() ; ++i) {
        if(!isdigit(queryPlanString[i])){
            queryPlanString[current] = queryPlanString[i];
            current++;
        }
    }
    queryPlanString=queryPlanString.substr(0,current);
    current=0;
    for (unsigned long i = 0; i <= queryPlanPatternString.size() ; ++i) {
        if(!isdigit(queryPlanPatternString[i])){
            queryPlanPatternString[current] = queryPlanPatternString[i];
            current++;
        }
    }
    queryPlanPatternString=queryPlanPatternString.substr(0,current);
    EXPECT_TRUE( queryPlanString==queryPlanPatternString);
}

TEST_F(PatternParsingServiceTest, simplePattern1HasAnd) {
    //pattern string as received from the NES UI
    std::string patternString =
        "PATTERN test:= (A AND B) FROM default_logical AS A, default_logical_b AS B  INTO Print :: testSink ";
    std::shared_ptr<PatternParsingService> patternParsingService;
    QueryPtr query = patternParsingService->createPatternFromCodeString(patternString);
    const QueryPlanPtr queryPlanPattern = query->getQueryPlan();

    //expected result
    QueryPlanPtr queryPlan = QueryPlan::create();
    QueryPlanPtr subQueryPlan = QueryPlan::create();
    LogicalOperatorNodePtr op1 =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"));
    queryPlan->addRootOperator(op1);
    LogicalOperatorNodePtr op2=LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical_b"));
    subQueryPlan->addRootOperator(op2);
    NES::Query query1=NES::Query(queryPlan);
    NES::Query query2=NES::Query(subQueryPlan);
    query1.andWith(query2);
    queryPlan=query1.getQueryPlan();
    LogicalOperatorNodePtr op4 =
        LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(op4);

    //Comparison of the expected and the actual generated query plan
    std::string queryPlanString=queryPlan->toString();
    std::string queryPlanPatternString=queryPlanPattern->toString();
    int current = 0;
    for (unsigned long i = 0; i <= queryPlanString.size() ; ++i) {
        if(!isdigit(queryPlanString[i])){
            queryPlanString[current] = queryPlanString[i];
            current++;
        }
    }
    queryPlanString=queryPlanString.substr(0,current);
    current=0;
    for (unsigned long i = 0; i <= queryPlanPatternString.size() ; ++i) {
        if(!isdigit(queryPlanPatternString[i])){
            queryPlanPatternString[current] = queryPlanPatternString[i];
            current++;
        }
    }
    queryPlanPatternString=queryPlanPatternString.substr(0,current);
    EXPECT_TRUE( queryPlanString==queryPlanPatternString);
}

TEST_F(PatternParsingServiceTest, simplePattern1HasFilter) {
    //pattern string as received from the NES UI
    std::string patternString =
        "PATTERN test:= (A) FROM default_logical AS A WHERE A.currentSpeed< A.allowedSpeed INTO Print :: testSink ";
    std::shared_ptr<PatternParsingService> patternParsingService;
    QueryPtr query = patternParsingService->createPatternFromCodeString(patternString);
    const QueryPlanPtr queryPlanPattern = query->getQueryPlan();

    //expected result
    QueryPlanPtr queryPlan = QueryPlan::create();
    QueryPlanPtr subQueryPlan = QueryPlan::create();
    LogicalOperatorNodePtr op1 =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"));
    queryPlan->addRootOperator(op1);
    NES::Query query1=NES::Query(queryPlan);
    query1.filter(NES::LessExpressionNode::create(NES::Attribute("currentSpeed").getExpressionNode() ,  NES::Attribute("allowedSpeed").getExpressionNode()));
    queryPlan=query1.getQueryPlan();
    LogicalOperatorNodePtr op4 =
        LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(op4);

    //Comparison of the expected and the actual generated query plan
    std::string queryPlanString=queryPlan->toString();
    std::string queryPlanPatternString=queryPlanPattern->toString();
    int current = 0;
    for (unsigned long i = 0; i <= queryPlanString.size() ; ++i) {
        if(!isdigit(queryPlanString[i])){
            queryPlanString[current] = queryPlanString[i];
            current++;
        }
    }
    queryPlanString=queryPlanString.substr(0,current);
    current=0;
    for (unsigned long i = 0; i <= queryPlanPatternString.size() ; ++i) {
        if(!isdigit(queryPlanPatternString[i])){
            queryPlanPatternString[current] = queryPlanPatternString[i];
            current++;
        }
    }
    queryPlanPatternString=queryPlanPatternString.substr(0,current);
    EXPECT_TRUE( queryPlanString==queryPlanPatternString);
}

TEST_F(PatternParsingServiceTest, simplePattern1HasMap) {
    //pattern string as received from the NES UI
    std::string patternString =
        "PATTERN test:= (A) FROM default_logical AS A RETURN ce := [name=TU] INTO Print :: testSink ";
    std::shared_ptr<PatternParsingService> patternParsingService;
    QueryPtr query = patternParsingService->createPatternFromCodeString(patternString);
    const QueryPlanPtr queryPlanPattern = query->getQueryPlan();

    //expected result
    QueryPlanPtr queryPlan = QueryPlan::create();
    QueryPlanPtr subQueryPlan = QueryPlan::create();
    LogicalOperatorNodePtr op1 =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"));
    queryPlan->addRootOperator(op1);

    NES::Query query1=NES::Query(queryPlan);
    query1.map(Attribute("name")="TU");
    queryPlan=query1.getQueryPlan();
    LogicalOperatorNodePtr op4 =
        LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(op4);
    //Comparison of the expected and the actual generated query plan
    std::string queryPlanString=queryPlan->toString();
    std::string queryPlanPatternString=queryPlanPattern->toString();
    int current = 0;
    for (unsigned long i = 0; i <= queryPlanString.size() ; ++i) {
        if(!isdigit(queryPlanString[i])){
            queryPlanString[current] = queryPlanString[i];
            current++;
        }
    }
    queryPlanString=queryPlanString.substr(0,current);
    current=0;
    for (unsigned long i = 0; i <= queryPlanPatternString.size() ; ++i) {
        if(!isdigit(queryPlanPatternString[i])){
            queryPlanPatternString[current] = queryPlanPatternString[i];
            current++;
        }
    }
    queryPlanPatternString = queryPlanPatternString.substr(0,current);
    EXPECT_TRUE( queryPlanString == queryPlanPatternString);
}















