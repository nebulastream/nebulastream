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

#include <gtest/gtest.h>

#include <API/Query.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>
#include <iostream>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/MQTTSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/NullOutputSinkDescriptor.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Services/PatternParsingService.h>

using namespace NES;

class PatternParsingServiceTest : public testing::Test {

  public:
    /* Will be called before a test is executed. */
    void SetUp() override {
        NES::setupLogging("QueryPlanTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup QueryPlanTest test case.");
    }

    /* Will be called before a test is executed. */
    void TearDown() override { NES_INFO("Setup QueryPlanTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down QueryPlanTest test class."); }
};

TEST_F(PatternParsingServiceTest, simplePattern1HasSink) {


    //Prepare
    std::string patternString =
        "PATTERN test:= (A) FROM default_logical AS A INTO Print :: testSink ";
    std::shared_ptr<PatternParsingService> patternparsingservice;
    QueryPtr query = patternparsingservice->createPatternFromCodeString(patternString);
    const QueryPlanPtr queryPlanPattern = query->getQueryPlan();

    QueryPlanPtr queryPlan = QueryPlan::create();
    LogicalOperatorNodePtr op1 =
        LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("default_logical"));
    queryPlan->appendOperatorAsNewRoot(op1);
    op1 =
        LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(op1);

    //Comparaison
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

TEST_F(PatternParsingServiceTest, simplePattern1HasMultipleSinks) {


    //Prepare
    std::string patternString =
        "PATTERN test:= (A) FROM default_logical AS A INTO Print :: testSink, File :: testSink2, NullOutput :: testSink3  ";
    std::shared_ptr<PatternParsingService> patternparsingservice;
    QueryPtr query = patternparsingservice->createPatternFromCodeString(patternString);
    const QueryPlanPtr queryPlanPattern = query->getQueryPlan();

    QueryPlanPtr queryPlan = QueryPlan::create();
    LogicalOperatorNodePtr op1 =
        LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("default_logical"));
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

    //Comparaison
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

TEST_F(PatternParsingServiceTest, simplePattern1HasTimes1) {


    //Prepare
    std::string patternString =
        "PATTERN test:= (A[2]) FROM default_logical AS A INTO Print :: testSink ";
    std::shared_ptr<PatternParsingService> patternparsingservice;
    QueryPtr query = patternparsingservice->createPatternFromCodeString(patternString);
    const QueryPlanPtr queryPlanPattern = query->getQueryPlan();

    QueryPlanPtr queryPlan = QueryPlan::create();
    LogicalOperatorNodePtr op1 =
        LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("default_logical"));
    queryPlan->appendOperatorAsNewRoot(op1);
    op1=LogicalOperatorFactory::createCEPIterationOperator(2, 2);
    queryPlan->appendOperatorAsNewRoot(op1);
    op1 =
        LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(op1);

    //Comparaison
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
TEST_F(PatternParsingServiceTest, simplePattern1HasTimes2) {


    //Prepare
    std::string patternString =
        "PATTERN test:= (A*) FROM default_logical AS A INTO Print :: testSink ";
    std::shared_ptr<PatternParsingService> patternparsingservice;
    QueryPtr query = patternparsingservice->createPatternFromCodeString(patternString);
    const QueryPlanPtr queryPlanPattern = query->getQueryPlan();

    QueryPlanPtr queryPlan = QueryPlan::create();
    LogicalOperatorNodePtr op1 =
        LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("default_logical"));
    queryPlan->appendOperatorAsNewRoot(op1);
    op1=LogicalOperatorFactory::createCEPIterationOperator(0, LLONG_MAX);
    queryPlan->appendOperatorAsNewRoot(op1);
    op1 =
        LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(op1);

    //Comparaison
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
TEST_F(PatternParsingServiceTest, simplePattern1HasTimes3) {


    //Prepare
    std::string patternString =
        "PATTERN test:= (A+) FROM default_logical AS A INTO Print :: testSink ";
    std::shared_ptr<PatternParsingService> patternparsingservice;
    QueryPtr query = patternparsingservice->createPatternFromCodeString(patternString);
    const QueryPlanPtr queryPlanPattern = query->getQueryPlan();

    QueryPlanPtr queryPlan = QueryPlan::create();
    LogicalOperatorNodePtr op1 =
        LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("default_logical"));
    queryPlan->appendOperatorAsNewRoot(op1);
    op1=LogicalOperatorFactory::createCEPIterationOperator(1, LLONG_MAX);
    queryPlan->appendOperatorAsNewRoot(op1);
    op1 =
        LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(op1);

    //Comparaison
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
TEST_F(PatternParsingServiceTest, simplePattern1HasTimes4) {


    //Prepare
    std::string patternString =
        "PATTERN test:= (A[2+]) FROM default_logical AS A INTO Print :: testSink ";
    std::shared_ptr<PatternParsingService> patternparsingservice;
    QueryPtr query = patternparsingservice->createPatternFromCodeString(patternString);
    const QueryPlanPtr queryPlanPattern = query->getQueryPlan();

    QueryPlanPtr queryPlan = QueryPlan::create();
    LogicalOperatorNodePtr op1 =
        LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("default_logical"));
    queryPlan->appendOperatorAsNewRoot(op1);
    op1=LogicalOperatorFactory::createCEPIterationOperator(2, LLONG_MAX);
    queryPlan->appendOperatorAsNewRoot(op1);
    op1 =
        LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(op1);

    //Comparaison
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
TEST_F(PatternParsingServiceTest, simplePattern1HasTimes5) {


    //Prepare
    std::string patternString =
        "PATTERN test:= (A[2:10]) FROM default_logical AS A INTO Print :: testSink ";
    std::shared_ptr<PatternParsingService> patternparsingservice;
    QueryPtr query = patternparsingservice->createPatternFromCodeString(patternString);
    const QueryPlanPtr queryPlanPattern = query->getQueryPlan();

    QueryPlanPtr queryPlan = QueryPlan::create();
    LogicalOperatorNodePtr op1 =
        LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("default_logical"));
    queryPlan->appendOperatorAsNewRoot(op1);
    op1=LogicalOperatorFactory::createCEPIterationOperator(2, 10);
    queryPlan->appendOperatorAsNewRoot(op1);
    op1 =
        LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(op1);

    //Comparaison
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
    //Prepare
    std::string patternString =
        "PATTERN test:= (A OR B) FROM default_logical AS A, default_logical_b AS B  INTO Print :: testSink ";
    std::shared_ptr<PatternParsingService> patternparsingservice;
    QueryPtr query = patternparsingservice->createPatternFromCodeString(patternString);
    const QueryPlanPtr queryPlanPattern = query->getQueryPlan();

    //expected result
    QueryPlanPtr queryPlan = QueryPlan::create();
    QueryPlanPtr subQueryPlan = QueryPlan::create();
    LogicalOperatorNodePtr op1 =
        LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("default_logical"));
    queryPlan->addRootOperator(op1);
    LogicalOperatorNodePtr op2=LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("default_logical_b"));
    subQueryPlan->addRootOperator(op2);
    NES::Query query1=NES::Query(queryPlan);
    NES::Query query2=NES::Query(subQueryPlan);
    query1.orWith(query2);
    queryPlan=query1.getQueryPlan();
    LogicalOperatorNodePtr op4 =
        LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(op4);


    //Comparaison
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
    //Prepare
    std::string patternString =
        "PATTERN test:= (A SEQ B) FROM default_logical AS A, default_logical_b AS B  INTO Print :: testSink ";
    std::shared_ptr<PatternParsingService> patternparsingservice;
    QueryPtr query = patternparsingservice->createPatternFromCodeString(patternString);
    const QueryPlanPtr queryPlanPattern = query->getQueryPlan();

    //expected result
    QueryPlanPtr queryPlan = QueryPlan::create();
    QueryPlanPtr subQueryPlan = QueryPlan::create();
    LogicalOperatorNodePtr op1 =
        LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("default_logical"));
    queryPlan->addRootOperator(op1);
    LogicalOperatorNodePtr op2=LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("default_logical_b"));
    subQueryPlan->addRootOperator(op2);
    NES::Query query1=NES::Query(queryPlan);
    NES::Query query2=NES::Query(subQueryPlan);
    query1.seqWith(query2);
    queryPlan=query1.getQueryPlan();
    LogicalOperatorNodePtr op4 =
        LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(op4);


    //Comparaison
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

    //Prepare
    std::string patternString =
        "PATTERN test:= (A AND B) FROM default_logical AS A, default_logical_b AS B  INTO Print :: testSink ";
    std::shared_ptr<PatternParsingService> patternparsingservice;
    QueryPtr query = patternparsingservice->createPatternFromCodeString(patternString);
    const QueryPlanPtr queryPlanPattern = query->getQueryPlan();

    //expected result
    QueryPlanPtr queryPlan = QueryPlan::create();
    QueryPlanPtr subQueryPlan = QueryPlan::create();
    LogicalOperatorNodePtr op1 =
        LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("default_logical"));
    queryPlan->addRootOperator(op1);
    LogicalOperatorNodePtr op2=LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("default_logical_b"));
    subQueryPlan->addRootOperator(op2);
    NES::Query query1=NES::Query(queryPlan);
    NES::Query query2=NES::Query(subQueryPlan);
    query1.andWith(query2);
    queryPlan=query1.getQueryPlan();
    LogicalOperatorNodePtr op4 =
        LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(op4);


    //Comparaison
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
    //Prepare
    std::string patternString =
        "PATTERN test:= (A) FROM default_logical AS A WHERE A.currentSpeed< A.allowedSpeed INTO Print :: testSink ";
    std::shared_ptr<PatternParsingService> patternparsingservice;
    QueryPtr query = patternparsingservice->createPatternFromCodeString(patternString);
    const QueryPlanPtr queryPlanPattern = query->getQueryPlan();

    //expected result
    QueryPlanPtr queryPlan = QueryPlan::create();
    QueryPlanPtr subQueryPlan = QueryPlan::create();
    LogicalOperatorNodePtr op1 =
        LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("default_logical"));
    queryPlan->addRootOperator(op1);
    NES::Query query1=NES::Query(queryPlan);
    query1.filter(NES::LessExpressionNode::create(NES::Attribute("currentSpeed").getExpressionNode() ,  NES::Attribute("allowedSpeed").getExpressionNode()));
    queryPlan=query1.getQueryPlan();
    LogicalOperatorNodePtr op4 =
        LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(op4);


    //Comparaison
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
    //Prepare
    std::string patternString =
        "PATTERN test:= (A) FROM default_logical AS A RETURN ce := [name=TU] INTO Print :: testSink ";
    std::shared_ptr<PatternParsingService> patternparsingservice;
    QueryPtr query = patternparsingservice->createPatternFromCodeString(patternString);
    const QueryPlanPtr queryPlanPattern = query->getQueryPlan();

    //expected result
    QueryPlanPtr queryPlan = QueryPlan::create();
    QueryPlanPtr subQueryPlan = QueryPlan::create();
    LogicalOperatorNodePtr op1 =
        LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("default_logical"));
    queryPlan->addRootOperator(op1);

    NES::Query query1=NES::Query(queryPlan);
    query1.map(Attribute("name")="TU");
    queryPlan=query1.getQueryPlan();
    LogicalOperatorNodePtr op4 =
        LogicalOperatorFactory::createSinkOperator(NES::PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(op4);


    //Comparaison
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















