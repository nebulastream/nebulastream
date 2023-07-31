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
// clang-format: off
// clang-format: on
#include <API/QueryAPI.hpp>
#include <API/Schema.hpp>
#include <NesBaseTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestExecutionEngine.hpp>
#include <Util/TestSinkDescriptor.hpp>
#include <Util/TestSourceDescriptor.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <iostream>
#include <utility>

using namespace NES;
using Runtime::TupleBuffer;

// Dump IR
constexpr auto dumpMode = NES::QueryCompilation::QueryCompilerOptions::DumpMode::NONE;

class MapQueryExecutionTest : public Testing::TestWithErrorHandling,
                              public ::testing::WithParamInterface<std::tuple<QueryCompilation::QueryCompilerOptions::QueryCompiler, std::string, std::vector<string>, std::vector<string>, int>> {
                              //public ::testing::WithParamInterface<QueryCompilation::QueryCompilerOptions::QueryCompiler> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MapQueryExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("QueryCatalogServiceTest: Setup QueryCatalogServiceTest test class.");
    }
    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::TestWithErrorHandling::SetUp();
        auto queryCompiler = std::get<0>(GetParam()); //this->GetParam();
        executionEngine = std::make_shared<Testing::TestExecutionEngine>(queryCompiler, dumpMode);
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_DEBUG("QueryCatalogServiceTest: Tear down QueryCatalogServiceTest test case.");
        ASSERT_TRUE(executionEngine->stop());
        Testing::TestWithErrorHandling::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("QueryCatalogServiceTest: Tear down QueryCatalogServiceTest test class."); }

    void fillBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& buf) {
        for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
            buf[recordIndex][0].write<int64_t>(recordIndex);
            buf[recordIndex][1].write<int64_t>(1);
        }
        buf.setNumberOfTuples(10);
    }

    std::shared_ptr<Testing::TestExecutionEngine> executionEngine;

    // The following methods create the test data for the parameterized test.
    // The test data is a four-tuple which contains the nautilus compiler, the name of the test,
    // the field names for the result schema and the names for the query.
    static auto createMapQueryArithmeticTestData(){
        return std::make_tuple(QueryCompilation::QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER,
                               "MapQueryArithmetic",
                               std::vector<string>{"test$one"},
                               std::vector<string>{"id"},
                               1);
    }
    static auto createLogTestData(){
    return std::make_tuple(QueryCompilation::QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER,
                           "MapLogarithmicFunctions",
                           std::vector<string>{"test$log10", "test$log2", "test$ln"},
                           std::vector<string>{"log10", "log2", "ln"},
                           1);
    }
    static auto createTwoMapQueryTestData(){
        return std::make_tuple(QueryCompilation::QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER,
                               "TwoMapQuery",
                               std::vector<string>{"test$new1", "test$new2"},
                               std::vector<string>{"test$new1", "test$new2"},
                               1);
    }
    static auto createAbsTestData(){
    return std::make_tuple(QueryCompilation::QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER,
                           "MapAbsFunction",
                           std::vector<string>{"test$abs"},
                           std::vector<string>{"abs"},
                           -1);
    }
    static auto createTrigTestData(){
        return std::make_tuple(QueryCompilation::QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER,
                               "MapTrigonometricFunctions",
                               std::vector<string>{"test$sin", "test$cos", "test$radians"},
                               std::vector<string>{"sin", "cos", "radians"},
                               1);
    }
};

static auto getExp(std::string exp) {  // Includes the names for the query
    if (exp == "id") {  // MapLogarithmicFunctions
        return Attribute("id") * 2;
    } else if (exp == "log10") {  // MapLogarithmicFunctions
        return LOG10(Attribute("id"));
    } else if (exp == "log2") {
        return LOG2(Attribute("id"));
    } else if (exp == "ln") {
        return LN(Attribute("id"));
    } else if (exp == "test$new1") {  // TwoMapQuery
        return Attribute("test$id") * 2;
    } else if (exp == "test$new2") {
        return Attribute("test$id") + 2;
    } else if (exp == "abs") {  // MapAbsFunctions
        return ABS(Attribute("id"));
    } else if (exp == "sin") {  // MapTrigonometricFunctions
        return SIN(Attribute("id"));
    } else if (exp == "cos") {
        return COS(Attribute("id"));
    } else if (exp == "radians") {
        return RADIANS(Attribute("id"));
    } else {
        return EXP(Attribute("id"));
    }
}
static auto getFunc(std::string func, int input) {  // Includes the names for the EXPECT_EQ statement
    if (func == "test$one") {  // MapLogarithmicFunctions
        return (double) input * 2;
    } else if (func == "test$log10") {  // MapLogarithmicFunctions
        return std::log10(input);
    } else if (func == "test$log2") {
        return std::log2(input);
    } else if (func == "test$ln") {
        return std::log(input);
    } else if (func == "test$new1") {  // TwoMapQuery
        return (double) input * 2;
    } else if (func == "test$new2") {
        return (double) input + 2;
    } else if (func == "test$abs") {  // MapAbsFunctions
        return std::fabs(input);
    } else if (func == "test$sin") {  // MapTrigonometricFunctions
        return std::sin(input);
    } else if (func == "test$cos") {
        return std::cos(input);
    } else if (func == "test$radians") {
        return (input * M_PI) / 180;
    } else {
        return 0.0;
    }
}

/*TEST_P(MapQueryExecutionTest, MapQueryArithmetic) {
    auto schema = Schema::create()->addField("test$id", BasicType::INT64)->addField("test$one", BasicType::INT64);
    auto testSink = executionEngine->createDataSink(schema);
    auto testSourceDescriptor = executionEngine->createDataSource(schema);

    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto query = TestQuery::from(testSourceDescriptor).map(Attribute("id") = Attribute("id") * 2).sink(testSinkDescriptor);
    auto plan = executionEngine->submitQuery(query.getQueryPlan());
    auto source = executionEngine->getDataSource(plan, 0);
    ASSERT_TRUE((bool) source);
    auto inputBuffer = executionEngine->getBuffer(schema);
    fillBuffer(inputBuffer);
    source->emitBuffer(inputBuffer);
    testSink->waitTillCompleted();
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);
    auto resultBuffer = testSink->getResultBuffer(0);

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 10u);
    for (uint32_t recordIndex = 0u; recordIndex < 10u; ++recordIndex) {
        EXPECT_EQ(resultBuffer[recordIndex][0].read<int64_t>(), recordIndex * 2);
        EXPECT_EQ(resultBuffer[recordIndex][1].read<int64_t>(), 1LL);
    }
    ASSERT_TRUE(executionEngine->stopQuery(plan));
    ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

TEST_P(MapQueryExecutionTest, MapLogarithmicFunctions) {
    auto schema = Schema::create()->addField("test$id", BasicType::FLOAT64);

    auto resultSchema = Schema::create()
                            ->addField("test$id", BasicType::FLOAT64)
                            ->addField("test$log10", BasicType::FLOAT64)
                            ->addField("test$log2", BasicType::FLOAT64)
                            ->addField("test$ln", BasicType::FLOAT64);
    auto testSink = executionEngine->createDataSink(resultSchema);
    auto testSourceDescriptor = executionEngine->createDataSource(schema);

    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto query = TestQuery::from(testSourceDescriptor)
                     .map(Attribute("log10") = LOG10(Attribute("id")))
                     .map(Attribute("log2") = LOG2(Attribute("id")))
                     .map(Attribute("ln") = LN(Attribute("id")))
                     .sink(testSinkDescriptor);
    auto plan = executionEngine->submitQuery(query.getQueryPlan());
    auto source = executionEngine->getDataSource(plan, 0);
    ASSERT_TRUE((bool) source);
    // add buffer
    auto inputBuffer = executionEngine->getBuffer(schema);
    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        inputBuffer[recordIndex][0].write<double>(recordIndex);
    }
    inputBuffer.setNumberOfTuples(10);
    source->emitBuffer(inputBuffer);
    testSink->waitTillCompleted();

    // compare results
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);
    auto resultBuffer = testSink->getResultBuffer(0);

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 10u);
    for (uint32_t recordIndex = 0u; recordIndex < 10u; ++recordIndex) {
        EXPECT_EQ(resultBuffer[recordIndex]["test$log10"].read<double>(), std::log10(recordIndex));
        EXPECT_EQ(resultBuffer[recordIndex]["test$log2"].read<double>(), std::log2(recordIndex));
        EXPECT_EQ(resultBuffer[recordIndex]["test$ln"].read<double>(), std::log(recordIndex));
    }
    ASSERT_TRUE(executionEngine->stopQuery(plan));
    ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

TEST_P(MapQueryExecutionTest, TwoMapQuery) {
    auto schema = Schema::create()->addField("test$id", BasicType::INT64);

    auto resultSchema = Schema::create()
                            ->addField("test$id", BasicType::INT64)
                            ->addField("test$new1", BasicType::INT64)
                            ->addField("test$new2", BasicType::INT64);
    auto testSink = executionEngine->createDataSink(resultSchema);
    auto testSourceDescriptor = executionEngine->createDataSource(schema);

    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto query = TestQuery::from(testSourceDescriptor)
                     .map(Attribute("test$new1") = Attribute("test$id") * 2)
                     .map(Attribute("test$new2") = Attribute("test$id") + 2)
                     .sink(testSinkDescriptor);
    auto plan = executionEngine->submitQuery(query.getQueryPlan());
    auto source = executionEngine->getDataSource(plan, 0);
    ASSERT_TRUE((bool) source);
    // add buffer
    auto inputBuffer = executionEngine->getBuffer(schema);
    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        inputBuffer[recordIndex][0].write<int64_t>(recordIndex);
    }
    inputBuffer.setNumberOfTuples(10);
    source->emitBuffer(inputBuffer);
    testSink->waitTillCompleted();

    // compare results
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);
    auto resultBuffer = testSink->getResultBuffer(0);

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 10u);
    for (uint32_t recordIndex = 0u; recordIndex < 10u; ++recordIndex) {
        EXPECT_EQ(resultBuffer[recordIndex]["test$new1"].read<int64_t>(), recordIndex * 2);
        EXPECT_EQ(resultBuffer[recordIndex]["test$new2"].read<int64_t>(), recordIndex + 2);
    }
    ASSERT_TRUE(executionEngine->stopQuery(plan));
    ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

TEST_P(MapQueryExecutionTest, MapAbsFunction) {
    auto schema = Schema::create()->addField("test$id", BasicType::FLOAT64);

    auto resultSchema = Schema::create()->addField("test$id", BasicType::FLOAT64)->addField("test$abs", BasicType::FLOAT64);
    auto testSink = executionEngine->createDataSink(resultSchema);
    auto testSourceDescriptor = executionEngine->createDataSource(schema);

    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto query = TestQuery::from(testSourceDescriptor).map(Attribute("abs") = ABS(Attribute("id"))).sink(testSinkDescriptor);
    auto plan = executionEngine->submitQuery(query.getQueryPlan());
    auto source = executionEngine->getDataSource(plan, 0);
    ASSERT_TRUE((bool) source);
    // add buffer
    auto inputBuffer = executionEngine->getBuffer(schema);
    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        inputBuffer[recordIndex]["test$id"].write<double>((double) -recordIndex);
    }
    inputBuffer.setNumberOfTuples(10);
    source->emitBuffer(inputBuffer);
    testSink->waitTillCompleted();

    // compare results
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);
    auto resultBuffer = testSink->getResultBuffer(0);

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 10u);
    for (uint32_t recordIndex = 0u; recordIndex < 10u; ++recordIndex) {
        EXPECT_EQ(resultBuffer[recordIndex]["test$abs"].read<double>(), std::fabs(recordIndex));
    }
    ASSERT_TRUE(executionEngine->stopQuery(plan));
    ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

TEST_P(MapQueryExecutionTest, MapPowerFunction) {
    auto schema = Schema::create()->addField("test$left$id", BasicType::FLOAT64)->addField("test$right$id", BasicType::FLOAT64);

    auto resultSchema = Schema::create()
                            ->addField("test$left$id", BasicType::FLOAT64)
                            ->addField("test$right$id", BasicType::FLOAT64)
                            ->addField("test$power", BasicType::FLOAT64);
    auto testSink = executionEngine->createDataSink(resultSchema);
    auto testSourceDescriptor = executionEngine->createDataSource(schema);

    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto query = TestQuery::from(testSourceDescriptor)
                     .map(Attribute("power") = POWER(Attribute("left$id"), Attribute("right$id")))
                     .sink(testSinkDescriptor);
    auto plan = executionEngine->submitQuery(query.getQueryPlan());
    auto source = executionEngine->getDataSource(plan, 0);
    ASSERT_TRUE((bool) source);
    // add buffer
    auto inputBuffer = executionEngine->getBuffer(schema);
    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        inputBuffer[recordIndex]["test$left$id"].write<double>((double) 2 * recordIndex);
        inputBuffer[recordIndex]["test$right$id"].write<double>((double) recordIndex);
    }
    inputBuffer.setNumberOfTuples(10);
    source->emitBuffer(inputBuffer);
    testSink->waitTillCompleted();

    // compare results
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);
    auto resultBuffer = testSink->getResultBuffer(0);

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 10u);
    for (uint32_t recordIndex = 0u; recordIndex < 10u; ++recordIndex) {
        EXPECT_EQ(resultBuffer[recordIndex]["test$power"].read<double>(), std::pow(2 * recordIndex, recordIndex));
    }
    ASSERT_TRUE(executionEngine->stopQuery(plan));
    ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

TEST_P(MapQueryExecutionTest, MapTrigonometricFunctions) {
    auto schema = Schema::create()->addField("test$id", BasicType::FLOAT64);

    auto resultSchema = Schema::create()
                            ->addField("test$id", BasicType::FLOAT64)
                            ->addField("test$sin", BasicType::FLOAT64)
                            ->addField("test$cos", BasicType::FLOAT64)
                            ->addField("test$radians", BasicType::FLOAT64);
    auto testSink = executionEngine->createDataSink(resultSchema);
    auto testSourceDescriptor = executionEngine->createDataSource(schema);

    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto query = TestQuery::from(testSourceDescriptor)
                     .map(Attribute("sin") = SIN(Attribute("id")))
                     .map(Attribute("cos") = COS(Attribute("id")))
                     .map(Attribute("radians") = RADIANS(Attribute("id")))
                     .sink(testSinkDescriptor);
    auto plan = executionEngine->submitQuery(query.getQueryPlan());
    auto source = executionEngine->getDataSource(plan, 0);
    ASSERT_TRUE((bool) source);
    // add buffer
    auto inputBuffer = executionEngine->getBuffer(schema);
    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        inputBuffer[recordIndex][0].write<double>(recordIndex);
    }
    inputBuffer.setNumberOfTuples(10);
    source->emitBuffer(inputBuffer);
    testSink->waitTillCompleted();

    // compare results
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);
    auto resultBuffer = testSink->getResultBuffer(0);

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 10u);
    for (uint32_t recordIndex = 0u; recordIndex < 10u; ++recordIndex) {
        EXPECT_EQ(resultBuffer[recordIndex]["test$sin"].read<double>(), std::sin(recordIndex));
        EXPECT_EQ(resultBuffer[recordIndex]["test$cos"].read<double>(), std::cos(recordIndex));
        EXPECT_EQ(resultBuffer[recordIndex]["test$radians"].read<double>(), (recordIndex * M_PI) / 180);
    }
    ASSERT_TRUE(executionEngine->stopQuery(plan));
    ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}*/

TEST_P(MapQueryExecutionTest, AllFunctions) {
    auto schema = Schema::create()->addField("test$id", BasicType::FLOAT64);

    auto resultArray = std::get<2>(GetParam());
    if (resultArray[0] == "test$one") {
         schema = Schema::create()
                          ->addField("test$id", BasicType::INT64)
                          ->addField("test$one", BasicType::INT64);
    }

    auto resultSchema = Schema::create()
                            ->addField("test$id", BasicType::FLOAT64);
    if (resultArray.size() == 1) {
        resultSchema = Schema::create()
                           ->addField("test$id", BasicType::FLOAT64)
                           ->addField(resultArray[0], BasicType::FLOAT64);
    } else if (resultArray.size() == 2) {
        resultSchema = Schema::create()
                           ->addField("test$id", BasicType::FLOAT64)
                           ->addField(resultArray[0], BasicType::FLOAT64)
                           ->addField(resultArray[1], BasicType::FLOAT64);
    } else if (resultArray.size() == 3) {
        resultSchema = Schema::create()
                                ->addField("test$id", BasicType::FLOAT64)
                                ->addField(resultArray[0], BasicType::FLOAT64)
                                ->addField(resultArray[1], BasicType::FLOAT64)
                                ->addField(resultArray[2], BasicType::FLOAT64);
    }

    auto testSink = executionEngine->createDataSink(resultSchema);
    if (resultArray[0] == "test$one") {
        testSink = executionEngine->createDataSink(schema);
    }
    auto testSourceDescriptor = executionEngine->createDataSource(schema);

    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto queryArray = std::get<3>(GetParam());

    auto query = TestQuery::from(testSourceDescriptor)
                     .map(Attribute(queryArray[0]) = getExp(queryArray[0]))
                     .sink(testSinkDescriptor);
    if (queryArray.size() == 2) {
        query = TestQuery::from(testSourceDescriptor)
                    .map(Attribute(queryArray[0]) = getExp(queryArray[0]))
                    .map(Attribute(queryArray[1]) = getExp(queryArray[1]))
                    .sink(testSinkDescriptor);
    } else if (queryArray.size() == 3) {
        query = TestQuery::from(testSourceDescriptor)
                         .map(Attribute(queryArray[0]) = getExp(queryArray[0])) // vorerst so hardcoden schauen obs mit strings funktioniert
                         .map(Attribute(queryArray[1]) = getExp(queryArray[1]))
                         .map(Attribute(queryArray[2]) = getExp(queryArray[2]))
                         .sink(testSinkDescriptor);
    }

    auto plan = executionEngine->submitQuery(query.getQueryPlan());
    auto source = executionEngine->getDataSource(plan, 0);
    ASSERT_TRUE((bool) source);
    // add buffer
    auto inputBuffer =  executionEngine->getBuffer(schema);
    int sign = std::get<4>(GetParam());
    if (resultArray[0] == "test$one") {
        fillBuffer(inputBuffer);
    } else {
        for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
            inputBuffer[recordIndex][0].write<double>(sign * recordIndex);
        }
        inputBuffer.setNumberOfTuples(10);
    }
    source->emitBuffer(inputBuffer);
    testSink->waitTillCompleted();

    // compare results
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);
    auto resultBuffer = testSink->getResultBuffer(0);

    //auto stdFuncArray = std::get<5>(GetParam());

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 10u);
    if (resultArray[0] == "test$one") {
        for (uint32_t recordIndex = 0u; recordIndex < 10u; ++recordIndex) {
            EXPECT_EQ(resultBuffer[recordIndex][0].read<int64_t>(), recordIndex * 2);
            EXPECT_EQ(resultBuffer[recordIndex][1].read<int64_t>(), 1LL);
        }
    } else {
        for (uint32_t recordIndex = 0u; recordIndex < 10u; ++recordIndex) {
            for (uint32_t index = 0; index < resultArray.size(); index++) {
                EXPECT_EQ(resultBuffer[recordIndex][resultArray[index]].read<double>(), getFunc(resultArray[index],sign * recordIndex));
            }
        }
    }
    ASSERT_TRUE(executionEngine->stopQuery(plan));
    ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

INSTANTIATE_TEST_CASE_P(testMapQueries,
                        MapQueryExecutionTest,
                        ::testing::Values(MapQueryExecutionTest::createMapQueryArithmeticTestData(),
                                          MapQueryExecutionTest::createLogTestData(),
                                          MapQueryExecutionTest::createTwoMapQueryTestData(),
                                          MapQueryExecutionTest::createAbsTestData(),
                                          MapQueryExecutionTest::createTrigTestData()
                                          ),
                        [](const testing::TestParamInfo<MapQueryExecutionTest::ParamType>& info) {
                            //return std::string(magic_enum::enum_name(info.param));
                            std::string name = std::get<1>(info.param);
                            return name;
                        });
