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

#include <API/QueryAPI.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <gtest/gtest.h>
#include <iostream>

using namespace std;

namespace NES {

using namespace Configurations;

struct Output {
    float iris0;
    float iris1;
    float iris2;

    // overload the == operator to check if two instances are the same
    bool operator==(Output const& rhs) const { return (iris0 == rhs.iris0 && iris1 == rhs.iris1 && iris2 == rhs.iris2); }
};

class TensorflowInferenceDeploymentTest
    : public Testing::BaseIntegrationTest,
      public testing::WithParamInterface<std::tuple<std::string, SchemaPtr, std::string, std::vector<Output>>> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MLModelDeploymentTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup MLModelDeploymentTest test class.");
    }

    // The following methods create the test data for the parameterized test.
    // The test data is a four-tuple which contains the name of the test, the input schema,
    // the CSV file containing the operator input, and the expected model output.
    static auto createBooleanTestData() {
        return std::make_tuple("Boolean",
                               Schema::create()
                                   ->addField("id", DataTypeFactory::createUInt64())
                                   ->addField("f1", DataTypeFactory::createBoolean())
                                   ->addField("f2", DataTypeFactory::createBoolean())
                                   ->addField("f3", DataTypeFactory::createBoolean())
                                   ->addField("f4", DataTypeFactory::createBoolean())
                                   ->addField("target", DataTypeFactory::createUInt64()),
                               "iris_short_bool.csv",
                               std::vector<Output>{{0.43428239, 0.31287873, 0.25283894},
                                                   {0.43428239, 0.31287873, 0.25283894},
                                                   {0.43428239, 0.31287873, 0.25283894},
                                                   {0.43428239, 0.31287873, 0.25283894},
                                                   {0.43428239, 0.31287873, 0.25283894},
                                                   {0.43428239, 0.31287873, 0.25283894},
                                                   {0.43428239, 0.31287873, 0.25283894},
                                                   {0.43428239, 0.31287873, 0.25283894},
                                                   {0.43428239, 0.31287873, 0.25283894},
                                                   {0.43428239, 0.31287873, 0.25283894}});
    }

    static auto createFloatTestData() {
        return std::make_tuple("Float",
                               Schema::create()
                                   ->addField("id", DataTypeFactory::createUInt64())
                                   ->addField("f1", DataTypeFactory::createFloat())
                                   ->addField("f2", DataTypeFactory::createFloat())
                                   ->addField("f3", DataTypeFactory::createFloat())
                                   ->addField("f4", DataTypeFactory::createFloat())
                                   ->addField("target", DataTypeFactory::createUInt64()),
                               "iris_short.csv",
                               std::vector<Output>{{{0.86352879, 0.12861125, 0.0078599472},
                                                    {0.82480621, 0.16215269, 0.013041073},
                                                    {0.84584343, 0.14335836, 0.010798273},
                                                    {0.81788188, 0.16869366, 0.013424426},
                                                    {0.86857224, 0.12411855, 0.0073091877},
                                                    {0.8524667, 0.13982011, 0.007713221},
                                                    {0.84102476, 0.14806809, 0.010907203},
                                                    {0.84742284, 0.14333251, 0.0092447177},
                                                    {0.80810225, 0.17601806, 0.01587967},
                                                    {0.82949907, 0.15858534, 0.011915659}}});
    }

    static auto createIntTestData() {
        return std::make_tuple("Int",
                               Schema::create()
                                   ->addField("id", DataTypeFactory::createUInt64())
                                   ->addField("f1", DataTypeFactory::createUInt64())
                                   ->addField("f2", DataTypeFactory::createUInt64())
                                   ->addField("f3", DataTypeFactory::createUInt64())
                                   ->addField("f4", DataTypeFactory::createUInt64())
                                   ->addField("target", DataTypeFactory::createUInt64()),
                               "iris_short.csv",
                               std::vector<Output>{{0.43428239, 0.31287873, 0.25283894},
                                                   {0.43428239, 0.31287873, 0.25283894},
                                                   {0.43428239, 0.31287873, 0.25283894},
                                                   {0.43428239, 0.31287873, 0.25283894},
                                                   {0.43428239, 0.31287873, 0.25283894},
                                                   {0.43428239, 0.31287873, 0.25283894},
                                                   {0.43428239, 0.31287873, 0.25283894},
                                                   {0.43428239, 0.31287873, 0.25283894},
                                                   {0.43428239, 0.31287873, 0.25283894},
                                                   {0.43428239, 0.31287873, 0.25283894}});
    }
};

/**
 * tests mixed input to ml inference operator
 *
 * Disabled because it is not clear what this test tests. There is no code path that supports different data types in the input values. The results are also non-deterministic.
 */
TEST_F(TensorflowInferenceDeploymentTest, DISABLED_testSimpleMLModelDeploymentMixedTypes) {
    struct IrisData {
        uint64_t id;
        float f1;
        uint32_t f2;
        int8_t f3;
        int64_t f4;
        uint64_t target;
    };

    auto irisSchema = Schema::create()
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("f1", DataTypeFactory::createFloat())
                          ->addField("f2", DataTypeFactory::createUInt32())
                          ->addField("f3", DataTypeFactory::createInt8())
                          ->addField("f4", DataTypeFactory::createInt64())
                          ->addField("target", DataTypeFactory::createUInt64());

    auto csvSourceType = CSVSourceType::create();
    csvSourceType->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "iris_short_bool.csv");
    csvSourceType->setNumberOfTuplesToProducePerBuffer(1);
    csvSourceType->setNumberOfBuffersToProduce(10);
    csvSourceType->setSkipHeader(false);

    //We set the predictions data type to FLOAT32 since the trained iris_95acc.tflite model defines tensors of data type float32 as output tensors.
    auto query = Query::from("irisData")
                     .inferModel(std::filesystem::path(TEST_DATA_DIRECTORY) / "iris_95acc.tflite",
                                 {Attribute("f1"), Attribute("f2"), Attribute("f3"), Attribute("f4")},
                                 {Attribute("iris0", BasicType::FLOAT32),
                                  Attribute("iris1", BasicType::FLOAT32),
                                  Attribute("iris2", BasicType::FLOAT32)})
                     .project(Attribute("iris0"), Attribute("iris1"), Attribute("iris2"));
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .enableNautilus()
                                  .addLogicalSource("irisData", irisSchema)
                                  .attachWorkerWithCSVSourceToCoordinator("irisData", csvSourceType)
                                  .validate()
                                  .setupTopology();

    struct Output {
        float iris0;
        float iris1;
        float iris2;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const { return (iris0 == rhs.iris0 && iris1 == rhs.iris1 && iris2 == rhs.iris2); }
    };

    std::vector<Output> expectedOutput = {
        {0.4731167, 0.31782052, 0.2090628},
        {0.4731167, 0.31782052, 0.2090628},
        {0.4731167, 0.31782052, 0.2090628},
        {0.4731167, 0.31782052, 0.2090628},
        {0.4731167, 0.31782052, 0.2090628},
        {0.4731167, 0.31782052, 0.2090628},
        {0.4731167, 0.31782052, 0.2090628},
        {0.4731167, 0.31782052, 0.2090628},
        {0.4731167, 0.31782052, 0.2090628},
        {0.4731167, 0.31782052, 0.2090628},
    };

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "TopDown", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    for (size_t i = 0; i < actualOutput.size(); ++i) {
        EXPECT_FLOAT_EQ(expectedOutput[i].iris0, actualOutput[i].iris0);
        EXPECT_FLOAT_EQ(expectedOutput[i].iris1, actualOutput[i].iris1);
        EXPECT_FLOAT_EQ(expectedOutput[i].iris2, actualOutput[i].iris2);
    }
}

TEST_P(TensorflowInferenceDeploymentTest, DISABLED_testSimpleMLModelDeployment) {

    auto irisSchema = std::get<1>(GetParam());

    auto csvSourceType = CSVSourceType::create();
    csvSourceType->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / std::get<2>(GetParam()));
    csvSourceType->setNumberOfTuplesToProducePerBuffer(1);
    csvSourceType->setNumberOfBuffersToProduce(10);
    csvSourceType->setSkipHeader(false);

    //We set the predictions data type to FLOAT32 since the trained iris_95acc.tflite model defines tensors of data type float32 as output tensors.
    auto query = Query::from("irisData")
                     .inferModel(std::filesystem::path(TEST_DATA_DIRECTORY) / "(iris_95acc.tflite",
                                 {Attribute("f1"), Attribute("f2"), Attribute("f3"), Attribute("f4")},
                                 {Attribute("iris0", BasicType::FLOAT32),
                                  Attribute("iris1", BasicType::FLOAT32),
                                  Attribute("iris2", BasicType::FLOAT32)})
                     .project(Attribute("iris0"), Attribute("iris1"), Attribute("iris2"));
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .enableNautilus()
                                  .addLogicalSource("irisData", irisSchema)
                                  .attachWorkerWithCSVSourceToCoordinator("irisData", csvSourceType)
                                  .validate()
                                  .setupTopology();

    std::vector<Output> expectedOutput = std::get<3>(GetParam());

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "TopDown", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    auto delta = 0.0000001;
    for (size_t i = 0; i < actualOutput.size(); ++i) {
        EXPECT_NEAR(expectedOutput[i].iris0, actualOutput[i].iris0, delta);
        EXPECT_NEAR(expectedOutput[i].iris1, actualOutput[i].iris1, delta);
        EXPECT_NEAR(expectedOutput[i].iris2, actualOutput[i].iris2, delta);
    }
}

INSTANTIATE_TEST_CASE_P(TestInputs,
                        TensorflowInferenceDeploymentTest,
                        ::testing::Values(TensorflowInferenceDeploymentTest::createBooleanTestData(),
                                          TensorflowInferenceDeploymentTest::createFloatTestData(),
                                          TensorflowInferenceDeploymentTest::createIntTestData()),
                        [](const testing::TestParamInfo<TensorflowInferenceDeploymentTest::ParamType>& info) {
                            std::string name = std::get<0>(info.param);
                            return name;
                        });

}// namespace NES