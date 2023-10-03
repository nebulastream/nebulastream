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

#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
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

class ONNXInferenceDeploymentTest
    : public Testing::BaseIntegrationTest,
      public testing::WithParamInterface<std::tuple<std::string, SchemaPtr, std::string, std::vector<Output>>> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MLModelDeploymentTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup MLModelDeploymentTest test class.");
    }
};
/**
* Tests the deployment of a simple ONNX model using base64 encoded input data
*/
TEST_F(ONNXInferenceDeploymentTest, testSimpleMLModelDeploymentUsingONNXAndBase64Encoding) {
    struct IrisData {
        uint64_t id;
        std::string data;
        uint64_t target;
    };

    auto irisSchema = Schema::create()
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("data", DataTypeFactory::createText())
                          ->addField("target", DataTypeFactory::createUInt64());

    auto csvSourceType = CSVSourceType::create("irisData", "irisData1");
    csvSourceType->setFilePath(std::string(TEST_DATA_DIRECTORY) + "iris_base64.csv");
    csvSourceType->setNumberOfTuplesToProducePerBuffer(1);
    csvSourceType->setNumberOfBuffersToProduce(10);
    csvSourceType->setSkipHeader(false);

    auto query = Query::from("irisData")
                     .inferModel(std::string(TEST_DATA_DIRECTORY) + "iris_95acc.onnx",
                                 {Attribute("data")},
                                 {Attribute("iris0", BasicType::FLOAT32),
                                  Attribute("iris1", BasicType::FLOAT32),
                                  Attribute("iris2", BasicType::FLOAT32)})
                     .project(Attribute("iris0"), Attribute("iris1"), Attribute("iris2"));

    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("irisData", irisSchema)
                                  .attachWorkerWithCSVSourceToCoordinator(csvSourceType)
                                  .validate()
                                  .setupTopology();

    struct Output {
        float iris0;
        float iris1;
        float iris2;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const { return (iris0 == rhs.iris0 && iris1 == rhs.iris1 && iris2 == rhs.iris2); }
    };

    std::vector<Output> expectedOutput{
        {0.8635288, 0.12861131, 0.00785995},
        {0.8248063, 0.16215266, 0.01304107},
        {0.8458433, 0.14335841, 0.01079828},
        {0.8178819, 0.16869366, 0.01342443},
        {0.8635288, 0.12861131, 0.00785995},
        {0.8248063, 0.16215266, 0.01304107},
        {0.8458433, 0.14335841, 0.01079828},
        {0.8178819, 0.16869366, 0.01342443},
        {0.8635288, 0.12861131, 0.00785995},
        {0.8178819, 0.16869366, 0.01342443},
    };

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "TopDown", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    for (size_t i = 0; i < actualOutput.size(); ++i) {
        EXPECT_FLOAT_EQ(expectedOutput[i].iris0, actualOutput[i].iris0);
        EXPECT_FLOAT_EQ(expectedOutput[i].iris1, actualOutput[i].iris1);
        EXPECT_FLOAT_EQ(expectedOutput[i].iris2, actualOutput[i].iris2);
    }
}
}// namespace NES