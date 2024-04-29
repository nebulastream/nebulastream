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
#include <Services/RequestHandlerService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <gtest/gtest.h>
#include <iostream>

using namespace std;

namespace NES {

using namespace Configurations;

class ONNXInferenceDeploymentTest : public Testing::BaseIntegrationTest {
  public:
    static constexpr auto ALLOWED_ERROR = 0.0001;
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MLModelDeploymentTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup MLModelDeploymentTest test class.");
    }
};
/**
* Tests the deployment of a simple ONNX model using base64 encoded input data
*/
TEST_F(ONNXInferenceDeploymentTest, testSimpleMLModelDeploymentUsingONNXAndBase64Encoding) {
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

    // Expected output
    auto expectedOutput = "0.8635288, 0.12861131, 0.00785995\n"
                          "0.8248063, 0.16215266, 0.01304107\n"
                          "0.8458433, 0.14335841, 0.01079828\n"
                          "0.8178819, 0.16869366, 0.01342443\n"
                          "0.8635288, 0.12861131, 0.00785995\n"
                          "0.8248063, 0.16215266, 0.01304107\n"
                          "0.8458433, 0.14335841, 0.01079828\n"
                          "0.8178819, 0.16869366, 0.01342443\n"
                          "0.8635288, 0.12861131, 0.00785995\n"
                          "0.8178819, 0.16869366, 0.01342443\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput), "TopDown").getOutput();

    // Expecting near a delta
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
    auto actualTuples = TestUtils::countTuples(actualBuffers);
    for (auto i = 0_u64; i < actualTuples; ++i) {
        EXPECT_NEAR(expectedBuffers[0][i]["irisData$iris0"].read<float>(),
                    actualBuffers[0][i]["irisData$iris0"].read<float>(),
                    ALLOWED_ERROR);
        EXPECT_NEAR(expectedBuffers[0][i]["irisData$iris1"].read<float>(),
                    actualBuffers[0][i]["irisData$iris1"].read<float>(),
                    ALLOWED_ERROR);
        EXPECT_NEAR(expectedBuffers[0][i]["irisData$iris2"].read<float>(),
                    actualBuffers[0][i]["irisData$iris2"].read<float>(),
                    ALLOWED_ERROR);
    }
}
}// namespace NES
