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

#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/LambdaSourceType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/ExecutableType/Array.hpp>
#include <Common/Identifiers.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <NesBaseTest.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/TestUtils.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <iostream>

using namespace std;

namespace NES {

using namespace Configurations;

class MLModelDeploymentTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MLModelDeploymentTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup MLModelDeploymentTest test class.");
    }
};

/**
 * tests integer input to ML inference model
 */
TEST_F(MLModelDeploymentTest, testSimpleMLModelDeployment) {
    struct IrisData {
        uint64_t id;
        uint64_t f1;
        uint64_t f2;
        uint64_t f3;
        uint64_t f4;
        uint64_t target;
    };

    auto irisSchema = Schema::create()
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("f1", DataTypeFactory::createUInt64())
                          ->addField("f2", DataTypeFactory::createUInt64())
                          ->addField("f3", DataTypeFactory::createUInt64())
                          ->addField("f4", DataTypeFactory::createUInt64())
                          ->addField("target", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(IrisData), irisSchema->getSchemaSizeInBytes());

    auto csvSourceType = CSVSourceType::create();
    csvSourceType->setFilePath(std::string(TEST_DATA_DIRECTORY) + "iris_short.csv");
    csvSourceType->setNumberOfTuplesToProducePerBuffer(1);
    csvSourceType->setNumberOfBuffersToProduce(10);
    csvSourceType->setSkipHeader(false);

    string query = R"(Query::from("irisData").inferModel(")" + std::string(TEST_DATA_DIRECTORY) + R"(iris_95acc.tflite",
                        {Attribute("f1"), Attribute("f2"), Attribute("f3"), Attribute("f4")},
                        {Attribute("iris0", FLOAT64), Attribute("iris1", FLOAT64), Attribute("iris2", FLOAT64)}).project(Attribute("iris0"), Attribute("iris1"), Attribute("iris2")))";
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("irisData", irisSchema)
                                  .attachWorkerWithCSVSourceToCoordinator("irisData", csvSourceType)
                                  .validate()
                                  .setupTopology();

    struct Output {
        double iris0;
        double iris1;
        double iris2;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (iris0 == rhs.iris0 && iris1 == rhs.iris1 && iris2 == rhs.iris2);
        }
    };

    std::vector<Output> expectedOutput = {{0, 1, 0 },
                                          {0, 1, 0 },
                                          {0, 1, 0},
                                          {0, 1, 0},
                                          {0, 1, 0},
                                          {0, 1, 0 },
                                          {0, 1, 0 },
                                          {0, 1, 0},
                                          {0, 1, 0},
                                          {0, 1, 0}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "TopDown", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
}

/**
 *  tests double input to ML inference model
 */
TEST_F(MLModelDeploymentTest, testSimpleMLModelDeploymentDoubles) {
    struct IrisData {
        uint64_t id;
        double f1;
        double f2;
        double f3;
        double f4;
        uint64_t target;
    };

    auto irisSchema = Schema::create()
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("f1", DataTypeFactory::createDouble())
                          ->addField("f2", DataTypeFactory::createDouble())
                          ->addField("f3", DataTypeFactory::createDouble())
                          ->addField("f4", DataTypeFactory::createDouble())
                          ->addField("target", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(IrisData), irisSchema->getSchemaSizeInBytes());

    auto csvSourceType = CSVSourceType::create();
    csvSourceType->setFilePath(std::string(TEST_DATA_DIRECTORY) + "iris_short.csv");
    csvSourceType->setNumberOfTuplesToProducePerBuffer(1);
    csvSourceType->setNumberOfBuffersToProduce(10);
    csvSourceType->setSkipHeader(false);

    string query = R"(Query::from("irisData").inferModel(")" + std::string(TEST_DATA_DIRECTORY) + R"(iris_95acc.tflite",
                        {Attribute("f1"), Attribute("f2"), Attribute("f3"), Attribute("f4")},
                        {Attribute("iris0", FLOAT64), Attribute("iris1", FLOAT64), Attribute("iris2", FLOAT64)}).project(Attribute("iris0"), Attribute("iris1"), Attribute("iris2")))";
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("irisData", irisSchema)
                                  .attachWorkerWithCSVSourceToCoordinator("irisData", csvSourceType)
                                  .validate()
                                  .setupTopology();

    struct Output {
        double iris0;
        double iris1;
        double iris2;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (iris0 == rhs.iris0 && iris1 == rhs.iris1 && iris2 == rhs.iris2);
        }
    };

    std::vector<Output> expectedOutput = {{0, 1, 0 },
                                          {0, 1, 0 },
                                          {0, 1, 0},
                                          {0, 1, 0},
                                          {0, 1, 0},
                                          {0, 1, 0 },
                                          {0, 1, 0 },
                                          {0, 1, 0},
                                          {0, 1, 0},
                                          {0, 1, 0}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "TopDown", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
}

/**
 * tests boolean input to ml inference operator
 */
TEST_F(MLModelDeploymentTest, testSimpleMLModelDeploymentBoolean) {

    auto irisSchema = Schema::create()
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("f1", DataTypeFactory::createBoolean())
                          ->addField("f2", DataTypeFactory::createBoolean())
                          ->addField("f3", DataTypeFactory::createBoolean())
                          ->addField("f4", DataTypeFactory::createBoolean())
                          ->addField("target", DataTypeFactory::createUInt64());

    auto csvSourceType = CSVSourceType::create();
    csvSourceType->setFilePath(std::string(TEST_DATA_DIRECTORY) + "iris_short_bool.csv");
    csvSourceType->setNumberOfTuplesToProducePerBuffer(1);
    csvSourceType->setNumberOfBuffersToProduce(10);
    csvSourceType->setSkipHeader(false);

    string query = R"(Query::from("irisData").inferModel(")" + std::string(TEST_DATA_DIRECTORY) + R"(iris_95acc.tflite",
                        {Attribute("f1"), Attribute("f2"), Attribute("f3"), Attribute("f4")},
                        {Attribute("iris0", FLOAT64), Attribute("iris1", FLOAT64), Attribute("iris2", FLOAT64)}).project(Attribute("iris0"), Attribute("iris1"), Attribute("iris2")))";
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("irisData", irisSchema)
                                  .attachWorkerWithCSVSourceToCoordinator("irisData", csvSourceType)
                                  .validate()
                                  .setupTopology();

    struct Output {
        double iris0;
        double iris1;
        double iris2;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (iris0 == rhs.iris0 && iris1 == rhs.iris1 && iris2 == rhs.iris2);
        }
    };

    std::vector<Output> expectedOutput = {{0, 1, 0 },
                                          {0, 1, 0 },
                                          {0, 1, 0},
                                          {0, 1, 0},
                                          {0, 1, 0},
                                          {0, 1, 0 },
                                          {0, 1, 0 },
                                          {0, 1, 0},
                                          {0, 1, 0},
                                          {0, 1, 0}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "TopDown", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
}

}// namespace NES