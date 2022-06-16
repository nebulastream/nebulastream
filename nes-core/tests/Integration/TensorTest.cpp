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

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-copy-dtor"
#include "../util/NesBaseTest.hpp"
#include <NesBaseTest.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#pragma clang diagnostic pop
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/LambdaSourceType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/ExecutableType/Array.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/TestUtils.hpp>
#include <iostream>

using namespace std;

namespace NES {

using namespace Configurations;

class TensorTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("JoinDeploymentTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup JoinDeploymentTest test class.");
    }
};

/**
* Test deploying join with same data and same schema
 * */
TEST_F(TensorTest, testCreateTensorInSchema) {

    struct Vector {
        float durationMonth;
        float creditAmount;
        float score;
        float age25;
    };

    std::vector<std::size_t> shape;
    shape.push_back(4);

    auto tensorSchema = Schema::create()->addField("value", DataTypeFactory::createTensor(shape, "FLOAT32", "DENSE"));

    auto test = tensorSchema->getSchemaSizeInBytes();

    ASSERT_EQ(sizeof(Vector), test);

    auto csvSourceType = CSVSourceType::create();
    csvSourceType->setFilePath(std::string(TEST_DATA_DIRECTORY) + "GermanCreditAge25.csv");
    csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
    csvSourceType->setNumberOfBuffersToProduce(0);
    csvSourceType->setSkipHeader(false);

    std::string outputFilePath = getTestResourceFolder() / "tensorTest.out";

    string query = R"(Query::from("tensorTest"))";
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("tensorTest", tensorSchema)
                                  .attachWorkerWithCSVSourceToCoordinator("tensorTest", csvSourceType)
                                  .validate()
                                  .setupTopology();

    struct Output {
        int64_t start;
        int64_t end;
        int64_t key;
        uint64_t value1;
        uint64_t id1;
        uint64_t timestamp1;
        uint64_t value2;
        uint64_t id2;
        uint64_t timestamp2;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (start == rhs.start && end == rhs.end && key == rhs.key && value1 == rhs.value1 && id1 == rhs.id1
                    && timestamp1 == rhs.timestamp1 && value2 == rhs.value2 && id2 == rhs.id2 && timestamp2 == rhs.timestamp2);
        }
    };

    std::vector<Output> expectedOutput = {{1000, 2000, 4, 1, 4, 1002, 1, 4, 1002},
                                          {1000, 2000, 12, 1, 12, 1001, 1, 12, 1001},
                                          {2000, 3000, 1, 2, 1, 2000, 2, 1, 2000},
                                          {2000, 3000, 11, 2, 11, 2001, 2, 11, 2001},
                                          {2000, 3000, 16, 2, 16, 2002, 2, 16, 2002}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "TopDown", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}
}// namespace NES