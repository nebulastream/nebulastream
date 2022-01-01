
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-copy-dtor"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#pragma clang diagnostic pop
#include <Catalogs/LambdaSourceStreamConfig.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfig.hpp>
#include <Configurations/Worker/WorkerConfig.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/TestUtils.hpp>

#include <Common/ExecutableType/Array.hpp>
#include <Configurations/Sources/CSVSourceConfig.hpp>
#include <Configurations/Sources/SourceConfigFactory.hpp>
#include <iostream>

namespace NES {

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case
static uint64_t restPort = 8081;
static uint64_t rpcPort = 4000;

class SingleWorkerTumblingWindowTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("SingleWorkerTumblingWindowTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup SingleWorkerTumblingWindowTest test class.");
    }

    void SetUp() override {
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
    }

    void TearDown() override { std::cout << "Tear down SingleWorkerTumblingWindowTest class." << std::endl; }
};

/*
 * Testing testHarness utility using one logical source and one physical source
 */
TEST_F(SingleWorkerTumblingWindowTest, testHarnessUtilWithSingleSource) {
    struct Car {
        uint32_t key;
        uint32_t value;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createUInt32())
                         ->addField("value", DataTypeFactory::createUInt32())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    std::string queryWithFilterOperator = R"(Query::from("car").filter(Attribute("key") < 1000))";
    TestHarness testHarness = TestHarness(queryWithFilterOperator, restPort, rpcPort);

    testHarness.addMemorySource("car", carSchema, "car1");

    testHarness.pushElement<Car>({40, 40, 40}, 0);
    testHarness.pushElement<Car>({30, 30, 30}, 0);
    testHarness.pushElement<Car>({71, 71, 71}, 0);
    testHarness.pushElement<Car>({21, 21, 21}, 0);

    struct Output {
        uint32_t key;
        uint32_t value;
        uint64_t timestamp;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const { return (key == rhs.key && value == rhs.value && timestamp == rhs.timestamp); }
    };

    std::vector<Output> expectedOutput = {{40, 40, 40},
                                          {21, 21, 21},
                                          {
                                              30,
                                              30,
                                              30,
                                          },
                                          {71, 71, 71}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}
}