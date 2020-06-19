#include <gtest/gtest.h>
#include <Util/Logger.hpp>

namespace NES {
class MonitoringStackTest : public testing::Test {
  public:

    static void SetUpTestCase() {
        NES::setupLogging("MonitoringStackTest.log", NES::LOG_DEBUG);
        NES_INFO("MonitoringStackTest: Setup MonitoringStackTest test class.");
    }

    static void TearDownTestCase() {
        std::cout << "MonitoringStackTest: Tear down MonitoringStackTest class." << std::endl;
    }

    /* Will be called before a  test is executed. */
    void SetUp() override {
        std::cout << "MonitoringStackTest: Setup MonitoringStackTest test case." << std::endl;
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        std::cout << "MonitoringStackTest: Tear down MonitoringStackTest test case." << std::endl;
    }
};

TEST_F(MonitoringStackTest, test) {
    NES_INFO("Test");
}

}