//#include "../../../../../cmake-build-debug/nes-dependencies-vOatpp-cpprestsdk-x64-linux-nes/installed/x64-linux-nes/include/oatpp-1.3.0/oatpp/oatpp/web/client/ApiClient.hpp"
#include <NesBaseTest.hpp>
#include <iostream>
#include <oatpp/network/tcp/client/ConnectionProvider.hpp>
#include <oatpp/parser/json/mapping/ObjectMapper.hpp>
#include <oatpp/web/client/HttpRequestExecutor.hpp>
#include <string>

#include <Components/NesCoordinator.hpp>
#include <../../tests/UnitTests/REST/OatppTests/MyControllerTest.hpp>

using namespace oatpp::network;
using namespace oatpp::web;
using namespace oatpp::parser;

namespace NES {

class TestsForOatppEndpoints : public NES::Testing::NESBaseTest {

  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TestsForOatppEndpoints.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup TestsForOatppEndpoints class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down TestsForOatppEndpoints test class."); }

    NesCoordinatorPtr createAndStartCoordinator() const {
        NES_INFO("TestsForOatppEndpoints: Start coordinator");
        CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
        coordinatorConfig->rpcPort = *rpcCoordinatorPort;
        coordinatorConfig->restPort = *restPort;
        coordinatorConfig->serverTypeOatpp = true;
        auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
        EXPECT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
        NES_INFO("TestsForOatppEndpoints: Coordinator started successfully");
        return coordinator;
    }
};

void runTests() {

    OATPP_RUN_TEST(MyControllerTest);

    // TODO - Add more tests here:
    // OATPP_RUN_TEST(MyAnotherTest);
}

TEST_F(TestsForOatppEndpoints, DISABLED_testStartServerWithOatpp) {

    /* start Coordinator */
    auto coordinator = createAndStartCoordinator();
    EXPECT_TRUE(coordinator->isCoordinatorRunning());
    NES_INFO("Server started successfully with Oatpp");
}

TEST_F(TestsForOatppEndpoints, testControllerSimpleTest) {

    oatpp::base::Environment::init();
    runTests();
    oatpp::base::Environment::destroy();

}

} //namespace NES

