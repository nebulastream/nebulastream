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

    /***************************************************************************
 *
 * Project         _____    __   ____   _      _
 *                (  _  )  /__\ (_  _)_| |_  _| |_
 *                 )(_)(  /(__)\  )( (_   _)(_   _)
 *                (_____)(__)(__)(__)  |_|    |_|
 *
 *
 * Copyright 2018-present, Leonid Stryzhevskyi <lganzzzo@gmail.com>
 *
 ***************************************************************************/

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
