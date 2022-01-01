/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <gtest/gtest.h>

#include <API/Query.hpp>
#include <Configurations/Sources/CSVSourceConfig.hpp>
#include <Configurations/Coordinator/CoordinatorConfig.hpp>
#include <Components/NesCoordinator.hpp>
#include <Catalogs/QueryCatalog.hpp>
#include <Util/Logger.hpp>
#include <CPPClient.hpp>

#include <unistd.h>

using namespace std;

namespace NES {
using namespace Configurations;
//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case
uint64_t rpcPort = 1289;
uint64_t dataPort = 1489;
uint64_t restPort = 8089;

class CPPClientTest : public testing::Test {
  protected:
    static void SetUpTestCase() { NES::setupLogging("CPPClientTest.log", NES::LOG_DEBUG); }
    static void TearDownTestCase() { NES_INFO("Tear down CPPClientTest test class."); }

    void SetUp() override {
        rpcPort += 10;
        dataPort += 10;
        restPort += 10;
    }
};

/**
 * @brief Test if doploying a query over the REST api works properly
 * @result deployed query ID is valid
 */
TEST_F(CPPClientTest, DeployQueryTest) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);

    NES_INFO("DeployQueryTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0U);
    NES_INFO("DeployQueryTest: Coordinator started successfully");

    Query query = Query::from("default_logical");

    CPPClient client = CPPClient("localhost", std::to_string(restPort));
    uint64_t queryId = client.submitQuery(query, "ButtomUp");

    EXPECT_TRUE(crd->getQueryCatalog()->queryExists(queryId));

    EXPECT_TRUE(crd->stopCoordinator(true));
}

}// namespace NES
