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
//
// Created by balint on 19.04.21.
//
#include <Util/Logger.hpp>
#include <gtest/gtest.h>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Topology/Topology.hpp>

namespace NES {

class QueryMigrationPhaseTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::setupLogging("GlobalQueryPlanUpdatePhaseTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup GlobalQueryPlanUpdatePhaseTest test case.");
    }

    /* Will be called before a  test is executed. */
    void SetUp() {}

    /* Will be called before a test is executed. */
    void TearDown() { NES_INFO("Tear down QueryMigrationPhase test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down QueryMigrationPhase test class."); }
};

/**
 * @brief In this test we test the functions findChild/ParentExecutionNodeAsTopologyNode of QueryMigrationPhase
 */
TEST_F(QueryMigrationPhaseTest, testFindChildAndParentExecutionNodes) {
    //need GlobalExecutionPlan
    //Topology
    //WorkerRPCClient
    auto globalExecutionPlan = GlobalExecutionPlan::create();
    auto workerRPCClient = WorkerRPCClient();
    auto topology = Topology::create();


}
}// nes namepsace