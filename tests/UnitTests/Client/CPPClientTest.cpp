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
#include <Client/CPPClient.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>

using namespace std;

namespace NES {

uint64_t rpcPort = 1200;
uint64_t dataPort = 1400;
uint64_t restPort = 8000;
uint16_t timeout = 5;

class CPPClientTest : public testing::Test {
  protected:
    static void SetUpTestCase() { NES::setupLogging("CPPClientTest.log", NES::LOG_DEBUG); }
    static void TearDownTestCase() { NES_INFO("Tear down CPPClientTest test class."); }

    CPPClient cppClient;
};

TEST_F(CPPClientTest, DeployQueryTest) {
    auto coordinator = TestUtils::startCoordinator({TestUtils::coordinatorPort(rpcPort), TestUtils::restPort(restPort)});
    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 0));

    std::stringstream schema;
    schema << "{\"streamName\" : \"window\",\"schema\" "
              ":\"Schema::create()->addField(createField(\\\"value\\\",UINT64))->addField(createField(\\\"id\\\",UINT64))->"
              "addField(createField(\\\"timestamp\\\",UINT64));\"}";
    schema << endl;
    NES_INFO("schema submit=" << schema.str());
    EXPECT_TRUE(TestUtils::addLogicalStream(schema.str(), std::to_string(restPort)));

    auto worker = TestUtils::startWorker({TestUtils::rpcPort(rpcPort + 3),
                                          TestUtils::dataPort(dataPort),
                                          TestUtils::coordinatorPort(rpcPort),
                                          TestUtils::logicalStreamName("window"),
                                          TestUtils::sourceType("CSVSource"),
                                          TestUtils::sourceConfig("../tests/test_data/window.csv"),
                                          TestUtils::numberOfBuffersToProduce(1),
                                          TestUtils::sourceFrequency(1000),
                                          TestUtils::numberOfTuplesToProducePerBuffer(28),
                                          TestUtils::physicalStreamName("test_stream")});

    /*auto worker = TestUtils::startWorker({TestUtils::rpcPort(rpcPort + 3),
                                          TestUtils::dataPort(dataPort),
                                          TestUtils::coordinatorPort(rpcPort),
                                          TestUtils::numberOfSlots(8)});*/

    EXPECT_TRUE(TestUtils::waitForWorkers(restPort, timeout, 1));

    auto query = Query::from("window").sink(PrintSinkDescriptor::create());
    auto queryPlan = query.getQueryPlan();
    cppClient.deployQuery(queryPlan, "localhost", std::to_string(restPort));
}

}// namespace NES