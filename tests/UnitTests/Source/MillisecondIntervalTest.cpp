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

#include <chrono>
#include <cmath>
#include <thread>

#include <Catalogs/PhysicalStreamConfig.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>

namespace NES {

class MillisecondIntervalTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("FractionedIntervalTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup FractionedIntervalTest test class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down FractionedIntervalTest test class."); }

    void SetUp() override { NES_INFO("Setup FractionedIntervalTest class."); }

    void TearDown() override { NES_INFO("Tear down FractionedIntervalTest test case."); }

};// FractionedIntervalTest

TEST_F(MillisecondIntervalTest, testCSVSourceWithLoopOverFile) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);
    std::string path_to_file = "../tests/test_data/ysb-tuples-100-campaign-100.csv";

    const std::string& del = ",";
    double frequency = 550;
    SchemaPtr schema = Schema::create()
                           ->addField("user_id", DataTypeFactory::createFixedChar(16))
                           ->addField("page_id", DataTypeFactory::createFixedChar(16))
                           ->addField("campaign_id", DataTypeFactory::createFixedChar(16))
                           ->addField("ad_type", DataTypeFactory::createFixedChar(9))
                           ->addField("event_type", DataTypeFactory::createFixedChar(9))
                           ->addField("current_ms", UINT64)
                           ->addField("ip", INT32);

    uint64_t numberOfBuffers = 5;

    const DataSourcePtr source = createCSVFileSource(schema, nodeEngine->getBufferManager(), nodeEngine->getQueryManager(),
                                                     path_to_file, del, 0, numberOfBuffers,
                                                     frequency, false, 1);

    for (uint64_t i = 0; i < numberOfBuffers; i++) {
        auto optBuf = source->receiveData();
    }

    uint64_t expectedNumberOfTuples = 260;
    EXPECT_EQ(source->getNumberOfGeneratedTuples(), expectedNumberOfTuples);
    EXPECT_EQ(source->getNumberOfGeneratedBuffers(), numberOfBuffers);
    EXPECT_EQ(source->getGatheringIntervalCount(), frequency);
}

}//namespace NES