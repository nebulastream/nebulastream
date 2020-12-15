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

#include <Catalogs/PhysicalStreamConfig.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <Sources/AdaptiveKFSource.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/KalmanFilter.hpp>
#include <Util/Logger.hpp>

#include <Eigen/Dense>

#include <gtest/gtest.h>

namespace NES {

class AdaptiveKFTest : public testing::Test {
  public:
    SchemaPtr schema;
    PhysicalStreamConfigPtr streamConf;
    NodeEnginePtr nodeEngine;
    uint64_t tuple_size;
    uint64_t buffer_size;
    uint64_t num_of_buffers;
    uint64_t num_tuples_to_process;

    static void SetUpTestCase() {
        NES::setupLogging("AdaptiveKFTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup AdaptiveKFTest test class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down AdaptiveKFTest test class."); }

    void SetUp() override {
        NES_INFO("Setup AdaptiveKFTest class.");
        streamConf = PhysicalStreamConfig::create();
        schema = Schema::create()->addField("temperature", UINT32);
        nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);
        tuple_size = schema->getSchemaSizeInBytes();
        buffer_size = nodeEngine->getBufferManager()->getBufferSize();
        num_of_buffers = 1;
        num_tuples_to_process = num_of_buffers * (buffer_size / tuple_size);

        const DataSourcePtr source =
            createAdaptiveKFSource(schema, nodeEngine->getBufferManager(),
                                   nodeEngine->getQueryManager(), num_tuples_to_process,
                                   num_of_buffers, 1, 1);
    }

    void TearDown() override { NES_INFO("Tear down AdaptiveKFTest class."); }
};

// TODO: add test here
TEST_F(AdaptiveKFTest, initialStateTest) {
    EXPECT_TRUE(true);
}

TEST_F(AdaptiveKFTest, kfTest) {
    EXPECT_TRUE(true);
}
}// namespace NES