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

#include "gtest/gtest.h"

#include <NodeEngine/QueryManager.hpp>
#include <cstring>
#include <iostream>
#include <limits>
#include <string>

#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Logger.hpp>

#include <Monitoring/MetricValues/GroupedValues.hpp>
#include <Monitoring/MetricValues/MetricValueType.hpp>
#include <Monitoring/Metrics/MetricCatalog.hpp>
#include <Monitoring/Metrics/MetricGroup.hpp>
#include <Monitoring/Metrics/MonitoringPlan.hpp>
#include <Monitoring/Util/MetricUtils.hpp>

#include <Sources/MonitoringSource.hpp>
#include <Sources/YSBSource.hpp>

namespace NES {

struct __attribute__((packed)) ysbRecord {
    char user_id[16];
    char page_id[16];
    char campaign_id[16];
    char ad_type[9];
    char event_type[9];
    int64_t current_ms;
    uint32_t ip;

    ysbRecord() {
        event_type[0] = '-';// invalid record
        current_ms = 0;
        ip = 0;
    }

    ysbRecord(const ysbRecord& rhs) {
        memcpy(&user_id, &rhs.user_id, 16);
        memcpy(&page_id, &rhs.page_id, 16);
        memcpy(&campaign_id, &rhs.campaign_id, 16);
        memcpy(&ad_type, &rhs.ad_type, 9);
        memcpy(&event_type, &rhs.event_type, 9);
        current_ms = rhs.current_ms;
        ip = rhs.ip;
    }
};
// size 78 bytes

struct __attribute__((packed)) everyIntTypeRecord {
    uint64_t uint64_entry;
    int64_t int64_entry;
    uint32_t uint32_entry;
    int32_t int32_entry;
    uint16_t uint16_entry;
    int16_t int16_entry;
    uint8_t uint8_entry;
    int8_t int8_entry;
};

struct __attribute__((packed)) everyFloatTypeRecord {
    double float64_entry;
    float float32_entry;
};

struct __attribute__((packed)) everyBooleanTypeRecord {
    bool false_entry;
    bool true_entry;
    bool falsey_entry;
    bool truthy_entry;
};

typedef const DataSourcePtr (*createFileSourceFuncPtr)(SchemaPtr, NodeEngine::BufferManagerPtr bufferManager,
                                                       NodeEngine::QueryManagerPtr queryManager, const std::string&);

typedef const DataSourcePtr (*createSenseSourceFuncPtr)(SchemaPtr, NodeEngine::BufferManagerPtr bufferManager,
                                                        NodeEngine::QueryManagerPtr queryManager, const std::string&, uint64_t);

typedef const DataSourcePtr (*createCSVSourceFuncPtr)(const SchemaPtr, NodeEngine::BufferManagerPtr bufferManager,
                                                      NodeEngine::QueryManagerPtr queryManager, const std::string&,
                                                      const std::string&, uint64_t, uint64_t);

class HdfsSourceTest : public testing::Test {
  public:
    void SetUp() {
        NES::setupLogging("HdfsSourceTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup HdfsSourceTest test class.");
    }

    static void TearDownTestCase() { std::cout << "Tear down HdfsSourceTest test class." << std::endl; }

    void TearDown() {}
};

TEST_F(HdfsSourceTest, testHdfsSource) {

    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);

    char *path_to_file = "/testData/ysb-tuples-100-campaign-100.bin";

    SchemaPtr schema = Schema::create()
        ->addField("user_id", DataTypeFactory::createFixedChar(16))
        ->addField("page_id", DataTypeFactory::createFixedChar(16))
        ->addField("campaign_id", DataTypeFactory::createFixedChar(16))
        ->addField("ad_type", DataTypeFactory::createFixedChar(9))
        ->addField("event_type", DataTypeFactory::createFixedChar(9))
        ->addField("current_ms", DataTypeFactory::createUInt64())
        ->addField("ip", DataTypeFactory::createInt32());

    uint64_t tuple_size = schema->getSchemaSizeInBytes();
    uint64_t buffer_size = nodeEngine->getBufferManager()->getBufferSize();
    uint64_t numberOfBuffers = 1;
    uint64_t numberOfTuplesToProcess = numberOfBuffers * (buffer_size / tuple_size);

    const DataSourcePtr source =
        createHdfsSource(schema, nodeEngine->getBufferManager(), nodeEngine->getQueryManager(), "192.168.1.104",
                         9000, "hdoop", path_to_file, ",", 0,
                         1, 1, false, 1);

    while (source->getNumberOfGeneratedBuffers() < numberOfBuffers) {
        auto optBuf = source->receiveData();
        uint64_t i = 0;
        while (i * tuple_size < buffer_size - tuple_size && optBuf.has_value()) {
            ysbRecord record(*((ysbRecord*) (optBuf->getBufferAs<char>() + i * tuple_size)));
            std::cout << "i=" << i << " record.ad_type: " << record.ad_type << ", record.event_type: " << record.event_type
                      << std::endl;
            EXPECT_STREQ(record.ad_type, "banner78");
            EXPECT_TRUE((!strcmp(record.event_type, "view") || !strcmp(record.event_type, "click")
                         || !strcmp(record.event_type, "purchase")));
            i++;
        }
    }

    EXPECT_EQ(source->getNumberOfGeneratedTuples(), numberOfTuplesToProcess);
    EXPECT_EQ(source->getNumberOfGeneratedBuffers(), numberOfBuffers);
}

}// namespace NES
