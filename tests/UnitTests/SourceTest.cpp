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

typedef const DataSourcePtr (*createFileSourceFuncPtr)(SchemaPtr, BufferManagerPtr bufferManager, QueryManagerPtr queryManager,
                                                       const std::string&);

typedef const DataSourcePtr (*createSenseSourceFuncPtr)(SchemaPtr, BufferManagerPtr bufferManager, QueryManagerPtr queryManager,
                                                        const std::string&, uint64_t);

typedef const DataSourcePtr (*createCSVSourceFuncPtr)(const SchemaPtr, BufferManagerPtr bufferManager,
                                                      QueryManagerPtr queryManager, const std::string&, const std::string&,
                                                      uint64_t, uint64_t);

class SourceTest : public testing::Test {
  public:
    void SetUp() {
        NES::setupLogging("SourceTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup SourceTest test class.");
    }

    static void TearDownTestCase() { std::cout << "Tear down SourceTest test class." << std::endl; }

    void TearDown() {}
};

TEST_F(SourceTest, testBinarySource) {

    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);

    std::string path_to_file = "../tests/test_data/ysb-tuples-100-campaign-100.bin";

    SchemaPtr schema = Schema::create()
                           ->addField("user_id", DataTypeFactory::createFixedChar(16))
                           ->addField("page_id", DataTypeFactory::createFixedChar(16))
                           ->addField("campaign_id", DataTypeFactory::createFixedChar(16))
                           ->addField("ad_type", DataTypeFactory::createFixedChar(9))
                           ->addField("event_type", DataTypeFactory::createFixedChar(9))
                           ->addField("current_ms", UINT64)
                           ->addField("ip", INT32);

    uint64_t tuple_size = schema->getSchemaSizeInBytes();
    uint64_t buffer_size = nodeEngine->getBufferManager()->getBufferSize();
    uint64_t numberOfBuffers = 1;
    uint64_t numberOfTuplesToProcess = numberOfBuffers * (buffer_size / tuple_size);

    const DataSourcePtr source =
        createBinaryFileSource(schema, nodeEngine->getBufferManager(), nodeEngine->getQueryManager(), path_to_file, 1);

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

TEST_F(SourceTest, testCSVSourceOnePassOverFile) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);
    std::string path_to_file = "../tests/test_data/ysb-tuples-100-campaign-100.csv";

    const std::string& del = ",";
    uint64_t frequency = 1;
    SchemaPtr schema = Schema::create()
                           ->addField("user_id", DataTypeFactory::createFixedChar(16))
                           ->addField("page_id", DataTypeFactory::createFixedChar(16))
                           ->addField("campaign_id", DataTypeFactory::createFixedChar(16))
                           ->addField("ad_type", DataTypeFactory::createFixedChar(9))
                           ->addField("event_type", DataTypeFactory::createFixedChar(9))
                           ->addField("current_ms", UINT64)
                           ->addField("ip", INT32);

    uint64_t tuple_size = schema->getSchemaSizeInBytes();

    const DataSourcePtr source = createCSVFileSource(schema, nodeEngine->getBufferManager(), nodeEngine->getQueryManager(),
                                                     path_to_file, del, 0, 0, frequency, false, 1);

    uint64_t bufferCnt = 0;
    while (source->getNumberOfGeneratedBuffers() < 5) {
        auto optBuf = source->receiveData();
        if (optBuf.has_value()) {
            std::cout << "buffer no=" << bufferCnt << std::endl;
            for (uint64_t i = 0; i < optBuf->getNumberOfTuples(); i++) {
                ysbRecord record(*((ysbRecord*) (optBuf->getBufferAs<char>() + i * tuple_size)));
                std::cout << "i=" << i << " record.ad_type: " << record.ad_type << ", record.event_type: " << record.event_type
                          << std::endl;
                EXPECT_STREQ(record.ad_type, "banner78");
                EXPECT_TRUE((!strcmp(record.event_type, "view") || !strcmp(record.event_type, "click")
                             || !strcmp(record.event_type, "purchase")));
            }
            if (bufferCnt == 0) {
                EXPECT_EQ(optBuf->getNumberOfTuples(), 52);
            } else if (bufferCnt == 1) {
                EXPECT_EQ(optBuf->getNumberOfTuples(), 48);
            } else {
                FAIL();
            }
            bufferCnt++;
        } else {
            break;
        }
    }

    uint64_t expectedNumberOfTuples = 100;
    uint64_t expectedNumberOfBuffers = 2;
    //we expect 52 tuples in the first buffer and 48 in the second
    EXPECT_EQ(source->getNumberOfGeneratedTuples(), expectedNumberOfTuples);
    EXPECT_EQ(source->getNumberOfGeneratedBuffers(), expectedNumberOfBuffers);
}

TEST_F(SourceTest, testCSVSourceWithLoopOverFile) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);
    std::string path_to_file = "../tests/test_data/ysb-tuples-100-campaign-100.csv";

    const std::string& del = ",";
    uint64_t frequency = 1;
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
                                                     path_to_file, del, 0, numberOfBuffers, frequency, false, 1);

    for (uint64_t i = 0; i < numberOfBuffers; i++) {
        auto optBuf = source->receiveData();
        std::cout << "receive buffer with " << optBuf->getNumberOfTuples() << " tuples" << std::endl;
    }

    uint64_t expectedNumberOfTuples = 260;
    EXPECT_EQ(source->getNumberOfGeneratedTuples(), expectedNumberOfTuples);
    EXPECT_EQ(source->getNumberOfGeneratedBuffers(), numberOfBuffers);
}

TEST_F(SourceTest, testCSVSourceWatermark) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);
    std::string path_to_file = "../tests/test_data/ysb-tuples-100-campaign-100.csv";

    const std::string& del = ",";
    uint64_t num = 1;
    uint64_t frequency = 3;
    SchemaPtr schema = Schema::create()
                           ->addField("user_id", DataTypeFactory::createFixedChar(16))
                           ->addField("page_id", DataTypeFactory::createFixedChar(16))
                           ->addField("campaign_id", DataTypeFactory::createFixedChar(16))
                           ->addField("ad_type", DataTypeFactory::createFixedChar(9))
                           ->addField("event_type", DataTypeFactory::createFixedChar(9))
                           ->addField("current_ms", UINT64)
                           ->addField("ip", INT32);

    uint64_t tuple_size = schema->getSchemaSizeInBytes();
    uint64_t buffer_size = nodeEngine->getBufferManager()->getBufferSize();
    uint64_t numberOfBuffers = 10;
    uint64_t numberOfTuplesToProcess = numberOfBuffers * (buffer_size / tuple_size);

    const DataSourcePtr source = createCSVFileSource(schema, nodeEngine->getBufferManager(), nodeEngine->getQueryManager(),
                                                     path_to_file, del, 0, num, frequency, false, 1);

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

TEST_F(SourceTest, testCSVSourceIntTypes) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);

    std::string path_to_file = "../tests/test_data/every-int.csv";

    const std::string& del = ",";
    uint64_t num = 1;
    uint64_t frequency = 1;
    SchemaPtr schema = Schema::create()
                           ->addField("uint64", UINT64)
                           ->addField("int64", INT64)
                           ->addField("uint32", UINT32)
                           ->addField("int32", INT32)
                           ->addField("uint16", UINT16)
                           ->addField("int16", INT16)
                           ->addField("uint8", UINT8)
                           ->addField("int8", INT8);

    uint64_t tuple_size = schema->getSchemaSizeInBytes();
    uint64_t buffer_size = nodeEngine->getBufferManager()->getBufferSize();
    uint64_t numberOfBuffers = 2;
    uint64_t numberOfTuplesToProcess = numberOfBuffers * (buffer_size / tuple_size);

    const DataSourcePtr source = createCSVFileSource(schema, nodeEngine->getBufferManager(), nodeEngine->getQueryManager(),
                                                     path_to_file, del, 0, num, frequency, false, 1);

    while (source->getNumberOfGeneratedBuffers() < numberOfBuffers) {
        auto optBuf = source->receiveData();
        uint64_t i = 0;
        while (i * tuple_size < buffer_size - tuple_size && optBuf.has_value()) {
            everyIntTypeRecord record(*((everyIntTypeRecord*) (optBuf->getBufferAs<char>() + i * tuple_size)));
            std::cout << "i=" << i << " record.uint64_entry: " << record.uint64_entry
                      << ", record.int64_entry: " << record.int64_entry << ", record.uint32_entry: " << record.uint32_entry
                      << ", record.int32_entry: " << record.int32_entry << ", record.uint16_entry: " << record.uint16_entry
                      << ", record.int16_entry: " << record.int16_entry << ", record.uint8_entry: " << record.uint8_entry
                      << ", record.int8_entry: " << record.int8_entry << std::endl;

            // number is in the expected range
            EXPECT_LE(record.uint64_entry, std::numeric_limits<uint64_t>::max());
            EXPECT_GE(record.uint64_entry, std::numeric_limits<uint64_t>::min());
            // unsigned number is equal to max, no specific numbers in code
            EXPECT_EQ(record.uint64_entry, std::numeric_limits<uint64_t>::max());

            // number is in the expected range
            EXPECT_LE(record.int64_entry, std::numeric_limits<int64_t>::max());
            EXPECT_GE(record.int64_entry, std::numeric_limits<int64_t>::min());
            // checks for min, covers signed case
            EXPECT_EQ(record.int64_entry, std::numeric_limits<int64_t>::min());

            EXPECT_LE(record.uint32_entry, std::numeric_limits<uint32_t>::max());
            EXPECT_GE(record.uint32_entry, std::numeric_limits<uint32_t>::min());
            EXPECT_EQ(record.uint32_entry, std::numeric_limits<uint32_t>::max());

            EXPECT_LE(record.int32_entry, std::numeric_limits<int32_t>::max());
            EXPECT_GE(record.int32_entry, std::numeric_limits<int32_t>::min());
            EXPECT_EQ(record.int32_entry, std::numeric_limits<int32_t>::min());

            EXPECT_LE(record.uint16_entry, std::numeric_limits<uint16_t>::max());
            EXPECT_GE(record.uint16_entry, std::numeric_limits<uint16_t>::min());
            EXPECT_EQ(record.uint16_entry, std::numeric_limits<uint16_t>::max());

            EXPECT_LE(record.int16_entry, std::numeric_limits<int16_t>::max());
            EXPECT_GE(record.int16_entry, std::numeric_limits<int16_t>::min());
            EXPECT_EQ(record.int16_entry, std::numeric_limits<int16_t>::min());

            EXPECT_LE(record.uint8_entry, std::numeric_limits<uint8_t>::max());
            EXPECT_GE(record.uint8_entry, std::numeric_limits<uint8_t>::min());
            EXPECT_EQ(record.uint8_entry, std::numeric_limits<uint8_t>::max());

            EXPECT_LE(record.int8_entry, std::numeric_limits<int8_t>::max());
            EXPECT_GE(record.int8_entry, std::numeric_limits<int8_t>::min());
            EXPECT_EQ(record.int8_entry, std::numeric_limits<int8_t>::min());

            i++;
        }
    }

    EXPECT_EQ(source->getNumberOfGeneratedTuples(), numberOfTuplesToProcess);
    EXPECT_EQ(source->getNumberOfGeneratedBuffers(), numberOfBuffers);
}

TEST_F(SourceTest, testCSVSourceFloatTypes) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);

    std::string path_to_file = "../tests/test_data/every-float.csv";

    const std::string& del = ",";
    uint64_t num = 1;
    uint64_t frequency = 1;
    SchemaPtr schema = Schema::create()->addField("float64", FLOAT64)->addField("float32", FLOAT32);

    uint64_t tuple_size = schema->getSchemaSizeInBytes();
    uint64_t buffer_size = nodeEngine->getBufferManager()->getBufferSize();
    uint64_t numberOfBuffers = 2;
    uint64_t numberOfTuplesToProcess = numberOfBuffers * (buffer_size / tuple_size);

    const DataSourcePtr source = createCSVFileSource(schema, nodeEngine->getBufferManager(), nodeEngine->getQueryManager(),
                                                     path_to_file, del, 0, num, frequency, false, 1);

    while (source->getNumberOfGeneratedBuffers() < numberOfBuffers) {
        auto optBuf = source->receiveData();
        uint64_t i = 0;
        while (i * tuple_size < buffer_size - tuple_size && optBuf.has_value()) {
            everyFloatTypeRecord record(*((everyFloatTypeRecord*) (optBuf->getBufferAs<char>() + i * tuple_size)));
            std::cout << "i=" << i << " record.float64_entry: " << record.float64_entry
                      << ", record.float32_entry: " << record.float32_entry << std::endl;

            EXPECT_LE(record.float64_entry, std::numeric_limits<double>::max());
            EXPECT_GE(record.float64_entry, std::numeric_limits<double>::min());
            EXPECT_DOUBLE_EQ(record.float64_entry, std::numeric_limits<double>::max());

            EXPECT_LE(record.float32_entry, std::numeric_limits<float>::max());
            EXPECT_GE(record.float32_entry, std::numeric_limits<float>::min());
            EXPECT_FLOAT_EQ(record.float32_entry, std::numeric_limits<float>::max());

            i++;
        }
    }

    EXPECT_EQ(source->getNumberOfGeneratedTuples(), numberOfTuplesToProcess);
    EXPECT_EQ(source->getNumberOfGeneratedBuffers(), numberOfBuffers);
}

TEST_F(SourceTest, testCSVSourceBooleanTypes) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);
    std::string path_to_file = "../tests/test_data/every-boolean.csv";

    const std::string& del = ",";
    uint64_t num = 1;
    uint64_t frequency = 1;
    SchemaPtr schema = Schema::create()
                           ->addField("false", BOOLEAN)
                           ->addField("true", BOOLEAN)
                           ->addField("falsey", BOOLEAN)
                           ->addField("truthy", BOOLEAN);

    uint64_t tuple_size = schema->getSchemaSizeInBytes();
    uint64_t buffer_size = nodeEngine->getBufferManager()->getBufferSize();
    uint64_t numberOfBuffers = 2;
    uint64_t numberOfTuplesToProcess = numberOfBuffers * (buffer_size / tuple_size);

    const DataSourcePtr source = createCSVFileSource(schema, nodeEngine->getBufferManager(), nodeEngine->getQueryManager(),
                                                     path_to_file, del, 0, num, frequency, false, 1);

    while (source->getNumberOfGeneratedBuffers() < numberOfBuffers) {
        auto optBuf = source->receiveData();
        uint64_t i = 0;
        while (i * tuple_size < buffer_size - tuple_size && optBuf.has_value()) {
            everyBooleanTypeRecord record(*((everyBooleanTypeRecord*) (optBuf->getBufferAs<char>() + i * tuple_size)));
            std::cout << "i=" << i << " record.false_entry: " << record.false_entry
                      << ", record.true_entry: " << record.true_entry << ", record.falsey_entry: " << record.falsey_entry
                      << ", record.truthy_entry: " << record.truthy_entry << std::endl;

            EXPECT_FALSE(record.false_entry);
            EXPECT_TRUE(record.true_entry);
            EXPECT_FALSE(record.falsey_entry);
            EXPECT_TRUE(record.truthy_entry);
            i++;
        }
    }

    EXPECT_EQ(source->getNumberOfGeneratedTuples(), numberOfTuplesToProcess);
    EXPECT_EQ(source->getNumberOfGeneratedBuffers(), numberOfBuffers);
}

TEST_F(SourceTest, testSenseSource) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);

    std::string testUDFS("...");
    createSenseSourceFuncPtr funcPtr = &createSenseSource;

    SchemaPtr schema = Schema::create()
                           ->addField("user_id", DataTypeFactory::createFixedChar(16))
                           ->addField("page_id", DataTypeFactory::createFixedChar(16))
                           ->addField("campaign_id", DataTypeFactory::createFixedChar(16))
                           ->addField("ad_type", DataTypeFactory::createFixedChar(9))
                           ->addField("event_type", DataTypeFactory::createFixedChar(9))
                           ->addField("current_ms", UINT64)
                           ->addField("ip", INT32);

    uint64_t numberOfTuplesToProcess = 1000;
    uint64_t numberOfBuffers = 1000;
    uint64_t tuple_size = schema->getSchemaSizeInBytes();
    uint64_t buffer_size = numberOfTuplesToProcess * tuple_size / numberOfBuffers;
    ASSERT_GT(buffer_size, 0);

    const DataSourcePtr source = (*funcPtr)(schema, nodeEngine->getBufferManager(), nodeEngine->getQueryManager(), testUDFS, 1);

    //TODO: please add here to code to test the setup
    std::cout << "Success" << std::endl;
}

TEST_F(SourceTest, testYSBSource) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);

    uint64_t numBuffers = 2;
    uint64_t numTuples = 30;

    auto source =
        std::make_shared<YSBSource>(nodeEngine->getBufferManager(), nodeEngine->getQueryManager(), numBuffers, numTuples, 1, 1);
    SchemaPtr schema = source->getSchema();

    while (source->getNumberOfGeneratedBuffers() < numBuffers) {
        auto optBuf = source->receiveData();
        auto ysbRecords = optBuf->getBufferAs<YSBSource::YsbRecord>();

        for (int i = 0; i < numTuples; i++) {
            auto record = ysbRecords[i];
            std::cout << "i=" << i << " record.current_ms: " << record.currentMs << ", record.ad_type: " << record.adType
                      << ", record.event_type: " << record.eventType << std::endl;
            EXPECT_TRUE(0 <= record.campaignId && record.campaignId < 10000);
            EXPECT_TRUE(0 <= record.eventType && record.eventType < 3);
        }
    }

    EXPECT_EQ(source->getNumberOfGeneratedBuffers(), numBuffers);
    EXPECT_EQ(source->getNumberOfGeneratedTuples(), numBuffers * numTuples);
}

TEST_F(SourceTest, testMonitoringSource) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);

    uint64_t numBuffers = 2;

    // create the MonitoringSource
    auto metrics = std::vector<MetricValueType>({CpuMetric, DiskMetric, MemoryMetric, NetworkMetric});
    auto plan = MonitoringPlan::create(metrics);

    auto source = std::make_shared<MonitoringSource>(plan, MetricCatalog::NesMetrics(), nodeEngine->getBufferManager(),
                                                     nodeEngine->getQueryManager(), numBuffers, 1, 1);

    SchemaPtr schema = source->getSchema();

    while (source->getNumberOfGeneratedBuffers() < numBuffers) {
        auto optBuf = source->receiveData();
        GroupedValues parsedValues = plan->fromBuffer(schema, optBuf.value());

        ASSERT_TRUE(parsedValues.cpuMetrics.value()->getTotal().USER > 0);
        ASSERT_TRUE(parsedValues.memoryMetrics.value()->FREE_RAM > 0);
        ASSERT_TRUE(parsedValues.diskMetrics.value()->fBavail > 0);
    }

    EXPECT_EQ(source->getNumberOfGeneratedBuffers(), numBuffers);
    EXPECT_EQ(source->getNumberOfGeneratedTuples(), 2);
}

}// namespace NES
