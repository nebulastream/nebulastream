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

#include "SerializableOperator.pb.h"
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Network/NetworkChannel.hpp>
#include <Operators/Serialization/SchemaSerializationUtil.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/NesThread.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Mediums/FileSink.hpp>
#include <Sinks/SinkCreator.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Common.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <Util/TestUtils.hpp>
#include <gtest/gtest.h>
#include <ostream>

#include <API/TestSchemas.hpp>
#include <Monitoring/MetricCollectors/CpuCollector.hpp>
#include <Monitoring/MetricCollectors/DiskCollector.hpp>
#include <Monitoring/Metrics/Gauge/CpuMetrics.hpp>
#include <Monitoring/Metrics/Gauge/DiskMetrics.hpp>
#include <Monitoring/Metrics/Metric.hpp>
#include <Monitoring/Metrics/Wrapper/CpuMetricsWrapper.hpp>
#include <Monitoring/Storage/AllEntriesMetricStore.hpp>
#include <Monitoring/Util/MetricUtils.hpp>

using namespace std;

/**
 * @brief tests for sinks
 */
namespace NES {
using Runtime::TupleBuffer;
class SinkTest : public Testing::BaseIntegrationTest {
  public:
    SchemaPtr test_schema;
    std::array<uint32_t, 8> test_data{};
    bool write_result{};
    std::string path_to_csv_file;
    std::string path_to_migrate_file;
    std::string path_to_bin_file;
    std::string path_to_osfile_file;
    Testing::BorrowedPortPtr borrowedZmqPort;
    int zmqPort;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("SinkTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup SinkTest class.");
    }

    /* Called before a single test. */
    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        test_schema = TestSchemas::getSchemaTemplate("id_val_u32");
        write_result = false;
        path_to_csv_file = getTestResourceFolder() / "sink.csv";
        path_to_migrate_file = getTestResourceFolder() / "serialized_state.bin";
        path_to_bin_file = getTestResourceFolder() / "sink.bin";
        path_to_osfile_file = getTestResourceFolder() / "testOs.txt";
        auto workerConfiguration = WorkerConfiguration::create();
        workerConfiguration->physicalSourceTypes.add(CSVSourceType::create("x", "x1"));
        this->nodeEngine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                               .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                               .build();

        borrowedZmqPort = getAvailablePort();
        zmqPort = *borrowedZmqPort;
    }

    /* Called after a single test. */
    void TearDown() override {
        ASSERT_TRUE(nodeEngine->stop());
        borrowedZmqPort.reset();
        Testing::BaseIntegrationTest::TearDown();
    }

    Runtime::NodeEnginePtr nodeEngine{nullptr};
};

TEST_F(SinkTest, testCSVFileSink) {
    PhysicalSourcePtr sourceConf = PhysicalSource::create("x", "x1");
    auto nodeEngine = this->nodeEngine;

    TupleBuffer buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    Runtime::WorkerContext wctx(Runtime::NesThread::getId(), nodeEngine->getBufferManager(), 64);
    const DataSinkPtr csvSink = createCSVFileSink(test_schema,
                                                  SharedQueryId(0),
                                                  INVALID_DECOMPOSED_QUERY_PLAN_ID,
                                                  INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                                  nodeEngine,
                                                  1,
                                                  path_to_csv_file,
                                                  true,
                                                  FaultToleranceType::NONE,
                                                  CheckpointStorageType::NONE);

    for (uint64_t i = 0; i < 2; ++i) {
        for (uint64_t j = 0; j < 2; ++j) {
            buffer.getBuffer<uint64_t>()[j] = j;
        }
    }
    buffer.setNumberOfTuples(4);
    csvSink->setup();
    auto rowLayoutBeforeWrite = Runtime::MemoryLayouts::RowLayout::create(test_schema, buffer.getBufferSize());
    auto testTupleBufferBeforeWrite = Runtime::MemoryLayouts::TestTupleBuffer(rowLayoutBeforeWrite, buffer);
    std::string bufferContentBeforeWrite = testTupleBufferBeforeWrite.toString(test_schema);
    NES_TRACE("Buffer Content= {}", bufferContentBeforeWrite);

    write_result = csvSink->writeData(buffer, wctx);
    EXPECT_TRUE(write_result);

    // get buffer content as string
    auto rowLayoutAfterWrite = Runtime::MemoryLayouts::RowLayout::create(test_schema, buffer.getBufferSize());
    auto testTupleBufferAfterWrite = Runtime::MemoryLayouts::TestTupleBuffer(rowLayoutAfterWrite, buffer);
    std::string bufferContentAfterWrite = testTupleBufferAfterWrite.toString(test_schema);
    NES_TRACE("Buffer Content= {}", bufferContentAfterWrite);

    ifstream testFile(path_to_csv_file.c_str());
    EXPECT_TRUE(testFile.good());
    std::ifstream ifs(path_to_csv_file.c_str());
    std::string fileContent((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    //search for each value
    string contentWOHeader = fileContent.erase(0, fileContent.find('\n') + 1);
    NES::Util::findAndReplaceAll(contentWOHeader, "\n", ",");
    stringstream ss(contentWOHeader);
    string item;
    while (getline(ss, item, ',')) {
        EXPECT_TRUE(bufferContentAfterWrite.find(item) != std::string::npos);
    }
}

/**
 * Writing buffer to the file using RawBufferSink, then reading from the file and comparing written and read buffers content
 */
TEST_F(SinkTest, testRawBufferSink) {
    auto sourceConf = PhysicalSource::create("x", "x1");

    auto buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    Runtime::WorkerContext wctx(Runtime::NesThread::getId(), nodeEngine->getBufferManager(), 64);
    const DataSinkPtr migrateFileSink = createMigrateFileSink(SharedQueryId(0),
                                                              INVALID_DECOMPOSED_QUERY_PLAN_ID,
                                                              INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                                              nodeEngine,
                                                              1,
                                                              path_to_migrate_file,
                                                              true,
                                                              FaultToleranceType::NONE,
                                                              CheckpointStorageType::NONE);

    // insert tuples to buffer
    constexpr auto expectedNumberOfTuples = 4;
    for (uint64_t i = 0; i < expectedNumberOfTuples; ++i) {
        buffer.getBuffer<uint64_t>()[i] = i;
    }
    buffer.setNumberOfTuples(expectedNumberOfTuples);

    // buffer content before writing
    auto rowLayoutExpected = Runtime::MemoryLayouts::RowLayout::create(test_schema, buffer.getBufferSize());
    auto testTupleBufferExpected = Runtime::MemoryLayouts::TestTupleBuffer(rowLayoutExpected, buffer);
    auto bufferContentExpected = testTupleBufferExpected.toString(test_schema);

    migrateFileSink->setup();
    // write buffer to file using sink
    write_result = migrateFileSink->writeData(buffer, wctx);

    EXPECT_TRUE(write_result);

    // open file to read
    std::ifstream deser_file(path_to_migrate_file, std::ios::binary | std::ios::in);
    if (!deser_file.is_open()) {
        NES_ERROR("Error: Failed to open file for reading.");
    }

    // read size and number of tuples from the file
    auto size = 0ULL, numberOfTuples = 0ULL;
    deser_file.read(reinterpret_cast<char*>(&size), sizeof(uint64_t));
    deser_file.read(reinterpret_cast<char*>(&numberOfTuples), sizeof(uint64_t));

    EXPECT_EQ(size, buffer.getBufferSize());
    EXPECT_EQ(numberOfTuples, expectedNumberOfTuples);

    auto newBuffer = nodeEngine->getBufferManager()->getBufferBlocking();
    // read buffer content
    deser_file.read(reinterpret_cast<char*>(newBuffer.getBuffer()), size);

    // compare buffers before writing and after reading
    auto rowLayoutDeserialized = Runtime::MemoryLayouts::RowLayout::create(test_schema, buffer.getBufferSize());
    auto testTupleBufferDeserialized = Runtime::MemoryLayouts::TestTupleBuffer(rowLayoutDeserialized, buffer);
    auto bufferContentDeserialized = testTupleBufferExpected.toString(test_schema);
    EXPECT_EQ(bufferContentDeserialized, bufferContentExpected);
    deser_file.close();
}

TEST_F(SinkTest, testCSVPrintSink) {
    PhysicalSourcePtr sourceConf = PhysicalSource::create("x", "x1");

    std::filebuf fb;
    fb.open(path_to_osfile_file, std::ios::out);
    std::ostream os(&fb);
    Runtime::WorkerContext wctx(Runtime::NesThread::getId(), this->nodeEngine->getBufferManager(), 64);
    TupleBuffer buffer = this->nodeEngine->getBufferManager()->getBufferBlocking();
    auto csvSink = createCSVPrintSink(test_schema,
                                      SharedQueryId(0),
                                      INVALID_DECOMPOSED_QUERY_PLAN_ID,
                                      INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                      this->nodeEngine,
                                      1,
                                      os,
                                      FaultToleranceType::NONE,
                                      CheckpointStorageType::NONE);
    for (uint64_t i = 0; i < 2; ++i) {
        for (uint64_t j = 0; j < 2; ++j) {
            buffer.getBuffer<uint64_t>()[j] = j;
        }
    }
    buffer.setNumberOfTuples(4);
    //cout << "bufferContent before write=" << Util::prettyPrintTupleBuffer(buffer, test_schema) << endl;
    write_result = csvSink->writeData(buffer, wctx);

    EXPECT_TRUE(write_result);
    auto rowLayout = Runtime::MemoryLayouts::RowLayout::create(test_schema, buffer.getBufferSize());
    auto testTupleBuffer = Runtime::MemoryLayouts::TestTupleBuffer(rowLayout, buffer);
    std::string bufferContent = testTupleBuffer.toString(test_schema);
    //cout << "Buffer Content= " << bufferContent << endl;

    ifstream testFile(path_to_osfile_file.c_str());
    EXPECT_TRUE(testFile.good());
    std::ifstream ifs(path_to_osfile_file.c_str());
    std::string fileContent((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    //cout << "File Content=" << fileContent << endl;
    //search for each value
    string contentWOHeader = fileContent.erase(0, fileContent.find('\n') + 1);
    NES::Util::findAndReplaceAll(contentWOHeader, "\n", ",");
    //cout << "File Content shrinked=" << contentWOHeader << endl;

    stringstream ss(contentWOHeader);
    string item;
    while (getline(ss, item, ',')) {
        //cout << item << endl;
        if (bufferContent.find(item) != std::string::npos) {
            //cout << "found token=" << item << endl;
        } else {
            //cout << "NOT found token=" << item << endl;
            EXPECT_TRUE(false);
        }
    }
    fb.close();
}

TEST_F(SinkTest, testNullOutSink) {
    auto sourceConf = PhysicalSource::create("x", "x1");
    auto nodeEngine = this->nodeEngine;

    std::filebuf fb;
    fb.open(path_to_osfile_file, std::ios::out);
    std::ostream os(&fb);
    Runtime::WorkerContext wctx(Runtime::NesThread::getId(), nodeEngine->getBufferManager(), 64);
    auto buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    auto nullSink = createNullOutputSink(SharedQueryId(1),
                                         INVALID_DECOMPOSED_QUERY_PLAN_ID,
                                         INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                         nodeEngine,
                                         1,
                                         FaultToleranceType::NONE,
                                         CheckpointStorageType::NONE);
    for (uint64_t i = 0; i < 2; ++i) {
        for (uint64_t j = 0; j < 2; ++j) {
            buffer.getBuffer<uint64_t>()[j] = j;
        }
    }
    buffer.setNumberOfTuples(4);
    //cout << "bufferContent before write=" << Util::prettyPrintTupleBuffer(buffer, test_schema) << endl;
    write_result = nullSink->writeData(buffer, wctx);

    EXPECT_TRUE(write_result);
    auto rowLayout = Runtime::MemoryLayouts::RowLayout::create(test_schema, buffer.getBufferSize());
    auto testTupleBuffer = Runtime::MemoryLayouts::TestTupleBuffer(rowLayout, buffer);
    std::string bufferContent = testTupleBuffer.toString(test_schema);
    //cout << "Buffer Content= " << bufferContent << endl;
}

TEST_F(SinkTest, testCSVZMQSink) {
    PhysicalSourcePtr sourceConf = PhysicalSource::create("x", "x1");

    Runtime::WorkerContext wctx(Runtime::NesThread::getId(), nodeEngine->getBufferManager(), 64);
    auto buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    const DataSinkPtr zmq_sink = createCSVZmqSink(test_schema,
                                                  SharedQueryId(0),
                                                  INVALID_DECOMPOSED_QUERY_PLAN_ID,
                                                  INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                                  nodeEngine,
                                                  1,
                                                  "localhost",
                                                  zmqPort,
                                                  FaultToleranceType::NONE,
                                                  CheckpointStorageType::NONE);
    for (uint64_t i = 1; i < 3; ++i) {
        for (uint64_t j = 0; j < 2; ++j) {
            buffer.getBuffer<uint64_t>()[j * i] = j;
        }
    }
    buffer.setNumberOfTuples(4);
    ////cout << "buffer before send=" << Util::prettyPrintTupleBuffer(buffer, test_schema);

    // Create ZeroMQ Data Source.
    auto zmq_source = createZmqSource(test_schema,
                                      nodeEngine->getBufferManager(),
                                      nodeEngine->getQueryManager(),
                                      "localhost",
                                      zmqPort,
                                      OperatorId(1),
                                      INVALID_ORIGIN_ID,
                                      INVALID_STATISTIC_ID,
                                      12,
                                      "defaultPhysicalSourceName",
                                      std::vector<Runtime::Execution::SuccessorExecutablePipeline>());

    // Start thread for receivingh the data.
    bool receiving_finished = false;
    auto receiving_thread = std::thread([&]() {
        // Receive data.
        zmq_source->open();
        auto schemaData = zmq_source->receiveData();
        TupleBuffer bufSchema = schemaData.value();
        std::string schemaStr;
        schemaStr.assign(bufSchema.getBuffer<char>(), bufSchema.getNumberOfTuples());
        //cout << "Schema=" << schemaStr << endl;
        EXPECT_EQ(Util::toCSVString(test_schema), schemaStr);

        auto bufferData = zmq_source->receiveData();
        TupleBuffer bufData = bufferData.value();
        //cout << "Buffer=" << bufData.getBuffer<char>() << endl;

        std::string bufferContent = Util::printTupleBufferAsCSV(buffer, test_schema);
        std::string dataStr;
        dataStr.assign(bufData.getBuffer<char>(), bufferContent.size());
        //cout << "Buffer Content received= " << bufferContent << endl;
        EXPECT_EQ(bufferContent, dataStr);
        receiving_finished = true;
    });

    // Wait until receiving is complete.
    zmq_sink->writeData(buffer, wctx);
    receiving_thread.join();
}

TEST_F(SinkTest, testWatermarkForZMQ) {
    Runtime::WorkerContext wctx(Runtime::NesThread::getId(), nodeEngine->getBufferManager(), 64);
    PhysicalSourcePtr sourceConf = PhysicalSource::create("x", "x1");
    auto nodeEngine = this->nodeEngine;

    TupleBuffer buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    buffer.setWatermark(1234567);
    const DataSinkPtr zmq_sink = createBinaryZmqSink(test_schema,
                                                     SharedQueryId(0),
                                                     INVALID_DECOMPOSED_QUERY_PLAN_ID,
                                                     INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                                     nodeEngine,
                                                     1,
                                                     "localhost",
                                                     zmqPort,
                                                     false,
                                                     FaultToleranceType::NONE,
                                                     CheckpointStorageType::NONE);
    for (uint64_t i = 1; i < 3; ++i) {
        for (uint64_t j = 0; j < 2; ++j) {
            buffer.getBuffer<uint64_t>()[j * i] = j;
        }
    }
    buffer.setNumberOfTuples(4);

    // Create ZeroMQ Data Source.
    auto zmq_source = createZmqSource(test_schema,
                                      nodeEngine->getBufferManager(),
                                      nodeEngine->getQueryManager(),
                                      "localhost",
                                      zmqPort,
                                      OperatorId(1),
                                      INVALID_ORIGIN_ID,
                                      INVALID_STATISTIC_ID,
                                      12,
                                      "defaultPhysicalSourceName",
                                      std::vector<Runtime::Execution::SuccessorExecutablePipeline>());
    //std::cout << zmq_source->toString() << std::endl;

    // Start thread for receivingh the data.
    auto receiving_thread = std::thread([&]() {
        zmq_source->open();
        auto schemaData = zmq_source->receiveData();

        auto bufferData = zmq_source->receiveData();
        TupleBuffer bufData = bufferData.value();
        EXPECT_EQ(bufData.getWatermark(), 1234567ull);
    });

    // Wait until receiving is complete.
    zmq_sink->writeData(buffer, wctx);
    receiving_thread.join();
}

TEST_F(SinkTest, testWatermarkCsvSource) {
    PhysicalSourcePtr sourceConf = PhysicalSource::create("x", "x1");
    auto nodeEngine = this->nodeEngine;
    Runtime::WorkerContext wctx(Runtime::NesThread::getId(), nodeEngine->getBufferManager(), 64);
    TupleBuffer buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    buffer.setWatermark(1234567);

    const DataSinkPtr csvSink = createCSVFileSink(test_schema,
                                                  SharedQueryId(0),
                                                  INVALID_DECOMPOSED_QUERY_PLAN_ID,
                                                  INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                                  nodeEngine,
                                                  1,
                                                  path_to_csv_file,
                                                  true,
                                                  FaultToleranceType::NONE,
                                                  CheckpointStorageType::NONE);
    for (uint64_t i = 0; i < 2; ++i) {
        for (uint64_t j = 0; j < 2; ++j) {
            buffer.getBuffer<uint64_t>()[j] = j;
        }
    }
    buffer.setNumberOfTuples(4);
    //cout << "watermark=" << buffer.getWatermark() << endl;
    write_result = csvSink->writeData(buffer, wctx);

    EXPECT_EQ(buffer.getWatermark(), 1234567ull);
}

TEST_F(SinkTest, testMonitoringSink) {
    auto nodeId1 = WorkerId(4711);
    auto nodeId2 = WorkerId(7356);

    PhysicalSourcePtr sourceConf = PhysicalSource::create("x", "x1");
    auto nodeEngine = this->nodeEngine;
    Runtime::WorkerContext wctx(Runtime::NesThread::getId(), nodeEngine->getBufferManager(), 64);

    auto metricStore = std::make_shared<Monitoring::AllEntriesMetricStore>();

    //write metrics to tuple buffer for disk collector
    auto diskCollector = Monitoring::DiskCollector();
    diskCollector.setNodeId(nodeId1);
    Monitoring::MetricPtr diskMetric = diskCollector.readMetric();
    Monitoring::DiskMetrics typedMetric = diskMetric->getValue<Monitoring::DiskMetrics>();
    EXPECT_EQ(diskMetric->getMetricType(), Monitoring::MetricType::DiskMetric);
    auto bufferSize = Monitoring::DiskMetrics::getSchema("")->getSchemaSizeInBytes();
    auto tupleBuffer = nodeEngine->getBufferManager()->getUnpooledBuffer(bufferSize).value();
    writeToBuffer(typedMetric, tupleBuffer, 0);
    ASSERT_TRUE(tupleBuffer.getNumberOfTuples() == 1);

    //write metrics to tuple buffer for cpu collector
    auto cpuCollector = Monitoring::CpuCollector();
    cpuCollector.setNodeId(nodeId2);
    Monitoring::MetricPtr cpuMetric = cpuCollector.readMetric();
    Monitoring::CpuMetricsWrapper typedMetricCpu = cpuMetric->getValue<Monitoring::CpuMetricsWrapper>();
    EXPECT_EQ(cpuMetric->getMetricType(), Monitoring::MetricType::WrappedCpuMetrics);
    auto bufferSizeCpu = Monitoring::CpuMetrics::getSchema("")->getSchemaSizeInBytes() * typedMetricCpu.size() + 64;
    auto tupleBufferCpu = nodeEngine->getBufferManager()->getUnpooledBuffer(bufferSizeCpu).value();
    writeToBuffer(typedMetricCpu, tupleBufferCpu, 0);
    ASSERT_TRUE(tupleBufferCpu.getNumberOfTuples() >= 1);

    // write disk metrics
    const DataSinkPtr monitoringSink = createMonitoringSink(metricStore,
                                                            diskCollector.getType(),
                                                            Monitoring::DiskMetrics::getSchema(""),
                                                            nodeEngine,
                                                            1,
                                                            SharedQueryId(0),
                                                            INVALID_DECOMPOSED_QUERY_PLAN_ID,
                                                            INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                                            FaultToleranceType::NONE,
                                                            CheckpointStorageType::NONE,
                                                            1);
    monitoringSink->writeData(tupleBuffer, wctx);

    // write cpu metrics
    const DataSinkPtr monitoringSinkCpu = createMonitoringSink(metricStore,
                                                               cpuCollector.getType(),
                                                               Monitoring::CpuMetrics::getSchema(""),
                                                               nodeEngine,
                                                               1,
                                                               SharedQueryId(0),
                                                               INVALID_DECOMPOSED_QUERY_PLAN_ID,
                                                               INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                                               FaultToleranceType::NONE,
                                                               CheckpointStorageType::NONE,
                                                               1);
    monitoringSinkCpu->writeData(tupleBufferCpu, wctx);

    // test disk metrics
    Monitoring::StoredNodeMetricsPtr storedMetrics = metricStore->getAllMetrics(nodeId1);
    auto metricVec = storedMetrics->at(Monitoring::MetricType::DiskMetric);
    Monitoring::TimestampMetricPtr pairedDiskMetric = metricVec->at(0);
    Monitoring::MetricPtr retMetric = pairedDiskMetric->second;
    Monitoring::DiskMetrics parsedMetrics = retMetric->getValue<Monitoring::DiskMetrics>();

    NES_INFO("MetricStoreTest: Stored metrics{}", Monitoring::MetricUtils::toJson(storedMetrics));
    ASSERT_TRUE(storedMetrics->size() == 1);
    EXPECT_EQ(parsedMetrics, typedMetric);

    // test cpu metrics
    Monitoring::StoredNodeMetricsPtr storedMetricsCpu = metricStore->getAllMetrics(nodeId2);
    auto metricVecCpu = storedMetricsCpu->at(Monitoring::MetricType::WrappedCpuMetrics);
    Monitoring::TimestampMetricPtr pairedCpuMetric = metricVecCpu->at(0);
    Monitoring::MetricPtr retMetricCpu = pairedCpuMetric->second;
    Monitoring::CpuMetricsWrapper parsedMetricsCpu = retMetricCpu->getValue<Monitoring::CpuMetricsWrapper>();

    NES_INFO("MetricStoreTest: Stored metrics{}", Monitoring::MetricUtils::toJson(storedMetricsCpu));
    ASSERT_TRUE(storedMetricsCpu->size() == 1);
    EXPECT_EQ(parsedMetricsCpu, typedMetricCpu);
}

}// namespace NES
