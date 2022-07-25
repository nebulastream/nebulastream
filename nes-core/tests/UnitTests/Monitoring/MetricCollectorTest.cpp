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

// clang-format: off
#include <NesBaseTest.hpp>
#include <gtest/gtest.h>
// clang-format: on

#include "../../../tests/util/MetricValidator.hpp"
#include <API/Schema.hpp>
#include <Monitoring/MetricCollectors/CpuCollector.hpp>
#include <Monitoring/MetricCollectors/DiskCollector.hpp>
#include <Monitoring/MetricCollectors/MemoryCollector.hpp>
#include <Monitoring/MetricCollectors/NetworkCollector.hpp>
#include <Monitoring/Metrics/Gauge/CpuMetrics.hpp>
#include <Monitoring/Metrics/Gauge/DiskMetrics.hpp>
#include <Monitoring/Metrics/Gauge/MemoryMetrics.hpp>
#include <Monitoring/Metrics/Metric.hpp>
#include <Monitoring/Metrics/Wrapper/CpuMetricsWrapper.hpp>
#include <Monitoring/Metrics/Wrapper/NetworkMetricsWrapper.hpp>
#include <Monitoring/MonitoringCatalog.hpp>
#include <Monitoring/ResourcesReader/SystemResourcesReaderFactory.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <Monitoring/Util/MetricUtils.hpp>

namespace NES {

using namespace Configurations;
using namespace Runtime;

class MetricCollectorTest : public Testing::NESBaseTest {
  public:
    Runtime::BufferManagerPtr bufferManager;
    AbstractSystemResourcesReaderPtr reader;
    TopologyNodeId nodeId;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("MetricCollectorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("ResourcesReaderTest: Setup MetricCollectorTest test class.");
    }

    static void TearDownTestCase() { std::cout << "MetricCollectorTest: Tear down MetricCollectorTest class." << std::endl; }

    /* Will be called before a  test is executed. */
    void SetUp() override {
        std::cout << "MetricCollectorTest: Setup MetricCollectorTest test case." << std::endl;

        auto bufferSize = 4096;
        nodeId = TopologyNodeId(4711);
        bufferManager = std::make_shared<Runtime::BufferManager>(bufferSize, 10);
        reader = SystemResourcesReaderFactory::getSystemResourcesReader();
    }

    /* Will be called after a test is executed. */
    void TearDown() override { std::cout << "MetricCollectorTest: Tear down MetricCollectorTest test case." << std::endl; }
};

TEST_F(MetricCollectorTest, testNetworkCollectorWrappedMetrics) {
    auto networkCollector = NetworkCollector();
    networkCollector.setNodeId(nodeId);
    MetricPtr networkMetric = networkCollector.readMetric();
    ASSERT_EQ(networkMetric->getMetricType(), MetricType::WrappedNetworkMetrics);

    NetworkMetricsWrapper wrappedMetric = networkMetric->getValue<NetworkMetricsWrapper>();

    if (reader->getReaderType() != SystemResourcesReaderType::AbstractReader) {
        auto bufferSize = NetworkMetrics::getSchema("")->getSchemaSizeInBytes() * wrappedMetric.size();
        auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();
        wrappedMetric.writeToBuffer(tupleBuffer, 0);
        ASSERT_TRUE(tupleBuffer.getNumberOfTuples() == wrappedMetric.size());

        NetworkMetricsWrapper parsedMetric{};
        readFromBuffer(parsedMetric, tupleBuffer, 0);
        NES_DEBUG("MetricCollectorTest:\nRead metric " << asJson(wrappedMetric) << "\nParsed metric: " << asJson(parsedMetric));
        ASSERT_EQ(wrappedMetric, parsedMetric);
        ASSERT_EQ(parsedMetric.getNodeId(), nodeId);
    } else {
        NES_DEBUG("MetricCollectorTest: Skipping testNetworkCollectorWrappedMetrics. Abstract reader found.");
    }
}

TEST_F(MetricCollectorTest, testNetworkCollectorSingleMetrics) {
    auto readMetrics = reader->readNetworkStats();
    readMetrics.setNodeId(nodeId);

    auto networkCollector = NetworkCollector();
    networkCollector.setNodeId(nodeId);
    MetricPtr networkMetric = networkCollector.readMetric();
    ASSERT_EQ(networkMetric->getMetricType(), MetricType::WrappedNetworkMetrics);

    NetworkMetricsWrapper wrappedMetric = networkMetric->getValue<NetworkMetricsWrapper>();
    ASSERT_EQ(readMetrics.size(), wrappedMetric.size());

    if (reader->getReaderType() != SystemResourcesReaderType::AbstractReader) {
        NetworkMetrics totalMetrics = wrappedMetric.getNetworkValue(0);
        auto bufferSize = NetworkMetrics::getSchema("")->getSchemaSizeInBytes();
        auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();
        writeToBuffer(totalMetrics, tupleBuffer, 0);
        ASSERT_TRUE(tupleBuffer.getNumberOfTuples() == 1);

        NetworkMetrics parsedMetric{};
        readFromBuffer(parsedMetric, tupleBuffer, 0);
        NES_DEBUG("MetricCollectorTest:\nRead metric " << asJson(wrappedMetric) << "\nParsed metric: " << asJson(parsedMetric));
        ASSERT_EQ(totalMetrics, parsedMetric);
        ASSERT_EQ(totalMetrics.nodeId, nodeId);
        ASSERT_EQ(readMetrics.getNodeId(), nodeId);
        ASSERT_EQ(parsedMetric.nodeId, nodeId);
    } else {
        NES_DEBUG("MetricCollectorTest: Skipping testNetworkCollectorSingleMetrics. Abstract reader found.");
    }
}

//TEST_F(MetricCollectorTest, testCpuCollectorWrappedMetrics) {
//    auto cpuCollector = CpuCollector();
//    cpuCollector.setNodeId(nodeId);
//    MetricPtr cpuMetric = cpuCollector.readMetric();
//    ASSERT_EQ(cpuMetric->getMetricType(), MetricType::WrappedCpuMetrics);
//
//    CpuMetricsWrapper wrappedMetric = cpuMetric->getValue<CpuMetricsWrapper>();
//
//    if (reader->getReaderType() != SystemResourcesReaderType::AbstractReader) {
//        auto bufferSize = CpuMetrics::getDefaultSchema("")->getSchemaSizeInBytes() * wrappedMetric.size();
//        auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();
//        wrappedMetric.writeToBuffer(tupleBuffer, 0);
//        ASSERT_TRUE(tupleBuffer.getNumberOfTuples() == wrappedMetric.size());
//
//        CpuMetricsWrapper parsedMetric{};
//        readFromBuffer(parsedMetric, tupleBuffer, 0);
//        NES_DEBUG("MetricCollectorTest:\nRead metric " << asJson(wrappedMetric) << "\nParsed metric: " << asJson(parsedMetric));
//        ASSERT_EQ(wrappedMetric, parsedMetric);
//        ASSERT_EQ(parsedMetric.getNodeId(), nodeId);
//    } else {
//        NES_DEBUG("MetricCollectorTest: Skipping testCpuCollectorWrappedMetrics. Abstract reader found.");
//    }
//}

TEST_F(MetricCollectorTest, testCpuCollectorSingleMetrics) {
    auto readMetrics = reader->readCpuStats();
    readMetrics.setNodeId(nodeId);

    auto cpuCollector = CpuCollector();
    cpuCollector.setNodeId(nodeId);
    MetricPtr cpuMetric = cpuCollector.readMetric();
    ASSERT_EQ(cpuMetric->getMetricType(), MetricType::WrappedCpuMetrics);

    CpuMetricsWrapper wrappedMetric = cpuMetric->getValue<CpuMetricsWrapper>();
    ASSERT_EQ(readMetrics.size(), wrappedMetric.size());

    if (reader->getReaderType() != SystemResourcesReaderType::AbstractReader) {
        CpuMetrics totalMetrics = wrappedMetric.getValue(0);
        auto bufferSize = CpuMetrics::getDefaultSchema("")->getSchemaSizeInBytes();
        auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();
        writeToBuffer(totalMetrics, tupleBuffer, 0);
        ASSERT_TRUE(tupleBuffer.getNumberOfTuples() == 1);

        CpuMetrics parsedMetric{};
        readFromBuffer(parsedMetric, tupleBuffer, 0);
        NES_DEBUG("MetricCollectorTest:\nRead metric " << asJson(wrappedMetric) << "\nParsed metric: " << asJson(parsedMetric));
        ASSERT_EQ(totalMetrics, parsedMetric);
        ASSERT_EQ(totalMetrics.nodeId, nodeId);
        ASSERT_EQ(readMetrics.getNodeId(), nodeId);
        ASSERT_EQ(parsedMetric.nodeId, nodeId);
    } else {
        NES_DEBUG("MetricCollectorTest: Skipping testcpuCollectorSingleMetrics. Abstract reader found.");
    }
}

TEST_F(MetricCollectorTest, testDiskCollector) {
    auto diskCollector = DiskCollector();
    diskCollector.setNodeId(nodeId);
    MetricPtr diskMetric = diskCollector.readMetric();
    DiskMetrics typedMetric = diskMetric->getValue<DiskMetrics>();
    ASSERT_EQ(diskMetric->getMetricType(), MetricType::DiskMetric);
    auto bufferSize = DiskMetrics::getDefaultSchema("")->getSchemaSizeInBytes();
    auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();
    writeToBuffer(typedMetric, tupleBuffer, 0);

    ASSERT_TRUE(tupleBuffer.getNumberOfTuples() == 1);
    ASSERT_TRUE(MetricValidator::isValid(SystemResourcesReaderFactory::getSystemResourcesReader(), typedMetric));

    MetricPtr parsedMetric = std::make_shared<Metric>(DiskMetrics{});
    readFromBuffer(parsedMetric, tupleBuffer, 0);
    NES_DEBUG("MetricCollectorTest:\nRead metric " << asJson(typedMetric) << "\nParsed metric: " << asJson(parsedMetric));
    ASSERT_EQ(typedMetric, parsedMetric->getValue<DiskMetrics>());
    ASSERT_EQ(parsedMetric->getValue<DiskMetrics>().nodeId, nodeId);
    ASSERT_EQ(typedMetric.nodeId, nodeId);
}

TEST_F(MetricCollectorTest, testMemoryCollector) {
    auto memoryCollector = MemoryCollector();
    memoryCollector.setNodeId(nodeId);

    MetricPtr memoryMetric = memoryCollector.readMetric();
    MemoryMetrics typedMetric = memoryMetric->getValue<MemoryMetrics>();
    ASSERT_EQ(memoryMetric->getMetricType(), MetricType::MemoryMetric);
    auto bufferSize = MemoryMetrics::getDefaultSchema("")->getSchemaSizeInBytes();
    auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();
    writeToBuffer(typedMetric, tupleBuffer, 0);

    ASSERT_TRUE(tupleBuffer.getNumberOfTuples() == 1);
    ASSERT_TRUE(MetricValidator::isValid(SystemResourcesReaderFactory::getSystemResourcesReader(), typedMetric));

    // TODO no direct MemoryMetric object, instead pointer
    MemoryMetrics parsedMetric{};
    readFromBuffer(parsedMetric, tupleBuffer, 0);
    NES_DEBUG("MetricCollectorTest:\nRead metric " << asJson(typedMetric) << "\nParsed metric: " << asJson(parsedMetric));
    ASSERT_EQ(typedMetric, parsedMetric);
}

TEST_F(MetricCollectorTest, testDiskCollectorConfiguration) {
    // Select i random attributes from the vector. This way different configurations will be tested.
    std::tuple<std::vector<std::string>, std::list<std::string>> randomTuple =
        MetricUtils::randomAttributes("DiskMetric", 3);
    std::vector<std::string>  attributesList = get<0>(randomTuple);
    std::list<std::string>  configuredMetrics = get<1>(randomTuple);
    std::string configuredMetricsString = MetricUtils::listToString(", ", configuredMetrics);
    NES_DEBUG("Configured attributes are: " << configuredMetricsString);

    SchemaPtr schema = DiskMetrics::createSchema("", configuredMetrics);

    for (const std::string& attribute : configuredMetrics) {
        ASSERT_TRUE(schema->contains(attribute));
    }
    for (const std::string& attribute : attributesList) {
        ASSERT_FALSE(schema->contains(attribute));
    }

    auto diskCollector = DiskCollector(schema);       //Schema übergeben
    diskCollector.setNodeId(nodeId);            //Set NodeId to right Value
    MetricPtr diskMetric = diskCollector.readMetric();
    DiskMetrics typedMetric = diskMetric->getValue<DiskMetrics>();
    typedMetric.setSchema(schema);
    ASSERT_EQ(diskMetric->getMetricType(), MetricType::DiskMetric);
    ASSERT_EQ(typedMetric.getSchema(), schema);             // Compare if DiskMetric object has the right schema
    auto bufferSize = typedMetric.getSchema()->getSchemaSizeInBytes();
    auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();
    writeToBuffer(typedMetric, tupleBuffer, 0);         // write Values to Buffer

    ASSERT_TRUE(tupleBuffer.getNumberOfTuples() == 1);
    ASSERT_TRUE(MetricValidator::isValid(SystemResourcesReaderFactory::getSystemResourcesReader(), typedMetric));

    MetricPtr parsedMetric = std::make_shared<Metric>(DiskMetrics(schema));
    readFromBuffer(parsedMetric, tupleBuffer, 0);
    NES_DEBUG("MetricCollectorTest:\nRead metric " << asJson(typedMetric) << "\nParsed metric: " << asJson(parsedMetric));
    DiskMetrics parsedMetricValues = parsedMetric->getValue<DiskMetrics>();
    ASSERT_EQ(parsedMetricValues.getSchema(), schema);
    // create Assert_EQ nur die Werte vom gewünschten Schema wurden ausgelesen
    for(const std::string& metricName : configuredMetrics) {
        ASSERT_EQ(parsedMetricValues.getValue(metricName), typedMetric.getValue(metricName));
    }
    // check if attributes that arent in the configuration are zero
    for(const std::string& metricName : attributesList) {
        ASSERT_EQ(parsedMetricValues.getValue(metricName), 0);
    }
    ASSERT_EQ(typedMetric.nodeId, nodeId);
}

TEST_F(MetricCollectorTest, testMemoryCollectorConfiguration) {
    // Select i random attributes from the vector. This way different configurations will be tested.
    std::tuple<std::vector<std::string>, std::list<std::string>> randomTuple =
        MetricUtils::randomAttributes("MemoryMetric", 5);
    std::vector<std::string>  attributesList = get<0>(randomTuple);
    std::list<std::string>  configuredMetrics = get<1>(randomTuple);
    std::string configuredMetricsString = MetricUtils::listToString(", ", configuredMetrics);
    NES_DEBUG("Configured attributes are: " << configuredMetricsString);

    SchemaPtr schema = MemoryMetrics::createSchema("", configuredMetrics);
    for (const std::string& attribute : configuredMetrics) {
        ASSERT_TRUE(schema->contains(attribute));
    }
    for (const std::string& attribute : attributesList) {
        ASSERT_FALSE(schema->contains(attribute));
    }

    auto memoryCollector = MemoryCollector(schema);
    memoryCollector.setNodeId(nodeId);
    MetricPtr memoryMetric = memoryCollector.readMetric();
    MemoryMetrics typedMetric = memoryMetric->getValue<MemoryMetrics>();
    typedMetric.setSchema(schema);
    ASSERT_EQ(memoryMetric->getMetricType(), MetricType::MemoryMetric);
    auto bufferSize = typedMetric.getSchema()->getSchemaSizeInBytes();
    auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();
    writeToBuffer(typedMetric, tupleBuffer, 0);

    ASSERT_TRUE(tupleBuffer.getNumberOfTuples() == 1);
    ASSERT_TRUE(MetricValidator::isValid(SystemResourcesReaderFactory::getSystemResourcesReader(), typedMetric));

    MetricPtr  parsedMetric = std::make_shared<Metric>(MemoryMetrics(schema));
    readFromBuffer(parsedMetric, tupleBuffer, 0);
    NES_DEBUG("MetricCollectorTest:\nRead metric " << asJson(typedMetric) << "\nParsed metric: " << asJson(parsedMetric));
    MemoryMetrics parsedMetricValues = parsedMetric->getValue<MemoryMetrics>();
    ASSERT_EQ(parsedMetricValues.getSchema(), schema);
    for(const std::string& metricName : configuredMetrics) {
        ASSERT_EQ(parsedMetricValues.getValue(metricName), typedMetric.getValue(metricName));
    }
    for(const std::string& metricName : attributesList) {
        ASSERT_EQ(parsedMetricValues.getValue(metricName), 0);
    }
    ASSERT_EQ(typedMetric.nodeId, nodeId);
}

TEST_F(MetricCollectorTest, testCpuCollectorWrappedMetricsConfiguration) {
    std::tuple<std::vector<std::string>, std::list<std::string>> randomTuple =
        MetricUtils::randomAttributes("CpuMetric", 5);
    std::vector<std::string>  attributesList = get<0>(randomTuple);
    std::list<std::string>  configuredMetrics = get<1>(randomTuple);
    std::string configuredMetricsString = MetricUtils::listToString(", ", configuredMetrics);
    NES_DEBUG("Configured attributes are: " << configuredMetricsString);

    SchemaPtr schema = CpuMetrics::createSchema("", configuredMetrics);
    for (std::string attribute : configuredMetrics) {
        ASSERT_TRUE(schema->contains(attribute));
    }
    for (std::string attribute : attributesList) {
        ASSERT_FALSE(schema->contains(attribute));
    }

    auto cpuCollector = CpuCollector(schema);
    cpuCollector.setNodeId(nodeId);
    MetricPtr cpuMetric = cpuCollector.readMetric();
    ASSERT_EQ(cpuMetric->getMetricType(), MetricType::WrappedCpuMetrics);

    CpuMetricsWrapper wrappedMetric = cpuMetric->getValue<CpuMetricsWrapper>();

    if (reader->getReaderType() != SystemResourcesReaderType::AbstractReader) {
        auto bufferSize = CpuMetrics::getDefaultSchema("")->getSchemaSizeInBytes() * wrappedMetric.size();
        auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();
        wrappedMetric.writeToBuffer(tupleBuffer, 0);
        ASSERT_TRUE(tupleBuffer.getNumberOfTuples() == wrappedMetric.size());

        CpuMetricsWrapper parsedMetric{};
        readFromBuffer(parsedMetric, tupleBuffer, 0);
        NES_DEBUG("MetricCollectorTest:\nRead metric " << asJson(wrappedMetric) << "\nParsed metric: " << asJson(parsedMetric));
        ASSERT_EQ(wrappedMetric, parsedMetric);
        ASSERT_EQ(parsedMetric.getNodeId(), nodeId);
    } else {
        NES_DEBUG("MetricCollectorTest: Skipping testCpuCollectorWrappedMetrics. Abstract reader found.");
    }
}

TEST_F(MetricCollectorTest, testCpuCollectorSingleMetricsConfiguration) {
    std::tuple<std::vector<std::string>, std::list<std::string>> randomTuple =
        MetricUtils::randomAttributes("CpuMetric", 5);
    std::vector<std::string>  attributesList = get<0>(randomTuple);
    std::list<std::string>  configuredMetrics = get<1>(randomTuple);
    std::string configuredMetricsString = MetricUtils::listToString(", ", configuredMetrics);
    NES_DEBUG("Configured attributes are: " << configuredMetricsString);

    SchemaPtr schema = CpuMetrics::createSchema("", configuredMetrics);
    for (std::string attribute : configuredMetrics) {
        ASSERT_TRUE(schema->contains(attribute));
    }
    for (std::string attribute : attributesList) {
        ASSERT_FALSE(schema->contains(attribute));
    }

    auto readMetrics = reader->readCpuStats();
    readMetrics.setNodeId(nodeId);

    auto cpuCollector = CpuCollector(schema);
    cpuCollector.setNodeId(nodeId);
    MetricPtr cpuMetric = cpuCollector.readMetric();
    ASSERT_EQ(cpuMetric->getMetricType(), MetricType::WrappedCpuMetrics);

    CpuMetricsWrapper wrappedMetric = cpuMetric->getValue<CpuMetricsWrapper>();
    ASSERT_EQ(readMetrics.size(), wrappedMetric.size());

    if (reader->getReaderType() != SystemResourcesReaderType::AbstractReader) {
        CpuMetrics totalMetrics = wrappedMetric.getValue(0);
        totalMetrics.setSchema(schema);
        auto bufferSize = totalMetrics.getSchema()->getSchemaSizeInBytes();
        auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();
        writeToBuffer(totalMetrics, tupleBuffer, 0);
        ASSERT_TRUE(tupleBuffer.getNumberOfTuples() == 1);

        MetricPtr  parsedMetric = std::make_shared<Metric>(CpuMetrics(schema));
        readFromBuffer(parsedMetric, tupleBuffer, 0);
        NES_DEBUG("MetricCollectorTest:\nRead metric " << asJson(wrappedMetric) << "\nParsed metric: " << asJson(parsedMetric));
        CpuMetrics parsedMetricValues = parsedMetric->getValue<CpuMetrics>();
        ASSERT_EQ(parsedMetricValues.getSchema(), schema);
        for(const std::string& metricName : configuredMetrics) {
            ASSERT_EQ(parsedMetricValues.getValue(metricName), totalMetrics.getValue(metricName));
        }
        for(const std::string& metricName : attributesList) {
            ASSERT_EQ(parsedMetricValues.getValue(metricName), 0);
        }
        ASSERT_EQ(totalMetrics.nodeId, nodeId);
        ASSERT_EQ(readMetrics.getNodeId(), nodeId);
        ASSERT_EQ(parsedMetricValues.nodeId, nodeId);
    } else {
        NES_DEBUG("MetricCollectorTest: Skipping testcpuCollectorSingleMetrics. Abstract reader found.");
    }
}

//TEST_F(MetricCollectorTest, testNetworkCollectorSingleMetricsConfiguration) {
//    auto readMetrics = reader->readNetworkStats();
//    readMetrics.setNodeId(nodeId);
//
//    auto networkCollector = NetworkCollector();
//    networkCollector.setNodeId(nodeId);
//    MetricPtr networkMetric = networkCollector.readMetric();
//    ASSERT_EQ(networkMetric->getMetricType(), MetricType::WrappedNetworkMetrics);
//
//    NetworkMetricsWrapper wrappedMetric = networkMetric->getValue<NetworkMetricsWrapper>();
//    ASSERT_EQ(readMetrics.size(), wrappedMetric.size());
//
//    if (reader->getReaderType() != SystemResourcesReaderType::AbstractReader) {
//        NetworkMetrics totalMetrics = wrappedMetric.getNetworkValue(0);
//        auto bufferSize = NetworkMetrics::getSchema("")->getSchemaSizeInBytes();
//        auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();
//        writeToBuffer(totalMetrics, tupleBuffer, 0);
//        ASSERT_TRUE(tupleBuffer.getNumberOfTuples() == 1);
//
//        NetworkMetrics parsedMetric{};
//        readFromBuffer(parsedMetric, tupleBuffer, 0);
//        NES_DEBUG("MetricCollectorTest:\nRead metric " << asJson(wrappedMetric) << "\nParsed metric: " << asJson(parsedMetric));
//        ASSERT_EQ(totalMetrics, parsedMetric);
//        ASSERT_EQ(totalMetrics.nodeId, nodeId);
//        ASSERT_EQ(readMetrics.getNodeId(), nodeId);
//        ASSERT_EQ(parsedMetric.nodeId, nodeId);
//    } else {
//        NES_DEBUG("MetricCollectorTest: Skipping testNetworkCollectorSingleMetrics. Abstract reader found.");
//    }
//}
}// namespace NES
