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
#include <Util/MetricValidator.hpp>
#include <Monitoring/Util/MetricUtils.hpp>

namespace NES {

using namespace Configurations;
using namespace Runtime;

class MetricCollectorTest : public Testing::NESBaseTest {
  public:
    Runtime::BufferManagerPtr bufferManager;
    Monitoring::AbstractSystemResourcesReaderPtr reader;
    TopologyNodeId nodeId;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("MetricCollectorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("ResourcesReaderTest: Setup MetricCollectorTest test class.");
    }

    /* Will be called before a  test is executed. */
    void SetUp() override {
        Testing::NESBaseTest::SetUp();
        NES_DEBUG("MetricCollectorTest: Setup MetricCollectorTest test case.");

        auto bufferSize = 4096;
        nodeId = TopologyNodeId(4711);
        bufferManager = std::make_shared<Runtime::BufferManager>(bufferSize, 10);
        reader = Monitoring::SystemResourcesReaderFactory::getSystemResourcesReader();
    }
};

TEST_F(MetricCollectorTest, testNetworkCollectorWrappedMetrics) {
    auto networkCollector = Monitoring::NetworkCollector();
    networkCollector.setNodeId(nodeId);
    Monitoring::MetricPtr networkMetric = networkCollector.readMetric();
    ASSERT_EQ(networkMetric->getMetricType(), Monitoring::MetricType::WrappedNetworkMetrics);

    Monitoring::NetworkMetricsWrapper wrappedMetric = networkMetric->getValue<Monitoring::NetworkMetricsWrapper>();

    if (reader->getReaderType() != SystemResourcesReaderType::AbstractReader) {
        auto bufferSize = Monitoring::NetworkMetrics::getDefaultSchema("")->getSchemaSizeInBytes() * wrappedMetric.size();
        auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();
        wrappedMetric.writeToBuffer(tupleBuffer, 0);
        ASSERT_TRUE(tupleBuffer.getNumberOfTuples() == wrappedMetric.size());

        Monitoring::NetworkMetricsWrapper parsedMetric{};
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

    auto networkCollector = Monitoring::NetworkCollector();
    networkCollector.setNodeId(nodeId);
    Monitoring::MetricPtr networkMetric = networkCollector.readMetric();
    ASSERT_EQ(networkMetric->getMetricType(), Monitoring::MetricType::WrappedNetworkMetrics);

    Monitoring::NetworkMetricsWrapper wrappedMetric = networkMetric->getValue<Monitoring::NetworkMetricsWrapper>();
    ASSERT_EQ(readMetrics.size(), wrappedMetric.size());

    if (reader->getReaderType() != SystemResourcesReaderType::AbstractReader) {
        Monitoring::NetworkMetrics totalMetrics = wrappedMetric.getNetworkValue(0);
        auto bufferSize = Monitoring::NetworkMetrics::getDefaultSchema("")->getSchemaSizeInBytes();
        auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();
        writeToBuffer(totalMetrics, tupleBuffer, 0);
        ASSERT_TRUE(tupleBuffer.getNumberOfTuples() == 1);

        Monitoring::NetworkMetrics parsedMetric{};
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

TEST_F(MetricCollectorTest, testCpuCollectorWrappedMetrics) {
    auto cpuCollector = Monitoring::CpuCollector();
    cpuCollector.setNodeId(nodeId);
    Monitoring::MetricPtr cpuMetric = cpuCollector.readMetric();
    ASSERT_EQ(cpuMetric->getMetricType(), Monitoring::MetricType::WrappedCpuMetrics);

    Monitoring::CpuMetricsWrapper wrappedMetric = cpuMetric->getValue<Monitoring::CpuMetricsWrapper>();

    if (reader->getReaderType() != SystemResourcesReaderType::AbstractReader) {
        auto bufferSize = Monitoring::CpuMetrics::getDefaultSchema("")->getSchemaSizeInBytes() * wrappedMetric.size();
        auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();
        wrappedMetric.writeToBuffer(tupleBuffer, 0);
        ASSERT_TRUE(tupleBuffer.getNumberOfTuples() == wrappedMetric.size());

        Monitoring::CpuMetricsWrapper parsedMetric{};
        readFromBuffer(parsedMetric, tupleBuffer, 0);
        NES_DEBUG("MetricCollectorTest:\nRead metric " << asJson(wrappedMetric) << "\nParsed metric: " << asJson(parsedMetric));
        ASSERT_EQ(wrappedMetric, parsedMetric);
        ASSERT_EQ(parsedMetric.getNodeId(), nodeId);
    } else {
        NES_DEBUG("MetricCollectorTest: Skipping testCpuCollectorWrappedMetrics. Abstract reader found.");
    }
}

TEST_F(MetricCollectorTest, testCpuCollectorSingleMetrics) {
    auto readMetrics = reader->readCpuStats();
    readMetrics.setNodeId(nodeId);

    auto cpuCollector = Monitoring::CpuCollector();
    cpuCollector.setNodeId(nodeId);
    Monitoring::MetricPtr cpuMetric = cpuCollector.readMetric();
    ASSERT_EQ(cpuMetric->getMetricType(), Monitoring::MetricType::WrappedCpuMetrics);

    Monitoring::CpuMetricsWrapper wrappedMetric = cpuMetric->getValue<Monitoring::CpuMetricsWrapper>();
    ASSERT_EQ(readMetrics.size(), wrappedMetric.size());

    if (reader->getReaderType() != SystemResourcesReaderType::AbstractReader) {
        Monitoring::CpuMetrics totalMetrics = wrappedMetric.getValue(0);
        auto bufferSize = Monitoring::CpuMetrics::getDefaultSchema("")->getSchemaSizeInBytes();
        auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();
        writeToBuffer(totalMetrics, tupleBuffer, 0);
        ASSERT_TRUE(tupleBuffer.getNumberOfTuples() == 1);

        Monitoring::CpuMetrics parsedMetric{};
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
    auto diskCollector = Monitoring::DiskCollector();
    diskCollector.setNodeId(nodeId);
    Monitoring::MetricPtr diskMetric = diskCollector.readMetric();
    Monitoring::DiskMetrics typedMetric = diskMetric->getValue<Monitoring::DiskMetrics>();
    ASSERT_EQ(diskMetric->getMetricType(), Monitoring::MetricType::DiskMetric);
    auto bufferSize = Monitoring::DiskMetrics::getDefaultSchema("")->getSchemaSizeInBytes();
    auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();
    writeToBuffer(typedMetric, tupleBuffer, 0);

    ASSERT_TRUE(tupleBuffer.getNumberOfTuples() == 1);
    ASSERT_TRUE(MetricValidator::isValid(Monitoring::SystemResourcesReaderFactory::getSystemResourcesReader(), typedMetric));

    Monitoring::MetricPtr parsedMetric = std::make_shared<Monitoring::Metric>(Monitoring::DiskMetrics{});
    readFromBuffer(parsedMetric, tupleBuffer, 0);
    NES_DEBUG("MetricCollectorTest:\nRead metric " << asJson(typedMetric) << "\nParsed metric: " << asJson(parsedMetric));
    ASSERT_EQ(typedMetric, parsedMetric->getValue<Monitoring::DiskMetrics>());
    ASSERT_EQ(parsedMetric->getValue<Monitoring::DiskMetrics>().nodeId, nodeId);
    ASSERT_EQ(typedMetric.nodeId, nodeId);
}

TEST_F(MetricCollectorTest, testMemoryCollector) {
    auto memoryCollector = Monitoring::MemoryCollector();
    memoryCollector.setNodeId(nodeId);

    Monitoring::MetricPtr memoryMetric = memoryCollector.readMetric();
    Monitoring::MemoryMetrics typedMetric = memoryMetric->getValue<Monitoring::MemoryMetrics>();
    ASSERT_EQ(memoryMetric->getMetricType(), Monitoring::MetricType::MemoryMetric);
    auto bufferSize = Monitoring::MemoryMetrics::getDefaultSchema("")->getSchemaSizeInBytes();
    auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();
    writeToBuffer(typedMetric, tupleBuffer, 0);

    ASSERT_TRUE(tupleBuffer.getNumberOfTuples() == 1);
    ASSERT_TRUE(MetricValidator::isValid(Monitoring::SystemResourcesReaderFactory::getSystemResourcesReader(), typedMetric));

    Monitoring::MemoryMetrics parsedMetric{};
    readFromBuffer(parsedMetric, tupleBuffer, 0);
    NES_DEBUG("MetricCollectorTest:\nRead metric " << asJson(typedMetric) << "\nParsed metric: " << asJson(parsedMetric));
    ASSERT_EQ(typedMetric, parsedMetric);
}

TEST_F(MetricCollectorTest, testDiskCollectorConfiguration) {
    // configuration of attributes
    std::list<std::string> configuredMetricsList = {"F_BLOCKS", "F_BSIZE", "F_BAVAIL"};
    std::list<std::string> notConfiguredMetricsList = {"F_FRSIZE", "F_BFREE"};
    SchemaPtr schema = Monitoring::DiskMetrics::createSchema("", configuredMetricsList);

    // checks, if the schema was created correct
    for (const std::string& attribute : configuredMetricsList) {
        ASSERT_TRUE(schema->contains(attribute));
    }
    for (const std::string& attribute : notConfiguredMetricsList) {
        ASSERT_FALSE(schema->contains(attribute));
    }

    auto diskCollector = Monitoring::DiskCollector(schema);
    diskCollector.setNodeId(nodeId);
    Monitoring::MetricPtr diskMetric = diskCollector.readMetric();
    Monitoring::DiskMetrics typedMetric = diskMetric->getValue<Monitoring::DiskMetrics>();
    ASSERT_EQ(diskMetric->getMetricType(), Monitoring::MetricType::DiskMetric);
    ASSERT_EQ(typedMetric.getSchema(), schema);
    auto bufferSize = typedMetric.getSchema()->getSchemaSizeInBytes();
    auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();
    writeToBuffer(typedMetric, tupleBuffer, 0);

    ASSERT_TRUE(tupleBuffer.getNumberOfTuples() == 1);
    ASSERT_TRUE(MetricValidator::isValid(Monitoring::SystemResourcesReaderFactory::getSystemResourcesReader(), typedMetric));

    Monitoring::MetricPtr parsedMetric = std::make_shared<Monitoring::Metric>(Monitoring::DiskMetrics(schema));
    readFromBuffer(parsedMetric, tupleBuffer, 0);
    NES_DEBUG("MetricCollectorTest:\nRead metric " << asJson(typedMetric) << "\nParsed metric: " << asJson(parsedMetric));
    Monitoring::DiskMetrics parsedMetricValues = parsedMetric->getValue<Monitoring::DiskMetrics>();
    ASSERT_EQ(parsedMetricValues.getSchema(), schema);

    for(const std::string& metricName : configuredMetricsList) {
        ASSERT_EQ(parsedMetricValues.getValue(metricName), typedMetric.getValue(metricName));
    }
    // check if attributes that are not in the configuration are zero
    for(const std::string& metricName : notConfiguredMetricsList) {
        ASSERT_EQ(parsedMetricValues.getValue(metricName), 0);
    }
    ASSERT_EQ(typedMetric.nodeId, nodeId);
}

TEST_F(MetricCollectorTest, testMemoryCollectorConfiguration) {
    // configuration of attributes
    std::list<std::string> configuredMetricsList = {"TOTAL_RAM", "LOADS_5MIN", "FREE_HIGH", "TOTAL_HIGH", "PROCS", "LOADS_15MIN"};
    std::list<std::string> notConfiguredMetricsList = {"TOTAL_SWAP", "FREE_RAM", "SHARED_RAM", "BUFFER_RAM", "FREE_SWAP", "MEM_UNIT", "LOADS_1MIN"};
    SchemaPtr schema = Monitoring::MemoryMetrics::createSchema("", configuredMetricsList);
    for (const std::string& attribute : configuredMetricsList) {
        ASSERT_TRUE(schema->contains(attribute));
    }
    for (const std::string& attribute : notConfiguredMetricsList) {
        ASSERT_FALSE(schema->contains(attribute));
    }

    auto memoryCollector = Monitoring::MemoryCollector(schema);
    memoryCollector.setNodeId(nodeId);
    Monitoring::MetricPtr memoryMetric = memoryCollector.readMetric();
    Monitoring::MemoryMetrics typedMetric = memoryMetric->getValue<Monitoring::MemoryMetrics>();
    ASSERT_EQ(memoryMetric->getMetricType(), Monitoring::MetricType::MemoryMetric);
    auto bufferSize = typedMetric.getSchema()->getSchemaSizeInBytes();
    auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();
    writeToBuffer(typedMetric, tupleBuffer, 0);

    ASSERT_TRUE(tupleBuffer.getNumberOfTuples() == 1);
    ASSERT_TRUE(MetricValidator::isValid(Monitoring::SystemResourcesReaderFactory::getSystemResourcesReader(), typedMetric));

    Monitoring::MetricPtr  parsedMetric = std::make_shared<Monitoring::Metric>(Monitoring::MemoryMetrics(schema));
    readFromBuffer(parsedMetric, tupleBuffer, 0);
    NES_DEBUG("MetricCollectorTest:\nRead metric " << asJson(typedMetric) << "\nParsed metric: " << asJson(parsedMetric));
    Monitoring::MemoryMetrics parsedMetricValues = parsedMetric->getValue<Monitoring::MemoryMetrics>();
    ASSERT_EQ(parsedMetricValues.getSchema(), schema);
    for(const std::string& metricName : configuredMetricsList) {
        ASSERT_EQ(parsedMetricValues.getValue(metricName), typedMetric.getValue(metricName));
    }
    for(const std::string& metricName : notConfiguredMetricsList) {
        ASSERT_EQ(parsedMetricValues.getValue(metricName), 0);
    }
    ASSERT_EQ(typedMetric.nodeId, nodeId);
}

TEST_F(MetricCollectorTest, testCpuCollectorWrappedMetricsConfiguration) {
    // configuration of attributes
    std::list<std::string> configuredMetricsList = {"idle", "guest", "nice", "steal", "guestnice"};
    std::list<std::string> notConfiguredMetricsList = {"user", "system", "iowait", "irq", "softirq"};
    SchemaPtr schema = Monitoring::CpuMetrics::createSchema("", configuredMetricsList);
    for (std::string attribute : configuredMetricsList) {
        ASSERT_TRUE(schema->contains(attribute));
    }
    for (std::string attribute : notConfiguredMetricsList) {
        ASSERT_FALSE(schema->contains(attribute));
    }

    auto cpuCollector = Monitoring::CpuCollector(schema);
    cpuCollector.setNodeId(nodeId);
    Monitoring::MetricPtr cpuMetric = cpuCollector.readMetric();
    ASSERT_EQ(cpuMetric->getMetricType(), Monitoring::MetricType::WrappedCpuMetrics);

    Monitoring::CpuMetricsWrapper wrappedMetric = cpuMetric->getValue<Monitoring::CpuMetricsWrapper>();
    ASSERT_EQ(wrappedMetric.getSchema(), schema);
    if (reader->getReaderType() != SystemResourcesReaderType::AbstractReader) {
        auto bufferSize = wrappedMetric.getSchema()->getSchemaSizeInBytes() * wrappedMetric.size();
        auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();
        wrappedMetric.writeToBuffer(tupleBuffer, 0);
        ASSERT_TRUE(tupleBuffer.getNumberOfTuples() == wrappedMetric.size());

        Monitoring::MetricPtr parsedMetric = std::make_shared<Monitoring::Metric>(Monitoring::CpuMetricsWrapper(schema));
        readFromBuffer(parsedMetric, tupleBuffer, 0);
        Monitoring::CpuMetricsWrapper parsedMetricValues = parsedMetric->getValue<Monitoring::CpuMetricsWrapper>();
        NES_DEBUG("MetricCollectorTest:\nRead metric " << asJson(wrappedMetric) << "\nParsed metric: " << asJson(parsedMetric));
        for (unsigned int i = 0; i < parsedMetricValues.size(); i++) {
            for (const std::string& metricName : configuredMetricsList) {
                ASSERT_EQ(parsedMetricValues.getValue(i).getValue(metricName), wrappedMetric.getValue(i).getValue(metricName));
            }
            for (const std::string& metricName : notConfiguredMetricsList) {
                ASSERT_EQ(parsedMetricValues.getValue(i).getValue(metricName), 0);
            }
        }
        ASSERT_EQ(parsedMetricValues.getNodeId(), nodeId);
    } else {
        NES_DEBUG("MetricCollectorTest: Skipping testCpuCollectorWrappedMetrics. Abstract reader found.");
    }
}

TEST_F(MetricCollectorTest, testCpuCollectorSingleMetricsConfiguration) {
    // configuration of metrics and cpu cores
    std::list<uint64_t> cpuList {0};
    std::list<std::string> configuredMetricsList = {"idle", "guest", "nice", "steal", "guestnice"};
    std::list<std::string> notConfiguredMetricsList = {"user", "system", "iowait", "irq", "softirq"};
    SchemaPtr schema = Monitoring::CpuMetrics::createSchema("", configuredMetricsList);
    for (std::string attribute : configuredMetricsList) {
        ASSERT_TRUE(schema->contains(attribute));
    }
    for (std::string attribute : notConfiguredMetricsList) {
        ASSERT_FALSE(schema->contains(attribute));
    }

    auto cpuCollector = Monitoring::CpuCollector(schema, cpuList);
    cpuCollector.setNodeId(nodeId);
    Monitoring::MetricPtr cpuMetric = cpuCollector.readMetric();
    ASSERT_EQ(cpuMetric->getMetricType(), Monitoring::MetricType::WrappedCpuMetrics);

    Monitoring::CpuMetricsWrapper wrappedMetric = cpuMetric->getValue<Monitoring::CpuMetricsWrapper>();
    ASSERT_EQ(wrappedMetric.size(), 1);

    if (reader->getReaderType() != SystemResourcesReaderType::AbstractReader) {
        auto bufferSize = wrappedMetric.getSchema()->getSchemaSizeInBytes() * wrappedMetric.size();
        auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();
        wrappedMetric.writeToBuffer(tupleBuffer, 0);
        ASSERT_TRUE(tupleBuffer.getNumberOfTuples() == 1);

        Monitoring::MetricPtr  parsedMetric = std::make_shared<Monitoring::Metric>(Monitoring::CpuMetrics(schema));
        readFromBuffer(parsedMetric, tupleBuffer, 0);
        NES_DEBUG("MetricCollectorTest:\nRead metric " << asJson(wrappedMetric) << "\nParsed metric: " << asJson(parsedMetric));
        Monitoring::CpuMetrics parsedMetricValues = parsedMetric->getValue<Monitoring::CpuMetrics>();
        ASSERT_EQ(parsedMetricValues.getSchema(), schema);
        for(const std::string& metricName : configuredMetricsList) {
            ASSERT_EQ(parsedMetricValues.getValue(metricName), wrappedMetric.getValue(0).getValue(metricName));
        }
        for(const std::string& metricName : notConfiguredMetricsList) {
            ASSERT_EQ(parsedMetricValues.getValue(metricName), 0);
        }
        ASSERT_EQ(wrappedMetric.getValue(0).getValue("nodeId"), nodeId);
        ASSERT_EQ(parsedMetricValues.nodeId, nodeId);
    } else {
        NES_DEBUG("MetricCollectorTest: Skipping testcpuCollectorSingleMetrics. Abstract reader found.");
    }
}

TEST_F(MetricCollectorTest, testNetworkCollectorSingleMetricsConfiguration) {
    std::list<std::string> configuredMetricsList = {"rMulticast", "rPackets", "tDrop", "rFrame", "tBytes", "rCompressed", "tFifo"};
    std::list<std::string> notConfiguredMetricsList = {"rBytes", "rErrs", "rDrop", "rFifo", "tPackets", "tErrs", "tColls", "tCarrier", "tCompressed"};
    SchemaPtr schema = Monitoring::NetworkMetrics::createSchema("", configuredMetricsList);
    for (std::string attribute : configuredMetricsList) {
        ASSERT_TRUE(schema->contains(attribute));
    }
    for (std::string attribute : notConfiguredMetricsList) {
        ASSERT_FALSE(schema->contains(attribute));
    }

    auto networkCollector = Monitoring::NetworkCollector(schema);
    networkCollector.setNodeId(nodeId);
    Monitoring::MetricPtr networkMetric = networkCollector.readMetric();
    ASSERT_EQ(networkMetric->getMetricType(), Monitoring::MetricType::WrappedNetworkMetrics);

    Monitoring::NetworkMetricsWrapper wrappedMetric = networkMetric->getValue<Monitoring::NetworkMetricsWrapper>();

    if (reader->getReaderType() != SystemResourcesReaderType::AbstractReader) {
        Monitoring::NetworkMetrics totalMetrics = wrappedMetric.getNetworkValue(0);
        ASSERT_EQ(totalMetrics.getSchema(), schema);
        auto bufferSize = totalMetrics.getSchema()->getSchemaSizeInBytes();
        auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();
        writeToBuffer(totalMetrics, tupleBuffer, 0);
        ASSERT_TRUE(tupleBuffer.getNumberOfTuples() == 1);

        Monitoring::MetricPtr parsedMetric = std::make_shared<Monitoring::Metric>(Monitoring::NetworkMetrics(schema));
        readFromBuffer(parsedMetric, tupleBuffer, 0);
        NES_DEBUG("MetricCollectorTest:\nRead metric " << asJson(wrappedMetric) << "\nParsed metric: " << asJson(parsedMetric));
        Monitoring::NetworkMetrics parsedMetricValues = parsedMetric->getValue<Monitoring::NetworkMetrics>();
        ASSERT_EQ(parsedMetricValues.getSchema(), schema);
        for(const std::string& metricName : configuredMetricsList) {
            ASSERT_EQ(parsedMetricValues.getValue(metricName), totalMetrics.getValue(metricName));
        }
        for(const std::string& metricName : notConfiguredMetricsList) {
            ASSERT_EQ(parsedMetricValues.getValue(metricName), 0);
        }
        ASSERT_EQ(totalMetrics.nodeId, nodeId);
        ASSERT_EQ(parsedMetricValues.nodeId, nodeId);
    } else {
        NES_DEBUG("MetricCollectorTest: Skipping testNetworkCollectorSingleMetrics. Abstract reader found.");
    }
}

TEST_F(MetricCollectorTest, testNetworkCollectorWrappedMetricsConfiguration) {
    std::list<std::string> configuredMetricsList = {"rMulticast", "rPackets", "tDrop", "rFrame", "tBytes", "rCompressed", "tFifo"};
    std::list<std::string> notConfiguredMetricsList = {"rBytes", "rErrs", "rDrop", "rFifo", "tPackets", "tErrs", "tColls", "tCarrier", "tCompressed"};
    SchemaPtr schema = Monitoring::NetworkMetrics::createSchema("", configuredMetricsList);
    for (std::string attribute : configuredMetricsList) {
        ASSERT_TRUE(schema->contains(attribute));
    }
    for (std::string attribute : notConfiguredMetricsList) {
        ASSERT_FALSE(schema->contains(attribute));
    }

    auto networkCollector = Monitoring::NetworkCollector(schema);
    networkCollector.setNodeId(nodeId);
    Monitoring::MetricPtr networkMetric = networkCollector.readMetric();
    ASSERT_EQ(networkMetric->getMetricType(), Monitoring::MetricType::WrappedNetworkMetrics);

    Monitoring::NetworkMetricsWrapper wrappedMetric = networkMetric->getValue<Monitoring::NetworkMetricsWrapper>();
    ASSERT_EQ(wrappedMetric.getSchema(), schema);
    if (reader->getReaderType() != SystemResourcesReaderType::AbstractReader) {
        auto bufferSize = wrappedMetric.getSchema()->getSchemaSizeInBytes() * wrappedMetric.size();
        auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();
        wrappedMetric.writeToBuffer(tupleBuffer, 0);
        ASSERT_TRUE(tupleBuffer.getNumberOfTuples() == wrappedMetric.size());

        Monitoring::MetricPtr parsedMetric = std::make_shared<Monitoring::Metric>(Monitoring::NetworkMetricsWrapper(schema));
        readFromBuffer(parsedMetric, tupleBuffer, 0);
        Monitoring::NetworkMetricsWrapper parsedMetricValues = parsedMetric->getValue<Monitoring::NetworkMetricsWrapper>();
        NES_DEBUG("MetricCollectorTest:\nRead metric " << asJson(wrappedMetric) << "\nParsed metric: " << asJson(parsedMetric));
        for (unsigned int i = 0; i < parsedMetricValues.size(); i++) {
            for (const std::string& metricName : configuredMetricsList) {
                ASSERT_EQ(parsedMetricValues.getNetworkValue(i).getValue(metricName), wrappedMetric.getNetworkValue(i).getValue(metricName));
            }
            for (const std::string& metricName : notConfiguredMetricsList) {
                ASSERT_EQ(parsedMetricValues.getNetworkValue(i).getValue(metricName), 0);
            }
        }
        ASSERT_EQ(parsedMetricValues.getNodeId(), nodeId);
    } else {
        NES_DEBUG("MetricCollectorTest: Skipping testNetworkCollectorWrappedMetrics. Abstract reader found.");
    }
}
}// namespace NES
