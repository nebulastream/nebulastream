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

#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger.hpp>

#include <Monitoring/MetricCollectors/DiskCollector.hpp>
#include "Monitoring/Metrics/Wrapper/CpuMetricsWrapper.hpp"
#include <Monitoring/Metrics/Gauge/CpuMetrics.hpp>
#include <Monitoring/Metrics/Gauge/DiskMetrics.hpp>

namespace NES {

    using namespace Configurations;
    using namespace Runtime;

    class MetricCollectorTest : public testing::Test {
      public:
        Runtime::BufferManagerPtr bufferManager;
        uint64_t bufferSize = 0;

        static void SetUpTestCase() {
            NES::setupLogging("MonitoringStackTest.log", NES::LOG_DEBUG);
            NES_INFO("MonitoringStackTest: Setup MonitoringStackTest test class.");
        }

        static void TearDownTestCase() { std::cout << "MonitoringStackTest: Tear down MonitoringStackTest class." << std::endl; }

        /* Will be called before a  test is executed. */
        void SetUp() override {
            std::cout << "MonitoringStackTest: Setup MonitoringStackTest test case." << std::endl;

            unsigned int numCPU = std::thread::hardware_concurrency();
            bufferManager = std::make_shared<Runtime::BufferManager>(4096, 10);
            bufferSize = 4096 + (numCPU + 1) * sizeof(CpuMetrics) + sizeof(CpuMetricsWrapper);
        }

        /* Will be called before a test is executed. */
        void TearDown() override { std::cout << "MonitoringStackTest: Tear down MonitoringStackTest test case." << std::endl; }
    };

    TEST_F(MetricCollectorTest, testDiskCollector) {
        auto diskCollector = DiskCollector();
        DiskMetrics origMetric = diskCollector.collect();
        auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();
        auto success = diskCollector.fillBuffer(tupleBuffer);

        EXPECT_TRUE(success);
        EXPECT_TRUE(tupleBuffer.getNumberOfTuples() == 1);

        DiskMetrics parsedMetric = DiskMetrics::fromBuffer(DiskMetrics::getSchema(""), tupleBuffer, "");
        EXPECT_EQ(origMetric.fBsize, parsedMetric.fBsize);
        EXPECT_EQ(origMetric.fFrsize, parsedMetric.fFrsize);
    }

}// namespace NES