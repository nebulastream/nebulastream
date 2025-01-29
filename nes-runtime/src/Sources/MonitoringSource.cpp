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

#include <Monitoring/MetricCollectors/MetricCollector.hpp>
#include <Monitoring/MonitoringCatalog.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/MonitoringSource.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <chrono>

namespace NES {

MonitoringSource::MonitoringSource(Monitoring::MetricCollectorPtr metricCollector,
                                   std::chrono::milliseconds waitTime,
                                   Runtime::BufferManagerPtr bufferManager,
                                   Runtime::QueryManagerPtr queryManager,
                                   OperatorId operatorId,
                                   OriginId originId,
                                   StatisticId statisticId,
                                   size_t numSourceLocalBuffers,
                                   const std::string& physicalSourceName,
                                   std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors)
    : DataSource(Schema::create(),
                 bufferManager,
                 queryManager,
                 operatorId,
                 originId,
                 statisticId,
                 numSourceLocalBuffers,
                 GatheringMode::INTERVAL_MODE,
                 physicalSourceName,
                 false,
                 false,
                 successors),
      metricCollector(metricCollector), waitTime(waitTime) {
    schema = metricCollector->getSchema();
    NES_INFO("MonitoringSources: Created with wait time {} and schema:{}\n", waitTime.count(), schema->toString());
}

std::optional<Runtime::TupleBuffer> MonitoringSource::receiveData() {
    auto buf = this->bufferManager->getBufferBlocking();
    metricCollector->fillBuffer(buf);
    NES_TRACE("MonitoringSource: Generated buffer with{} tuple and size {}",
              buf.getNumberOfTuples(),
              schema->getSchemaSizeInBytes());

    //update statistics
    generatedTuples += buf.getNumberOfTuples();
    generatedBuffers++;

    if (Logger::getInstance()->getCurrentLogLevel() == LogLevel::LOG_TRACE) {
        auto layout = Runtime::MemoryLayouts::RowLayout::create(schema, buf.getBufferSize());
        auto buffer = Runtime::MemoryLayouts::TestTupleBuffer(layout, buf);

        NES_TRACE("MonitoringSource::Buffer content:  {}", buffer.toString(schema));
    }

    std::this_thread::sleep_for(waitTime);

    return buf;
}

Monitoring::MetricCollectorType MonitoringSource::getCollectorType() { return metricCollector->getType(); }

SourceType MonitoringSource::getType() const { return SourceType::MONITORING_SOURCE; }

std::string MonitoringSource::toString() const {
    std::stringstream ss;
    ss << "MonitoringSource(SCHEMA(" << schema->toString() << ")"
       << ")";
    return ss.str();
}
std::chrono::milliseconds MonitoringSource::getWaitTime() const { return waitTime; }

}// namespace NES
