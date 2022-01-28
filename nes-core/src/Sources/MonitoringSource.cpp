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

#include <Monitoring/Metrics/MetricCatalog.hpp>
#include <Monitoring/Metrics/MetricGroup.hpp>
#include <Monitoring/Metrics/MonitoringPlan.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/MonitoringSource.hpp>

#include <Runtime/BufferManager.hpp>
#include <Util/Logger.hpp>

#include <Util/UtilityFunctions.hpp>
#include <chrono>
#include <utility>

namespace NES {

MonitoringSource::MonitoringSource(const MonitoringPlanPtr& monitoringPlan,
                                   MetricCatalogPtr metricCatalog,
                                   Runtime::BufferManagerPtr bufferManager,
                                   Runtime::QueryManagerPtr queryManager,
                                   const uint64_t numbersOfBufferToProduce,
                                   uint64_t frequency,
                                   OperatorId operatorId,
                                   size_t numSourceLocalBuffers,
                                   std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors)
    : DefaultSource(Schema::create(),
                    std::move(bufferManager),
                    std::move(queryManager),
                    numbersOfBufferToProduce,
                    frequency,
                    operatorId,
                    numSourceLocalBuffers,
                    std::move(successors)),
      monitoringPlan(monitoringPlan), metricGroup(monitoringPlan->createMetricGroup(std::move(metricCatalog))) {
    schema = metricGroup->createSchema();
    NES_INFO("MonitoringSources: Created with schema:\n" << schema->toString());
}

std::optional<Runtime::TupleBuffer> MonitoringSource::receiveData() {
    NES_DEBUG("MonitoringSource:" << this << " requesting buffer");
    auto buf = this->bufferManager->getBufferBlocking();
    NES_DEBUG("MonitoringSource:" << this << " got buffer");

    NES_DEBUG("MonitoringSource: Filling buffer with 1 monitoring tuple.");
    metricGroup->getSample(buf);

    buf.setNumberOfTuples(1);
    NES_DEBUG("MonitoringSource: Generated buffer with 1 tuple and size " << schema->getSchemaSizeInBytes());

    //update statistics
    generatedTuples++;
    generatedBuffers++;

    NES_DEBUG("MonitoringSource::Buffer content: " << Util::prettyPrintTupleBuffer(buf, schema));

    return buf;
}

SourceType MonitoringSource::getType() const { return MONITORING_SOURCE; }

std::string MonitoringSource::toString() const {
    std::stringstream ss;
    ss << "MonitoringSource(SCHEMA(" << schema->toString() << ")"
       << ")";
    return ss.str();
}

}// namespace NES
