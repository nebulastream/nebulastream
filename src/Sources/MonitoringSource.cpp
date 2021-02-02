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

#include <Sources/MonitoringSource.hpp>

#include <Monitoring/Metrics/MetricCatalog.hpp>
#include <Monitoring/Metrics/MetricGroup.hpp>
#include <Monitoring/Metrics/MonitoringPlan.hpp>

#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>
#include <Util/Logger.hpp>

#include <Util/UtilityFunctions.hpp>

namespace NES {

MonitoringSource::MonitoringSource(MonitoringPlanPtr monitoringPlan, MetricCatalogPtr metricCatalog,
                                   NodeEngine::BufferManagerPtr bufferManager, NodeEngine::QueryManagerPtr queryManager,
                                   const uint64_t numbersOfBufferToProduce, uint64_t frequency, OperatorId operatorId)
    : DefaultSource(Schema::create(), bufferManager, queryManager, numbersOfBufferToProduce, frequency, operatorId),
      monitoringPlan(monitoringPlan), metricGroup(monitoringPlan->createMetricGroup(metricCatalog)) {

    auto tupleBuffer = bufferManager->getBufferBlocking();
    metricGroup->getSample(schema, tupleBuffer);
    tupleBuffer.release();
    NES_INFO("MonitoringSources: Created with schema:\n" << schema->toString());
}

std::optional<NodeEngine::TupleBuffer> MonitoringSource::receiveData() {
    NES_DEBUG("MonitoringSource:" << this << " requesting buffer");
    auto buf = this->bufferManager->getBufferBlocking();
    NES_DEBUG("MonitoringSource:" << this << " got buffer");

    NES_DEBUG("MonitoringSource: Filling buffer with 1 monitoring tuple.");
    metricGroup->getSample(Schema::create(), buf);

    buf.setNumberOfTuples(1);
    NES_DEBUG("MonitoringSource: Generated buffer with 1 tuple and size " << schema->getSchemaSizeInBytes());

    //update statistics
    generatedTuples++;
    generatedBuffers++;

    NES_DEBUG("MonitoringSource::Buffer content: " << UtilityFunctions::prettyPrintTupleBuffer(buf, schema));

    return buf;
}

SourceType MonitoringSource::getType() const { return MONITORING_SOURCE; }

const std::string MonitoringSource::toString() const {
    std::stringstream ss;
    ss << "MonitoringSource(SCHEMA(" << schema->toString() << ")"
       << ")";
    return ss.str();
}

}// namespace NES
