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

#include <API/Schema.hpp>
#include <Monitoring/MetricCollectors/MemoryCollector.hpp>
#include <Monitoring/Metrics/Gauge/MemoryMetrics.hpp>
#include <Monitoring/Metrics/Metric.hpp>
#include <Monitoring/ResourcesReader/AbstractSystemResourcesReader.hpp>
#include <Monitoring/ResourcesReader/SystemResourcesReaderFactory.hpp>
#include <Monitoring/Util/MetricUtils.hpp>

#include <Util/Logger/Logger.hpp>

namespace NES::Monitoring {
MemoryCollector::MemoryCollector()
    : MetricCollector(), resourceReader(SystemResourcesReaderFactory::getSystemResourcesReader()),
      schema(MemoryMetrics::getSchema("")) {
    NES_INFO("MemoryCollector: Init MemoryCollector with schema {}", schema->toString());
}

MetricCollectorType MemoryCollector::getType() { return MetricCollectorType::MEMORY_COLLECTOR; }

bool MemoryCollector::fillBuffer(Runtime::TupleBuffer& tupleBuffer) {
    try {
        MemoryMetrics measuredVal = resourceReader->readMemoryStats();
        measuredVal.nodeId = getWorkerId();
        writeToBuffer(measuredVal, tupleBuffer, 0);
    } catch (const std::exception& ex) {
        NES_ERROR("MemoryCollector: Error while collecting metrics {}", ex.what());
        return false;
    }
    return true;
}

SchemaPtr MemoryCollector::getSchema() { return schema; }

const MetricPtr MemoryCollector::readMetric() const {
    MemoryMetrics metrics = resourceReader->readMemoryStats();
    metrics.nodeId = getWorkerId();
    return std::make_shared<Metric>(std::move(metrics), MetricType::MemoryMetric);
}

}// namespace NES::Monitoring