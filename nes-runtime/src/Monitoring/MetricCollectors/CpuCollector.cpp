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
#include <Monitoring/MetricCollectors/CpuCollector.hpp>
#include <Monitoring/Metrics/Metric.hpp>
#include <Monitoring/Metrics/Wrapper/CpuMetricsWrapper.hpp>
#include <Monitoring/ResourcesReader/AbstractSystemResourcesReader.hpp>
#include <Monitoring/ResourcesReader/SystemResourcesReaderFactory.hpp>
#include <Monitoring/Util/MetricUtils.hpp>

#include <Util/Logger/Logger.hpp>

namespace NES::Monitoring {
CpuCollector::CpuCollector()
    : MetricCollector(), resourceReader(SystemResourcesReaderFactory::getSystemResourcesReader()),
      schema(CpuMetrics::getSchema("")) {
    NES_INFO("CpuCollector: Init CpuCollector with schema {}", schema->toString());
}

MetricCollectorType CpuCollector::getType() { return MetricCollectorType::CPU_COLLECTOR; }

bool CpuCollector::fillBuffer(Runtime::TupleBuffer& tupleBuffer) {
    try {
        CpuMetricsWrapper measuredVal = resourceReader->readCpuStats();
        measuredVal.setNodeId(getNodeId());
        writeToBuffer(measuredVal, tupleBuffer, 0);
        NES_TRACE("CpuCollector: Written metrics for {}: {}", getNodeId(), asJson(measuredVal));
    } catch (const std::exception& ex) {
        NES_ERROR("CpuCollector: Error while collecting metrics {}", ex.what());
        return false;
    }
    return true;
}

SchemaPtr CpuCollector::getSchema() { return schema; }

const MetricPtr CpuCollector::readMetric() const {
    CpuMetricsWrapper wrapper = resourceReader->readCpuStats();
    wrapper.setNodeId(getNodeId());
    return std::make_shared<Metric>(std::move(wrapper), MetricType::WrappedCpuMetrics);
}

}// namespace NES::Monitoring