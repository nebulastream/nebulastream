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
#include <Monitoring/MetricCollectors/NetworkCollector.hpp>
#include <Monitoring/Metrics/Gauge/NetworkMetrics.hpp>
#include <Monitoring/Metrics/Metric.hpp>
#include <Monitoring/ResourcesReader/AbstractSystemResourcesReader.hpp>
#include <Monitoring/ResourcesReader/SystemResourcesReaderFactory.hpp>
#include <Monitoring/Util/MetricUtils.hpp>

#include <Monitoring/Metrics/Wrapper/NetworkMetricsWrapper.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Monitoring {
NetworkCollector::NetworkCollector()
    : MetricCollector(), resourceReader(SystemResourcesReaderFactory::getSystemResourcesReader()),
      schema(NetworkMetrics::getSchema("")) {
    NES_INFO("NetworkCollector: Init NetworkCollector with schema {}", schema->toString());
}

MetricCollectorType NetworkCollector::getType() { return MetricCollectorType::NETWORK_COLLECTOR; }

bool NetworkCollector::fillBuffer(Runtime::TupleBuffer& tupleBuffer) {
    try {
        NetworkMetricsWrapper measuredVal = resourceReader->readNetworkStats();
        measuredVal.setNodeId(getNodeId());
        writeToBuffer(measuredVal, tupleBuffer, 0);
        NES_TRACE("NetworkCollector: Written metrics for {}: {}", getNodeId(), asJson(measuredVal));
    } catch (const std::exception& ex) {
        NES_ERROR("NetworkCollector: Error while collecting metrics {}", ex.what());
        return false;
    }
    return true;
}

SchemaPtr NetworkCollector::getSchema() { return schema; }

const MetricPtr NetworkCollector::readMetric() const {
    NetworkMetricsWrapper wrapper = resourceReader->readNetworkStats();
    wrapper.setNodeId(getNodeId());
    return std::make_shared<Metric>(std::move(wrapper), MetricType::WrappedNetworkMetrics);
}

}// namespace NES::Monitoring