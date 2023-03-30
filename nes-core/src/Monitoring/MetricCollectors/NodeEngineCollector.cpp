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
#include <Monitoring/MetricCollectors/NodeEngineCollector.hpp>
#include <Monitoring/Metrics/Gauge/NodeEngineMetrics.hpp>
#include <Monitoring/Metrics/Metric.hpp>
#include <Monitoring/ResourcesReader/AbstractSystemResourcesReader.hpp>
#include <Monitoring/ResourcesReader/SystemResourcesReaderFactory.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Monitoring/Util/MetricUtils.hpp>
#include <Monitoring/MonitoringAgent.hpp>
#include <Runtime/QueryStatistics.hpp>

#include <Util/Logger/Logger.hpp>

namespace NES::Monitoring {
NodeEngineCollector::NodeEngineCollector()
    : MetricCollector(), resourceReader(SystemResourcesReaderFactory::getSystemResourcesReader()),
      schema(NodeEngineMetrics::getSchema("")) {
    NES_INFO("NodeEngineCollector: Init NodeEngineCollector with schema " << schema->toString());

}

MetricCollectorType NodeEngineCollector::getType() { return NODE_ENGINE_COLLECTOR; }

bool NodeEngineCollector::fillBuffer(Runtime::TupleBuffer& tupleBuffer) {
    try {
        MonitoringAgentPtr agent = MonitoringAgent::create();
        Runtime::NodeEnginePtr engine = agent->getNodeEngine();

        std::vector<Runtime::QueryStatistics> vector = engine->getQueryStatistics(true);
        int s = vector.size();
        Runtime::QueryStatistics measuredVal1 = vector[s-1];
        NodeEngineMetrics measuredVal = Runtime::QueryStatistics::convertToNodeEngineMetrics(measuredVal1);

        measuredVal.nodeId = getNodeId();
        writeToBuffer(measuredVal, tupleBuffer, 0);
    } catch (const std::exception& ex) {
        NES_ERROR("NodeEngineCollector: Error while collecting metrics " << ex.what());
        return false;
    }
    return true;
}

SchemaPtr NodeEngineCollector::getSchema() { return schema; }

const MetricPtr NodeEngineCollector::readMetric() const {
    MonitoringAgentPtr agent = MonitoringAgent::create();
    Runtime::NodeEnginePtr engine = agent->getNodeEngine();

    std::vector<Runtime::QueryStatistics> vector = engine->getQueryStatistics(true);
    int s = vector.size();
    Runtime::QueryStatistics metrics1 = vector[s-1];  //TODO: ist das die letzte?
    NodeEngineMetrics metrics = Runtime::QueryStatistics::convertToNodeEngineMetrics(metrics1);

    metrics.nodeId = getNodeId();
    return std::make_shared<Metric>(std::move(metrics), MetricType::NodeEngineMetric);
}

}// namespace NES::Monitoring
