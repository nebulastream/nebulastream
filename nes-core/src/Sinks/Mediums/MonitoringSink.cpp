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

#include <Monitoring/MetricCollectors/MetricCollectorType.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sinks/Mediums/MonitoringSink.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <sstream>
#include <string>

#include <Monitoring/Metrics/Metric.hpp>
#include <Monitoring/Metrics/Wrapper/CpuMetricsWrapper.hpp>
#include <Monitoring/Storage/AbstractMetricStore.hpp>
#include <Monitoring/Util/MetricUtils.hpp>
#include <Monitoring/MonitoringPlan.hpp>

namespace NES {
MonitoringSink::MonitoringSink(SinkFormatPtr sinkFormat,
                               Monitoring::MetricStorePtr metricStore,
                               Monitoring::MonitoringManagerPtr monitoringManager,
                               Monitoring::MetricCollectorType collectorType,
                               Runtime::NodeEnginePtr nodeEngine,
                               uint32_t numOfProducers,
                               QueryId queryId,
                               QuerySubPlanId querySubPlanId,
                               FaultToleranceType::Value faultToleranceType,
                               uint64_t numberOfOrigins)
    : SinkMedium(std::move(sinkFormat),
                 std::move(nodeEngine),
                 numOfProducers,
                 queryId,
                 querySubPlanId,
                 faultToleranceType,
                 numberOfOrigins,
                 std::make_unique<Windowing::MultiOriginWatermarkProcessor>(numberOfOrigins)),
      metricStore(metricStore), collectorType(collectorType), monitoringManager(monitoringManager) {
    NES_DEBUG("MonitoringSink: Descriptor started!")
    NES_ASSERT(metricStore != nullptr, "MonitoringSink: MetricStore is null.");
}

MonitoringSink::~MonitoringSink() = default;

SinkMediumTypes MonitoringSink::getSinkMediumType() { return MONITORING_SINK; }

bool MonitoringSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef) {
    std::unique_lock lock(writeMutex);

    if (!inputBuffer) {
        throw Exceptions::RuntimeException("MonitoringSink::writeData input buffer invalid");
    }

    auto dataBuffers = sinkFormat->getData(inputBuffer);
    for (auto& buffer : dataBuffers) {
        auto nodeIdPtr = (uint64_t*) buffer.getBuffer();
        uint64_t nodeId = nodeIdPtr[0];

        std::string configString = " - cpu: attributes: \"user, nice, system, idle, iowait, irq, softirq, steal, guest, guestnice\" sampleRate: 6000"
                                   " - disk: attributes: \"F_BSIZE, F_BLOCKS, F_FRSIZE\" sampleRate: 5000"
                                   " - memory: attributes: \"FREE_RAM\" sampleRate: 4000"
                                   " - network: attributes: \"rBytes, rPackets, rErrs, rDrop, rFifo, rFrame, rCompressed, rMulticast, "
                                   "tBytes, tPackets, tErrs, tDrop, tFifo, tColls, tCarrier, tCompressed\" ";
        web::json::value configurationMonitoringJson =
            Monitoring::MetricUtils::parseMonitoringConfigStringToJson(configString);

        Monitoring::MonitoringPlanPtr monitoringPlan = Monitoring::MonitoringPlan::setSchemaJson(configurationMonitoringJson);


        //        Monitoring::MonitoringManagerPtr monitoringManager = metricStore->getMonitoringManager();
        NES_DEBUG("MonitoringSink: write data: get monitoringplan for node " + std::to_string(nodeId));
//        Monitoring::MonitoringPlanPtr monitoringPlan = monitoringManager->getMonitoringPlan(nodeId);
        NES_DEBUG("MonitoringSink: write data: metric types of monitoring plan" + monitoringPlan->toString());
        NES_DEBUG("MonitoringSink: write data: collectorType: " + Monitoring::toString(collectorType))
        Monitoring::MetricType metricType = Monitoring::MetricUtils::getMetricTypeFromCollectorType(collectorType);
        NES_DEBUG("MonitoringSink: write data: metricType " + Monitoring::toString(metricType));
        SchemaPtr schema = monitoringPlan->getSchema(metricType);
        Monitoring::MetricPtr parsedMetric;
        if (schema) {
            NES_DEBUG("MonitoringSink: write data: print schema for node " + std::to_string(nodeId) + "\n"+ schema->toString());
            parsedMetric = Monitoring::MetricUtils::createMetricFromCollectorTypeAndSchema(collectorType, schema);
        } else {
            NES_DEBUG("MonitoringSink: write data: no fitting metrictype in monitoring plan: " + Monitoring::toString(metricType));
            parsedMetric = Monitoring::MetricUtils::createMetricFromCollectorType(collectorType);
        }

        Monitoring::readFromBuffer(parsedMetric, buffer, 0);

        NES_TRACE("MonitoringSink: Received buffer for " << nodeId << " with " << inputBuffer.getNumberOfTuples()
                                                         << " tuple and size " << getSchemaPtr()->getSchemaSizeInBytes() << ": "
                                                         << asJson(parsedMetric));

        metricStore->addMetrics(nodeId, std::move(parsedMetric));
    }

    updateWatermarkCallback(inputBuffer);

    return true;
}

std::string MonitoringSink::toString() const {
    std::stringstream ss;
    ss << "MONITORING_SINK(";
    ss << "COLLECTOR(" << NES::Monitoring::toString(collectorType) << ")";
    ss << "SCHEMA(" << sinkFormat->getSchemaPtr()->toString() << ")";
    ss << ")";
    return ss.str();
}

void MonitoringSink::setup() {
    // currently not required
}

void MonitoringSink::shutdown() {
    // currently not required
}

}// namespace NES