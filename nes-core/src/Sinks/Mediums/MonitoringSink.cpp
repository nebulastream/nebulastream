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

#include <Runtime/QueryManager.hpp>
#include <Sinks/Mediums/MonitoringSink.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <sstream>
#include <string>

#include <Monitoring/Storage/AbstractMetricStore.hpp>

namespace NES {
MonitoringSink::MonitoringSink(MetricStorePtr metricStore,
                               Runtime::NodeEnginePtr nodeEngine,
                               uint32_t numOfProducers,
                               QueryId queryId,
                               QuerySubPlanId querySubPlanId,
                               FaultToleranceType faultToleranceType,
                               uint64_t numberOfOrigins)
    : SinkMedium(nullptr, std::move(nodeEngine), numOfProducers, queryId, querySubPlanId, faultToleranceType, numberOfOrigins),
      metricStore(metricStore) {}

MonitoringSink::~MonitoringSink() = default;

SinkMediumTypes MonitoringSink::getSinkMediumType() { return MONITORING_SINK; }

bool MonitoringSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef) {
    std::unique_lock lock(writeMutex);
    NES_TRACE("MonitoringSink: getSchema medium " << toString() << " format " << sinkFormat->toString(););
    return true;
}

std::string MonitoringSink::toString() const {
    std::stringstream ss;
    ss << "MONITORING_SINK(";
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