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

#include <Sinks/Mediums/StatisticCollectorStorageSink.hpp>

namespace NES::Experimental::Statistics {
StatisticCollectorStorageSink::StatisticCollectorStorageSink(StatisticCollectorType statisticCollectorType,
                                                             SinkFormatPtr sinkFormat,
                                                             Runtime::NodeEnginePtr nodeEngine,
                                                             uint32_t numOfProducers,
                                                             QueryId queryId,
                                                             QuerySubPlanId querySubPlanId,
                                                             FaultToleranceType faultToleranceType,
                                                             uint64_t numberOfOrigins)
    : SinkMedium(std::move(sinkFormat),
                 std::move(nodeEngine),
                 numOfProducers,
                 queryId,
                 querySubPlanId,
                 faultToleranceType,
                 numberOfOrigins,
                 std::make_unique<Windowing::MultiOriginWatermarkProcessor>(numberOfOrigins)),
      statisticCollectorType(statisticCollectorType) {}

void StatisticCollectorStorageSink::setup() {}

void StatisticCollectorStorageSink::shutdown() {}

bool StatisticCollectorStorageSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef) {
    std::unique_lock lock(writeMutex);

    if (!inputBuffer) {
        throw Exceptions::RuntimeException("StatisticCollectorStorageSink::writeData input buffer invalid");
    }

//    auto dynTupleBuffer =
//        Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(inputBuffer, inputSchema) std::vector<Synopsis&>
//            synopses = Synopses::readFromBuffer(inputBuffer) {}

//    statisticCollectorStorage.createStatistic(statisticCollectorIdentifier, 2.0);

    updateWatermarkCallback(inputBuffer);

    return true;
}

std::string StatisticCollectorStorageSink::getTypeAsString() const {
    switch (statisticCollectorType) {
        case StatisticCollectorType::COUNT_MIN: return "CountMin";
        case StatisticCollectorType::HYPER_LOG_LOG: return "HyperLogLog";
        case StatisticCollectorType::DDSKETCH: return "DDSketch";
        case StatisticCollectorType::RESERVOIR: return "Reservoir Sample";
        default: return "Unknown Synopsis Type";
    }
}

std::string StatisticCollectorStorageSink::toString() const {
    std::stringstream ss;
    ss << "StatisticCollectorStorageSink: ";
    ss << " StatisticCollectorType: " << this->getTypeAsString();
    ss << " Schema: " << this->sinkFormat->getSchemaPtr()->toString();
    return ss.str();
}

SinkMediumTypes StatisticCollectorStorageSink::getSinkMediumType() { return SinkMediumTypes::STATISTIC_COLLECTOR_STORAGE_SINK; }

}// namespace NES::Experimental::Statistics