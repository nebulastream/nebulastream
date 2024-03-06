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

#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Sinks/Formats/StatisticCollectorFormats/CountMinFormat.hpp>
#include <Sinks/Formats/StatisticCollectorFormats/ReservoirSampleFormat.hpp>
#include <Sinks/Mediums/StatisticSink.hpp>
#include <Statistics/Statistic.hpp>
#include <Statistics/StatisticCollectorStorage.hpp>
#include <Util/StatisticCollectorType.hpp>

namespace NES::Experimental::Statistics {

StatisticSink::StatisticSink(StatisticCollectorStoragePtr statisticCollectorStorage,
                             StatisticCollectorType statisticCollectorType,
                             const std::string& logicalSourceName,
                             SinkFormatPtr sinkFormat,
                             Runtime::NodeEnginePtr const& nodeEngine,
                             uint32_t numOfProducers,
                             QueryId queryId,
                             QuerySubPlanId querySubPlanId,
                             uint64_t numberOfOrigins)
    : SinkMedium(std::move(sinkFormat), std::move(nodeEngine), numOfProducers, queryId, querySubPlanId, numberOfOrigins),
      statisticCollectorStorage(std::move(statisticCollectorStorage)), statisticCollectorType(statisticCollectorType),
      logicalSourceName(logicalSourceName) {

    switch (statisticCollectorType) {
        case StatisticCollectorType::COUNT_MIN: statisticCollectorFormat = std::make_shared<CountMinFormat>(); break;
        case StatisticCollectorType::DDSKETCH: NES_ERROR("Not yet implemented!"); break;
        case StatisticCollectorType::HYPER_LOG_LOG: NES_ERROR("Not yet implemented!"); break;
        case StatisticCollectorType::RESERVOIR: statisticCollectorFormat = std::make_shared<ReservoirSampleFormat>(); break;
        default: NES_ERROR("Synopsis not implemented!");
    }
}

void StatisticSink::setup() {}

void StatisticSink::shutdown() {}

bool StatisticSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef) {

    if (!inputBuffer) {
        throw Exceptions::RuntimeException("StatisticSink:writeData input buffer invalid");
    }

    auto schema = this->getSchemaPtr();
    auto dynBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(inputBuffer, schema);

    std::vector<StatisticPtr> allStatistics = statisticCollectorFormat->readFromBuffer(dynBuffer, logicalSourceName);
    statisticCollectorStorage->addStatistics(allStatistics);

    return true;
}

std::string StatisticSink::getStatisticCollectorTypeAsString() const {
    switch (statisticCollectorType) {
        case StatisticCollectorType::COUNT_MIN: return "Count-Min";
        case StatisticCollectorType::DDSKETCH: return "DDSketch";
        case StatisticCollectorType::HYPER_LOG_LOG: return "HLL";
        case StatisticCollectorType::RESERVOIR: return "Reservoir Sample";
        default: return "Invalid StatisticCollectorType!";
    }
}

std::string StatisticSink::toString() const {
    std::stringstream ss;
    ss << "StatisticCollectorStorageSink: ";
    ss << "StatisticCollector type: " << getStatisticCollectorTypeAsString();
    ss << "Schema: " << sinkFormat->getSchemaPtr()->toString();
    return ss.str();
}

SinkMediumTypes StatisticSink::getSinkMediumType() { return SinkMediumTypes::STATISTIC_SINK; };
}// namespace NES::Experimental::Statistics