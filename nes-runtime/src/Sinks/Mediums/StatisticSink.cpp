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

#include <Sinks/Formats/StatisticCollection/AbstractStatisticFormat.hpp>
#include <Sinks/Mediums/StatisticSink.hpp>
#include <StatisticCollection/StatisticStorage/AbstractStatisticStore.hpp>
#include <utility>

namespace NES::Statistic {

void StatisticSink::setup() {}

void StatisticSink::shutdown() {}

bool StatisticSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext&) {
    // Calls the overloaded operator ! from TupleBuffer and checks if this inputBuffer is valid
    if (!inputBuffer) {
        throw Exceptions::RuntimeException("StatisticSink:writeData input buffer invalid");
    }

    // We pass the tuple buffer to the statisticSinkFormat and expect vector<StatisticHash, StatisticPtr>.
    const auto allHashesAndStatistics = statisticSinkFormat->readStatisticsFromBuffer(inputBuffer);

    // Store the extracted statistics into the statistics store
    for (auto& [statisticHash, statistic] : allHashesAndStatistics) {
        statisticStore->insertStatistic(statisticHash, statistic);
    }
    NES_DEBUG("Inserted {} statistics into the statistic store.", allHashesAndStatistics.size());

    return true;
}

std::string StatisticSink::toString() const {
    std::stringstream ss;
    ss << "STATISTIC_SINK(";
    ss << "SinkFormat(" << statisticSinkFormat->toString() << ")";
    ss << ")";
    return ss.str();
}

SinkMediumTypes StatisticSink::getSinkMediumType() { return SinkMediumTypes::STATISTIC_SINK; }

StatisticSink::StatisticSink(const SinkFormatPtr& sinkFormat,
                             const Runtime::NodeEnginePtr& nodeEngine,
                             uint32_t numOfProducers,
                             SharedQueryId sharedQueryId,
                             DecomposedQueryId decomposedQueryId,
                             DecomposedQueryPlanVersion decomposedQueryVersion,
                             FaultToleranceType faultToleranceType,
                             CheckpointStorageType checkpointStorageType,
                             uint64_t numberOfOrigins,
                             StatisticStorePtr statisticStore,
                             StatisticFormatPtr statisticSinkFormat)
    : SinkMedium(sinkFormat,
                 nodeEngine,
                 numOfProducers,
                 sharedQueryId,
                 decomposedQueryId,
                 decomposedQueryVersion,
                 faultToleranceType,
                 checkpointStorageType,
                 numberOfOrigins),
      statisticStore(std::move(statisticStore)), statisticSinkFormat(std::move(statisticSinkFormat)) {}
}// namespace NES::Statistic
