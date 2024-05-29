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

#include <Execution/Operators/Streaming/StatisticCollection/AbstractSynopsesOperatorHandler.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <StatisticCollection/StatisticStorage/BuildStatisticStore.hpp>
#include <Statistics/Synopses/SynopsesStatistic.hpp>
#include <Util/Common.hpp>
#include <Util/StdInt.hpp>

namespace NES::Runtime::Execution::Operators {


void incrementObservedTuplesStatisticProxy(void* ptrSynopsis, uint64_t increment) {
    NES_ASSERT2_FMT(ptrSynopsis != nullptr, "synopsis should not be null!");
    auto* synopsis = static_cast<Statistic::Statistic*>(ptrSynopsis);
    synopsis->incrementObservedTuples(increment);
}

uint64_t getSynopsisStartProxy(void* ptrOpHandler,
                               Statistic::StatisticMetricHash metricHash,
                               StatisticId statisticId,
                               WorkerThreadId workerId,
                               uint64_t timestamp) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = static_cast<AbstractSynopsesOperatorHandler*>(ptrOpHandler);

    const auto statisticHash = Statistic::StatisticKey::combineStatisticIdWithMetricHash(metricHash, statisticId);
    auto statistic = opHandler->getStatistic(workerId, statisticHash, timestamp);
    return statistic->getStartTs().getTime();
}

uint64_t getSynopsisEndProxy(void* ptrOpHandler,
                             Statistic::StatisticMetricHash metricHash,
                             StatisticId statisticId,
                             WorkerThreadId workerId,
                             uint64_t timestamp) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = static_cast<AbstractSynopsesOperatorHandler*>(ptrOpHandler);

    const auto statisticHash = Statistic::StatisticKey::combineStatisticIdWithMetricHash(metricHash, statisticId);
    auto statistic = opHandler->getStatistic(workerId, statisticHash, timestamp);
    return statistic->getEndTs().getTime();
}

void* getNullPtrMemRefProxy() { return nullptr; }


void AbstractSynopsesOperatorHandler::start(PipelineExecutionContextPtr pipelineExecutionContext, uint32_t) {
    /* For now, we just create the BuildStatisticStore. Later on, we can change this to be a configuration via the
     * lowering pipeline by using an enum to specify what store to use
     */
    for (auto i = 0_u64; i < pipelineExecutionContext->getNumberOfWorkerThreads(); ++i) {
        operatorStatisticStores.emplace_back(Statistic::BuildStatisticStore::create());
    }
}

void AbstractSynopsesOperatorHandler::stop(QueryTerminationType terminationType, PipelineExecutionContextPtr pipelineCtx) {
    if (terminationType == QueryTerminationType::Graceful) {
        // Write all statistics for all statistics stores
        std::vector<Statistic::HashStatisticPair> allStatisticsPlusHashesToSend;
        for (auto& statisticStore : operatorStatisticStores) {
            auto statisticsPlusHashesToSend = statisticStore->getAllStatistics();
            allStatisticsPlusHashesToSend.insert(allStatisticsPlusHashesToSend.end(),
                                                 statisticsPlusHashesToSend.begin(),
                                                 statisticsPlusHashesToSend.end());
        }

        // Merging the count min sketches according to their timestamp and hash
        auto combinedStatistics = mergeStatistics(allStatisticsPlusHashesToSend);

        // Writing them into tuple buffers and then emitting the sketches
        auto statisticTupleBuffers =
            statisticFormat->writeStatisticsIntoBuffers(combinedStatistics, *pipelineCtx->getBufferManager());
        for (auto& buf : statisticTupleBuffers) {
            pipelineCtx->dispatchBuffer(buf);
        }
    }
}

Statistic::StatisticPtr AbstractSynopsesOperatorHandler::getStatistic(WorkerThreadId workerThreadId,
                                                                      Statistic::StatisticHash statisticHash,
                                                                      uint64_t timestamp) {
    auto sliceStart = Windowing::TimeMeasure(sliceAssigner.getSliceStartTs(timestamp));
    auto sliceEnd = Windowing::TimeMeasure(sliceAssigner.getSliceEndTs(timestamp));
    // We have to do this modulo, as the workerThreadIds might not always start at 0
    auto workerSpecificStatisticStore = operatorStatisticStores[workerThreadId % operatorStatisticStores.size()];

    auto statistics = workerSpecificStatisticStore->getSingleStatistic(statisticHash, sliceStart, sliceEnd);
    if (!statistics.has_value()) {
        // Creating a  statistic and inserting it into the local operator store
        auto newStatistic = createInitStatistic(sliceStart, sliceEnd);
        workerSpecificStatisticStore->insertStatistic(statisticHash, newStatistic);
        return newStatistic;
    } else {
        // For now, we expect only a single statistic per slice, therefore, we return the 0th position
        return statistics.value();
    }
}

void AbstractSynopsesOperatorHandler::checkStatisticsSending(const BufferMetaData& bufferMetaData,
                                                             Statistic::StatisticHash statisticHash,
                                                             PipelineExecutionContext* pipelineCtx) {
    // The watermark processor handles the minimal watermark across the stream
    uint64_t newGlobalWatermark =
        watermarkProcessor->updateWatermark(bufferMetaData.watermarkTs, bufferMetaData.seqNumber, bufferMetaData.originId);
    NES_DEBUG("newGlobalWatermark {} bufferMetaData {} ", newGlobalWatermark, bufferMetaData.toString());

    // Get statistics from all statistics stores
    std::vector<Statistic::HashStatisticPair> allStatisticsPlusHashesToSend;
    for (auto& statisticStore : operatorStatisticStores) {
        auto statisticsToSend =
            statisticStore->getStatistics(statisticHash, Windowing::TimeMeasure(0), Windowing::TimeMeasure(newGlobalWatermark));
        std::vector<Statistic::HashStatisticPair> statisticsPlusHashesToSend(statisticsToSend.size());
        std::transform(statisticsToSend.begin(),
                       statisticsToSend.end(),
                       std::back_inserter(allStatisticsPlusHashesToSend),
                       [statisticHash](const Statistic::StatisticPtr statistic) {
                           return std::make_pair(statisticHash, statistic);
                       });
    }

    // Merging the synopses according to their timestamp and hash
    auto combinedStatisticsToSend = mergeStatistics(allStatisticsPlusHashesToSend);

    // If there are no statistics, then just return
    if (combinedStatisticsToSend.empty()) {
        NES_DEBUG("Not sending any statistics for statistic hash = {}, bufferMetaData = {}",
                  statisticHash,
                  bufferMetaData.toString());
        return;
    }

    // Sending these statistics to the sink
    auto statisticTupleBuffers =
        statisticFormat->writeStatisticsIntoBuffers(combinedStatisticsToSend, *pipelineCtx->getBufferManager());
    for (auto& buf : statisticTupleBuffers) {
        pipelineCtx->dispatchBuffer(buf);
    }

    // Deleting the statistics from the store that we sent upwards
    for (auto& statisticStore : operatorStatisticStores) {
        statisticStore->deleteStatistics(statisticHash, Windowing::TimeMeasure(0), Windowing::TimeMeasure(newGlobalWatermark));
    }
}

std::vector<Statistic::HashStatisticPair>
AbstractSynopsesOperatorHandler::mergeStatistics(const std::vector<Statistic::HashStatisticPair>& statisticsPlusHashes) {
    /* General comment to this method:
     * This is not the most efficient way of merging statistics, we will improve this later on, if necessary.
     * Maybe by having a separate nautilus operator that always sits after this build and then merges all statistics for
     * a given statistic hash and a given time [startTs, endTs] as discussed in issue #4736
     */
    // Create here a MergeStatisticKey that identifies all sketches that are the same
    struct MergeStatisticKey {
        Statistic::StatisticHash statisticHash;
        Windowing::TimeMeasure startTs;
        Windowing::TimeMeasure endTs;

        /**
         * @brief Override the equal operator to use it in the unordered_map
         * @param other
         * @return True, if equal, false otherwise
         */
        bool operator==(const MergeStatisticKey& other) const {
            return statisticHash == other.statisticHash && startTs == other.startTs && endTs == other.endTs;
        }

        /**
          * @brief Constructor for a MergeStatisticKey
          * @param statisticHash
          * @param startTs
          * @param endTs
          */
        MergeStatisticKey(Statistic::StatisticHash statisticHash,
                          const Windowing::TimeMeasure& startTs,
                          const Windowing::TimeMeasure& endTs)
            : statisticHash(statisticHash), startTs(startTs), endTs(endTs) {}

        // Define a hash function for MergeStatisticKey
        struct Hash {
            size_t operator()(const MergeStatisticKey& key) const {
                // Combine hashes of individual members
                size_t hash = 17;
                hash = hash * 31 + std::hash<uint64_t>()(key.statisticHash);
                hash = hash * 31 + std::hash<uint64_t>()(key.startTs.getTime());
                hash = hash * 31 + std::hash<uint64_t>()(key.endTs.getTime());
                return hash;
            }
        };
    };
    std::unordered_map<MergeStatisticKey, Statistic::StatisticPtr, MergeStatisticKey::Hash> mergeStatisticKeyToStatistics;

    // 1. Insert each statistic for the MergeStatisticKey and merge if a MergeStatisticKey already exists.
    for (auto& [statisticHash, statistic] : statisticsPlusHashes) {
        MergeStatisticKey key(statisticHash, statistic->getStartTs(), statistic->getEndTs());
        if (mergeStatisticKeyToStatistics.contains(key)) {
            mergeStatisticKeyToStatistics[key]->as<Statistic::SynopsesStatistic>()->merge(
                *statistic->as<Statistic::SynopsesStatistic>());
        } else {
            mergeStatisticKeyToStatistics[key] = statistic;
        }
    }

    // 2. Read from the hashmap and create pairs of <StatisticHash, Statistic>, as the Statistic contains all merged statistics
    std::vector<Statistic::HashStatisticPair> mergedStatisticPlusHas;
    for (auto& [mergeStatisticKey, statistic] : mergeStatisticKeyToStatistics) {
        mergedStatisticPlusHas.emplace_back(std::make_pair(mergeStatisticKey.statisticHash, statistic));
    }
    return mergedStatisticPlusHas;
}

AbstractSynopsesOperatorHandler::AbstractSynopsesOperatorHandler(const uint64_t windowSize,
                                                                 const uint64_t windowSlide,
                                                                 const Statistic::SendingPolicyPtr& sendingPolicy,
                                                                 const Statistic::StatisticFormatPtr& statisticFormat,
                                                                 const std::vector<OriginId>& inputOrigins)
    : sliceAssigner(windowSize, windowSlide), sendingPolicy(sendingPolicy), statisticFormat(statisticFormat),
      watermarkProcessor(std::make_unique<MultiOriginWatermarkProcessor>(inputOrigins)) {}

}// namespace NES::Runtime::Execution::Operators
