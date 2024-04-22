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

#ifndef NES_NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_STATISTICCOLLECTION_STATISTICOPERATORHANDLER_HPP_
#define NES_NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_STATISTICCOLLECTION_STATISTICOPERATORHANDLER_HPP_
#include <Execution/Operators/Streaming/MultiOriginWatermarkProcessor.hpp>
#include <Execution/Operators/Streaming/SliceAssigner.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperators/StatisticCollection/SendingPolicy/SendingPolicy.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Sinks/Formats/StatisticCollection/AbstractStatisticFormat.hpp>
#include <StatisticCollection/StatisticKey.hpp>
#include <StatisticCollection/StatisticStorage/AbstractStatisticStore.hpp>
#include <Util/Common.hpp>
#include <folly/Synchronized.h>


namespace NES::Runtime::Execution::Operators {

/**
 * @brief Abstract class for handling operators that collect statistics.
 */
class AbstractSynopsesOperatorHandler : public OperatorHandler {
  public:
    AbstractSynopsesOperatorHandler(const uint64_t windowSize,
                                    const uint64_t windowSlide,
                                    const Statistic::SendingPolicyPtr& sendingPolicy,
                                    const Statistic::AbstractStatisticFormatPtr& statisticFormat,
                                    const std::vector<OriginId>& inputOrigins);

    void start(PipelineExecutionContextPtr pipelineExecutionContext, uint32_t localStateVariableId) override;
    void stop(QueryTerminationType terminationType, PipelineExecutionContextPtr pipelineExecutionContext) override;

    /**
     * @brief Returns the statistic for the statisticHash and the timestamp. If no statistic exists, we create one by
     * calling @see createInitStatistic
     * @param workerId
     * @param statisticHash
     * @param timestamp
     * @return StatisticPtr
     */
    Statistic::StatisticPtr getStatistic(uint64_t workerId, Statistic::StatisticHash statisticHash, uint64_t timestamp);

    /**
     * @brief Checks if we can/have to send/emit sketches to the sink
     * @param bufferMetaData
     * @param statisticHash
     * @param pipelineCtx
     */
    void checkStatisticsSending(const BufferMetaData& bufferMetaData,
                                Statistic::StatisticHash statisticHash,
                                PipelineExecutionContext* pipelineCtx);

    /**
     * @brief Merges all statistics in statisticsPlusHashes. This is necessary, as each thread creates their own count min sketches
     * that have to be merged together. We merge sketches that have the same statisticHash and time period [startTs, endTs]
     * @param statisticsPlusHashes
     * @return Vector of merged statistics.
     */
    std::vector<Statistic::HashStatisticPair> mergeStatistics(const std::vector<Statistic::HashStatisticPair>& statisticsPlusHashes);

    /**
     * @brief Abstract method that each synopsis has to implement. It creates a new statistic for the given time period.
     * @param sliceStart
     * @param sliceEnd
     * @return StatisticPtr
     */
    virtual Statistic::StatisticPtr createInitStatistic(Windowing::TimeMeasure sliceStart, Windowing::TimeMeasure sliceEnd) = 0;

  private:
    // For now, we simply use our existing statistic store, even though, we only store one type of statistic.
    // This might not be the most efficient, but this way, we have the most flexibility moving forward.
    std::vector<Statistic::AbstractStatisticStorePtr> operatorStatisticStores;
    SliceAssigner sliceAssigner;
    Statistic::SendingPolicyPtr sendingPolicy;
    const Statistic::AbstractStatisticFormatPtr statisticFormat;
    std::unique_ptr<MultiOriginWatermarkProcessor> watermarkProcessor;
};

}// namespace NES::Runtime::Execution::Operators

#endif//NES_NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_STATISTICCOLLECTION_STATISTICOPERATORHANDLER_HPP_
