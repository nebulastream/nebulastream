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

#ifndef NES_NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_STATISTICCOLLECTION_HYPERLOGLOG_HYPERLOGLOGOPERATORHANDLER_HPP_
#define NES_NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_STATISTICCOLLECTION_HYPERLOGLOG_HYPERLOGLOGOPERATORHANDLER_HPP_

#include <Execution/Operators/Streaming/StatisticCollection/AbstractSynopsesOperatorHandler.hpp>


namespace NES::Runtime::Execution::Operators {
class HyperLogLogOperatorHandler;
using HyperLogLogOperatorHandlerPtr = std::shared_ptr<HyperLogLogOperatorHandler>;

/**
 * @brief Operator handler for HyperLogLog sketches.
 */
class HyperLogLogOperatorHandler : public AbstractSynopsesOperatorHandler {
  public:
    static HyperLogLogOperatorHandlerPtr create(const uint64_t windowSize,
                                                const uint64_t windowSlide,
                                                Statistic::SendingPolicyPtr sendingPolicy,
                                                Statistic::AbstractStatisticFormatPtr statisticFormat,
                                                const uint64_t width,
                                                const std::vector<OriginId>& inputOrigins);

    Statistic::StatisticPtr createInitStatistic(Windowing::TimeMeasure sliceStart, Windowing::TimeMeasure sliceEnd) override;

  private:
    HyperLogLogOperatorHandler(const uint64_t windowSize,
                               const uint64_t windowSlide,
                               const Statistic::SendingPolicyPtr& sendingPolicy,
                               const Statistic::AbstractStatisticFormatPtr& statisticFormat,
                               const std::vector<OriginId>& inputOrigins,
                               const uint64_t width);

  private:
    const uint64_t width;
};

} //namespace NES::Runtime::Execution::Operators
#endif//NES_NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_STATISTICCOLLECTION_HYPERLOGLOG_HYPERLOGLOGOPERATORHANDLER_HPP_
