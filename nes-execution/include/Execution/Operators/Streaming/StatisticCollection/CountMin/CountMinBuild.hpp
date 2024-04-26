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

#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_STATISTICCOLLECTION_COUNTMIN_COUNTMINBUILD_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_STATISTICCOLLECTION_COUNTMIN_COUNTMINBUILD_HPP_

#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Metrics/StatisticMetric.hpp>
#include <Util/Common.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief Operator for creating count min sketches across time-based windows. We create one sketch per window and
 * emit the sketch, once we see a larger watermarkTs than the endTs of the window.
 */
class CountMinBuild : public ExecutableOperator {
  public:
    CountMinBuild(const uint64_t operatorHandlerIndex,
                  const std::string_view fieldToTrackFieldName,
                  const uint64_t numberOfBitsInKey,
                  const uint64_t width,
                  const uint64_t depth,
                  const Statistic::StatisticMetricHash metricHash,
                  TimeFunctionPtr timeFunction,
                  const uint64_t numberOfBitsInHashValue = NUMBER_OF_BITS_IN_HASH_VALUE);

    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void execute(ExecutionContext& ctx, Record& record) const override;
    void close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;

  private:
    const uint64_t operatorHandlerIndex;
    const std::string fieldToTrackFieldName;
    const uint64_t sizeOfOneRowInBytes;
    const uint64_t width;
    const uint64_t depth;
    const Statistic::StatisticMetricHash metricHash;
    const TimeFunctionPtr timeFunction;
    const std::unique_ptr<Nautilus::Interface::HashFunction> h3HashFunction;
};
}// namespace NES::Runtime::Execution::Operators

#endif // NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_STATISTICCOLLECTION_COUNTMIN_COUNTMINBUILD_HPP_
