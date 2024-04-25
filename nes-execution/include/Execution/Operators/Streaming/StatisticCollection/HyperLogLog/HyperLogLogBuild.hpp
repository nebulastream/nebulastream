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

#ifndef NES_NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_STATISTICCOLLECTION_HYPERLOGLOG_HYPERLOGLOGBUILD_HPP_
#define NES_NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_STATISTICCOLLECTION_HYPERLOGLOG_HYPERLOGLOGBUILD_HPP_
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Metrics/StatisticMetric.hpp>
#include <Util/Common.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief Operator that builds a HyperLogLog sketch (https://en.wikipedia.org/wiki/HyperLogLog) from a field in the input record
 */
class HyperLogLogBuild : public ExecutableOperator {
  public:
    HyperLogLogBuild(const uint64_t operatorHandlerIndex,
                     const std::string_view fieldToTrackFieldName,
                     const Statistic::StatisticMetricHash metricHash,
                     TimeFunctionPtr timeFunction);

    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void execute(ExecutionContext& ctx, Record& record) const override;
    void close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;

  private:
    const uint64_t operatorHandlerIndex;
    const std::string fieldToTrackFieldName;
    const Statistic::StatisticMetricHash metricHash;
    const TimeFunctionPtr timeFunction;
    const std::unique_ptr<Nautilus::Interface::HashFunction> murmurHash;
};

}// namespace NES::Runtime::Execution::Operators

#endif//NES_NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_STATISTICCOLLECTION_HYPERLOGLOG_HYPERLOGLOGBUILD_HPP_
