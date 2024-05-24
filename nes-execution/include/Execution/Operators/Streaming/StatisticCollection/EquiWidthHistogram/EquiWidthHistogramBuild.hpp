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

#ifndef NES_NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_STATISTICCOLLECTION_EQUIWIDTHHISTOGRAM_EQUIWIDTHHISTOGRAMBUILD_HPP_
#define NES_NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_STATISTICCOLLECTION_EQUIWIDTHHISTOGRAM_EQUIWIDTHHISTOGRAMBUILD_HPP_
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Metrics/StatisticMetric.hpp>
#include <Statistics/StatisticKey.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Util/Common.hpp>

namespace NES::Runtime::Execution::Operators {
class EquiWidthHistogramBuild : public ExecutableOperator {
  public:
    EquiWidthHistogramBuild(const uint64_t operatorHandlerIndex,
                            const std::string& fieldToTrackFieldName,
                            const uint64_t binWidth,
                            const Statistic::StatisticMetricHash metricHash,
                            TimeFunctionPtr timeFunction);

    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void execute(ExecutionContext& ctx, Record& record) const override;
    void close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;

  private:
    const uint64_t operatorHandlerIndex;
    const std::string fieldToTrackFieldName;
    const uint64_t binWidth;
    const Statistic::StatisticMetricHash metricHash;
    const TimeFunctionPtr timeFunction;

};
} // namespace NES::Runtime::Execution::Operators
#endif//NES_NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_STATISTICCOLLECTION_EQUIWIDTHHISTOGRAM_EQUIWIDTHHISTOGRAMBUILD_HPP_
