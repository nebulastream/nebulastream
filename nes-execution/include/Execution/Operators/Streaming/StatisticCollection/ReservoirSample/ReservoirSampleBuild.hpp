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

#ifndef NES_NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_STATISTICCOLLECTION_RESERVOIRSAMPLE_RESERVOIRSAMPLEBUILD_HPP_
#define NES_NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_STATISTICCOLLECTION_RESERVOIRSAMPLE_RESERVOIRSAMPLEBUILD_HPP_

#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Execution/Operators/Streaming/StatisticCollection/SynopsisLocalState.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Metrics/StatisticMetric.hpp>
#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Util/Common.hpp>
#include <Util/StdInt.hpp>
#include <cstdint>


namespace NES::Runtime::Execution::Operators {

class ReservoirSampleBuild : public ExecutableOperator{
  public:

    struct ReservoirSampleLocalState : public SynopsisLocalState {
        ReservoirSampleLocalState(const Value<UInt64>& synopsisStartTs,
                                  const Value<UInt64>& synopsisEndTs,
                                  const Value<MemRef>& synopsisReference,
                                  const Value<MemRef>& sampleBaseAddress)
            : SynopsisLocalState(synopsisStartTs, synopsisEndTs, synopsisReference), sampleBaseAddress(sampleBaseAddress) {}
        ReservoirSampleLocalState(const Value<MemRef>& synopsisReference,
                                  const Value<MemRef>& sampleBaseAddress)
            : ReservoirSampleLocalState(0_u64, 0_u64, synopsisReference, sampleBaseAddress) {}
        Value<MemRef> sampleBaseAddress;
    };

    ReservoirSampleBuild(const uint64_t operatorHandlerIndex,
                         const Statistic::StatisticMetricHash metricHash,
                         TimeFunctionPtr timeFunction,
                         uint64_t sampleSize,
                         const SchemaPtr schema);

    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void execute(ExecutionContext& ctx, Record& record) const override;
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void updateLocalState(ExecutionContext& ctx, ReservoirSampleLocalState& localState, const Value<UInt64>& timestamp) const;

  private:
    const uint64_t operatorHandlerIndex;
    const Statistic::StatisticMetricHash metricHash;
    const TimeFunctionPtr timeFunction;
    MemoryProvider::MemoryProviderPtr reservoirMemoryProvider;
};

}// namespace NES::Runtime::Execution::Operators

#endif//NES_NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_STATISTICCOLLECTION_RESERVOIRSAMPLE_RESERVOIRSAMPLEBUILD_HPP_
