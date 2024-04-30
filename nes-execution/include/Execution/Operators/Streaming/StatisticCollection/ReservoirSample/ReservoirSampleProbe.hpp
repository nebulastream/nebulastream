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

#ifndef NES_NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_STATISTICCOLLECTION_RESERVOIRSAMPLE_RESERVOIRSAMPLEPROBE_HPP_
#define NES_NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_STATISTICCOLLECTION_RESERVOIRSAMPLE_RESERVOIRSAMPLEPROBE_HPP_

#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Nautilus/Interface/DataTypes/Text/TextValue.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Statistics/Synopses/ReservoirSampleStatistic.hpp>
#include <future>

namespace NES::Runtime::Execution::Operators {

class ReservoirSampleProbeHandler;
using ReservoirSampleProbeHandlerPtr = std::shared_ptr<ReservoirSampleProbeHandler>;

class ReservoirSampleProbeHandler : public OperatorHandler {
  public:

    static ReservoirSampleProbeHandlerPtr create();
    void start(PipelineExecutionContextPtr pipelineExecutionContext, uint32_t localStateVariableId) override;
    void stop(QueryTerminationType terminationType, PipelineExecutionContextPtr pipelineExecutionContext) override;

    void setNewStatistic(Statistic::StatisticPtr newStatistic);
    uint64_t getSampleSize() const;
    void* getReservoirSampleData() const;
    void writeCount(uint64_t count);
    Statistic::StatisticValue<> getStatisticValue();
    uint64_t getObservedTuples() const;

  private:
    ReservoirSampleProbeHandler();


    Statistic::StatisticPtr reservoirSample;
    std::vector<uint64_t> h3Seeds;
    uint64_t count;
};


class ReservoirSampleProbe : public ExecutableOperator {
  public:
    ReservoirSampleProbe(MemoryProvider::MemoryProviderPtr sampleMemoryProvider,
                         const uint64_t operatorHandlerIndex,
                         const std::string_view fieldNameToCountInput,
                         const std::string_view fieldNameToCountSample);
    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void execute(ExecutionContext& ctx, Record& record) const override;

  private:
    const MemoryProvider::MemoryProviderPtr sampleMemoryProvider;
    const uint64_t operatorHandlerIndex;
    const std::string fieldNameToCountInput;
    const std::string fieldNameToCountSample; // Talk with Philipp how I can get this from the operator handler
};
} // namespace NES::Runtime::Execution::Operators
#endif//NES_NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_STATISTICCOLLECTION_RESERVOIRSAMPLE_RESERVOIRSAMPLEPROBE_HPP_
