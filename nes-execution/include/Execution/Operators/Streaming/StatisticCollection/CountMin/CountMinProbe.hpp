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

#ifndef NES_NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_STATISTICCOLLECTION_COUNTMIN_COUNTMINPROBE_HPP_
#define NES_NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_STATISTICCOLLECTION_COUNTMIN_COUNTMINPROBE_HPP_

#include <Execution/Operators/ExecutableOperator.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Statistics/Statistic.hpp>
#include <future>

namespace NES::Runtime::Execution::Operators {

class CountMinProbeHandler;
using CountMinProbeHandlerPtr = std::shared_ptr<CountMinProbeHandler>;

class CountMinProbeHandler : public OperatorHandler {
  public:

    static CountMinProbeHandlerPtr create();
    void start(PipelineExecutionContextPtr pipelineExecutionContext, uint32_t localStateVariableId) override;
    void stop(QueryTerminationType terminationType, PipelineExecutionContextPtr pipelineExecutionContext) override;

    void setNewStatistic(Statistic::StatisticPtr newStatistic, const uint64_t numberOfBitsInKey);
    void* getCountMinStatisticData() const;
    const std::vector<uint64_t>& getH3Seeds() const;
    void writeMinCount(uint64_t minCount);
    Statistic::StatisticValue<> getStatisticValue();
    uint64_t getSizeOfOneRowInBytesSeeds() const;
    uint64_t getSizeOfOneCounterInBytes() const;
    uint64_t getDepth() const;
    uint64_t getWidth() const;

  private:
    CountMinProbeHandler();


    Statistic::StatisticPtr countMinStatistic;
    std::vector<uint64_t> h3Seeds;
    uint64_t minCount;
    uint64_t sizeOfOneRowInBytesSeeds;
    uint64_t sizeOfOneCounterInBytes;
};

class CountMinProbe : public ExecutableOperator {
  public:
    CountMinProbe(const uint64_t operatorHandlerIndex,
                  const std::string_view keyFieldName,
                  std::unique_ptr<Nautilus::Interface::HashFunction> h3HashFunction);

    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void execute(ExecutionContext& ctx, Record& record) const override;

  private:
    const uint64_t operatorHandlerIndex;
    const std::string keyFieldName;
    const std::unique_ptr<Nautilus::Interface::HashFunction> h3HashFunction;
};
} // namespace NES::Runtime::Execution::Operators
#endif//NES_NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_STATISTICCOLLECTION_COUNTMIN_COUNTMINPROBE_HPP_
