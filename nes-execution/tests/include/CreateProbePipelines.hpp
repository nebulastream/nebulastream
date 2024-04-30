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

#ifndef NES_NES_EXECUTION_BENCHMARK_STATISTICPROBE_CREATEPROBEPIPELINES_HPP_
#define NES_NES_EXECUTION_BENCHMARK_STATISTICPROBE_CREATEPROBEPIPELINES_HPP_
#include <Execution/Pipelines/ExecutablePipelineProvider.hpp>
#include <Execution/Operators/Streaming/StatisticCollection/CountMin/CountMinProbe.hpp>
#include <Execution/Operators/Streaming/StatisticCollection/ReservoirSample/ReservoirSampleProbe.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Statistics/Statistic.hpp>
#include <cstdint>
namespace NES::Runtime::Execution {

class ProbePipelineExecutionContext : public Runtime::Execution::PipelineExecutionContext {
  public:
    ProbePipelineExecutionContext(OperatorHandlerPtr operatorHandler)
        : PipelineExecutionContext(
              PipelineId(1),         // mock pipeline id
              DecomposedQueryPlanId(1),         // mock query id
              nullptr,
              1, // numberOfWorkerThreads
              {},
              {},
              {operatorHandler}){};
};


class CreateProbePipelines {
  public:
    CreateProbePipelines();

    std::tuple<std::shared_ptr<PhysicalOperatorPipeline>, Operators::CountMinProbeHandlerPtr>
    createCountMinProbePipeline(const uint64_t numberOfBitsInKey,
                                const SchemaPtr inputProbeSchema,
                                const std::string_view keyFieldName);

    std::tuple<std::shared_ptr<PhysicalOperatorPipeline>, Operators::ReservoirSampleProbeHandlerPtr>
    createReservoirSamplePipeline(const SchemaPtr inputProbeSchema,
                                  const SchemaPtr sampleSchema,
                                  const std::string_view fieldNameToCountInput,
                                  const std::string_view fieldNameToCountSample);

    BufferManagerPtr bufferManager;
    PipelineExecutionContextPtr pipelineExecutionContext;
    WorkerContextPtr workerContext;
};
} // namespace NES::Runtime::Execution
#endif//NES_NES_EXECUTION_BENCHMARK_STATISTICPROBE_CREATEPROBEPIPELINES_HPP_
