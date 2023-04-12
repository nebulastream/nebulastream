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
#include <Execution/StatisticsCollector/CollectorTrigger.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Pipelines/NautilusExecutablePipelineStage.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/IR/Types/StampFactory.hpp>

namespace NES::Runtime::Execution {

NautilusExecutablePipelineStage::NautilusExecutablePipelineStage(
    std::shared_ptr<PhysicalOperatorPipeline> physicalOperatorPipeline)
    : physicalOperatorPipeline(physicalOperatorPipeline) {}

uint32_t NautilusExecutablePipelineStage::setup(PipelineExecutionContext& pipelineExecutionContext) {
    auto pipelineExecutionContextRef = Value<MemRef>((int8_t*) &pipelineExecutionContext);
    auto workerContextRef = Value<MemRef>((int8_t*) nullptr);
    auto ctx = ExecutionContext(workerContextRef, pipelineExecutionContextRef);
    physicalOperatorPipeline->getRootOperator()->setup(ctx);
    pipelineId = pipelineExecutionContext.getPipelineID();
    return 0;
}

ExecutionResult NautilusExecutablePipelineStage::execute(TupleBuffer& inputTupleBuffer,
                                                         PipelineExecutionContext& pipelineExecutionContext,
                                                         WorkerContext& workerContext) {
    auto pipelineExecutionContextRef = Value<MemRef>((int8_t*) &pipelineExecutionContext);
    auto workerContextRef = Value<MemRef>((int8_t*) &workerContext);
    auto ctx = ExecutionContext(workerContextRef, pipelineExecutionContextRef);
    auto bufferRef = Value<MemRef>((int8_t*) std::addressof(inputTupleBuffer));
    auto recordBuffer = RecordBuffer(bufferRef);

    numberOfInputTuples = inputTupleBuffer.getNumberOfTuples();
    if (profiler != nullptr) profiler->startProfiling();
    auto runtimeStart = std::chrono::high_resolution_clock::now();

    physicalOperatorPipeline->getRootOperator()->open(ctx, recordBuffer);
    physicalOperatorPipeline->getRootOperator()->close(ctx, recordBuffer);

    auto runtimeEnd = std::chrono::high_resolution_clock::now();
    if (profiler != nullptr) profiler->stopProfiling();
    auto duration = duration_cast<std::chrono::microseconds>(runtimeEnd - runtimeStart);
    runtimePerBuffer = duration.count();
    numberOfOutputTuples = pipelineExecutionContext.getNumberOfEmittedTuples();

    return ExecutionResult::Finished;
}

uint32_t NautilusExecutablePipelineStage::start(PipelineExecutionContext&) {
    // nop as we don't need this function in nautilus
    return 0;
}

uint32_t NautilusExecutablePipelineStage::open(Execution::PipelineExecutionContext&, WorkerContext&) {
    // nop as we don't need this function in nautilus
    return 0;
}
uint32_t NautilusExecutablePipelineStage::close(PipelineExecutionContext&, WorkerContext&) {
    // nop as we don't need this function in nautilus
    return 0;
}

uint32_t NautilusExecutablePipelineStage::stop(PipelineExecutionContext&) {
    // nop as we don't need this function in nautilus
    return 0;
}

uint32_t NautilusExecutablePipelineStage::stop(std::shared_ptr<StatisticsCollector> statisticsCollector){
    auto collectorTrigger = CollectorTrigger(PipelineStatisticsTrigger, pipelineId);
    statisticsCollector->updateStatisticsHandler(collectorTrigger);
    return 0;
}


uint64_t NautilusExecutablePipelineStage::getNumberOfInputTuples() const{
    return numberOfInputTuples;
}

uint64_t NautilusExecutablePipelineStage::getNumberOfOutputTuples() const{
    return numberOfOutputTuples;
}

uint64_t NautilusExecutablePipelineStage::getRuntimePerBuffer() const{
    return runtimePerBuffer;
}

void NautilusExecutablePipelineStage::setProfiler(std::shared_ptr<Profiler> profilerPtr){
    profiler = std::move(profilerPtr);
}

std::string NautilusExecutablePipelineStage::getCodeAsString() { return "<no_code>"; }

}// namespace NES::Runtime::Execution