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

#pragma once

#include <cstdint>
#include <random>
#include <Execution/Operators/Operator.hpp>
#include <Identifiers/Identifiers.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>

namespace NES::Runtime::Execution::Operators
{

class ShuffleTuplesOperatorHandler : public OperatorHandler
{
public:
    ShuffleTuplesOperatorHandler(float degreeOfDisorder);

    void setup(uint64_t numberOfWorkerThreads);
    void start(PipelineExecutionContext& pipelineExecutionContext, uint32_t localStateVariableId) override;
    void stop(Runtime::QueryTerminationType terminationType, PipelineExecutionContext& pipelineExecutionContext) override;

    void createEmitIndicesForInputBuffer(WorkerThreadId workerThreadId, uint64_t numberOfTuples);
    std::vector<uint64_t> getEmitIndicesForWorker(WorkerThreadId workerThreadId);

private:
    const double degreeOfDisorder;
    std::mt19937 gen;
    uint64_t numberOfWorkerThreads;
    std::vector<std::vector<uint64_t>> emitIndicesForWorker;
};

/// Shuffles the tuples in the input buffer. We choose not to have the fastest shuffle algorithm here, but a simple one
class ShuffleTuples final : public Operator
{
public:
    ShuffleTuples(
        std::unique_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider,
        const uint64_t operatorHandlerIndex);

    /// Sets the number of worker threads for the operator handler
    void setup(ExecutionContext& executionCtx) const override;

    /// Shuffles the input buffer by writing the records to the output buffer at the positions specified by the emitIndices
    /// This is not the most efficient way to shuffle records, but it is sufficient for the purpose.
    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

private:
    std::unique_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider;
    uint64_t operatorHandlerIndex;
    std::vector<Record::RecordFieldIdentifier> projections;
};
}
