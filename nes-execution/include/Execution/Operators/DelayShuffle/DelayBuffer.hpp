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

#include <random>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/Operator.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>

namespace NES::Runtime::Execution::Operators
{

class DelayBufferOperatorHandler final : public OperatorHandler
{
public:
    DelayBufferOperatorHandler(float degreeOfDisorder, std::chrono::milliseconds minDelay, std::chrono::milliseconds maxDelay);

    void start(PipelineExecutionContext& pipelineExecutionContext, uint32_t localStateVariableId) override;
    void stop(Runtime::QueryTerminationType terminationType, PipelineExecutionContext& pipelineExecutionContext) override;

    /// Sleeps for a random time [minDelay, maxDelay] with a given probability. We calculate the probability by
    /// taking the modulo of the sequence number with 100 and comparing it with the degreeOfDisorder.
    /// This way, we ensure that the sleep is only executed for a certain percentage of the tuple buffers.
    void sleepOrNot(SequenceNumber sequenceNumber);

private:
    float degreeOfDisorder;
    std::mt19937 gen;
    std::uniform_int_distribution<> delayDistrib;
};

class DelayBuffer final : public ExecutableOperator
{
public:
    explicit DelayBuffer(const uint64_t operatorHandlerIndex);

    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void execute(ExecutionContext& ctx, Record& record) const override;

private:
    uint64_t operatorHandlerIndex;
};
}
