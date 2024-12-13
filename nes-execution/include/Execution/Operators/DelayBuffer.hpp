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

#include "ExecutableOperator.hpp"
#include "Identifiers/Identifiers.hpp"


#include <random>
#include <Execution/Operators/Operator.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>

namespace NES::Runtime::Execution::Operators
{

class DelayBufferOperatorHandler : public OperatorHandler
{
public:
    DelayBufferOperatorHandler(float unorderedness, uint64_t minDelay = 1, uint64_t maxDelay = 10);

    void start(PipelineExecutionContext& pipelineExecutionContext, uint32_t localStateVariableId) override;
    void stop(Runtime::QueryTerminationType terminationType, PipelineExecutionContext& pipelineExecutionContext) override;

    void sleepOrNot(SequenceNumber sequenceNumber);

private:
    const float unorderedness;
    std::mt19937 gen;
    std::uniform_real_distribution<> unorderednessDistrib;
    std::uniform_int_distribution<> delayDistrib;
};

class DelayBuffer : public ExecutableOperator
{
public:
    DelayBuffer(const uint64_t operatorHandlerIndex);

    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

    void execute(ExecutionContext& ctx, Record& record) const override;

private:
    uint64_t operatorHandlerIndex;
};
} // namespace NES::Runtime::Execution::Operators
