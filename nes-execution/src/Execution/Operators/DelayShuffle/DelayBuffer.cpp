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

#include <thread>
#include <Execution/Operators/DelayShuffle/DelayBuffer.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Identifiers/Identifiers.hpp>

namespace NES::Runtime::Execution::Operators
{
DelayBufferOperatorHandler::DelayBufferOperatorHandler(float unorderedness, uint64_t minDelay, uint64_t maxDelay)
    : unorderedness(unorderedness), gen(std::mt19937(std::random_device()())), unorderednessDistrib(0, 1), delayDistrib(minDelay, maxDelay)
{
}

void DelayBufferOperatorHandler::start(PipelineExecutionContext&, uint32_t)
{
}
void DelayBufferOperatorHandler::stop(Runtime::QueryTerminationType, PipelineExecutionContext&)
{
}

void DelayBufferOperatorHandler::sleepOrNot(SequenceNumber sequenceNumber)
{
    if (sequenceNumber.getRawValue()%100 < unorderedness*100)
    {
        std::this_thread::sleep_for((std::chrono::seconds(1)));
    }
}

DelayBuffer::DelayBuffer(const uint64_t operatorHandlerIndex) : operatorHandlerIndex(operatorHandlerIndex)
{
}

void DelayBuffer::execute(ExecutionContext& ctx, Record& record) const
{
    child->execute(ctx, record);
}

void DelayBuffer::open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const
{
    Operator::open(ctx, recordBuffer);

    nautilus::invoke(
        +[](OperatorHandler* handler, SequenceNumber sequenceNumber) { dynamic_cast<DelayBufferOperatorHandler*>(handler)->sleepOrNot(sequenceNumber); },
        ctx.getGlobalOperatorHandler(operatorHandlerIndex), recordBuffer.getSequenceNumber());
}
}
