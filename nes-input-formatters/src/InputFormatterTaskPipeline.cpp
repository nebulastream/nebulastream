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

#include <InputFormatters/InputFormatterTaskPipeline.hpp>

#include <ostream>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <PipelineExecutionContext.hpp>
#include <RawTupleBuffer.hpp>

void NES::InputFormatters::InputFormatterTaskPipeline::scan(
    ExecutionContext& executionCtx,
    Nautilus::RecordBuffer& recordBuffer,
    const PhysicalOperator& child,
    const std::vector<Record::RecordFieldIdentifier>& projections,
    const size_t configuredBufferSize,
    const bool isFirstOperatorAfterSource,
    const std::vector<Record::RecordFieldIdentifier>& requiredFields) const
{
    /// If the buffer is empty, we simply return without submitting any unnecessary work on empty buffers.
    // Todo: can't check if numRecords == 0, since it may be initialized with 0 during tracing
    // if (recordBuffer.getNumRecords() == 0)
    // {
    //     NES_WARNING("Received empty buffer in InputFormatterTask.");
    //     return;
    // }
    this->inputFormatterTask->scanTask(executionCtx, recordBuffer, child, projections, configuredBufferSize, isFirstOperatorAfterSource, requiredFields);
}

void NES::InputFormatters::InputFormatterTaskPipeline::stop(PipelineExecutionContext&) const
{
    this->inputFormatterTask->stopTask();
}

// void NES::InputFormatters::InputFormatterTaskPipeline::execute(const Memory::TupleBuffer& rawTupleBuffer, PipelineExecutionContext& pec)
// {
//     /// If the buffer is empty, we simply return without submitting any unnecessary work on empty buffers.
//     if (rawTupleBuffer.getBufferSize() == 0)
//     {
//         NES_WARNING("Received empty buffer in InputFormatterTask.");
//         return;
//     }
//
//     this->inputFormatterTask->executeTask(RawTupleBuffer{rawTupleBuffer}, pec);
// }

std::ostream& NES::InputFormatters::InputFormatterTaskPipeline::toString(std::ostream& os) const
{
    return this->inputFormatterTask->toString(os);
}
