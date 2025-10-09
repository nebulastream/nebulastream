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

#include <InputFormatters/InputFormatterTupleBufferRef.hpp>

#include <ostream>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <PipelineExecutionContext.hpp>
#include <RawTupleBuffer.hpp>

namespace NES
{
// OpenReturnState
// InputFormatterTupleBufferRef::scan(ExecutionContext& executionCtx, Nautilus::RecordBuffer& recordBuffer, const PhysicalOperator& child) const
// {
//     return this->inputFormatterTask->scanTask(executionCtx, recordBuffer, child);
// }

std::shared_ptr<MemoryLayout> InputFormatterTupleBufferRef::getMemoryLayout() const
{
    return this->inputFormatterTask->getMemoryLayout();
}

Record InputFormatterTupleBufferRef::readRecord(
    const std::vector<Record::RecordFieldIdentifier>& projections,
    const RecordBuffer& recordBuffer,
    nautilus::val<uint64_t>& recordIndex) const
{
    return this->inputFormatterTask->readRecord(projections, recordBuffer, recordIndex);
}

void InputFormatterTupleBufferRef::open(RecordBuffer& recordBuffer, ArenaRef& arenaRef)
{
    this->inputFormatterTask->open(recordBuffer, arenaRef);
}

// nautilus::val<uint64_t> InputFormatterTupleBufferRef::getNumberOfRecords(const RecordBuffer& recordBuffer) const
// {
//     return this->inputFormatterTask->getNumberOfRecords(recordBuffer);
// }

void InputFormatterTupleBufferRef::close(PipelineExecutionContext&) const
{
    this->inputFormatterTask->close();
}

std::ostream& InputFormatterTupleBufferRef::toString(std::ostream& os) const
{
    return this->inputFormatterTask->toString(os);
}

}
