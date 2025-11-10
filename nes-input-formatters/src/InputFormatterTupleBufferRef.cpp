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

#include <cstdint>
#include <memory>
#include <ostream>
#include <vector>

#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <ExecutionContext.hpp>

namespace NES
{

Record InputFormatterTupleBufferRef::readRecord(
    const std::vector<Record::RecordFieldIdentifier>& projections,
    const RecordBuffer& recordBuffer,
    nautilus::val<uint64_t>& recordIndex) const
{
    return this->InputFormatter->readRecord(projections, recordBuffer, recordIndex);
}

Interface::BufferRef::IndexBufferResult InputFormatterTupleBufferRef::indexBuffer(RecordBuffer& recordBuffer, ArenaRef& arenaRef)
{
    return this->InputFormatter->indexBuffer(recordBuffer, arenaRef);
}

std::ostream& operator<<(std::ostream& os, const InputFormatterTupleBufferRef& inputFormatterTupleBufferRef)
{
    return inputFormatterTupleBufferRef.InputFormatter->toString(os);
}

}
