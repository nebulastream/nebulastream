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
#include <Interface/RecordView.hpp>

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>
#include <Interface/MemoryLayout/MemoryLayout.hpp>
#include <Interface/Record.hpp>
#include <Interface/TaskBufferRef.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <ErrorHandling.hpp>
#include <val_arith.hpp>
#include <val_base.hpp>

namespace NES
{

RecordView::RecordView(const TaskBufferRef& buffer, std::shared_ptr<MemoryLayout> layout) : recordBuffer(buffer), layout(std::move(layout))
{
    PRECONDITION(this->layout != nullptr, "RecordView requires a memory layout");
}

Record RecordView::readRecord(const std::vector<Record::RecordFieldIdentifier>& projections, nautilus::val<uint64_t>& recordIndex) const
{
    return layout->readRecord(projections, recordBuffer, recordIndex);
}

MemoryLayout::WriteRecordResult RecordView::writeRecord(
    nautilus::val<uint64_t>& recordIndex, const Record& record, const nautilus::val<AbstractBufferProvider*>& bufferProvider)
{
    return layout->writeRecord(recordIndex, recordBuffer, record, bufferProvider);
}

TaskBufferRef& RecordView::getBuffer()
{
    return recordBuffer;
}

const TaskBufferRef& RecordView::getBuffer() const
{
    return recordBuffer;
}

}
