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
#include <memory>
#include <vector>
#include <Interface/MemoryLayout/MemoryLayout.hpp>
#include <Interface/Record.hpp>
#include <Interface/TaskBufferRef.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <val_arith.hpp>
#include <val_base.hpp>

namespace NES
{

/// Interprets a TaskBufferRef's bytes as Records through a memory layout, hiding layout and buffer ownership from operators.
class RecordView
{
public:
    RecordView(const TaskBufferRef& buffer, std::shared_ptr<MemoryLayout> layout);

    [[nodiscard]] Record
    readRecord(const std::vector<Record::RecordFieldIdentifier>& projections, nautilus::val<uint64_t>& recordIndex) const;

    MemoryLayout::WriteRecordResult
    writeRecord(nautilus::val<uint64_t>& recordIndex, const Record& record, const nautilus::val<AbstractBufferProvider*>& bufferProvider);

    TaskBufferRef& getBuffer();
    [[nodiscard]] const TaskBufferRef& getBuffer() const;

private:
    TaskBufferRef recordBuffer;
    std::shared_ptr<MemoryLayout> layout;
};

}
