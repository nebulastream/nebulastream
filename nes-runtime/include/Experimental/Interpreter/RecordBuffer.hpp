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

#ifndef NES_RUNTIME_INCLUDE_EXPERIMENTAL_INTERPRETER_RECORDBUFFER_HPP_
#define NES_RUNTIME_INCLUDE_EXPERIMENTAL_INTERPRETER_RECORDBUFFER_HPP_

#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <memory>
#include <ostream>
#include <vector>

namespace NES::Nautilus {
class Record;
class RecordBuffer {
  public:
    explicit RecordBuffer(Value<MemRef> tupleBufferRef);
    ~RecordBuffer() = default;
    Record read(const Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout,
                const std::vector<Record::RecordFieldIdentifier>& projections,
                Value<MemRef> bufferAddress,
                Value<UInt64> recordIndex);
    Value<UInt64> getNumRecords();
    Value<MemRef> getBuffer();
    void write(const Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout, Value<UInt64> recordIndex, Record& record);
    const Value<MemRef>& getReference();

  public:
    Value<MemRef> tupleBufferRef;
    void setNumRecords(Value<UInt64> value);

  private:
    Record readRowLayout(const Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout,
                         const std::vector<Record::RecordFieldIdentifier>& projections,
                         Value<MemRef> bufferAddress,
                         Value<UInt64> recordIndex);
    Record readColumnarLayout(const Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout,
                              const std::vector<Record::RecordFieldIdentifier>& projections,
                              Value<MemRef> bufferAddress,
                              Value<UInt64> recordIndex);

    bool includeField(const std::vector<Record::RecordFieldIdentifier>& projections, Record::RecordFieldIdentifier fieldIndex);
};

using RecordBufferPtr = std::shared_ptr<RecordBuffer>;

}// namespace NES::Nautilus

#endif// NES_RUNTIME_INCLUDE_EXPERIMENTAL_INTERPRETER_RECORDBUFFER_HPP_
