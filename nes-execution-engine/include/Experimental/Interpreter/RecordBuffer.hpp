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

#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_RECORD_BUFFER_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_RECORD_BUFFER_HPP_

#include <Experimental/Interpreter/DataValue/MemRef.hpp>
#include <Experimental/Interpreter/DataValue/Value.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <memory>
#include <ostream>
#include <vector>

namespace NES::ExecutionEngine::Experimental::Interpreter {
class Record;
class RecordBuffer {
  public:
    explicit RecordBuffer(Value<MemRef> tupleBufferRef);
    ~RecordBuffer() = default;
    Record
    read(const Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout, Value<MemRef> bufferAddress, Value<UInt64> recordIndex);
    Value<UInt64> getNumRecords();
    Value<MemRef> getBuffer();
    void write(const Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout, Value<UInt64> recordIndex, Record& record);
    const Value<MemRef>& getReference();
  public:
    Value<MemRef> tupleBufferRef;
    void setNumRecords(Value<UInt64> value);
  private:
    Record
    readRowLayout(const Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout, Value<MemRef> bufferAddress, Value<UInt64> recordIndex);
    Record readColumnarLayout(const Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout, Value<MemRef> bufferAddress, Value<UInt64> recordIndex);

};

using RecordBufferPtr = std::shared_ptr<RecordBuffer>;

}// namespace NES::ExecutionEngine::Experimental::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_RECORD_BUFFER_HPP_
