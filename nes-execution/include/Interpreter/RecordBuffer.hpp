//
// Created by pgrulich on 27.03.22.
//

#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_RECORD_BUFFER_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_RECORD_BUFFER_HPP_

#include <Interpreter/DataValue/MemRef.hpp>
#include <Interpreter/DataValue/Value.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <memory>
#include <ostream>
#include <vector>

namespace NES::Interpreter {
class Record;
class RecordBuffer {
  public:
    explicit RecordBuffer(Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout, Value<MemRef> tupleBufferRef);
    ~RecordBuffer() = default;
    Record read(Value<Integer> recordIndex);
    Value<Integer> getNumRecords();
    void write(uint64_t recordIndex, std::shared_ptr<Record> record);

  private:
    Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout;
    Value<MemRef> tupleBufferRef;
};

using RecordBufferPtr = std::shared_ptr<RecordBuffer>;

}// namespace NES::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_RECORD_BUFFER_HPP_
