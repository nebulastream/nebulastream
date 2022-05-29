//
// Created by pgrulich on 27.03.22.
//

#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_RECORD_BUFFER_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_RECORD_BUFFER_HPP_

#include <Experimental/Interpreter/DataValue/MemRef.hpp>
#include <Experimental/Interpreter/DataValue/Value.hpp>
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
    void write(Value<Integer> recordIndex, Record& record);

  private:
    Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout;

  public:
    Value<MemRef> tupleBufferRef;
    void setNumRecords(Value<Integer> value);
};

using RecordBufferPtr = std::shared_ptr<RecordBuffer>;

}// namespace NES::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_RECORD_BUFFER_HPP_
