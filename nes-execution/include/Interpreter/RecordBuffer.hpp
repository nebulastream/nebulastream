//
// Created by pgrulich on 27.03.22.
//

#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_RECORDBUFFER_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_RECORDBUFFER_HPP_

#include <Interpreter/DataValue/Address.hpp>
#include <Interpreter/DataValue/Value.hpp>
#include <Interpreter/FunctionCall.hpp>
#include <memory>
#include <ostream>
#include <vector>
namespace NES::Interpreter {

class TupleBuffer {
  public:
    uint64_t getNumberOfRecords() { return 42; }
};

static uint64_t getNumberOfRecordsProxy(void* ) { return 0; }

class RecordBuffer {
  public:
    explicit RecordBuffer();
    ~RecordBuffer() = default;

    Value getNumberOfRecords() {
        auto* ptr = address.getPtr();
        auto res = FunctionCall(getNumberOfRecordsProxy, ptr);
        return Value(0,0);
    }

  private:
    Address address;
};

}// namespace NES::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_RECORDBUFFER_HPP_
