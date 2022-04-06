//
// Created by pgrulich on 27.03.22.
//

#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_RECORD_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_RECORD_HPP_

#include <Interpreter/DataValue/Value.hpp>
#include <memory>
#include <ostream>
#include <vector>

namespace NES::Interpreter {

class Record {
  public:
    explicit Record(std::vector<Value> records);
    ~Record() = default;
    Value& read(uint64_t fieldIndex);
    void write(uint64_t fieldIndex, Value& value);
    //  virtual Value<AnyPtr> read(std::string fieldName);
    //  virtual Value<AnyPtr> read(uint64_t fieldIndex);
    // virtual void write(std::string fieldName, Value<AnyPtr> value);
    // virtual void write(uint64_t fieldIndex, Value<AnyPtr> value);
    //friend std::ostream& operator<<(std::ostream&, const RecordPtr&);

  private:
    std::vector<Value> records;
};

using RecordPtr = std::shared_ptr<Record>;

}// namespace NES::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_RECORD_HPP_
