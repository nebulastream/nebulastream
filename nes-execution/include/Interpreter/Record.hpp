//
// Created by pgrulich on 27.03.22.
//

#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_RECORD_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_RECORD_HPP_

#include <Interpreter/DataValue/Value.hpp>
#include <memory>
#include <ostream>
#include <vector>

namespace NES {

class Record {
  public:
    explicit Record(std::vector<ValuePtr> records);
    virtual ~Record() = default;
    virtual ValuePtr read(std::string fieldName);
    virtual void write(std::string fieldName, ValuePtr value);
    friend std::ostream& operator<<(std::ostream&, const RecordPtr&);

  private:
    std::vector<ValuePtr> records;
};

using RecordPtr = std::shared_ptr<Record>;

}// namespace NES

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_RECORD_HPP_
