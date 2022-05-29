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

#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_RECORD_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_RECORD_HPP_

#include <Experimental/Interpreter/DataValue/Value.hpp>
#include <memory>
#include <ostream>
#include <vector>

namespace NES::Interpreter {

class Record {
  public:
    explicit Record(std::vector<Value<>> records);
    ~Record() = default;
    Value<>& read(uint64_t fieldIndex);
    void write(uint64_t fieldIndex, Value<>& value);
    //  virtual Value<AnyPtr> read(std::string fieldName);
    //  virtual Value<AnyPtr> read(uint64_t fieldIndex);
    // virtual void write(std::string fieldName, Value<AnyPtr> value);
    // virtual void write(uint64_t fieldIndex, Value<AnyPtr> value);
    //friend std::ostream& operator<<(std::ostream&, const RecordPtr&);

  private:
    std::vector<Value<>> records;
};

using RecordPtr = std::shared_ptr<Record>;

}// namespace NES::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_RECORD_HPP_
