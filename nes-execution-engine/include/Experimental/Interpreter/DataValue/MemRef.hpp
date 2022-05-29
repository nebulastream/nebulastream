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
#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_MEMREF_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_MEMREF_HPP_
#include <Experimental/Interpreter/DataValue/Any.hpp>
#include <Experimental/Interpreter/DataValue/Boolean.hpp>
#include <Experimental/Interpreter/DataValue/Integer.hpp>
namespace NES::Experimental::Interpreter {

class MemRef : public Any {
  public:
    const static Kind type = Kind::IntegerValue;

    MemRef(int64_t value) : Any(type), value(value){};
    MemRef(MemRef&& a) : MemRef(a.value) {}
    MemRef(MemRef& a) : MemRef(a.value) {}
    MemRef(Integer& a) : MemRef(a.value) {}
    std::unique_ptr<Any> copy() { return std::make_unique<MemRef>(this->value); }
    std::unique_ptr<MemRef> add(std::unique_ptr<MemRef>& otherValue) const {
        return std::make_unique<MemRef>(value + otherValue->value);
    };
    ~MemRef() {}

    int64_t getValue() { return value; }
    template<class ResultType>
    std::unique_ptr<ResultType> load() { return std::make_unique<Integer>(value); }

    template<class ValueType>
    void store(ValueType value) { std::make_unique<Integer>(value); }


    const int64_t value;
};

}// namespace NES::Experimental::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_MEMREF_HPP_
