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
#include <Experimental/Interpreter/DataValue/Integer/Int.hpp>
namespace NES::ExecutionEngine::Experimental::Interpreter {

class MemRef : public TraceableType {
  public:
    static const inline auto type = TypeIdentifier::create<Boolean>();

    MemRef(int8_t* value) : TraceableType(&type), value(value){};
    MemRef(MemRef&& a) : MemRef(a.value) {}
    MemRef(MemRef& a) : MemRef(a.value) {}
    std::unique_ptr<Any> copy() override { return std::make_unique<MemRef>(this->value); }

    std::unique_ptr<MemRef> add(Int8&) const {
        auto val1 = value + (int64_t) 45;
        return std::make_unique<MemRef>(val1);
    };
    ~MemRef() {}

    void* getValue() { return value; }

    template<class ResultType>
    std::unique_ptr<ResultType> load() {
        auto rawValue = (int64_t*) value;
        return std::make_unique<ResultType>(*rawValue);
    }

    void store(Any& valueType) {
        auto v = valueType.staticCast<Int64>();
        *reinterpret_cast<int64_t*>(value) = v.getValue();
    }

    IR::Types::StampPtr getType() const override { return IR::Types::StampFactory::createAddressStamp(); }
    int8_t* value;
};

}// namespace NES::ExecutionEngine::Experimental::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_MEMREF_HPP_
