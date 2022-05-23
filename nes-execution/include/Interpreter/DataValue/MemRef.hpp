#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_MEMREF_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_MEMREF_HPP_
#include <Interpreter/DataValue/Any.hpp>
#include <Interpreter/DataValue/Boolean.hpp>
#include <Interpreter/DataValue/Integer.hpp>
namespace NES::Interpreter {

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

}// namespace NES::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_MEMREF_HPP_
