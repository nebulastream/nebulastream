#include <Interpreter/DataValue/Any.hpp>
#include <Interpreter/Trace/ConstantValue.hpp>

namespace NES::Interpreter {
ConstantValue::ConstantValue(AnyPtr anyPtr) : value(std::move(anyPtr)){};

std::ostream& operator<<(std::ostream& os, const ConstantValue& valueRef) {
    os << "c" << valueRef.value;
    return os;
}

}// namespace NES::Interpreter