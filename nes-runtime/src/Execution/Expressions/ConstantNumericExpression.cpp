#include <Execution/Expressions/ConstantDecimalExpression.hpp>
#include <Nautilus/Interface/DataTypes/Numeric/Numeric.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <memory>

namespace NES::Runtime::Execution::Expressions {

ConstantDecimalExpression::ConstantDecimalExpression(int8_t precision, int64_t value) : precision(precision), value(value) {}

Value<> ConstantDecimalExpression::execute(NES::Nautilus::Record&) const {
    return Value<Numeric>(std::make_shared<Numeric>(precision, value));
}

}// namespace NES::Runtime::Execution::Expressions