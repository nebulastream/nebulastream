
#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CodeExpression.hpp>
#include <QueryCompiler/DataTypes/BasicValueType.hpp>

#include <boost/serialization/access.hpp>
#include <boost/serialization/export.hpp>

namespace NES {

BasicValueType::BasicValueType(const BasicType& type, std::string value) : type(type), value(std::move(value)){};

const DataTypePtr BasicValueType::getType() const { return createDataType(type); }

const CodeExpressionPtr BasicValueType::getCodeExpression() const { return std::make_shared<CodeExpression>(value); }

const ValueTypePtr BasicValueType::copy() const {
    return std::make_shared<BasicValueType>(*this);
}

const std::string BasicValueType::toString() const {
    return value + ": " + getType()->toString();
}

bool BasicValueType::isArrayValueType() const { return false; }

bool BasicValueType::operator==(const ValueType& _rhs) const {
    auto rhs = dynamic_cast<const BasicValueType&>(_rhs);
    return type == rhs.type && value == rhs.value;
}

std::string BasicValueType::getValue() {
    return value;
}
bool BasicValueType::equals(const ValueTypePtr rhs) const {
    auto otherValueType = std::dynamic_pointer_cast<BasicValueType>(rhs);
    if (otherValueType == nullptr)
        return false;
    return type == otherValueType->type && value == otherValueType->value;
}

const ValueTypePtr createBasicTypeValue(const BasicType& type, const std::string& value) {

    /** \todo: create instance of datatype and add a parseValue() method to datatype, so we can check whether the value
     * inside the string matches the type */
    return std::make_shared<BasicValueType>(type, value);
};

}// namespace NES
BOOST_CLASS_EXPORT(NES::BasicValueType);