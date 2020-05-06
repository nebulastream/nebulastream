
#include <QueryCompiler/CCodeGenerator/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CodeExpression.hpp>
#include <QueryCompiler/DataTypes/BasicValueType.hpp>

#include <boost/serialization/access.hpp>
#include <boost/serialization/export.hpp>

namespace NES {

BasicValueType::BasicValueType(const BasicType& type, std::string value) : type_(type), value_(std::move(value)){};

const DataTypePtr BasicValueType::getType() const { return createDataType(type_); }

const CodeExpressionPtr BasicValueType::getCodeExpression() const { return std::make_shared<CodeExpression>(value_); }

const ValueTypePtr BasicValueType::copy() const {
    return std::make_shared<BasicValueType>(*this);
}

const std::string BasicValueType::toString() const {
    return value_ + ": " + getType()->toString();
}

const bool BasicValueType::isArrayValueType() const { return false; }

bool BasicValueType::operator==(const ValueType& _rhs) const {
    auto rhs = dynamic_cast<const BasicValueType&>(_rhs);
    return type_ == rhs.type_ && value_ == rhs.value_;
}

const ValueTypePtr createBasicTypeValue(const BasicType& type, const std::string& value) {

    /** \todo: create instance of datatype and add a parseValue() method to datatype, so we can check whether the value
     * inside the string matches the type */
    return std::make_shared<BasicValueType>(type, value);
};

}// namespace NES
BOOST_CLASS_EXPORT(NES::BasicValueType);