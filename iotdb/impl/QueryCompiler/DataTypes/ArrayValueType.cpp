
#include <QueryCompiler/CCodeGenerator/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statement.hpp>
#include <QueryCompiler/CodeExpression.hpp>
#include <QueryCompiler/DataTypes/ArrayDataType.hpp>
#include <QueryCompiler/DataTypes/ArrayValueType.hpp>
#include <QueryCompiler/DataTypes/BasicDataType.hpp>
#include <QueryCompiler/DataTypes/ValueType.hpp>

namespace NES {

/**
 * class ArrayValueType keeps a field of values of basic types
 */
ArrayValueType::ArrayValueType(const ArrayDataType& type, const std::vector<std::string>& value)
    : type_(std::make_shared<
            ArrayDataType>(type)),
      value_(value){};

ArrayValueType::ArrayValueType(const ArrayDataType& type, const std::string& value)
    : type_(std::make_shared<ArrayDataType>(type)), isString_(true) {
    value_.push_back(value);
};

const DataTypePtr ArrayValueType::getType() const { return type_; }

const CodeExpressionPtr ArrayValueType::getCodeExpression() const {
    std::stringstream str;
    if (isString_) {
        str << "\"" << value_.at(0) << "\"";
        return std::make_shared<CodeExpression>(str.str());
    }
    bool isCharArray = (type_->isCharDataType());
    str << "{";
    u_int32_t i;
    for (i = 0; i < value_.size(); i++) {
        if (i != 0)
            str << ", ";
        if (isCharArray)
            str << "\'";
        str << value_.at(i);
        if (isCharArray)
            str << "\'";
    }
    str << "}";
    return std::make_shared<CodeExpression>(str.str());
}

const ValueTypePtr ArrayValueType::copy() const {
    return std::make_shared<ArrayValueType>(*this);
}

const bool ArrayValueType::isArrayValueType() const { return true; }

bool ArrayValueType::operator==(const ArrayValueType& rhs) const {
    return static_cast<const NES::ValueType&>(*this) == static_cast<const NES::ValueType&>(rhs) && type_ == rhs.type_ && isString_ == rhs.isString_ && value_ == rhs.value_;
}

bool ArrayValueType::operator==(const ValueType& rhs) const {
    return type_ == dynamic_cast<const NES::ArrayValueType&>(rhs).type_ && isString_ == dynamic_cast<const NES::ArrayValueType&>(rhs).isString_ && value_ == dynamic_cast<const NES::ArrayValueType&>(rhs).value_;
}

ArrayValueType::~ArrayValueType() {}
const std::string ArrayValueType::toString() const {

    return "[]: " + getType()->toString();
}

/**
 * creates a "string"-value (means char *)
 * @param value : std:string : the string value
 * @return ValueTypePtr : the structure keeping the given values (-- here it keeps it as a single string)
 */
const ValueTypePtr createStringValueType(const std::string& value) {
    return std::make_shared<ArrayValueType>(ArrayDataType(BasicDataType(BasicType::CHAR).copy(), value.size()), value);
}

const ValueTypePtr createStringValueType(const char* value, u_int16_t dimension) {
    u_int32_t i = 0;
    std::stringstream str;
    /**
     * caused by while-loop it could make sense to include a maximum string-size here.
     * ERROR-POSSIBILITY: INFINITE-LOOP - CHARPOINTER WITHOUT PREDEFINED SIZE DOES NOT END WITH '\0'
     */
    if (dimension == 0)
        while (value[i] != '\0') {
            str << value[i];
            i++;
        }
    else {
        u_int32_t j;
        for (j = 0; j < dimension; j++) {
            str << value[j];
            if (value[j] == '\0')
                break;
        }
        if (j == dimension) {
            str << '\0';
            j++;
        }
        i = j;
    }

    return std::make_shared<ArrayValueType>(ArrayDataType(BasicDataType(BasicType::CHAR).copy(), i), str.str());
}

const ValueTypePtr createArrayValueType(const BasicType& type, const std::vector<std::string>& value) {
    /** \todo: as above. Missing type-check for the values*/
    return std::make_shared<ArrayValueType>(ArrayDataType(BasicDataType(type).copy(), value.size()), value);
}
}// namespace NES