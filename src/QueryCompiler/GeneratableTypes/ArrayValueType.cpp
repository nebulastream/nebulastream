
#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CodeExpression.hpp>
#include <QueryCompiler/GeneratableTypes/ArrayDataType.hpp>
#include <QueryCompiler/GeneratableTypes/ArrayValueType.hpp>
#include <QueryCompiler/GeneratableTypes/BasicDataType.hpp>
#include <QueryCompiler/GeneratableTypes/ValueType.hpp>

namespace NES {

/**
 * class ArrayValueType keeps a field of values of basic types
 */
ArrayValueType::ArrayValueType(const ArrayDataType& type, const std::vector<std::string>& value)
    : type(std::make_shared<
           ArrayDataType>(type)),
      values(value){};

ArrayValueType::ArrayValueType(const ArrayDataType& type, const std::string& value)
    : type(std::make_shared<ArrayDataType>(type)), isString(true) {
    values.push_back(value);
};

const DataTypePtr ArrayValueType::getType() const { return type; }

const CodeExpressionPtr ArrayValueType::getCodeExpression() const {
    std::stringstream str;
    if (isString) {
        str << "\"" << values.at(0) << "\"";
        return std::make_shared<CodeExpression>(str.str());
    }
    bool isCharArray = (type->isCharDataType());
    str << "{";
    u_int32_t i;
    for (i = 0; i < values.size(); i++) {
        if (i != 0)
            str << ", ";
        if (isCharArray)
            str << "\'";
        str << values.at(i);
        if (isCharArray)
            str << "\'";
    }
    str << "}";
    return std::make_shared<CodeExpression>(str.str());
}

const ValueTypePtr ArrayValueType::copy() const {
    return std::make_shared<ArrayValueType>(*this);
}

bool ArrayValueType::isArrayValueType() const { return true; }

bool ArrayValueType::operator==(const ArrayValueType& rhs) const {
    return static_cast<const NES::ValueType&>(*this) == static_cast<const NES::ValueType&>(rhs) && type == rhs.type && isString == rhs.isString && values == rhs.values;
}

bool ArrayValueType::operator==(const ValueType& rhs) const {
    return type == dynamic_cast<const NES::ArrayValueType&>(rhs).type && isString == dynamic_cast<const NES::ArrayValueType&>(rhs).isString && values == dynamic_cast<const NES::ArrayValueType&>(rhs).values;
}

ArrayValueType::~ArrayValueType() {}
const std::string ArrayValueType::toString() const {

    return "[]: " + getType()->toString();
}

std::vector<std::string> ArrayValueType::getValues() {
    return values;
}
bool ArrayValueType::equals(ValueTypePtr rhs) const {
    auto otherArrayType = std::dynamic_pointer_cast<ArrayValueType>(rhs);
    if (otherArrayType == nullptr)
        return false;
    return type->isEqual(otherArrayType->type) && values == otherArrayType->values && isString == otherArrayType->isString;
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