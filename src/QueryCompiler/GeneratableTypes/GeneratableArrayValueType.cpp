
#include <Common/ValueTypes/BasicValue.hpp>
#include <QueryCompiler/CodeExpression.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableArrayValueType.hpp>
#include <sstream>

namespace NES {

GeneratableArrayValueType::GeneratableArrayValueType(ValueTypePtr valueTypePtr, std::vector<std::string> values, bool isString) : GeneratableValueType(), valueType(valueTypePtr), values(values), isString(isString) {}

CodeExpressionPtr GeneratableArrayValueType::getCodeExpression() {
    std::stringstream str;
    if (isString) {
        str << "\"" << values.at(0) << "\"";
        return std::make_shared<CodeExpression>(str.str());
    }
    bool isCharArray = (valueType->isCharValue());
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
}// namespace NES