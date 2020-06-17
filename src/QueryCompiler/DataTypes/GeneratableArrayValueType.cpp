
#include <QueryCompiler/DataTypes/GeneratableArrayValueType.hpp>
#include <QueryCompiler/CodeExpression.hpp>
#include <DataTypes/ValueTypes/BasicValue.hpp>
#include <utility>
#include <sstream>

namespace NES{

GeneratableArrayValueType::GeneratableArrayValueType(ValueTypePtr  valueTypePtr, std::vector<std::string> values) : GeneratableValueType(), valueType(valueTypePtr), values(values) {}

CodeExpressionPtr GeneratableArrayValueType::getCodeExpression() {
    std::stringstream str;
    if (valueType->isCharValue()) {
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
}