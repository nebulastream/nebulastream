/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/ArrayPhysicalType.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/ConstantExpressionStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/FunctionCallStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CodeExpression.hpp>
#include <QueryCompiler/CompilerTypesFactory.hpp>
#include <QueryCompiler/GeneratableTypes/ArrayGeneratableType.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableDataType.hpp>
#include <memory>
#include <sstream>
namespace NES {

ArrayGeneratableType::ArrayGeneratableType(ArrayPhysicalTypePtr type, GeneratableDataTypePtr component)
    : type(type), component(component) {}

const CodeExpressionPtr ArrayGeneratableType::getTypeDefinitionCode() const { return std::make_shared<CodeExpression>(""); }

const CodeExpressionPtr ArrayGeneratableType::getCode() const {
    std::stringstream str;
    str << "[" << type->getLength() << "]";
    return combine(component->getCode(), std::make_shared<CodeExpression>(str.str()));
}

CodeExpressionPtr ArrayGeneratableType::getDeclarationCode(std::string identifier) {
    CodeExpressionPtr ptr;
    if (identifier != "") {
        ptr = component->getCode();
        ptr = combine(ptr, std::make_shared<CodeExpression>(" " + identifier));
        ptr = combine(ptr, std::make_shared<CodeExpression>("["));
        ptr = combine(ptr, std::make_shared<CodeExpression>(std::to_string(type->getLength())));
        ptr = combine(ptr, std::make_shared<CodeExpression>("]"));
    } else {
        ptr = component->getCode();
    }
    return ptr;
}
StatementPtr ArrayGeneratableType::getStmtCopyAssignment(const AssignmentStatment& assignmentStatment) {
    auto functionCall = FunctionCallStatement("memcpy");
    functionCall.addParameter(VarRef(assignmentStatment.lhs_tuple_var)[VarRef(assignmentStatment.lhs_index_var)].accessRef(
        VarRef(assignmentStatment.lhs_field_var)));
    functionCall.addParameter(VarRef(assignmentStatment.rhs_tuple_var)[VarRef(assignmentStatment.rhs_index_var)].accessRef(
        VarRef(assignmentStatment.rhs_field_var)));
    auto tf = CompilerTypesFactory();
    functionCall.addParameter(ConstantExpressionStatement(tf.createValueType(
        DataTypeFactory::createBasicValue(DataTypeFactory::createUInt64(), std::to_string(type->getLength())))));
    return functionCall.copy();
}

}// namespace NES