
#include <DataTypes/DataTypeFactory.hpp>
#include <DataTypes/PhysicalTypes/ArrayPhysicalType.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/ConstantExpressionStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/FunctionCallStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CodeExpression.hpp>
#include <QueryCompiler/CompilerTypesFactory.hpp>
#include <QueryCompiler/DataTypes/ArrayGeneratableType.hpp>
#include <QueryCompiler/DataTypes/GeneratableDataType.hpp>
#include <memory>
#include <sstream>
namespace NES {

ArrayGeneratableType::ArrayGeneratableType(ArrayPhysicalTypePtr type,
                                           GeneratableDataTypePtr component) : type(type), component(component) {}

const CodeExpressionPtr ArrayGeneratableType::getTypeDefinitionCode() const {
    return std::make_shared<CodeExpression>("");
}

CodeExpressionPtr ArrayGeneratableType::generateCode() {}

const CodeExpressionPtr ArrayGeneratableType::getCode() const {
    std::stringstream str;
    str << "[" << type->getLength() << "]";

    return combine(component->getCode(), std::make_shared<CodeExpression>(str.str()));
}

CodeExpressionPtr ArrayGeneratableType::getDeclCode(std::string identifier) {
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
StatementPtr ArrayGeneratableType::getStmtCopyAssignment(const AssignmentStatment& aParam) {
    FunctionCallStatement func_call("memcpy");
    func_call.addParameter(VarRef(aParam.lhs_tuple_var)[VarRef(aParam.lhs_index_var)].accessRef(VarRef(aParam.lhs_field_var)));
    func_call.addParameter(VarRef(aParam.rhs_tuple_var)[VarRef(aParam.rhs_index_var)].accessRef(VarRef(aParam.rhs_field_var)));
    auto tf = CompilerTypesFactory();
    func_call.addParameter(ConstantExpressionStatement(
        tf.createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt64(), std::to_string(this->type->getLength())))
        ));
    return func_call.copy();
}

}// namespace NES