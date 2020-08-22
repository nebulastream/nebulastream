#include <API/Expressions/Expressions.hpp>
#include <API/Window/WindowAggregation.hpp>
#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/Compiler/Compiler.hpp>
#include <QueryCompiler/GeneratedCode.hpp>
#include <iostream>

namespace NES {

WindowAggregation::WindowAggregation(const NES::AttributeFieldPtr onField) : onField(onField),
                                                                             asField(const_cast<AttributeFieldPtr&>(onField)) {
}

WindowAggregation::WindowAggregation(const NES::AttributeFieldPtr onField, const NES::AttributeFieldPtr asField) : onField(onField), asField(asField) {
}

WindowAggregation& WindowAggregation::as(const NES::AttributeFieldPtr asField) {
    this->asField = asField;
    return *this;
}

Sum::Sum(NES::AttributeFieldPtr field) : WindowAggregation(field) {}
Sum::Sum(AttributeFieldPtr field, AttributeFieldPtr asField) : WindowAggregation(field, asField) {}

WindowAggregationPtr Sum::on(ExpressionItem onField) {
    auto keyExpression = onField.getExpressionNode();
    if (!keyExpression->instanceOf<FieldAccessExpressionNode>()) {
        NES_ERROR("Query: window key has to be an FieldAccessExpression but it was a " + keyExpression->toString());
    }
    auto fieldAccess = keyExpression->as<FieldAccessExpressionNode>();
    auto keyField = AttributeField::create(fieldAccess->getFieldName(), fieldAccess->getStamp());
    return std::make_shared<Sum>(Sum(keyField));
}

void Sum::compileLiftCombine(CompoundStatementPtr currentCode,
                             BinaryOperatorStatement partialRef,
                             StructDeclaration inputStruct,
                             BinaryOperatorStatement inputRef) {
    auto var_decl_input = inputStruct.getVariableDeclaration(this->onField->name);
    auto sum = partialRef + inputRef.accessRef(VarRefStatement(var_decl_input));
    auto updatedPartial = partialRef.assign(sum);
    currentCode->addStatement(std::make_shared<BinaryOperatorStatement>(updatedPartial));
}

}// namespace NES