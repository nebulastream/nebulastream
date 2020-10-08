#include <API/Window/AggregationTypes/Sum.hpp>
#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/GeneratedCode.hpp>

namespace NES {

Sum::Sum(NES::AttributeFieldPtr field) : WindowAggregation(field) {

}
Sum::Sum(AttributeFieldPtr field, AttributeFieldPtr asField) : WindowAggregation(field, asField) {

}

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
    auto varDeclInput = inputStruct.getVariableDeclaration(this->onField->name);
    auto sum = partialRef + inputRef.accessRef(VarRefStatement(varDeclInput));
    auto updatedPartial = partialRef.assign(sum);
    currentCode->addStatement(std::make_shared<BinaryOperatorStatement>(updatedPartial));
}
}// namespace NES