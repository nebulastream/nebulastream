#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/GeneratedCode.hpp>
#include <Windowing/WindowAggregations/Count.hpp>
#include <utility>

namespace NES {

Count::Count(NES::AttributeFieldPtr field) : WindowAggregation(std::move(field)) {}
Count::Count(AttributeFieldPtr field, AttributeFieldPtr asField) : WindowAggregation(std::move(field), std::move(asField)) {}

WindowAggregationPtr Count::create(NES::AttributeFieldPtr onField, NES::AttributeFieldPtr asField) {
    return std::make_shared<Count>(Count(std::move(onField), std::move(asField)));
}

WindowAggregationPtr Count::on(ExpressionItem onField) {
    auto keyExpression = onField.getExpressionNode();
    if (!keyExpression->instanceOf<FieldAccessExpressionNode>()) {
        NES_ERROR("Query: window key has to be an FieldAccessExpression but it was a " + keyExpression->toString());
    }
    auto fieldAccess = keyExpression->as<FieldAccessExpressionNode>();
    auto keyField = AttributeField::create(fieldAccess->getFieldName(), fieldAccess->getStamp());
    return std::make_shared<Count>(Count(keyField));
}

void Count::compileLiftCombine(CompoundStatementPtr currentCode,
                               BinaryOperatorStatement partialRef,
                               StructDeclaration inputStruct,
                               BinaryOperatorStatement) {
    auto varDeclInput = inputStruct.getVariableDeclaration(this->onField->name);
    auto increment = ++partialRef;
    auto updatedPartial = partialRef.assign(increment);
    currentCode->addStatement(std::make_shared<BinaryOperatorStatement>(updatedPartial));
}
}// namespace NES