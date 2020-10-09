#include <Windowing/AggregationTypes/Min.hpp>
#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/IFStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/GeneratedCode.hpp>

namespace NES {

Min::Min(NES::AttributeFieldPtr field) : WindowAggregation(field) {}
Min::Min(AttributeFieldPtr field, AttributeFieldPtr asField) : WindowAggregation(field, asField) {}

WindowAggregationPtr Min::on(ExpressionItem onField) {
    auto keyExpression = onField.getExpressionNode();
    if (!keyExpression->instanceOf<FieldAccessExpressionNode>()) {
        NES_ERROR("Query: window key has to be an FieldAccessExpression but it was a " + keyExpression->toString());
    }
    auto fieldAccess = keyExpression->as<FieldAccessExpressionNode>();
    auto keyField = AttributeField::create(fieldAccess->getFieldName(), fieldAccess->getStamp());
    return std::make_shared<Min>(Min(keyField));
}

void Min::compileLiftCombine(CompoundStatementPtr currentCode,
                             BinaryOperatorStatement expressionStatement,
                             StructDeclaration inputStruct,
                             BinaryOperatorStatement inputRef) {
    auto varDeclInput = inputStruct.getVariableDeclaration(this->onField->name);
    auto ifStatement = IF(
        expressionStatement > inputRef.accessRef(VarRefStatement(varDeclInput)),
        assign(expressionStatement, inputRef.accessRef(VarRefStatement(varDeclInput))));
    currentCode->addStatement(ifStatement.createCopy());
}
}// namespace NES