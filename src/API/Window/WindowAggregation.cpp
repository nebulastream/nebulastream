#include <API/Expressions/Expressions.hpp>
#include <API/Window/WindowAggregation.hpp>
#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/IFStatement.hpp>
#include <QueryCompiler/GeneratedCode.hpp>
#include <iostream>

#include<QueryCompiler/CodeExpression.hpp>
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

//SUM aggregation
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
    auto varDeclInput = inputStruct.getVariableDeclaration(this->onField->name);
    auto sum = partialRef + inputRef.accessRef(VarRefStatement(varDeclInput));
    auto updatedPartial = partialRef.assign(sum);
    currentCode->addStatement(std::make_shared<BinaryOperatorStatement>(updatedPartial));
}

//MAX aggregation
Max::Max(NES::AttributeFieldPtr field) : WindowAggregation(field) {}
Max::Max(AttributeFieldPtr field, AttributeFieldPtr asField) : WindowAggregation(field, asField) {}

WindowAggregationPtr Max::on(ExpressionItem onField) {
    auto keyExpression = onField.getExpressionNode();
    if (!keyExpression->instanceOf<FieldAccessExpressionNode>()) {
        NES_ERROR("Query: window key has to be an FieldAccessExpression but it was a " + keyExpression->toString());
    }
    auto fieldAccess = keyExpression->as<FieldAccessExpressionNode>();
    auto keyField = AttributeField::create(fieldAccess->getFieldName(), fieldAccess->getStamp());
    return std::make_shared<Max>(Max(keyField));
}

void Max::compileLiftCombine(CompoundStatementPtr currentCode,
                             BinaryOperatorStatement partialRef,
                             StructDeclaration inputStruct,
                             BinaryOperatorStatement inputRef) {
    auto varDeclInput = inputStruct.getVariableDeclaration(this->onField->name);
    auto ifStatement = IF(
        partialRef < inputRef.accessRef(VarRefStatement(varDeclInput)),
        assign(partialRef, inputRef.accessRef(VarRefStatement(varDeclInput))));
    currentCode->addStatement(ifStatement.createCopy());
}

//MIN aggregation
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
                             BinaryOperatorStatement partialRef,
                             StructDeclaration inputStruct,
                             BinaryOperatorStatement inputRef) {
    auto varDeclInput = inputStruct.getVariableDeclaration(this->onField->name);
    auto ifStatement = IF(
        partialRef > inputRef.accessRef(VarRefStatement(varDeclInput)),
        assign(partialRef, inputRef.accessRef(VarRefStatement(varDeclInput))));
    currentCode->addStatement(ifStatement.createCopy());
}

//COUNT aggregation
Count::Count(NES::AttributeFieldPtr field) : WindowAggregation(field) {}
Count::Count(AttributeFieldPtr field, AttributeFieldPtr asField) : WindowAggregation(field, asField) {}

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
                             BinaryOperatorStatement inputRef) {
    auto varDeclInput = inputStruct.getVariableDeclaration(this->onField->name);
    auto increment = ++partialRef;
    auto updatedPartial = partialRef.assign(increment);
    currentCode->addStatement(std::make_shared<BinaryOperatorStatement>(updatedPartial));
}
}// namespace NES