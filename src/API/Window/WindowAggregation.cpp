#include <API/Window/WindowAggregation.hpp>
#include <Operators/Operator.hpp>
#include <QueryCompiler/CCodeGenerator/BinaryOperatorStatement.hpp>
#include <QueryCompiler/Compiler/Compiler.hpp>
#include <QueryCompiler/GeneratedCode.hpp>
#include <iostream>

namespace NES {

WindowAggregation::WindowAggregation(const NES::AttributeFieldPtr onField) : _onField(onField), _asField(
                                                                                                    const_cast<AttributeFieldPtr&>(onField)) {
}

WindowAggregation& WindowAggregation::as(const NES::AttributeFieldPtr asField) {
    this->_asField = asField;
    return *this;
}

Sum::Sum(NES::Field onField) : WindowAggregation(onField.getAttributeField()) {}

WindowAggregationPtr Sum::on(NES::Field onField) {
    return std::make_shared<Sum>(Sum(onField));
}

void Sum::compileLiftCombine(CompoundStatementPtr currentCode,
                             BinaryOperatorStatement partialRef,
                             StructDeclaration inputStruct,
                             BinaryOperatorStatement inputRef) {
    auto var_decl_input = inputStruct.getVariableDeclaration(this->_onField->name);
    auto sum = partialRef + inputRef.accessRef(VarRefStatement(var_decl_input));
    auto updatedPartial = partialRef.assign(sum);
    currentCode->addStatement(std::make_shared<BinaryOperatorStatement>(updatedPartial));
}

}// namespace NES