
#include <QueryCompiler/CCodeGenerator/Declarations/FunctionDeclaration.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableDataType.hpp>

namespace NES {

FunctionDeclaration::FunctionDeclaration(Code code) : function_code(code) {}

const GeneratableDataTypePtr FunctionDeclaration::getType() const { return GeneratableDataTypePtr(); }
const std::string FunctionDeclaration::getIdentifierName() const { return ""; }

const Code FunctionDeclaration::getTypeDefinitionCode() const { return Code(); }

const Code FunctionDeclaration::getCode() const { return function_code; }
const DeclarationPtr FunctionDeclaration::copy() const { return std::make_shared<FunctionDeclaration>(*this); }

}// namespace NES