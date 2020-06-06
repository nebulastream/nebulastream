
#include <QueryCompiler/CCodeGenerator/Declarations/FunctionDeclaration.hpp>

namespace NES {

FunctionDeclaration::FunctionDeclaration(Code code) : function_code(code) {}

const DataTypePtr FunctionDeclaration::getType() const { return DataTypePtr(); }
const std::string FunctionDeclaration::getIdentifierName() const { return ""; }

const Code FunctionDeclaration::getTypeDefinitionCode() const { return Code(); }

const Code FunctionDeclaration::getCode() const { return function_code; }
const DeclarationPtr FunctionDeclaration::copy() const { return std::make_shared<FunctionDeclaration>(*this); }

}// namespace NES