
#include <QueryCompiler/CCodeGenerator/Runtime/SharedPointerGen.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/FunctionCallStatement.hpp>
#include <QueryCompiler/CodeExpression.hpp>
#include <QueryCompiler/CompilerTypesFactory.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableDataType.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableDataType.hpp>
namespace NES {

GeneratableDataTypePtr SharedPointerGen::createSharedPtrType(GeneratableDataTypePtr type) {
    return CompilerTypesFactory().createAnonymusDataType("std::shared_ptr<" + type->getCode()->code_ + ">");
}

StatementPtr SharedPointerGen::makeShared(GeneratableDataTypePtr type) {
    return FunctionCallStatement::create("std::make_shared<"+type->getTypeDefinitionCode()->code_+">");
}

}// namespace NES