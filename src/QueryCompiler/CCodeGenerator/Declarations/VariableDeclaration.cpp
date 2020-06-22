#include <QueryCompiler/CCodeGenerator/Declarations/VariableDeclaration.hpp>
#include <QueryCompiler/CompilerTypesFactory.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableDataType.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableValueType.hpp>
#include <QueryCompiler/GeneratedCode.hpp>
#include <Util/Logger.hpp>
namespace NES {

const GeneratableDataTypePtr VariableDeclaration::getType() const { return type_; }
const std::string VariableDeclaration::getIdentifierName() const { return identifier_; }

const Code VariableDeclaration::getTypeDefinitionCode() const {
    CodeExpressionPtr code = type_->getTypeDefinitionCode();
    if (code)
        return code->code_;
    else
        return Code();
}

const Code VariableDeclaration::getCode() const {
    std::stringstream str;
    str << type_->getDeclarationCode(identifier_)->code_;
    if (init_value_) {
        auto valueType = CompilerTypesFactory().createValueType(init_value_);
        str << " = " << valueType->getCodeExpression()->code_;
    }
    return str.str();
}

const CodeExpressionPtr VariableDeclaration::getIdentifier() const {
    return CodeExpressionPtr(new CodeExpression(identifier_));
}

const GeneratableDataTypePtr VariableDeclaration::getDataType() const { return type_; }

const DeclarationPtr VariableDeclaration::copy() const { return std::make_shared<VariableDeclaration>(*this); }

VariableDeclaration::~VariableDeclaration() {}

VariableDeclaration::VariableDeclaration(GeneratableDataTypePtr type, const std::string& identifier, ValueTypePtr value)
    : type_(type), identifier_(identifier), init_value_(value) {
}

VariableDeclaration::VariableDeclaration(const VariableDeclaration& var_decl)
    : type_(var_decl.type_), identifier_(var_decl.identifier_), init_value_(var_decl.init_value_) {
}

VariableDeclaration VariableDeclaration::create(GeneratableDataTypePtr type, const std::string& identifier, ValueTypePtr value) {
    if (!type)
        NES_ERROR("DataTypePtr type is nullptr!");
    return VariableDeclaration(type, identifier, value);
}
VariableDeclaration VariableDeclaration::create(DataTypePtr type, const std::string& identifier, ValueTypePtr value) {
    auto typeFactory = CompilerTypesFactory();
    return VariableDeclaration(typeFactory.createDataType(type), identifier, value);
}

}// namespace NES