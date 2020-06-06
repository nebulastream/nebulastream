
#include <QueryCompiler/CCodeGenerator/Declarations/StructDeclaration.hpp>
#include <QueryCompiler/CCodeGenerator/Declarations/VariableDeclaration.hpp>
#include <Util/Logger.hpp>

namespace NES{

StructDeclaration StructDeclaration::create(const std::string& type_name, const std::string& variable_name) {
    return StructDeclaration(type_name, variable_name);
}

const DataTypePtr StructDeclaration::getType() const { return createUserDefinedType(*this); }

const std::string StructDeclaration::getIdentifierName() const { return variable_name_; }

const Code StructDeclaration::getTypeDefinitionCode() const {
    std::stringstream expr;
    expr << "struct " << type_name_ << "{" << std::endl;
    for (auto& decl : decls_) {
        expr << decl->getCode() << ";" << std::endl;
    }
    expr << "}";
    return expr.str();
}

const Code StructDeclaration::getCode() const {
    std::stringstream expr;
    expr << "struct ";
    if (packed_struct_)
        expr << "__attribute__((packed)) ";
    expr << type_name_ << "{" << std::endl;
    for (auto& decl : decls_) {
        expr << decl->getCode() << ";" << std::endl;
    }
    expr << "}";
    expr << variable_name_;
    return expr.str();
}

uint32_t StructDeclaration::getTypeSizeInBytes() const {
    NES_ERROR("Called unimplemented function!");
    return 0;
}

const std::string StructDeclaration::getTypeName() const { return type_name_; }

const DeclarationPtr StructDeclaration::copy() const { return std::make_shared<StructDeclaration>(*this); }

DeclarationPtr StructDeclaration::getField(const std::string& field_name) const {
    for (auto& decl : decls_) {
        if (decl->getIdentifierName() == field_name) {
            return decl;
        }
    }
    return DeclarationPtr();
}

const bool StructDeclaration::containsField(const std::string& field_name, const DataTypePtr dataType) const {
    for (auto& decl : decls_) {
        if (decl->getIdentifierName() == field_name && decl->getType()->isEqual(dataType)) {
            return true;
        }
    }
    return false;
}

StructDeclaration& StructDeclaration::addField(const Declaration& decl) {
    DeclarationPtr decl_p = decl.copy();
    if (decl_p)
        decls_.push_back(decl_p);
    return *this;
}

StructDeclaration& StructDeclaration::makeStructCompact() {
    packed_struct_ = true;
    return *this;
}

StructDeclaration::StructDeclaration(const std::string& type_name, const std::string& variable_name)
    : type_name_(type_name), variable_name_(variable_name), decls_(), packed_struct_(false) {
}

VariableDeclaration StructDeclaration::getVariableDeclaration(const std::string& field_name) const {
    DeclarationPtr decl = getField(field_name);
    if (!decl)
        NES_ERROR("Error during Code Generation: Field '" << field_name << "' does not exist in struct '"
                                                          << getTypeName() << "'");
    return VariableDeclaration::create(decl->getType(), decl->getIdentifierName());
}


StructDeclaration::~StructDeclaration() {}


}