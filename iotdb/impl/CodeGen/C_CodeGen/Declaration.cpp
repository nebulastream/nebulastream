
#include <memory>
#include <string>

#include <Core/DataTypes.hpp>

#include <CodeGen/C_CodeGen/Declaration.hpp>
#include <CodeGen/C_CodeGen/Statement.hpp>
#include <CodeGen/CodeExpression.hpp>

#include <Util/ErrorHandling.hpp>

namespace iotdb {

Declaration::~Declaration() {}

StructDeclaration StructDeclaration::create(const std::string& type_name, const std::string& variable_name)
{
    return StructDeclaration(type_name, variable_name);
}

const DataTypePtr StructDeclaration::getType() const { return createUserDefinedType(*this); }

const std::string StructDeclaration::getIdentifierName() const { return variable_name_; }

const Code StructDeclaration::getTypeDefinitionCode() const
{
    std::stringstream expr;
    expr << "struct " << type_name_ << "{" << std::endl;
    for (auto& decl : decls_) {
        expr << decl->getCode() << ";" << std::endl;
    }
    expr << "}";
    return expr.str();
}

const Code StructDeclaration::getCode() const
{
    std::stringstream expr;
    expr << "struct " << type_name_ << "{" << std::endl;
    for (auto& decl : decls_) {
        expr << decl->getCode() << ";" << std::endl;
    }
    expr << "}";
    expr << variable_name_;
    return expr.str();
}

const uint32_t StructDeclaration::getTypeSizeInBytes() const
{
    IOTDB_FATAL_ERROR("Called unimplemented function!");
    return 0;
}

const std::string StructDeclaration::getTypeName() const { return type_name_; }

const DeclarationPtr StructDeclaration::copy() const { return std::make_shared<StructDeclaration>(*this); }

DeclarationPtr StructDeclaration::getField(const std::string& field_name) const
{
    for (auto& decl : decls_) {
        if (decl->getIdentifierName() == field_name) {
            return decl;
        }
    }
    return DeclarationPtr();
}

StructDeclaration& StructDeclaration::addField(const Declaration& decl)
{
    DeclarationPtr decl_p = decl.copy();
    if (decl_p)
        decls_.push_back(decl_p);
    return *this;
}

StructDeclaration::StructDeclaration(const std::string& type_name, const std::string& variable_name)
    : type_name_(type_name), variable_name_(variable_name), decls_()
{
}

StructDeclaration::~StructDeclaration() {}

// VariableDeclaration VariableDeclaration::create(DataTypePtr type, const std::string &identifier, ValueTypePtr value =
// nullptr);

const DataTypePtr VariableDeclaration::getType() const { return type_; }
const std::string VariableDeclaration::getIdentifierName() const { return identifier_; }

const Code VariableDeclaration::getTypeDefinitionCode() const
{
    CodeExpressionPtr code = type_->getTypeDefinitionCode();
    if (code)
        return code->code_;
    else
        return Code();
}

const Code VariableDeclaration::getCode() const
{
    std::stringstream str;
    str << type_->getCode()->code_ << " " << identifier_;
    if (init_value_) {
        str << " = " << init_value_->getCodeExpression()->code_;
    }
    return str.str();
}

const CodeExpressionPtr VariableDeclaration::getIdentifier() const
{
    return CodeExpressionPtr(new CodeExpression(identifier_));
}

const DataTypePtr VariableDeclaration::getDataType() const { return type_; }

const DeclarationPtr VariableDeclaration::copy() const { return std::make_shared<VariableDeclaration>(*this); }

VariableDeclaration StructDeclaration::getVariableDeclaration(const std::string& field_name) const
{
    DeclarationPtr decl = getField(field_name);
    if (!decl)
        IOTDB_FATAL_ERROR("Error during Code Generation: Field '" << field_name << "' does not exist in struct '"
                                                                  << getTypeName() << "'");
    return VariableDeclaration::create(decl->getType(), decl->getIdentifierName());
}

VariableDeclaration::~VariableDeclaration() {}

VariableDeclaration::VariableDeclaration(DataTypePtr type, const std::string& identifier, ValueTypePtr value)
    : type_(type), identifier_(identifier), init_value_(value)
{
}

VariableDeclaration::VariableDeclaration(const VariableDeclaration& var_decl)
    : type_(var_decl.type_), identifier_(var_decl.identifier_), init_value_(var_decl.init_value_)
{
}

VariableDeclaration VariableDeclaration::create(DataTypePtr type, const std::string& identifier, ValueTypePtr value)
{
    if (!type)
        IOTDB_FATAL_ERROR("DataTypePtr type is nullptr!");
    return VariableDeclaration(type, identifier, value);
}

FunctionDeclaration::FunctionDeclaration(Code code) : function_code(code) {}

const DataTypePtr FunctionDeclaration::getType() const { return DataTypePtr(); }
const std::string FunctionDeclaration::getIdentifierName() const { return ""; }

const Code FunctionDeclaration::getTypeDefinitionCode() const { return Code(); }

const Code FunctionDeclaration::getCode() const { return function_code; }
const DeclarationPtr FunctionDeclaration::copy() const { return std::make_shared<FunctionDeclaration>(*this); }

} // namespace iotdb
