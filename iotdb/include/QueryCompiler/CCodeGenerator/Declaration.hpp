
#pragma once

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "API/Types/DataTypes.hpp"
#include <QueryCompiler/CodeExpression.hpp>

namespace NES {

typedef std::string Code;

class Declaration;
typedef std::shared_ptr<Declaration> DeclarationPtr;

class Declaration {
  public:
    virtual const DataTypePtr getType() const = 0;
    virtual const std::string getIdentifierName() const = 0;
    virtual const Code getTypeDefinitionCode() const = 0;
    virtual const Code getCode() const = 0;
    virtual const DeclarationPtr copy() const = 0;
    virtual ~Declaration();
};

class StructDeclaration;
const DataTypePtr createUserDefinedType(const StructDeclaration& decl);

class VariableDeclaration;

class StructDeclaration : public Declaration {
  public:
    static StructDeclaration create(const std::string& type_name, const std::string& variable_name);

    virtual const DataTypePtr getType() const override;

    virtual const std::string getIdentifierName() const override;

    const Code getTypeDefinitionCode() const override;

    const Code getCode() const override;

    const uint32_t getTypeSizeInBytes() const;

    const std::string getTypeName() const;

    const DeclarationPtr copy() const override;

    DeclarationPtr getField(const std::string& field_name) const;

    const bool containsField(const std::string& field_name, const DataTypePtr dataType) const;

    VariableDeclaration getVariableDeclaration(const std::string& field_name) const;

    StructDeclaration& addField(const Declaration& decl);
    StructDeclaration& makeStructCompact();

    ~StructDeclaration();

  private:
    StructDeclaration(const std::string& type_name, const std::string& variable_name);
    std::string type_name_;
    std::string variable_name_;
    std::vector<DeclarationPtr> decls_;
    bool packed_struct_;
};

class VariableDeclaration;
typedef std::shared_ptr<VariableDeclaration> VariableDeclarationPtr;

class VariableDeclaration : public Declaration {
  public:
    VariableDeclaration(const VariableDeclaration& var_decl);
    static VariableDeclaration create(DataTypePtr type, const std::string& identifier, ValueTypePtr value = nullptr);

    virtual const DataTypePtr getType() const override;
    virtual const std::string getIdentifierName() const override;

    const Code getTypeDefinitionCode() const override;

    const Code getCode() const override;

    const CodeExpressionPtr getIdentifier() const;

    const DataTypePtr getDataType() const;

    const DeclarationPtr copy() const override;

    virtual ~VariableDeclaration() override;

  private:
    VariableDeclaration(DataTypePtr type, const std::string& identifier, ValueTypePtr value = nullptr);
    DataTypePtr type_;
    std::string identifier_;
    ValueTypePtr init_value_;
};

class FunctionDeclaration : public Declaration {
  private:
    Code function_code;

  public:
    FunctionDeclaration(Code code);

    virtual const DataTypePtr getType() const override;
    virtual const std::string getIdentifierName() const override;

    const Code getTypeDefinitionCode() const override;

    const Code getCode() const override;
    const DeclarationPtr copy() const override;
};

}// namespace NES
