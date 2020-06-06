#ifndef NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_DECLARATIONS_STRUCTDECLARATION_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_DECLARATIONS_STRUCTDECLARATION_HPP_
#include <QueryCompiler/CCodeGenerator/Declarations/Declaration.hpp>
namespace NES {

class VariableDeclaration;

class StructDeclaration : public Declaration {
  public:
    static StructDeclaration create(const std::string& type_name, const std::string& variable_name);

    virtual const DataTypePtr getType() const override;

    virtual const std::string getIdentifierName() const override;

    const Code getTypeDefinitionCode() const override;

    const Code getCode() const override;

    uint32_t getTypeSizeInBytes() const;

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

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_DECLARATIONS_STRUCTDECLARATION_HPP_
