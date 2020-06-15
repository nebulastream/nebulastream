
#pragma once

#include <memory>
#include <string>


namespace NES {

typedef std::string Code;

class Declaration;
typedef std::shared_ptr<Declaration> DeclarationPtr;

class DataType;
typedef std::shared_ptr<DataType> DataTypePtr;

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

}// namespace NES
