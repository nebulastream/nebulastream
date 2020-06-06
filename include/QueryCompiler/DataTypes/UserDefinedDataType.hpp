

#ifndef NES_INCLUDE_QUERYCOMPILER_DATATYPES_USERDEFINEDDATATYPE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_DATATYPES_USERDEFINEDDATATYPE_HPP_

#include <memory>
#include <string>

#include <API/Types/DataTypes.hpp>
#include <QueryCompiler/CCodeGenerator/Declarations/StructDeclaration.hpp>
#include <QueryCompiler/CodeExpression.hpp>
#include <QueryCompiler/DataTypes/ValueType.hpp>
#include <utility>

namespace NES {

class UserDefinedDataType : public DataType {
  public:
    UserDefinedDataType(const StructDeclaration& decl);

    ValueTypePtr getDefaultInitValue() const;

    ValueTypePtr getNullValue() const;
    uint32_t getSizeBytes() const;
    const std::string toString() const;
    const std::string convertRawToString(void* data) const override;
    const CodeExpressionPtr getTypeDefinitionCode() const;
    const CodeExpressionPtr getCode() const;
    const CodeExpressionPtr getDeclCode(const std::string& identifier) const override;
    bool isArrayDataType() const override;
    bool isCharDataType() const override;
    const DataTypePtr copy() const override;
    bool isEqual(DataTypePtr ptr) const override;

    // \todo: isEqual for structType
    const bool isEqual(std::shared_ptr<UserDefinedDataType> btr);

    bool operator==(const DataType& _rhs) const override;

    ~UserDefinedDataType();

  private:
    StructDeclaration declaration;
};

const DataTypePtr createUserDefinedType(const StructDeclaration& decl);

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_DATATYPES_USERDEFINEDDATATYPE_HPP_
