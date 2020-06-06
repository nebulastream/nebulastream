#ifndef NES_INCLUDE_QUERYCOMPILER_DATATYPES_ANONYMOUSUSERDEFINEDDATATYPE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_DATATYPES_ANONYMOUSUSERDEFINEDDATATYPE_HPP_

#include <memory>
#include <string>

#include <API/Types/DataTypes.hpp>
#include <QueryCompiler/CCodeGenerator/Declarations/Declaration.hpp>
#include <QueryCompiler/CodeExpression.hpp>
#include <QueryCompiler/DataTypes/ValueType.hpp>
#include <utility>

namespace NES {

class AnonymousUserDefinedDataType : public DataType {
  public:
    AnonymousUserDefinedDataType(const std::string name);

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
    const bool isEqual(std::shared_ptr<AnonymousUserDefinedDataType> btr);

    bool operator==(const DataType& _rhs) const override;

    ~AnonymousUserDefinedDataType();

  private:
    const std::string name;
};

const DataTypePtr createAnonymUserDefinedType(const std::string name);

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_DATATYPES_ANONYMOUSUSERDEFINEDDATATYPE_HPP_
