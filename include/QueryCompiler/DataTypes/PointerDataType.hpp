#ifndef INCLUDE_POINTERDATATYPE_HPP_
#define INCLUDE_POINTERDATATYPE_HPP_

#pragma once

#include <QueryCompiler/DataTypes/GeneratableDataType.hpp>

namespace NES {
class PointerDataType : public GeneratableDataType {
  public:
    PointerDataType(GeneratableDataTypePtr baseType);
    ~PointerDataType() = default;

    const CodeExpressionPtr getDeclCode(const std::string& identifier) const ;

    const CodeExpressionPtr getCode() const ;

    const CodeExpressionPtr getTypeDefinitionCode() const ;
    CodeExpressionPtr generateCode() override;

  private:
    GeneratableDataTypePtr baseType;
};
}// namespace NES
#endif//INCLUDE_POINTERDATATYPE_HPP_
