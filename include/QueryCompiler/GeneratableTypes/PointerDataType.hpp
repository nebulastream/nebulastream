#ifndef INCLUDE_POINTERDATATYPE_HPP_
#define INCLUDE_POINTERDATATYPE_HPP_

#pragma once

#include <QueryCompiler/GeneratableTypes/GeneratableDataType.hpp>

namespace NES {
class PointerDataType : public GeneratableDataType {
  public:
    PointerDataType(GeneratableDataTypePtr baseType);
    ~PointerDataType() = default;

    const CodeExpressionPtr getDeclCode(const std::string& identifier) const ;

    const CodeExpressionPtr getCode() const ;

    const CodeExpressionPtr getTypeDefinitionCode() const override ;
    CodeExpressionPtr generateCode() override;
    CodeExpressionPtr getDeclCode(std::string identifier) override;

  private:
    GeneratableDataTypePtr baseType;
};
}// namespace NES
#endif//INCLUDE_POINTERDATATYPE_HPP_
