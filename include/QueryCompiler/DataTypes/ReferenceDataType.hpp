#ifndef INCLUDE_REFEREMCEDATATYPE_HPP_
#define INCLUDE_REFEREMCEDATATYPE_HPP_

#pragma once

#include <QueryCompiler/DataTypes/GeneratableDataType.hpp>

namespace NES {
class ReferenceDataType : public GeneratableDataType {
  public:

    ReferenceDataType(GeneratableDataTypePtr baseType);

    const CodeExpressionPtr getTypeDefinitionCode() const override;

    const CodeExpressionPtr getCode() const;

    CodeExpressionPtr generateCode() override;

    CodeExpressionPtr getDeclCode(std::string identifier) override;

  private:
    GeneratableDataTypePtr baseType;
};
}// namespace NES
#endif//INCLUDE_REFEREMCEDATATYPE_HPP_
