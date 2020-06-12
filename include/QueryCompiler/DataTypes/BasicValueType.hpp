#ifndef INCLUDE_BASICVALUETYPE_HPP_
#define INCLUDE_BASICVALUETYPE_HPP_

#pragma once

#include <API/Types/DataTypes.hpp>
#include <QueryCompiler/DataTypes/ValueType.hpp>

namespace NES {

class BasicValueType : public ValueType {
  public:
    BasicValueType() = default;
    ~BasicValueType() override = default;

    BasicValueType(const BasicType& type, std::string value);
    BasicValueType(const DataTypePtr type, std::string value);

    const DataTypePtr getType() const override;

    const CodeExpressionPtr getCodeExpression() const override;

    const ValueTypePtr copy() const override;
    const std::string toString() const override;
    bool isArrayValueType() const override;

    bool operator==(const ValueType& _rhs) const;
    bool equals(const ValueTypePtr rhs) const override;
    std::string getValue();

  private:
    BasicType type;
    std::string value;
};
typedef std::shared_ptr<BasicValueType> BasicValueTypePtr;
}// namespace NES

#endif//INCLUDE_BASICVALUETYPE_HPP_