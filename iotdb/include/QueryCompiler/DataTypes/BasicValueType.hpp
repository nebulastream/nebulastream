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

    const DataTypePtr getType() const override;

    const CodeExpressionPtr getCodeExpression() const override;

    const ValueTypePtr copy() const override;
    const std::string toString() const override;
    const bool isArrayValueType() const override;

    bool operator==(const ValueType& _rhs) const;

  private:
    BasicType type_;
    std::string value_;

    friend class boost::serialization::access;

    template<class Archive>
    void serialize(Archive& ar, unsigned) {
        ar& BOOST_SERIALIZATION_BASE_OBJECT_NVP(ValueType)
            & BOOST_SERIALIZATION_NVP(type_)
            & BOOST_SERIALIZATION_NVP(value_);
    }
};
}// namespace NES

#endif//INCLUDE_BASICVALUETYPE_HPP_