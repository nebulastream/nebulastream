#ifndef INCLUDE_ARRAYVALUETYPE_HPP_
#define INCLUDE_ARRAYVALUETYPE_HPP_

#pragma once

#include <QueryCompiler/DataTypes/ValueType.hpp>

namespace NES {

class ArrayDataType;
typedef std::shared_ptr<ArrayDataType> ArrayDataTypePtr;
/**
 * class ArrayValueType keeps a field of values of basic types
 */
class ArrayValueType : public ValueType {
  public:
    ArrayValueType() = default;
    ArrayValueType(const ArrayDataType& type, const std::vector<std::string>& value);
    ArrayValueType(const ArrayDataType& type, const std::string& value);

    const DataTypePtr getType() const override;

    const CodeExpressionPtr getCodeExpression() const override;

    const ValueTypePtr copy() const override;

    const std::string toString() const override;

    bool isArrayValueType() const override;

    virtual ~ArrayValueType() override;

    bool operator==(const ArrayValueType& rhs) const;

    bool operator==(const ValueType& rhs) const override;
    bool equals(ValueTypePtr rhs) const override;
    std::vector<std::string> getValues();

  private:
    ArrayDataTypePtr type;
    bool isString = false;
    std::vector<std::string> values;

    friend class boost::serialization::access;

    template<class Archive>
    void serialize(Archive& ar, unsigned) {
        ar& BOOST_SERIALIZATION_BASE_OBJECT_NVP(ValueType)
            & BOOST_SERIALIZATION_NVP(type)
            & BOOST_SERIALIZATION_NVP(isString)
            & BOOST_SERIALIZATION_NVP(values);
    }
};

}// namespace NES

#endif//INCLUDE_ARRAYVALUETYPE_HPP_
