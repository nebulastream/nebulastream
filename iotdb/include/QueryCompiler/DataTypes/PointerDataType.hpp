#ifndef INCLUDE_POINTERDATATYPE_HPP_
#define INCLUDE_POINTERDATATYPE_HPP_

#pragma once

#include <API/Types/DataTypes.hpp>

namespace NES {
class PointerDataType : public DataType {
  public:
    PointerDataType() = default;
    ~PointerDataType() override = default;
    PointerDataType(const DataTypePtr& type);
    ValueTypePtr getDefaultInitValue() const override;

    ValueTypePtr getNullValue() const override;
    uint32_t getSizeBytes() const override;
    const std::string toString() const override;
    const std::string convertRawToString(void* data) const override;
    const bool isArrayDataType() const;

    const CodeExpressionPtr getDeclCode(const std::string& identifier) const override;

    const CodeExpressionPtr getCode() const override;

    const bool isCharDataType() const override;

    const bool isEqual(DataTypePtr ptr) const override;

    const bool isEqual(std::shared_ptr<PointerDataType> btr) const;

    const CodeExpressionPtr getTypeDefinitionCode() const override;

    const DataTypePtr copy() const override;

    bool operator==(const DataType& _rhs) const override;

  private:
    DataTypePtr base_type_;

    friend class boost::serialization::access;

    template<class Archive>
    void serialize(Archive& ar, unsigned) {
        ar& BOOST_SERIALIZATION_BASE_OBJECT_NVP(DataType)
            & BOOST_SERIALIZATION_NVP(base_type_);
    }
};
}// namespace NES
#endif//INCLUDE_POINTERDATATYPE_HPP_
