#ifndef INCLUDE_REFEREMCEDATATYPE_HPP_
#define INCLUDE_REFEREMCEDATATYPE_HPP_

#pragma once

#include <API/Types/DataTypes.hpp>

namespace NES {
class ReferenceDataType : public DataType {
  public:
    ReferenceDataType() = default;
    ~ReferenceDataType() override = default;
    ReferenceDataType(const DataTypePtr& type);
    ValueTypePtr getDefaultInitValue() const override;

    ValueTypePtr getNullValue() const override;
    uint32_t getSizeBytes() const override;
    const std::string toString() const override;
    const std::string convertRawToString(void* data) const override;
    bool isArrayDataType() const;

    const CodeExpressionPtr getDeclCode(const std::string& identifier) const override;

    const CodeExpressionPtr getCode() const override;

    bool isCharDataType() const override;

    bool isEqual(DataTypePtr ptr) const override;

    const bool isEqual(std::shared_ptr<ReferenceDataType> btr) const;

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
#endif//INCLUDE_REFEREMCEDATATYPE_HPP_
