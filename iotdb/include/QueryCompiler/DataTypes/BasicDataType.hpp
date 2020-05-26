#ifndef INCLUDE_BASICDATATYPE_HPP_
#define INCLUDE_BASICDATATYPE_HPP_

#pragma once

#include <API/Types/DataTypes.hpp>
namespace NES {
class BasicDataType : public DataType {
  public:
    BasicDataType() = default;
    BasicDataType(const BasicType& type);
    BasicDataType(const BasicType& type, uint32_t size);

    ValueTypePtr getDefaultInitValue() const override;

    ValueTypePtr getNullValue() const override;

    bool isEqual(DataTypePtr ptr) const override;

    const bool isEqual(std::shared_ptr<BasicDataType> btr) const;

    bool isArrayDataType() const override;

    bool isCharDataType() const override;

    bool isNumerical() const override;

    BasicType getType();

    const DataTypePtr join(DataTypePtr other) const override;
    uint32_t getSizeBytes() const override;
    bool isUndefined() const override;
    uint32_t getFixSizeBytes() const;
    const std::string toString() const override;
    const std::string convertRawToString(void* data) const override;
    const CodeExpressionPtr getDeclCode(const std::string& identifier) const override;
    const CodeExpressionPtr getCode() const override;
    const CodeExpressionPtr getTypeDefinitionCode() const override;
    const DataTypePtr copy() const override;
    bool operator==(const DataType& _rhs) const override;

  private:
    BasicType type;
    uint32_t dataSize;

    friend class boost::serialization::access;

    template<class Archive>
    void serialize(Archive& ar, unsigned) {
        ar& BOOST_SERIALIZATION_BASE_OBJECT_NVP(DataType)
            & BOOST_SERIALIZATION_NVP(type)
            & BOOST_SERIALIZATION_NVP(dataSize);
    }
};
}// namespace NES
#endif//INCLUDE_BASICDATATYPE_HPP_
