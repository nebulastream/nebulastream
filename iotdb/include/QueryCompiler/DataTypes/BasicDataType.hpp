#ifndef INCLUDE_BASICDATATYPE_HPP_
#define INCLUDE_BASICDATATYPE_HPP_

#pragma once

#include <API/Types/DataTypes.hpp>
namespace NES {
class BasicDataType : public DataType {
 public:
  BasicDataType() = default;
  BasicDataType(const BasicType &_type);
  BasicDataType(const BasicType &_type, uint32_t _size);

  ValueTypePtr getDefaultInitValue() const override;

  ValueTypePtr getNullValue() const override;

  const bool isEqual(DataTypePtr ptr) const override;

  const bool isEqual(std::shared_ptr <BasicDataType> btr) const;

  const bool isArrayDataType() const override;

  const bool isCharDataType() const override;



  BasicType getType();

  uint32_t getSizeBytes() const override;
    const bool isUndefined() const override;
    uint32_t getFixSizeBytes() const;
  const std::string toString() const override;
  const std::string convertRawToString(void *data) const override;
  const CodeExpressionPtr getDeclCode(const std::string &identifier) const override;
  const CodeExpressionPtr getCode() const override;
  const CodeExpressionPtr getTypeDefinitionCode() const override;
  const DataTypePtr copy() const override;
  bool operator==(const DataType &_rhs) const override;

 private:
  BasicType type;
  uint32_t dataSize;

  friend class boost::serialization::access;

  template<class Archive>
  void serialize(Archive &ar, unsigned) {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(DataType)
        & BOOST_SERIALIZATION_NVP(type)
        & BOOST_SERIALIZATION_NVP(dataSize);
  }
};
}
#endif //INCLUDE_BASICDATATYPE_HPP_
