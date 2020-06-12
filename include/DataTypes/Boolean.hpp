#ifndef NES_INCLUDE_DATATYPES_BOOLEAN_HPP_
#define NES_INCLUDE_DATATYPES_BOOLEAN_HPP_

#include <DataTypes/DataType.hpp>
namespace NES {

class Boolean : public DataType {
  public:
    bool isBoolean() override;
    bool isEquals(DataTypePtr otherDataType) override;
    DataTypePtr join(DataTypePtr otherDataType) override;
};

}// namespace NES
#endif//NES_INCLUDE_DATATYPES_BOOLEAN_HPP_
