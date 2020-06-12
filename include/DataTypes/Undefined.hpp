#ifndef NES_INCLUDE_DATATYPES_UNDEFINED_HPP_
#define NES_INCLUDE_DATATYPES_UNDEFINED_HPP_

#include <DataTypes/DataType.hpp>
namespace NES {

class Undefined : public DataType {
  public:
    bool isUndefined() override;
    bool isEquals(DataTypePtr otherDataType) override;
    DataTypePtr join(DataTypePtr otherDataType) override;
};

}// namespace NES

#endif//NES_INCLUDE_DATATYPES_UNDEFINED_HPP_
