#ifndef NES_INCLUDE_DATATYPES_ARRAY_HPP_
#define NES_INCLUDE_DATATYPES_ARRAY_HPP_


#include <DataTypes/DataType.hpp>
namespace NES {

class Array : public DataType {

  public:
    Array(uint64_t length, DataTypePtr component);

    bool isArray() override;

    bool isEquals(DataTypePtr otherDataType) override;

    DataTypePtr getComponent();

    uint64_t getLength() const;

    DataTypePtr join(DataTypePtr otherDataType) override;

  private:
    const uint64_t length;
    const DataTypePtr component;
};

}// namespace NES

#endif//NES_INCLUDE_DATATYPES_ARRAY_HPP_
