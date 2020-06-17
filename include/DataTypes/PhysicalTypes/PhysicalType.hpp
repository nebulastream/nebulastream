#ifndef NES_INCLUDE_DATATYPES_PHYSICALTYPES_PHYSICALTYPES_HPP_
#define NES_INCLUDE_DATATYPES_PHYSICALTYPES_PHYSICALTYPES_HPP_
#include <memory>
namespace NES {

class DataType;
typedef std::shared_ptr<DataType> DataTypePtr;

class PhysicalType;
typedef std::shared_ptr<PhysicalType> PhysicalTypePtr;

class PhysicalType {
  public:
    PhysicalType(DataTypePtr type);
    virtual bool isBasicType();
    virtual bool isArrayType();
    virtual u_int8_t size() const = 0;
    virtual std::string convertRawToString(void* rawData) = 0;
    DataTypePtr getType();
    virtual std::string toString() = 0;

  private:
    const DataTypePtr type;
};

}// namespace NES

#endif//NES_INCLUDE_DATATYPES_PHYSICALTYPES_PHYSICALTYPES_HPP_
