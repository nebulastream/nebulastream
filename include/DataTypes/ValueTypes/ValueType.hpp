#ifndef NES_INCLUDE_DATATYPES_VALUETYPE_HPP_
#define NES_INCLUDE_DATATYPES_VALUETYPE_HPP_
#include <memory>
namespace NES{

class DataType;
typedef std::shared_ptr<DataType> DataTypePtr;

class ValueType{
  public:
    ValueType(DataTypePtr type);

    DataTypePtr getType();

    virtual bool isArrayValue();
    virtual bool isBasicValue();

    virtual std::string toString() = 0;

  private:
    DataTypePtr dataType;

};

}

#endif//NES_INCLUDE_DATATYPES_VALUETYPE_HPP_
