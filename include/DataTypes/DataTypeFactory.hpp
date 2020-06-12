#ifndef NES_INCLUDE_DATATYPES_DATATYPEFACTORY_HPP_
#define NES_INCLUDE_DATATYPES_DATATYPEFACTORY_HPP_
#include <memory>
namespace NES{

class DataType;
typedef std::shared_ptr<DataType> DataTypePtr;

class DataTypeFactory{

  public:
    static DataTypePtr createUndefined();
    static DataTypePtr createBoolean();
    static DataTypePtr createFloat(int8_t bits, double lowerBound, double upperBound);
    static DataTypePtr createInteger(int8_t bits, int64_t lowerBound, int64_t upperBound);
    static DataTypePtr createArray(uint64_t length, DataTypePtr component);
    static DataTypePtr createChar(uint64_t length);
};

}

#endif//NES_INCLUDE_DATATYPES_DATATYPEFACTORY_HPP_
