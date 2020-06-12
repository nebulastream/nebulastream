#ifndef NES_INCLUDE_DATATYPES_DATATYPEFACTORY_HPP_
#define NES_INCLUDE_DATATYPES_DATATYPEFACTORY_HPP_
#include <memory>
namespace NES{

class DataType;
typedef std::shared_ptr<DataType> DataTypePtr;

/**
 * @brief The data type factory offers multiple methods to construct data types
 */
class DataTypeFactory{

  public:
    static DataTypePtr createUndefined();
    static DataTypePtr createBoolean();
    static DataTypePtr createFloat(int8_t bits, double lowerBound, double upperBound);
    static DataTypePtr createFloat(double lowerBound, double upperBound);
    static DataTypePtr createFloat();
    static DataTypePtr createDouble();
    static DataTypePtr createInteger(int8_t bits, int64_t lowerBound, int64_t upperBound);
    static DataTypePtr createInteger(int64_t lowerBound, int64_t upperBound);
    static DataTypePtr createInt16();
    static DataTypePtr createUInt16();
    static DataTypePtr createInt32();
    static DataTypePtr createUInt32();
    static DataTypePtr createInt64();
    static DataTypePtr createUInt64();
    static DataTypePtr createArray(uint64_t length, DataTypePtr component);
    static DataTypePtr createChar(uint64_t length);

};

}

#endif//NES_INCLUDE_DATATYPES_DATATYPEFACTORY_HPP_
