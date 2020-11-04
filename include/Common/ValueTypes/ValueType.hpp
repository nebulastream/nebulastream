#ifndef NES_INCLUDE_DATATYPES_VALUETYPE_HPP_
#define NES_INCLUDE_DATATYPES_VALUETYPE_HPP_
#include <memory>
#include <string>

namespace NES {

class DataType;
typedef std::shared_ptr<DataType> DataTypePtr;

class ValueType;
typedef std::shared_ptr<ValueType> ValueTypePtr;

/**
 * @brief Represents a typed value in the NES type system.
 * This is is needed to represent constants in the type system.
 */
class ValueType {
  public:
    ValueType(DataTypePtr type);

    /**
     * @brief Returns the nes data type for this value.
     * @return
     */
    DataTypePtr getType();

    /**
     * @brief Indicates if this value is a array.
     */
    virtual bool isArrayValue();

    /**
     * @brief Indicates if this value is a basic value.
     */
    virtual bool isBasicValue();

    /**
     * @brief Indicates if this value is a char value.
     */
    virtual bool isCharValue();

    /**
     * @brief Checks if two values are equal
     * @param valueType
     * @return bool
     */
    virtual bool isEquals(ValueTypePtr valueType) = 0;

    /**
     * @brief Returns a string representation of this value
     * @return string
     */
    virtual std::string toString() = 0;

  private:
    DataTypePtr dataType;
};

}// namespace NES

#endif//NES_INCLUDE_DATATYPES_VALUETYPE_HPP_
