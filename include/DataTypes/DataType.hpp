#ifndef NES_INCLUDE_DATATYPES_DATATYPE_HPP_
#define NES_INCLUDE_DATATYPES_DATATYPE_HPP_

#include <memory>

namespace NES {

class DataType;
typedef std::shared_ptr<DataType> DataTypePtr;

/**
 * @brief Base data type, which is the parent class for all other data types.
 */
class DataType {
  public:
    /**
     * @brief Checks if this data type is Undefined.
     */
    virtual bool isUndefined();

    /**
   * @brief Checks if this data type is Boolean.
   */
    virtual bool isBoolean();

    /**
    * @brief Checks if this data type is Numeric.
    */
    virtual bool isNumeric();

    /**
    * @brief Checks if this data type is Integer.
    */
    virtual bool isInteger();

    /**
    * @brief Checks if this data type is Float.
    */
    virtual bool isFloat();

    /**
    * @brief Checks if this data type is Array.
    */
    virtual bool isArray();


    /**
    * @brief Checks if this data type is Char.
    */
    virtual bool isChar();

    /**
    * @brief Checks if this data type is Char.
    */
    virtual bool isFixedChar();

    template<class DataType>
    static std::shared_ptr<DataType> as(DataTypePtr ptr) {
        return std::dynamic_pointer_cast<DataType>(ptr);
    }

    /**
     * @brief Checks if two data types are equal.
     * @param otherDataType
     * @return
     */
    virtual bool isEquals(DataTypePtr otherDataType) = 0;

    /**
     * @brief Calculates the joined data type between this data type and the other.
     * If they have no possible joined data type, the coined type is Undefined.
     * @param other data type
     * @return DataTypePtr joined data type
     */
    virtual DataTypePtr join(DataTypePtr otherDataType) = 0;

    /**
     * @brief Returns a string representation of the data type.
     * @return string
     */
    virtual std::string toString() = 0;
};

}// namespace NES

#endif//NES_INCLUDE_DATATYPES_DATATYPE_HPP_
