#ifndef NES_INCLUDE_DATATYPES_PHYSICALTYPES_PHYSICALTYPES_HPP_
#define NES_INCLUDE_DATATYPES_PHYSICALTYPES_PHYSICALTYPES_HPP_
#include <memory>

namespace NES {

class DataType;
typedef std::shared_ptr<DataType> DataTypePtr;

class PhysicalType;
typedef std::shared_ptr<PhysicalType> PhysicalTypePtr;

/**
 * @brief The physical data type represents the physical representation of a NES data type.
 */
class PhysicalType {
  public:
    PhysicalType(DataTypePtr type);

    /**
     * @brief Indicates if this is a basic data type.
     * @return true if type is basic type
     */
    virtual bool isBasicType();

    /**
    * @brief Indicates if this is a array data type.
    * @return true if type is array
    */
    virtual bool isArrayType();

    /**
     * @brief Returns the number of bytes occupied by this data type.
     * @return u_int64_t
     */
    virtual u_int64_t size() const = 0;

    /**
     * @brief Converts the binary representation of this value to a string.
     * @param rawData a pointer to the raw value
     * @return string
     */
    virtual std::string convertRawToString(void* rawData) = 0;

    /**
     * @brief Returns the underling nes data type, which is represented by this physical type.
     * @return DataTypePtr
     */
    DataTypePtr getType();

    /**
     * @brief Returns the string representation of this physical data type.
     * @return string
     */
    virtual std::string toString() = 0;

  protected:
    const DataTypePtr type;
};

}// namespace NES

#endif//NES_INCLUDE_DATATYPES_PHYSICALTYPES_PHYSICALTYPES_HPP_
