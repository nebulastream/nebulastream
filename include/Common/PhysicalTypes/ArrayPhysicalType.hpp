#ifndef NES_INCLUDE_DATATYPES_PHYSICALTYPES_ARRAYPHYSICALTYPE_HPP_
#define NES_INCLUDE_DATATYPES_PHYSICALTYPES_ARRAYPHYSICALTYPE_HPP_

#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace NES {

/**
 * @brief The array physical type, which represent Array and FixedChar types in NES.
 */
class ArrayPhysicalType : public PhysicalType {

  public:

    /**
     * @brief Factory function to create a new Array Physical Type.
     * @param type the logical data type.
     * @param length the length of the array.
     * @param component the physical component type of this array.
     */
    ArrayPhysicalType(DataTypePtr type, uint64_t length, PhysicalTypePtr component);

    /**
     * @brief Factory function to create a new Array Physical Type.
     * @param type the logical data type.
     * @param length the length of the array.
     * @param component the physical component type of this array.
     * @return PhysicalTypePtr
     */
    static PhysicalTypePtr create(DataTypePtr type, uint64_t length, PhysicalTypePtr component);

    /**
    * @brief Indicates if this is a array data type.
    * @return true if type is array
    */
    bool isArrayType() override;

    /**
     * @brief Returns the number of bytes occupied by this data type.
     * @return u_int64_t
     */
    u_int64_t size() const override;

    /**
     * @brief Converts the binary representation of this value to a string.
     * @param rawData a pointer to the raw value
     * @return string
     */
    std::string convertRawToString(void* rawData) override;

    /**
     * @brief Returns the physical type of the array component.
     * @return PhysicalTypePtr
     */
    const PhysicalTypePtr getPhysicalComponentType() const;

    /**
     * @brief Returns the length of the array
     * @return uint64_t
     */
    const uint64_t getLength() const;

    /**
     * @brief Returns the string representation of this physical data type.
     * @return string
     */
    std::string toString() override;

  private:
    const PhysicalTypePtr physicalComponentType;
    const uint64_t length;
};

}// namespace NES

#endif//NES_INCLUDE_DATATYPES_PHYSICALTYPES_ARRAYPHYSICALTYPE_HPP_
