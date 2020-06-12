#ifndef NES_INCLUDE_DATATYPES_ARRAY_HPP_
#define NES_INCLUDE_DATATYPES_ARRAY_HPP_

#include <DataTypes/DataType.hpp>
namespace NES {

/**
 * @brief Arrays con be constructed of any built-in type.
 * Arrays always have a fixed sized and can not be extended.
 */
class Array : public DataType {

  public:
    /**
     * @brief Constructs a new Array.
     * @param length length of the array
     * @param component component type
     */
    Array(uint64_t length, DataTypePtr component);

    /**
    * @brief Checks if this data type is an Array.
    */
    bool isArray() override;

    /**
     * @brief Gets the component type of this array.
     * @return component
     */
    DataTypePtr getComponent();

    /**
     * @brief Gets the length of this array
     * @return length
     */
    [[nodiscard]] uint64_t getLength() const;

    /**
    * @brief Checks if two data types are equal.
    * @param otherDataType
    * @return
    */
    bool isEquals(DataTypePtr otherDataType) override;

    /**
     * @brief Calculates the joined data type between this data type and the other.
     * If they have no possible joined data type, the coined type is Undefined.
     * @param other data type
     * @return DataTypePtr joined data type
     */
    DataTypePtr join(DataTypePtr otherDataType) override;

  private:
    const uint64_t length;
    const DataTypePtr component;
};

}// namespace NES

#endif//NES_INCLUDE_DATATYPES_ARRAY_HPP_
