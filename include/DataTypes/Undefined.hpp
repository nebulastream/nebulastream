#ifndef NES_INCLUDE_DATATYPES_UNDEFINED_HPP_
#define NES_INCLUDE_DATATYPES_UNDEFINED_HPP_

#include <DataTypes/DataType.hpp>
namespace NES {

/**
 * @brief The Undefined type represents a type for without any meaning.
 */
class Undefined : public DataType {
  public:

    /**
    * @brief Checks if this data type is Undefined.
    */
    bool isUndefined() override;

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
};

}// namespace NES

#endif//NES_INCLUDE_DATATYPES_UNDEFINED_HPP_
