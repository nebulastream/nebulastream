#ifndef NES_INCLUDE_DATATYPES_CHAR_HPP_
#define NES_INCLUDE_DATATYPES_CHAR_HPP_

#include <DataTypes/DataType.hpp>
namespace NES {

/**
 * @brief The char type.
 */
class Char : public DataType {
  public:

    /**
    * @brief Checks if this data type is Boolean.
    */
    bool isChar() override;

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
    std::string toString() override;
};

}// namespace NES
#endif//NES_INCLUDE_DATATYPES_CHAR_HPP_
