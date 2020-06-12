#ifndef NES_INCLUDE_DATATYPES_INTEGER_HPP_
#define NES_INCLUDE_DATATYPES_INTEGER_HPP_

#include <DataTypes/Numeric.hpp>

namespace NES {

/**
 * @brief The Integer type represents whole numbers, that is,
 * numbers without fractional components, of various ranges.
 * The internal Integer type is parameterised by its bit size, and its lower and upper bound
 * Integer(bitSize, lowerBound, upperBound)
 */
class Integer : public Numeric {

  public:

    /**
     * @brief Constructs a new Integer type.
     * @param bits the number of bits in which this type is represented.
     * @param lowerBound the lower bound, which is contained in that integer.
     * @param upperBound the upper bound, which is contained in that integer.
     */
    Integer(int8_t bits, int64_t lowerBound, int64_t upperBound);

    /**
    * @brief Checks if this data type is Integer.
    */
    bool isInteger() override;

    /**
    * @brief Checks if two data types are equal.
    * @param otherDataType
    * @return
    */
    bool isEquals(DataTypePtr otherDataType) override;

    /**
    * @brief Calculates the joined data type between this data type and the other.
    * If they have no possible joined data type, the coined type is Undefined.
    * For integers, we can join with all numeric data types.
    * @param other data type
    * @return DataTypePtr joined data type
    */
    DataTypePtr join(DataTypePtr otherDataType) override;

    /**
     * @brief Returns the lower bound of this integer.
     * @return lowerBound
     */
    [[nodiscard]] int64_t getLowerBound() const;

    /**
     * @brief Returns the upper bound of this integer.
     * @return upperBound
     */
    [[nodiscard]] int64_t getUpperBound() const;

  private:
    const int64_t lowerBound;
    const int64_t upperBound;
};

}// namespace NES

#endif//NES_INCLUDE_DATATYPES_INTEGER_HPP_
