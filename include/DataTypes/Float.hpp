#ifndef NES_INCLUDE_DATATYPES_FLOAT_HPP_
#define NES_INCLUDE_DATATYPES_FLOAT_HPP_

#include <DataTypes/Numeric.hpp>

namespace NES {

/**
 * @brief Float precision are inexact, variable-precision numeric types.
 * Inexact means that some values cannot be converted exactly to the internal format and are stored as approximations,
 * so that storing and retrieving a value might show slight discrepancies.
 */
class Float : public Numeric {

  public:
    /**
     * @brief Constructs a new Float type.
     * @param bits the number of bits in which this type is represented.
     * @param lowerBound the lower bound, which is contained in that float.
     * @param upperBound the upper bound, which is contained in that float.
     */
    Float(int8_t bits, double lowerBound, double upperBound);

    /**
    * @brief Checks if this data type is Float.
    */
    bool isFloat() override;

    /**
     * @brief Returns the lower bound of this integer.
     * @return lowerBound
     */
    [[nodiscard]] double getLowerBound() const;

    /**
     * @brief Returns the upper bound of this integer.
     * @return lowerBound
     */
    [[nodiscard]] double getUpperBound() const;

    /**
    * @brief Checks if two data types are equal.
    * @param otherDataType
    * @return
    */
    bool isEquals(DataTypePtr otherDataType) override;

    /**
    * @brief Calculates the joined data type between this data type and the other.
    * If they have no possible joined data type, the coined type is Undefined.
    * Floats, we can join with all numeric data types.
    * @param other data type
    * @return DataTypePtr joined data type
    */
    DataTypePtr join(DataTypePtr otherDataType) override;

    bool isNumeric() override;
    std::string toString() override;

  private:
    const double lowerBound;
    const double upperBound;
};

}// namespace NES

#endif//NES_INCLUDE_DATATYPES_FLOAT_HPP_
