#ifndef NES_INCLUDE_DATATYPES_NUMERIC_HPP_
#define NES_INCLUDE_DATATYPES_NUMERIC_HPP_
#include <DataTypes/DataType.hpp>
namespace NES {

/**
 * @brief The Numeric type represents integers and floats.
 */
class Numeric : public DataType {
  public:
    explicit Numeric(int8_t bits);

    /**
    * @brief Checks if this data type is Numeric.
    * @return bool
    */
    bool isNumeric() override;

    /**
     * @brief Gets the bit size of this type.
     * @return int8_t
     */
    [[nodiscard]] int8_t getBits() const;

  protected:
    const int8_t bits;

};

}// namespace NES

#endif//NES_INCLUDE_DATATYPES_NUMERIC_HPP_
