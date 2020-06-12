#ifndef NES_INCLUDE_DATATYPES_NUMERIC_HPP_
#define NES_INCLUDE_DATATYPES_NUMERIC_HPP_
#include <DataTypes/DataType.hpp>
namespace NES {

class Numeric : public DataType {
  public:
    explicit Numeric(int8_t bits);
    bool isNumeric() override;
    [[nodiscard]] int8_t getBits() const;

  protected:
    const int8_t bits;

};

}// namespace NES

#endif//NES_INCLUDE_DATATYPES_NUMERIC_HPP_
