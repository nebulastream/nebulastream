#ifndef NES_INCLUDE_DATATYPES_INTEGER_HPP_
#define NES_INCLUDE_DATATYPES_INTEGER_HPP_

#include <DataTypes/Numeric.hpp>

namespace NES {

class Integer : public Numeric {

  public:
    Integer(int8_t bits, int64_t lowerBound, int64_t upperBound);
    bool isInteger() override;
    bool isEquals(DataTypePtr otherDataType) override;
    DataTypePtr join(DataTypePtr otherDataType) override;
    [[nodiscard]] int64_t getLowerBound() const;
    [[nodiscard]] int64_t getUpperBound() const;

  private:
    const int64_t lowerBound;
    const int64_t upperBound;
};

}// namespace NES

#endif//NES_INCLUDE_DATATYPES_INTEGER_HPP_
