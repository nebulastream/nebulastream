#ifndef NES_INCLUDE_DATATYPES_FLOAT_HPP_
#define NES_INCLUDE_DATATYPES_FLOAT_HPP_

#include <DataTypes/Numeric.hpp>

namespace NES {

class Float : public Numeric {

  public:
    Float(int8_t bits, double lowerBound, double upperBound);
    bool isFloat() override;
    bool isEquals(DataTypePtr otherDataType) override;
    double getLowerBound() const;
    double getUpperBound() const;
    DataTypePtr join(DataTypePtr otherDataType) override;

  private:
    const double lowerBound;
    const double upperBound;
};

}// namespace NES

#endif//NES_INCLUDE_DATATYPES_FLOAT_HPP_
