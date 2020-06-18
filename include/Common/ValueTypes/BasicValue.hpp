#ifndef NES_INCLUDE_DATATYPES_BASICVALUETYPE_HPP_
#define NES_INCLUDE_DATATYPES_BASICVALUETYPE_HPP_

#include "ValueType.hpp"

namespace NES {

class BasicValue : public ValueType {

  public:
    BasicValue(DataTypePtr type, std::string value);
    bool isBasicValue() override;
    std::string toString() override;
    std::string getValue();
    bool isEquals(ValueTypePtr valueType) override;

  private:
    std::string value;
};

}// namespace NES

#endif//NES_INCLUDE_DATATYPES_BASICVALUETYPE_HPP_
