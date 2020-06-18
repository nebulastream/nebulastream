#ifndef NES_INCLUDE_DATATYPES_VALUETYPES_ARRAYVALUETYPE_HPP_
#define NES_INCLUDE_DATATYPES_VALUETYPES_ARRAYVALUETYPE_HPP_
#include <Common/ValueTypes/ValueType.hpp>
#include <vector>
namespace NES {

class ArrayValue : public ValueType {
  public:
    ArrayValue(DataTypePtr type, std::vector<std::string> values);
    std::vector<std::string> getValues();
    bool isArrayValue() override;
    std::string toString() override;
    bool isEquals(ValueTypePtr valueType) override;

  private:
    std::vector<std::string> values;
};

}// namespace NES

#endif//NES_INCLUDE_DATATYPES_VALUETYPES_ARRAYVALUETYPE_HPP_
