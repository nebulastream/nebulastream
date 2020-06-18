#ifndef NES_INCLUDE_DATATYPES_VALUETYPES_CHARVALUETYPE_HPP_
#define NES_INCLUDE_DATATYPES_VALUETYPES_CHARVALUETYPE_HPP_
#include <DataTypes/ValueTypes/ValueType.hpp>
#include <string>
#include <vector>
namespace NES {

class CharValueType : public ValueType {
  public:
    CharValueType(std::vector<std::string> values);
    CharValueType(const std::string& value);
    bool isEquals(ValueTypePtr valueType) override;
    std::string toString() override;
    bool isCharValue() override;
    const std::vector<std::string>& getValues() const;
    bool isString1() const;

  private:
    std::vector<std::string> values;
    bool isString;
};
}// namespace NES

#endif//NES_INCLUDE_DATATYPES_VALUETYPES_CHARVALUETYPE_HPP_
