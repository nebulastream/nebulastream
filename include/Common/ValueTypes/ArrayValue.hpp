#ifndef NES_INCLUDE_DATATYPES_VALUETYPES_ARRAYVALUETYPE_HPP_
#define NES_INCLUDE_DATATYPES_VALUETYPES_ARRAYVALUETYPE_HPP_
#include <Common/ValueTypes/ValueType.hpp>
#include <vector>
namespace NES {

class ArrayValue : public ValueType {
  public:
    ArrayValue(DataTypePtr type, std::vector<std::string> values);
    std::vector<std::string> getValues();

    /**
     * @brief Indicates if this value is a array.
     */
    bool isArrayValue() override;

    /**
    * @brief Returns a string representation of this value
    * @return string
    */
    std::string toString() override;

    /**
     * @brief Checks if two values are equal
     * @param valueType
     * @return bool
     */
    bool isEquals(ValueTypePtr valueType) override;

  private:
    std::vector<std::string> values;
};

}// namespace NES

#endif//NES_INCLUDE_DATATYPES_VALUETYPES_ARRAYVALUETYPE_HPP_
