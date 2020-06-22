#ifndef NES_INCLUDE_DATATYPES_VALUETYPES_CHARVALUETYPE_HPP_
#define NES_INCLUDE_DATATYPES_VALUETYPES_CHARVALUETYPE_HPP_

#include <Common/ValueTypes/ValueType.hpp>
#include <string>
#include <vector>

namespace NES {

/**
 * @brief Represents a FixedCharValue. This can ether constructed from a vector of string values, or a string.
 */
class FixedCharValue : public ValueType {
  public:
    FixedCharValue(std::vector<std::string> values);
    FixedCharValue(const std::string& value);

    /**
     * @brief Checks if two values are equal
     * @param valueType
     * @return bool
     */
    bool isEquals(ValueTypePtr valueType) override;

    /**
    * @brief Returns a string representation of this value
    * @return string
    */
    std::string toString() override;

    /**
     * @brief Indicates if this value is a char value.
     */
    bool isCharValue() override;

    /**
     * @brief Returns the values represented by the fixed char.
     * @return
     */
    const std::vector<std::string>& getValues() const;

    /**
     * @brief Indicates if this value is a string
     * @return
     */
    bool getIsString() const;

  private:
    std::vector<std::string> values;
    bool isString;
};
}// namespace NES

#endif//NES_INCLUDE_DATATYPES_VALUETYPES_CHARVALUETYPE_HPP_
