#ifndef NES_INCLUDE_DATATYPES_BASICVALUETYPE_HPP_
#define NES_INCLUDE_DATATYPES_BASICVALUETYPE_HPP_


#include "ValueType.hpp"

namespace NES {

class BasicValue : public ValueType {

  public:
    BasicValue(DataTypePtr type, std::string value);

    /**
    * @brief Indicates if this value is a basic value.
    */
    bool isBasicValue() override;

    /**
    * @brief Returns a string representation of this value
    * @return string
    */
    std::string toString() override;

    /**
     * @brief Get the value of this value type.
     * @return string
     */
    std::string getValue();

    /**
     * @brief Checks if two values are equal
     * @param valueType
     * @return bool
     */
    bool isEquals(ValueTypePtr valueType) override;

  private:
    std::string value;
};

}// namespace NES

#endif//NES_INCLUDE_DATATYPES_BASICVALUETYPE_HPP_
