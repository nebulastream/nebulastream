#ifndef NES_INCLUDE_QUERYCOMPILER_DATATYPES_GENERATABLEARRAYVALUETYPE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_DATATYPES_GENERATABLEARRAYVALUETYPE_HPP_

#include <Common/ValueTypes/ValueType.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableValueType.hpp>
#include <string>
#include <vector>
namespace NES {

/**
 * @brief Generates code for array values.
 * To this end it takes into account if the value is a string
 * todo we may want to factor string handling out in the future.
 */
class GeneratableArrayValueType : public GeneratableValueType {
  public:
    /**
     * @brief Constructs a new GeneratableArrayValueType
     * @param valueType the value type
     * @param values the values of the value type
     * @param isString indicates if this array value represents a string.
     */
    GeneratableArrayValueType(ValueTypePtr valueType, std::vector<std::string> values, bool isString = false);

    /**
     * @brief Generates code expresion, which represents this value.
     * @return
     */
    CodeExpressionPtr getCodeExpression() override;

  private:
    ValueTypePtr valueType;
    std::vector<std::string> values;
    bool isString;
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_DATATYPES_GENERATABLEBASICVALUETYPE_HPP_
