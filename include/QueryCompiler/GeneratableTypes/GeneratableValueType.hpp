#ifndef NES_INCLUDE_QUERYCOMPILER_DATATYPES_GENERATABLEVALUETYPE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_DATATYPES_GENERATABLEVALUETYPE_HPP_
#include <memory>
namespace NES {

class CodeExpression;
typedef std::shared_ptr<CodeExpression> CodeExpressionPtr;

class GeneratableValueType;
typedef std::shared_ptr<GeneratableValueType> GeneratableValueTypePtr;

/**
 * @brief A generatable value type generates code for values.
 * For instance BasicValues, and Array Values.
 */
class GeneratableValueType {
  public:
    /**
     * @brief Generate the code expression for this value type.
     * @return CodeExpressionPtr
     */
    virtual CodeExpressionPtr getCodeExpression() = 0;
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_DATATYPES_GENERATABLEVALUETYPE_HPP_
