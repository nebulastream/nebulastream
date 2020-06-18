#ifndef NES_INCLUDE_QUERYCOMPILER_DATATYPES_GENERATABLEBASICVALUETYPE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_DATATYPES_GENERATABLEBASICVALUETYPE_HPP_

#include <QueryCompiler/GeneratableTypes/GeneratableValueType.hpp>

namespace NES {

class BasicValue;
typedef std::shared_ptr<BasicValue> BasicValuePtr;

/**
 * @brief Generates code for basic values.
 */
class GeneratableBasicValueType : public GeneratableValueType {
  public:
    GeneratableBasicValueType(BasicValuePtr basicValue);

    /**
    * @brief Generate the code expression for this value type.
    * @return CodeExpressionPtr
    */
    CodeExpressionPtr getCodeExpression() override;

  private:
    BasicValuePtr value;
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_DATATYPES_GENERATABLEBASICVALUETYPE_HPP_
