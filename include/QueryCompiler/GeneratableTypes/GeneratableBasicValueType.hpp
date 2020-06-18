#ifndef NES_INCLUDE_QUERYCOMPILER_DATATYPES_GENERATABLEBASICVALUETYPE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_DATATYPES_GENERATABLEBASICVALUETYPE_HPP_

#include <QueryCompiler/GeneratableTypes/GeneratableValueType.hpp>

namespace NES{

class BasicValue;
typedef std::shared_ptr<BasicValue> BasicValuePtr;

class GeneratableBasicValueType : public GeneratableValueType{
  public:
    GeneratableBasicValueType(BasicValuePtr basicValue);
    CodeExpressionPtr getCodeExpression() override;


  private:
    BasicValuePtr value;
};

}

#endif//NES_INCLUDE_QUERYCOMPILER_DATATYPES_GENERATABLEBASICVALUETYPE_HPP_
