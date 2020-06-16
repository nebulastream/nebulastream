#ifndef NES_INCLUDE_QUERYCOMPILER_DATATYPES_GENERATABLEVALUETYPE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_DATATYPES_GENERATABLEVALUETYPE_HPP_
#include <memory>
namespace NES{

class CodeExpression;
typedef std::shared_ptr<CodeExpression> CodeExpressionPtr;

class GeneratableValueType;
typedef std::shared_ptr<GeneratableValueType> GeneratableValueTypePtr;

class GeneratableValueType {
  public:
    CodeExpressionPtr generateCode();
};

}

#endif//NES_INCLUDE_QUERYCOMPILER_DATATYPES_GENERATABLEVALUETYPE_HPP_
