#ifndef NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_WINDOWING_AGGREGATIONS_GENERATABLEWINDOWAGGREGATION_HPP_
#define NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_WINDOWING_AGGREGATIONS_GENERATABLEWINDOWAGGREGATION_HPP_
#include <memory>

namespace NES::Windowing{
class WindowAggregationDescriptor;
typedef std::shared_ptr<WindowAggregationDescriptor> WindowAggregationDescriptorPtr;

}

namespace NES{

class BinaryOperatorStatement;
class StructDeclaration;
class CompoundStatement;
typedef std::shared_ptr<CompoundStatement> CompoundStatementPtr;

class GeneratableWindowAggregation{
  public:
    /**
     * @brief Generates code for window aggregate
     * @param currentCode current code pointer
     * @param partialValueRef partial value ref
     * @param inputStruct input struct
     * @param inputRef input value reference
     */
    virtual void compileLiftCombine(CompoundStatementPtr currentCode,
                                    BinaryOperatorStatement partialValueRef,
                                    StructDeclaration inputStruct,
                                    BinaryOperatorStatement inputRef) = 0;

  protected:
    explicit GeneratableWindowAggregation(Windowing::WindowAggregationDescriptorPtr aggregationDescriptor);

    Windowing::WindowAggregationDescriptorPtr aggregationDescriptor;
};

typedef std::shared_ptr<GeneratableWindowAggregation> GeneratableWindowAggregationPtr;

}

#endif//NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_WINDOWING_AGGREGATIONS_GENERATABLEWINDOWAGGREGATION_HPP_
