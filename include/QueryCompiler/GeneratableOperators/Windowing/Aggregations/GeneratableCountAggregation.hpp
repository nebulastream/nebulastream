#ifndef NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_WINDOWING_AGGREGATIONS_GENERATABLECOUNTAGGREGATION_HPP_
#define NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_WINDOWING_AGGREGATIONS_GENERATABLECOUNTAGGREGATION_HPP_
#include <QueryCompiler/GeneratableOperators/Windowing/Aggregations/GeneratableWindowAggregation.hpp>
namespace NES {

class GeneratableCountAggregation : public GeneratableWindowAggregation {
  public:
    explicit GeneratableCountAggregation(Windowing::WindowAggregationDescriptorPtr aggregationDescriptor);
    /**
     * @brief Factory Method to create a new GeneratableWindowAggregation
     * @param aggregationDescriptor Window aggregation descriptor
     * @return GeneratableWindowAggregationPtr
     */
    static GeneratableWindowAggregationPtr create(Windowing::WindowAggregationDescriptorPtr aggregationDescriptor);

    /**
     * @brief Generates code for window aggregate
     * @param currentCode current code pointer
     * @param partialValueRef partial value ref
     * @param inputStruct input struct
     * @param inputRef input value reference
     */
    void compileLiftCombine(CompoundStatementPtr currentCode, BinaryOperatorStatement expressionStatement, StructDeclaration inputStruct, BinaryOperatorStatement inputRef) override;
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_WINDOWING_AGGREGATIONS_GENERATABLECOUNTAGGREGATION_HPP_
