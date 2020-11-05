#ifndef NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_CCODEGENERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_CCODEGENERATOR_HPP_

#include <QueryCompiler/CCodeGenerator/Statements/TypeCastExprStatement.hpp>
#include <QueryCompiler/CodeGenerator.hpp>

namespace NES {

/**
 * @brief A code generator that generates C++ code optimized for X86 architectures.
 */
class CCodeGenerator : public CodeGenerator {

  public:
    CCodeGenerator();
    static CodeGeneratorPtr create();

    /**
     * @brief Code generation for a scan, which depends on a particular input schema.
     * @param schema The input schema, in which we receive the input buffer.
     * @param context The context of the current pipeline.
     * @return flag if the generation was successful.
     */
    bool generateCodeForScan(SchemaPtr schema, PipelineContextPtr context) override;

    /**
    * @brief Code generation for a filter operator, which depends on a particular filter predicate.
    * @param predicate The filter predicate, which selects input records.
    * @param context The context of the current pipeline.
    * @return flag if the generation was successful.
    */
    bool generateCodeForFilter(PredicatePtr pred, PipelineContextPtr context) override;

    /**
    * @brief Code generation for a map operator, which depends on a particular map predicate.
    * @param field The field, which we want to manipulate with the map predicate.
    * @param predicate The map predicate.
    * @param context The context of the current pipeline.
    * @return flag if the generation was successful.
    */
    bool generateCodeForMap(AttributeFieldPtr field, UserAPIExpressionPtr pred, PipelineContextPtr context) override;

    /**
    * @brief Code generation for a emit, which depends on a particular output schema.
    * @param schema The output schema.
    * @param context The context of the current pipeline.
    * @return flag if the generation was successful.
    */
    bool generateCodeForEmit(SchemaPtr sinkSchema, PipelineContextPtr context) override;

    /**
    * @brief Code generation for a central window operator, which depends on a particular window definition.
    * @param window The window definition, which contains all properties of the window.
    * @param generatableWindowAggregation window aggregation.
    * @param context The context of the current pipeline.
    * @return flag if the generation was successful.
    */
    bool generateCodeForCompleteWindow(Windowing::LogicalWindowDefinitionPtr window, GeneratableWindowAggregationPtr generatableWindowAggregation, PipelineContextPtr context) override;

    /**
    * @brief Code generation for a slice creation operator for distributed window operator, which depends on a particular window definition.
    * @param window The window definition, which contains all properties of the window.
    * @param generatableWindowAggregation window aggregation.
    * @param context The context of the current pipeline.
    * @return flag if the generation was successful.
    */
    bool generateCodeForSlicingWindow(Windowing::LogicalWindowDefinitionPtr window, GeneratableWindowAggregationPtr generatableWindowAggregation, PipelineContextPtr context) override;

    /**
    * @brief Code generation for a combiner operator for distributed window operator, which depends on a particular window definition.
    * @param window The window definition, which contains all properties of the window.
    * @param generatableWindowAggregation window aggregation.
    * @param context The context of the current pipeline.
    * @return flag if the generation was successful.
    */
    bool generateCodeForCombiningWindow(Windowing::LogicalWindowDefinitionPtr window, GeneratableWindowAggregationPtr generatableWindowAggregation, PipelineContextPtr context) override;

#if 0
    /**
    * @brief Code generation for a combiner operator for distributed window operator, which depends on a particular window definition.
    * @param The join definition, which contains all properties of the join.
    * @param context The context of the current pipeline.
    * @return flag if the generation was successful.
    */
    bool generateCodeForJoin(Join::LogicalJoinDefinitionPtr joinDef, PipelineContextPtr context) override;
#endif
    /**
     * @brief Performs the actual compilation the generated code pipeline.
     * @param code generated code.
     * @return ExecutablePipelinePtr returns the compiled and executable pipeline.
     */
    ExecutablePipelinePtr compile(GeneratedCodePtr code) override;

    std::string getSourceCode(GeneratedCodePtr code) override;

    ~CCodeGenerator() override;

  private:
    BinaryOperatorStatement getBuffer(VariableDeclaration tupleBufferVariable);
    BinaryOperatorStatement getWatermark(VariableDeclaration tupleBufferVariable);
    BinaryOperatorStatement getOriginId(VariableDeclaration tupleBufferVariable);

    TypeCastExprStatement getTypedBuffer(VariableDeclaration tupleBufferVariable, StructDeclaration structDeclaration);
    BinaryOperatorStatement getBufferSize(VariableDeclaration tupleBufferVariable);
    BinaryOperatorStatement setNumberOfTuples(VariableDeclaration tupleBufferVariable,
                                              VariableDeclaration inputBufferVariable);
    BinaryOperatorStatement setWatermark(VariableDeclaration tupleBufferVariable,
                                         VariableDeclaration inputBufferVariable);
    BinaryOperatorStatement setOriginId(VariableDeclaration tupleBufferVariable,
                                        VariableDeclaration inputBufferVariable);

    BinaryOperatorStatement allocateTupleBuffer(VariableDeclaration pipelineContext);
    BinaryOperatorStatement emitTupleBuffer(VariableDeclaration pipelineContext,
                                            VariableDeclaration tupleBufferVariable,
                                            VariableDeclaration workerContextVariable);
    void generateTupleBufferSpaceCheck(PipelineContextPtr context,
                                       VariableDeclaration varDeclResultTuple,
                                       StructDeclaration structDeclarationResultTuple);

    StructDeclaration getStructDeclarationFromSchema(std::string structName, SchemaPtr schema);

    BinaryOperatorStatement getAggregationWindowHandler(VariableDeclaration pipelineContextVariable, DataTypePtr keyType, DataTypePtr inputType, DataTypePtr partialAggregateType, DataTypePtr finalAggregateType);

    BinaryOperatorStatement getJoinWindowHandler(VariableDeclaration pipelineContextVariable, DataTypePtr keyType, DataTypePtr inputType, DataTypePtr partialAggregateType, DataTypePtr finalAggregateType);

    BinaryOperatorStatement getStateVariable(VariableDeclaration);

    BinaryOperatorStatement getWindowManager(VariableDeclaration);

    StructDeclaration getStructDeclarationFromWindow(std::string structName);
    CompilerPtr compiler;
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_CCODEGENERATOR_HPP_
