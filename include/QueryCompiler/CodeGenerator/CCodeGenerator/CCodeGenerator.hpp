/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#ifndef NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_CCODEGENERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_CCODEGENERATOR_HPP_

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/TypeCastExprStatement.hpp>
#include <QueryCompiler/CodeGenerator/CodeGenerator.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>

namespace NES {
namespace QueryCompilation {
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
     * @param schema The out schema, in which we forward to the next operator
     * @param context The context of the current pipeline.
     * @return flag if the generation was successful.
     */
    bool generateCodeForScan(SchemaPtr inputSchema, SchemaPtr outputSchema, PipelineContextPtr context) override;

    /**
     * @brief Code generation for a setup of a scan, which depends on a particular input schema.
     * @param context The context of the current pipeline.
     * @return flag if the generation was successful.
     */
    bool generateCodeForScanSetup(PipelineContextPtr context) override;

    /**
     * @brief Code generation for a projection, which depends on a particular input schema.
     * @param projectExpressions The projection expression nodes.
     * @param context The context of the current pipeline.
     * @return flag if the generation was successful.
     */
    bool generateCodeForProjection(std::vector<ExpressionNodePtr> projectExpressions, PipelineContextPtr context) override;

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
    bool generateCodeForMap(AttributeFieldPtr field, LegacyExpressionPtr pred, PipelineContextPtr context) override;

    /**
    * @brief Code generation for a emit, which depends on a particular output schema.
    * @param schema The output schema.
    * @param context The context of the current pipeline.
    * @return flag if the generation was successful.
    */
    bool generateCodeForEmit(SchemaPtr sinkSchema, PipelineContextPtr context) override;

    /**
     * @brief Code generation for a watermark assigner operator.
     * @param watermarkStrategy strategy used for watermark assignment.
     * @param context The context of the current pipeline.
     * @return flag if the generation was successful.
     */
    bool generateCodeForWatermarkAssigner(Windowing::WatermarkStrategyPtr watermarkStrategy, PipelineContextPtr context) override;

    /**
    * @brief Code generation for a central window operator, which depends on a particular window definition.
    * @param window The window definition, which contains all properties of the window.
    * @param context The context of the current pipeline.
    * @return the operator id
    */
    uint64_t generateWindowSetup(Windowing::LogicalWindowDefinitionPtr window,
                                 SchemaPtr windowOutputSchema,
                                 PipelineContextPtr context,
                                 uint64_t id,
                                 Windowing::WindowOperatorHandlerPtr windowOperatorHandler) override;

    /**
    * @brief Code generation for a central window operator, which depends on a particular window definition.
    * @param window The window definition, which contains all properties of the window.
    * @param generatableWindowAggregation window aggregation.
    * @param context The context of the current pipeline.
    * @param operatorHandlerIndex index for the operator handler.
    * @return flag if the generation was successful.
    */
    bool generateCodeForCompleteWindow(
        Windowing::LogicalWindowDefinitionPtr window,
        QueryCompilation::GeneratableOperators::GeneratableWindowAggregationPtr generatableWindowAggregation,
        PipelineContextPtr context,
        uint64_t windowOperatorIndex) override;

    /**
    * @brief Code generation for a slice creation operator for distributed window operator, which depends on a particular window definition.
    * @param window The window definition, which contains all properties of the window.
    * @param generatableWindowAggregation window aggregation.
    * @param context The context of the current pipeline.
    * @param operatorHandlerIndex index for the operator handler.
    * @return flag if the generation was successful.
    */
    bool generateCodeForSlicingWindow(
        Windowing::LogicalWindowDefinitionPtr window,
        QueryCompilation::GeneratableOperators::GeneratableWindowAggregationPtr generatableWindowAggregation,
        PipelineContextPtr context,
        uint64_t windowOperatorId) override;

    /**
    * generates code for the CEP Iteration operator. This operator counts the occurrences of events and expects at least the given minIterations and at most the given maxIterations.
    * @param minIteration - defined minimal occurrence of the event
    * @param maxIteration - defined maximal occurrence of the event
    * @param context - includes the context of the used fields
    * @return flag if the generation was successful.
    */
    bool generateCodeForCEPIterationOperator(uint64_t minIteration, uint64_t maxIteration, PipelineContextPtr context) override;

    /**
    * @brief Code generation for a combiner operator for distributed window operator, which depends on a particular window definition.
    * @param window The window definition, which contains all properties of the window.
    * @param generatableWindowAggregation window aggregation.
    * @param context The context of the current pipeline.
    * @param operatorHandlerIndex index for the operator handler.
    * @return flag if the generation was successful.
    */
    bool generateCodeForCombiningWindow(
        Windowing::LogicalWindowDefinitionPtr window,
        QueryCompilation::GeneratableOperators::GeneratableWindowAggregationPtr generatableWindowAggregation,
        PipelineContextPtr context,
        uint64_t windowOperatorIndex) override;

    /**
    * @brief Code generation the setup method for join operators, which depends on a particular join definition.
    * @param join The join definition, which contains all properties of the window.
    * @param context The context of the current pipeline.
    * @return the operator id
    */
    uint64_t generateJoinSetup(Join::LogicalJoinDefinitionPtr join, PipelineContextPtr context, uint64_t id) override;

    /**
    * @brief Code generation the setup method for join operators, which depends on a particular join definition.
    * @param join The join definition, which contains all properties of the window.
    * @param context The context of the current pipeline.
    * @return the operator id
    */
    uint64_t generateCodeForJoinSinkSetup(Join::LogicalJoinDefinitionPtr join,
                                          PipelineContextPtr context,
                                          uint64_t id,
                                          Join::JoinOperatorHandlerPtr joinOperatorHandler) override;

    /**
    * @brief Code generation for a combiner operator for distributed window operator, which depends on a particular window definition.
    * @param The join definition, which contains all properties of the join.
    * @param context The context of the current pipeline.
    * @param operatorHandlerIndex index for the operator handler.
    * @return flag if the generation was successful.
    */
    bool generateCodeForJoin(Join::LogicalJoinDefinitionPtr joinDef,
                             PipelineContextPtr context,
                             uint64_t operatorHandlerIndex) override;

    /**
    * @brief Code generation for a join operator, which depends on a particular join definition
    * @param window The join definition, which contains all properties of the join.
    * @param context The context of the current pipeline.
    * @param operatorHandlerIndex index for the operator handler.
    * todo refactor parameter
    * @return flag if the generation was successful.
    */
    bool generateCodeForJoinBuild(Join::LogicalJoinDefinitionPtr joinDef,
                                  PipelineContextPtr context,
                                  Join::JoinOperatorHandlerPtr joinOperatorHandler,
                                  QueryCompilation::JoinBuildSide buildSide) override;

    /**
     * @brief Performs the actual compilation the generated code pipeline.
     * @param code generated code.
     * @return ExecutablePipelinePtr returns the compiled and executable pipeline.
     */
    Runtime::Execution::ExecutablePipelineStagePtr compile(Compiler::JITCompilerPtr jitCompiler,
                                                           PipelineContextPtr context) override;

    std::string generateCode(PipelineContextPtr context) override;

    ~CCodeGenerator() override;

  private:
    static BinaryOperatorStatement getBuffer(const VariableDeclaration& tupleBufferVariable);
    VariableDeclaration
    getWindowOperatorHandler(const PipelineContextPtr& context, const VariableDeclaration& tupleBufferVariable, uint64_t index);
    VariableDeclaration getCEPIterationOperatorHandler(const PipelineContextPtr& context,
                                                       const VariableDeclaration& tupleBufferVariable,
                                                       uint64_t index);
    static BinaryOperatorStatement getWatermark(const VariableDeclaration& tupleBufferVariable);
    static BinaryOperatorStatement getOriginId(const VariableDeclaration& tupleBufferVariable);
    BinaryOperatorStatement getSequenceNumber(VariableDeclaration tupleBufferVariable);

    TypeCastExprStatement getTypedBuffer(const VariableDeclaration& tupleBufferVariable,
                                         const StructDeclaration& structDeclaration);
    static BinaryOperatorStatement getBufferSize(const VariableDeclaration& tupleBufferVariable);
    static BinaryOperatorStatement setNumberOfTuples(const VariableDeclaration& tupleBufferVariable,
                                                     const VariableDeclaration& numberOfResultTuples);
    BinaryOperatorStatement setWatermark(const VariableDeclaration& tupleBufferVariable,
                                         const VariableDeclaration& inputBufferVariable);
    BinaryOperatorStatement setOriginId(const VariableDeclaration& tupleBufferVariable,
                                        const VariableDeclaration& inputBufferVariable);
    BinaryOperatorStatement setSequenceNumber(VariableDeclaration tupleBufferVariable, VariableDeclaration inputBufferVariable);

    static BinaryOperatorStatement allocateTupleBuffer(const VariableDeclaration& pipelineContext);
    static BinaryOperatorStatement emitTupleBuffer(const VariableDeclaration& pipelineContext,
                                                   const VariableDeclaration& tupleBufferVariable,
                                                   const VariableDeclaration& workerContextVariable);
    void generateTupleBufferSpaceCheck(const PipelineContextPtr& context,
                                       const VariableDeclaration& varDeclResultTuple,
                                       const StructDeclaration& structDeclarationResultTuple);

    static StructDeclaration getStructDeclarationFromSchema(const std::string& structName, const SchemaPtr& schema);

    BinaryOperatorStatement
    getAggregationWindowHandler(const VariableDeclaration& pipelineContextVariable,
                                DataTypePtr keyType,
                                const Windowing::WindowAggregationDescriptorPtr& windowAggregationDescriptor);

    BinaryOperatorStatement getJoinWindowHandler(const VariableDeclaration& pipelineContextVariable,
                                                 DataTypePtr KeyType,
                                                 const std::string& leftType,
                                                 const std::string& rightType);

    static BinaryOperatorStatement getStateVariable(const VariableDeclaration&);

    static BinaryOperatorStatement getLeftJoinState(const VariableDeclaration& windowHandlerVariable);
    static BinaryOperatorStatement getRightJoinState(const VariableDeclaration& windowHandlerVariable);

    static BinaryOperatorStatement getWindowManager(const VariableDeclaration&);

    void generateCodeForWatermarkUpdaterWindow(const PipelineContextPtr& context, const VariableDeclaration& handler);
    void
    generateCodeForWatermarkUpdaterJoin(const PipelineContextPtr& context, const VariableDeclaration& handler, bool leftSide);

    void generateCodeForAggregationInitialization(const BlockScopeStatementPtr& setupScope,
                                                  const VariableDeclaration& executableAggregation,
                                                  const VariableDeclaration& partialAggregateInitialValue,
                                                  const GeneratableDataTypePtr& aggregationInputType,
                                                  const Windowing::WindowAggregationDescriptorPtr& aggregation);

    StructDeclaration getStructDeclarationFromWindow(std::string structName);

    VariableDeclaration getJoinOperatorHandler(const PipelineContextPtr& context,
                                               const VariableDeclaration& tupleBufferVariable,
                                               uint64_t joinOperatorIndex);
};
}// namespace QueryCompilation
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_CCODEGENERATOR_HPP_
