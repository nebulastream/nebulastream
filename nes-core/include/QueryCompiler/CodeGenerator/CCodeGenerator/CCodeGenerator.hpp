/*
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

#ifndef NES_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_CCODEGENERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_CCODEGENERATOR_HPP_

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/TypeCastExprStatement.hpp>
#include <QueryCompiler/CodeGenerator/CodeGenerator.hpp>
#include <QueryCompiler/Phases/OutputBufferAllocationStrategies.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>

namespace NES {
namespace QueryCompilation {
class ForLoopStatement;
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
    * @brief Code generation for a (branchless) predicated filter operator.
    * @param predicate The filter predicate, which selects input records.
    * @param context The context of the current pipeline.
    * @return flag if the generation was successful.
    */
    bool generateCodeForFilterPredicated(PredicatePtr pred, PipelineContextPtr context) override;

    /**
    * @brief Code generation for a map operator, which depends on a particular map predicate.
    * @param field The field, which we want to manipulate with the map predicate.
    * @param predicate The map predicate.
    * @param context The context of the current pipeline.
    * @return flag if the generation was successful.
    */
    bool generateCodeForMap(AttributeFieldPtr field, LegacyExpressionPtr pred, PipelineContextPtr context) override;
    bool generateCodeForSliceStoreAppend(PipelineContextPtr context, uint64_t windowOperatorIndex) override;
    std::shared_ptr<ForLoopStatement>
    keyedSliceMergeLoop(VariableDeclaration& buffers,
                                        FunctionCallStatement& tupleBufferGetNumberOfTupleCall,
                                        StructDeclaration& partialAggregationEntry,
                                        VariableDeclaration& keyVariableDeclaration,
                                        DataTypePtr keyStamp,
                                        QueryCompilation::GeneratableOperators::GeneratableWindowAggregationPtr aggregation);
        /**
    * @brief Code generation for an emit, which depends on a particular output schema.
    * @param schema The output schema.
    * @param bufferStrategy Strategy for allocation of and writing to result buffer.
    * @param context The context of the current pipeline.
    * @return flag if the generation was successful.
    */
    bool generateCodeForEmit(SchemaPtr sinkSchema,
                             OutputBufferAllocationStrategy bufferStrategy,
                             OutputBufferAssignmentStrategy bufferAllocationStrategy,
                             PipelineContextPtr context) override;

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
    uint64_t generateKeyedThreadLocalPreAggregationSetup(Windowing::LogicalWindowDefinitionPtr window,
                                                         SchemaPtr windowOutputSchema,
                                                         PipelineContextPtr context,
                                                         uint64_t id,
                                                         uint64_t windowOperatorIndex,
                                                         GeneratableOperators::GeneratableWindowAggregationPtr ptr) override;

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

    bool generateCodeForThreadLocalPreAggregationOperator(
        Windowing::LogicalWindowDefinitionPtr window,
        QueryCompilation::GeneratableOperators::GeneratableWindowAggregationPtr generatableWindowAggregation,
        PipelineContextPtr context,
        uint64_t windowOperatorIndex) override;

    bool generateCodeForKeyedSliceMergingOperator(
        Windowing::LogicalWindowDefinitionPtr window,
        QueryCompilation::GeneratableOperators::GeneratableWindowAggregationPtr generatableWindowAggregation,
        PipelineContextPtr context,
        uint64_t windowOperatorIndex) override;

    bool
    generateCodeForKeyedTumblingWindowSink(Windowing::LogicalWindowDefinitionPtr window,
                                           GeneratableOperators::GeneratableWindowAggregationPtr generatableWindowAggregation,
                                           PipelineContextPtr context,
                                           uint64_t windowOperatorIndex,
                                           SchemaPtr ptr) override;

    bool
    generateCodeForKeyedSlidingWindowSink(Windowing::LogicalWindowDefinitionPtr window,
                                           GeneratableOperators::GeneratableWindowAggregationPtr generatableWindowAggregation,
                                           PipelineContextPtr context,
                                           uint64_t windowOperatorIndex,
                                           SchemaPtr ptr) override;


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
    Runtime::Execution::ExecutablePipelineStagePtr
    compile(Compiler::JITCompilerPtr jitCompiler,
            PipelineContextPtr context,
            QueryCompilerOptions::CompilationStrategy compilationStrategy) override;

    std::string generateCode(PipelineContextPtr context) override;

    ~CCodeGenerator() override;

  private:
    /**
     * @brief returns tupleBufferVariable.getBuffer()
     * @param tupleBufferVariable
     * @return
     */
    static BinaryOperatorStatement getBuffer(const VariableDeclaration& tupleBufferVariable);

    /**
     * @brief returns tupleBufferVariable.getBuffer()
     * @param tupleBufferVariable
     * @return
     */
    static StructDeclaration generatePartialAggregationEntry(const Windowing::LogicalWindowDefinitionPtr window, GeneratableOperators::GeneratableWindowAggregationPtr generatableAggregation);

    /**
     * @brief returns getOperatorHandler<Windowing::WindowOperatorHandler>(windowOperatorIndex);
     * @param context
     * @param tupleBufferVariable
     * @param index
     * @return
     */
    VariableDeclaration getWindowOperatorHandler(const PipelineContextPtr& context,
                                                 const VariableDeclaration& tupleBufferVariable,
                                                 uint64_t windowOperatorIndex);

    /**
     * @brief returns getOperatorHandler<NES::CEP::CEPOperatorHandler>(CEPOperatorIndex)
     * @param context
     * @param tupleBufferVariable
     * @param index
     * @return
     */
    VariableDeclaration getCEPIterationOperatorHandler(const PipelineContextPtr& context,
                                                       const VariableDeclaration& tupleBufferVariable,
                                                       uint64_t index);
    /**
     * @brief returns tupleBufferVariable.getWatermark()
     * @param tupleBufferVariable
     * @return
     */
    static BinaryOperatorStatement getWatermark(const VariableDeclaration& tupleBufferVariable);

    /**
     * @brief tupleBufferVariable.getOriginId()
     * @param tupleBufferVariable
     * @return
     */
    static BinaryOperatorStatement getOriginId(const VariableDeclaration& tupleBufferVariable);

    /**
     * @brief return tupleBufferVariable.getSequenceNumber()
     * @param tupleBufferVariable
     * @return
     */
    BinaryOperatorStatement getSequenceNumber(VariableDeclaration tupleBufferVariable);

    /**
     * @brief casts tupleBufferVariable to pointer type of structDeclaration
     * @param tupleBufferVariable
     * @param structDeclaration
     * @return
     */
    TypeCastExprStatement getTypedBuffer(const VariableDeclaration& tupleBufferVariable,
                                         const StructDeclaration& structDeclaration);

    /**
     * @brief returns tupleBufferVariable.getBuffer()
     * @param tupleBufferVariable
     * @return
     */
    static BinaryOperatorStatement getBufferSize(const VariableDeclaration& tupleBufferVariable);

    /**
     * @brief returns tupleBufferVariable.setNumberOfTuples(numberOfResultTuples)
     * @param tupleBufferVariable
     * @param numberOfResultTuples
     * @return
     */
    static BinaryOperatorStatement setNumberOfTuples(const VariableDeclaration& tupleBufferVariable,
                                                     const VariableDeclaration& numberOfResultTuples);
    /**
     * @brief returns tupleBufferVariable.setWatermark(inputBufferVariable)
     * @param tupleBufferVariable
     * @param inputBufferVariable
     * @return
     */
    BinaryOperatorStatement setWatermark(const VariableDeclaration& tupleBufferVariable,
                                         const VariableDeclaration& inputBufferVariable);

    /**
     * @brief returns tupleBufferVariable.setOriginId(inputBufferVariable)
     * @param tupleBufferVariable
     * @param inputBufferVariable
     * @return
     */
    BinaryOperatorStatement setOriginId(const VariableDeclaration& tupleBufferVariable,
                                        const VariableDeclaration& inputBufferVariable);

    /**
     * @brief returns tupleBufferVariable.setSequenceNumber(inputBufferVariable)
     * @param tupleBufferVariable
     * @param inputBufferVariable
     * @return
     */
    BinaryOperatorStatement setSequenceNumber(VariableDeclaration tupleBufferVariable, VariableDeclaration inputBufferVariable);

    /**
     * @brief returns pipelineContext.allocateTupleBuffer()
     * @param pipelineContext
     * @return
     */
    static BinaryOperatorStatement allocateTupleBuffer(const VariableDeclaration& pipelineContext);

    /**
     * @brief pipelineContext.emitBuffer(tupleBufferVariable, workerContextVariable)
     * @param pipelineContext
     * @param tupleBufferVariable
     * @param workerContextVariable
     * @return
     */
    static BinaryOperatorStatement emitTupleBuffer(const VariableDeclaration& pipelineContext,
                                                   const VariableDeclaration& tupleBufferVariable,
                                                   const VariableDeclaration& workerContextVariable);

    /**
     * @brief creates an if block that checks if a new buffer has to be allocated as the current one is full
     * @param context
     * @param varDeclResultTuple
     * @param structDeclarationResultTuple
     * @param schema
     */
    void generateTupleBufferSpaceCheck(const PipelineContextPtr& context,
                                       const VariableDeclaration& varDeclResultTuple,
                                       const StructDeclaration& structDeclarationResultTuple,
                                       SchemaPtr schema);

    /**
     * @brief creates a struct definition from schema
     * @param structName
     * @param schema
     * @return
     */
    static StructDeclaration getStructDeclarationFromSchema(const std::string& structName, const SchemaPtr& schema);

    /**
     * @brief creates the aggregation window handler
     * @param pipelineContextVariable
     * @param keyType
     * @param windowAggregationDescriptor
     * @return
     */
    BinaryOperatorStatement
    getAggregationWindowHandler(const VariableDeclaration& pipelineContextVariable,
                                DataTypePtr keyType,
                                const Windowing::WindowAggregationDescriptorPtr& windowAggregationDescriptor);

    /**
     * @brief returns getJoinHandler<NES::Join::JoinHandler,KeyType,leftType,rightType>()
     * @param pipelineContextVariable
     * @param KeyType
     * @param leftType
     * @param rightType
     * @return
     */
    BinaryOperatorStatement getJoinWindowHandler(const VariableDeclaration& pipelineContextVariable,
                                                 DataTypePtr KeyType,
                                                 const std::string& leftType,
                                                 const std::string& rightType);

    /**
     * @brief returns windowHandlerVariable.getTypedWindowState()
     * @param windowHandlerVariable
     * @return
     */
    static BinaryOperatorStatement getStateVariable(const VariableDeclaration& windowHandlerVariable);

    /**
     * @brief returns windowHandlerVariable.getLeftJoinState()
     * @param windowHandlerVariable
     * @return
     */
    static BinaryOperatorStatement getLeftJoinState(const VariableDeclaration& windowHandlerVariable);

    /**
     * @brief returns windowHandlerVariable.getRightJoinState()
     * @param windowHandlerVariable
     * @return
     */
    static BinaryOperatorStatement getRightJoinState(const VariableDeclaration& windowHandlerVariable);

    /**
     * @brief windowHandlerVariable.getWindowManager()
     * @param windowHandlerVariable
     * @return
     */
    static BinaryOperatorStatement getWindowManager(const VariableDeclaration& windowHandlerVariable);

    /**
     * @brief creates handler.updateMaxTs(getWaterMark(inputBuffer), getOriginId(inputBuffer), getSequenceNumber(inputBuffer)) and
     * appends it to the cleanup statements
     * @param context
     * @param handler
     */
    void generateCodeForWatermarkUpdaterWindow(const PipelineContextPtr& context, const VariableDeclaration& handler);

    /**
     * @brief creates handler.updateMaxTs(getWaterMark(inputBuffer), getOriginId(inputBuffer), getSequenceNumber(inputBuffer), leftSide) and
     * appends it to the cleanup statements
     * @param context
     * @param handler
     * @param leftSide
     */
    void
    generateCodeForWatermarkUpdaterJoin(const PipelineContextPtr& context, const VariableDeclaration& handler, bool leftSide);

    /**
     * @brief Generates the code for initializing partialAggregate and executable aggregation depending on its type. For example,
     * the sum aggregate with int auto partialAggregateInitialValue = 0; Windowing::ExecutableSumAggregation<int64_t>::create()
     * @param setupScope
     * @param executableAggregation
     * @param partialAggregateInitialValue
     * @param aggregationInputType
     * @param aggregation
     */
    void generateCodeForAggregationInitialization(const BlockScopeStatementPtr& setupScope,
                                                  const VariableDeclaration& executableAggregation,
                                                  const VariableDeclaration& partialAggregateInitialValue,
                                                  const GeneratableDataTypePtr& aggregationInputType,
                                                  const Windowing::WindowAggregationDescriptorPtr& aggregation);

    /**
     * @brief creates joinOperatorHandler = tupleBufferVariable.getOperatorHandler<Join::JoinOperatorHandler>(joinOperatorIndex) and
     * appends it to the variableInitStmts
     * @param context
     * @param tupleBufferVariable
     * @param joinOperatorIndex
     * @return
     */
    VariableDeclaration getJoinOperatorHandler(const PipelineContextPtr& context,
                                               const VariableDeclaration& tupleBufferVariable,
                                               uint64_t joinOperatorIndex);

    /**
     * @brief in a col layout, all pointers in the structDeclaration have to be set to the correct field
     * @param schema
     * @param tf
     * @param varDeclarationInputBuffer
     * @param structDeclaration
     * @param varTuples
     * @param statements
     * @param capacityVarName
     */
    void generateCodeInitStructFieldsColLayout(const SchemaPtr& schema,
                                               CompilerTypesFactoryPtr& tf,
                                               const VariableDeclaration& varDeclarationInputBuffer,
                                               const StructDeclaration structDeclaration,
                                               const VariableDeclaration varTuples,
                                               std::vector<StatementPtr>& statements,
                                               const std::string& capacityVarName);
};
}// namespace QueryCompilation
}// namespace NES

#endif  // NES_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_CCODEGENERATOR_HPP_
