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

#ifndef NES_INCLUDE_QUERY_COMPILER_CODE_GENERATOR_CODE_GENERATOR_HPP_
#define NES_INCLUDE_QUERY_COMPILER_CODE_GENERATOR_CODE_GENERATOR_HPP_

#include <memory>

#include <API/Schema.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Declarations/Declaration.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/FileBuilder.hpp>
#include <QueryCompiler/CodeGenerator/CodeGeneratorForwardRef.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Runtime/NodeEngineForwaredRefs.hpp>

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/UnaryOperatorStatement.hpp>
#include <QueryCompiler/CodeGenerator/CodeGenerator.hpp>
#include <QueryCompiler/Phases/OutputBufferAllocationStrategies.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>

namespace NES {
namespace QueryCompilation {

/**
 * @brief The code generator encapsulates the code generation for different operators.
 */
class CodeGenerator {
  public:
    CodeGenerator();

    /**
     * @brief Code generation for a scan, which depends on a particular input schema.
     * @param inputSchema The input schema, in which we receive the input buffer.
     * @param outputSchema The out schema, in which we forward to the next operator
     * @param context The context of the current pipeline.
     * @return flag if the generation was successful.
     */
    virtual bool generateCodeForScan(SchemaPtr inputSchema, SchemaPtr outputSchema, PipelineContextPtr context) = 0;

    /**
     * @brief Code generation for a setup of a scan, which depends on a particular input schema.
     * @param context The context of the current pipeline.
     * @return flag if the generation was successful.
     */
    virtual bool generateCodeForScanSetup(PipelineContextPtr context) = 0;

    /**
     * @brief Code generation for a projection, which depends on a particular input schema.
     * @param projectExpressions The projection expression nodes.
     * @param context The context of the current pipeline.
     * @return flag if the generation was successful.
     */
    virtual bool generateCodeForProjection(std::vector<ExpressionNodePtr> projectExpressions, PipelineContextPtr context) = 0;

    /**
     * @brief Code generation for a filter operator, which depends on a particular filter predicate.
     * @param predicate The filter predicate, which selects input records.
     * @param context The context of the current pipeline.
     * @return flag if the generation was successful.
     */
    virtual bool generateCodeForFilter(PredicatePtr predicate, PipelineContextPtr context) = 0;

    /**
     * @brief Code generation for a (branchless) predicated filter operator.
     * @param predicate The filter predicate, which selects input records.
     * @param context The context of the current pipeline.
     * @return flag if the generation was successful.
     */
    virtual bool generateCodeForFilterPredicated(PredicatePtr predicate, PipelineContextPtr context) = 0;

    /**
     * @brief Code generation for a map operator, which depends on a particular map predicate.
     * @param field The field, which we want to manipulate with the map predicate.
     * @param predicate The map predicate.
     * @param context The context of the current pipeline.
     * @return flag if the generation was successful.
     */
    virtual bool generateCodeForMap(AttributeFieldPtr field, LegacyExpressionPtr pred, PipelineContextPtr context) = 0;

    /**
    * @brief Code generation for a emit, which depends on a particular output schema.
    * @param schema The output schema.
    * @param context The context of the current pipeline.
    * @param bufferStrategy Strategy for allocation of and writing to result buffer.
    * @return flag if the generation was successful.
    */
    virtual bool
    generateCodeForEmit(SchemaPtr sinkSchema, OutputBufferAllocationStrategy bufferStrategy, PipelineContextPtr context) = 0;

    /**
     * @brief Code generation for a watermark assigner operator.
     * @param watermarkStrategy strategy used for watermark assignment.
     * @param context The context of the current pipeline.
     * @param withTimeUnitOffset, the offset has only be added for slicer not for combiner
     * @return flag if the generation was successful.
     */
    virtual bool generateCodeForWatermarkAssigner(Windowing::WatermarkStrategyPtr watermarkStrategy,
                                                  PipelineContextPtr context) = 0;

    /**
    * @brief Code generation for a central window operator, which depends on a particular window definition.
    * @param window The window definition, which contains all properties of the window.
    * @param context The context of the current pipeline.
    * @return the operator id
    */
    virtual uint64_t generateWindowSetup(Windowing::LogicalWindowDefinitionPtr window,
                                         SchemaPtr windowOutputSchema,
                                         PipelineContextPtr context,
                                         uint64_t id,
                                         Windowing::WindowOperatorHandlerPtr windowOperatorHandler) = 0;

    /**
    * @brief Code generation for a central window operator, which depends on a particular window definition.
    * @param window The window definition, which contains all properties of the window.
    * @param context The context of the current pipeline.
    * @param operatorHandlerIndex index for the operator handler.
    * @return flag if the generation was successful.
    */
    virtual bool generateCodeForCompleteWindow(
        Windowing::LogicalWindowDefinitionPtr window,
        QueryCompilation::GeneratableOperators::GeneratableWindowAggregationPtr generatableWindowAggregation,
        PipelineContextPtr context,
        uint64_t operatorHandlerIndex) = 0;

    /**
   * @brief Code generation for a slice creation operator for distributed window operator, which depends on a particular window definition.
   * @param window The window definition, which contains all properties of the window.
   * @param context The context of the current pipeline.
   * @param operatorHandlerIndex index for the operator handler.
   * @return flag if the generation was successful.
   */
    virtual bool generateCodeForSlicingWindow(
        Windowing::LogicalWindowDefinitionPtr window,
        QueryCompilation::GeneratableOperators::GeneratableWindowAggregationPtr generatableWindowAggregation,
        PipelineContextPtr context,
        uint64_t windowOperatorIndex) = 0;

    /**
    * generates code for CEP Iterations
    * @param minIteration - defined minimal occurence of the event
    * @param maxIteration - defined maximal occurence of the event
    * @param context - includes the context of the used fields
    * @return flag if the generation was successful.
    */

    virtual bool generateCodeForCEPIterationOperator(uint64_t minIteration, uint64_t maxIeration, PipelineContextPtr context) = 0;

    /**
    * @brief Code generation for a combiner operator for distributed window operator, which depends on a particular window definition.
    * @param window The window definition, which contains all properties of the window.
    * @param context The context of the current pipeline.
    * @param operatorHandlerIndex index for the operator handler.
    * @return flag if the generation was successful.
    */
    virtual bool generateCodeForCombiningWindow(
        Windowing::LogicalWindowDefinitionPtr window,
        QueryCompilation::GeneratableOperators::GeneratableWindowAggregationPtr generatableWindowAggregation,
        PipelineContextPtr context,
        uint64_t windowOperatorIndex) = 0;

    /**
    * @brief Code generation the setup method for join operators, which depends on a particular join definition.
    * @param join The join definition, which contains all properties of the window.
    * @param context The context of the current pipeline.
    * @return the operator id
    */
    virtual uint64_t generateJoinSetup(Join::LogicalJoinDefinitionPtr window, PipelineContextPtr context, uint64_t id) = 0;

    /**
    * @brief Code generation the setup method for join operators, which depends on a particular join definition.
    * @param join The join definition, which contains all properties of the window.
    * @param context The context of the current pipeline.
    * @return the operator id
    */
    virtual uint64_t generateCodeForJoinSinkSetup(Join::LogicalJoinDefinitionPtr window,
                                                  PipelineContextPtr context,
                                                  uint64_t id,
                                                  Join::JoinOperatorHandlerPtr joinOperatorHandler) = 0;

    /**
    * @brief Code generation for a join operator, which depends on a particular join definition
    * @param window The join definition, which contains all properties of the join.
    * @param context The context of the current pipeline.
    * @param operatorHandlerIndex index for the operator handler.
    * @return flag if the generation was successful.
    */
    virtual bool
    generateCodeForJoin(Join::LogicalJoinDefinitionPtr joinDef, PipelineContextPtr context, uint64_t operatorHandlerIndex) = 0;

    /**
    * @brief Code generation for a join operator, which depends on a particular join definition
    * @param window The join definition, which contains all properties of the join.
    * @param context The context of the current pipeline.
    * @param operatorHandlerIndex index for the operator handler.
    * todo refactor parameter
    * @return flag if the generation was successful.
    */
    virtual bool generateCodeForJoinBuild(Join::LogicalJoinDefinitionPtr joinDef,
                                          PipelineContextPtr context,
                                          Join::JoinOperatorHandlerPtr joinOperatorHandler,
                                          QueryCompilation::JoinBuildSide buildSide) = 0;

    /**
     * @brief Performs the actual compilation the generated code pipeline.
     * @param code generated code.
     * @return ExecutablePipelinePtr returns the compiled and executable pipeline.
     */

    virtual Runtime::Execution::ExecutablePipelineStagePtr
    compile(Compiler::JITCompilerPtr jitCompiler,
            PipelineContextPtr pipelineContext,
            QueryCompilerOptions::CompilationStrategy compilationStrategy) = 0;

    virtual std::string generateCode(PipelineContextPtr pipelineContext) = 0;

    virtual ~CodeGenerator();

    static CompilerTypesFactoryPtr getTypeFactory();
    static FunctionCallStatementPtr call(std::string function);
};
}// namespace QueryCompilation
}// namespace NES
#endif  // NES_INCLUDE_QUERY_COMPILER_CODE_GENERATOR_CODE_GENERATOR_HPP_
