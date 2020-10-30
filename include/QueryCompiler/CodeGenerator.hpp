#pragma once

#include <memory>

#include <API/Schema.hpp>
#include <QueryCompiler/CCodeGenerator/Declarations/Declaration.hpp>
#include <QueryCompiler/CCodeGenerator/FileBuilder.hpp>
#include <QueryCompiler/CCodeGenerator/FunctionBuilder.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/UnaryOperatorStatement.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/Compiler/Compiler.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Join/LogicalJoinDefinition.hpp>

namespace NES::Windowing {

class LogicalWindowDefinition;
typedef std::shared_ptr<LogicalWindowDefinition> LogicalWindowDefinitionPtr;

}// namespace NES::Windowing
namespace NES {

class AttributeReference;
typedef std::shared_ptr<AttributeReference> AttributeReferencePtr;

class CodeGenerator;
typedef std::shared_ptr<CodeGenerator> CodeGeneratorPtr;

class PipelineContext;
typedef std::shared_ptr<PipelineContext> PipelineContextPtr;

class PipelineStage;
typedef std::shared_ptr<PipelineStage> PipelineStagePtr;

class ExecutablePipeline;
typedef std::shared_ptr<ExecutablePipeline> ExecutablePipelinePtr;

class Predicate;
typedef std::shared_ptr<Predicate> PredicatePtr;

class GeneratedCode;
typedef std::shared_ptr<GeneratedCode> GeneratedCodePtr;

class Schema;
typedef std::shared_ptr<Schema> SchemaPtr;

class CompilerTypesFactory;
typedef std::shared_ptr<CompilerTypesFactory> CompilerTypesFactoryPtr;

class UserAPIExpression;
typedef std::shared_ptr<UserAPIExpression> UserAPIExpressionPtr;

/**
 * @brief The code generator encapsulates the code generation for different operators.
 */
class CodeGenerator {
  public:
    CodeGenerator();

    /**
     * @brief Code generation for a scan, which depends on a particular input schema.
     * @param schema The input schema, in which we receive the input buffer.
     * @param context The context of the current pipeline.
     * @return flag if the generation was successful.
     */
    virtual bool generateCodeForScan(SchemaPtr schema, PipelineContextPtr context) = 0;

    /**
     * @brief Code generation for a filter operator, which depends on a particular filter predicate.
     * @param predicate The filter predicate, which selects input records.
     * @param context The context of the current pipeline.
     * @return flag if the generation was successful.
     */
    virtual bool generateCodeForFilter(PredicatePtr predicate, PipelineContextPtr context) = 0;

    /**
     * @brief Code generation for a map operator, which depends on a particular map predicate.
     * @param field The field, which we want to manipulate with the map predicate.
     * @param predicate The map predicate.
     * @param context The context of the current pipeline.
     * @return flag if the generation was successful.
     */
    virtual bool generateCodeForMap(AttributeFieldPtr field, UserAPIExpressionPtr pred, PipelineContextPtr context) = 0;

    /**
    * @brief Code generation for a emit, which depends on a particular output schema.
    * @param schema The output schema.
    * @param context The context of the current pipeline.
    * @return flag if the generation was successful.
    */
    virtual bool generateCodeForEmit(SchemaPtr schema, PipelineContextPtr context) = 0;

    /**
    * @brief Code generation for a central window operator, which depends on a particular window definition.
    * @param window The window definition, which contains all properties of the window.
    * @param context The context of the current pipeline.
    * @return flag if the generation was successful.
    */
    virtual bool generateCodeForCompleteWindow(Windowing::LogicalWindowDefinitionPtr window, PipelineContextPtr context) = 0;

    /**
   * @brief Code generation for a slice creation operator for distributed window operator, which depends on a particular window definition.
   * @param window The window definition, which contains all properties of the window.
   * @param context The context of the current pipeline.
   * @return flag if the generation was successful.
   */
    virtual bool generateCodeForSlicingWindow(Windowing::LogicalWindowDefinitionPtr window, PipelineContextPtr context) = 0;

    /**
    * @brief Code generation for a combiner operator for distributed window operator, which depends on a particular window definition.
    * @param window The window definition, which contains all properties of the window.
    * @param context The context of the current pipeline.
    * @return flag if the generation was successful.
    */
    virtual bool generateCodeForCombiningWindow(Windowing::LogicalWindowDefinitionPtr window, PipelineContextPtr context) = 0;

    /**
    * @brief Code generation for a join operator, which depends on a particular join definition
    * @param window The join definition, which contains all properties of the join.
    * @param context The context of the current pipeline.
    * @return flag if the generation was successful.
    */
    virtual bool generateCodeForJoin(Join::LogicalJoinDefinitionPtr joinDef, PipelineContextPtr context) = 0;

    /**
     * @brief Performs the actual compilation the generated code pipeline.
     * @param code generated code.
     * @return ExecutablePipelinePtr returns the compiled and executable pipeline.
     */
    virtual ExecutablePipelinePtr compile(GeneratedCodePtr code) = 0;

    virtual ~CodeGenerator();

    CompilerTypesFactoryPtr getTypeFactory();
};
}// namespace NES
