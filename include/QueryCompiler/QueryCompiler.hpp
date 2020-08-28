#ifndef INCLUDE_QUERYCOMPILER_QUERYCOMPILER_HPP_
#define INCLUDE_QUERYCOMPILER_QUERYCOMPILER_HPP_
#include <QueryCompiler/GeneratedQueryExecutionPlanBuilder.hpp>
#include <memory>
#include <vector>
namespace NES {
class QueryManager;
typedef std::shared_ptr<QueryManager> QueryManagerPtr;

class BufferManager;
typedef std::shared_ptr<BufferManager> BufferManagerPtr;

class QueryExecutionPlan;
typedef std::shared_ptr<QueryExecutionPlan> QueryExecutionPlanPtr;

class QueryCompiler;
typedef std::shared_ptr<QueryCompiler> QueryCompilerPtr;

class CodeGenerator;
typedef std::shared_ptr<CodeGenerator> CodeGeneratorPtr;

class PipelineStage;
typedef std::shared_ptr<PipelineStage> PipelineStagePtr;

class PipelineContext;
typedef std::shared_ptr<PipelineContext> PipelineContextPtr;

class GeneratableOperator;
typedef std::shared_ptr<GeneratableOperator> GeneratableOperatorPtr;

/**
 * @brief The query compiler compiles physical query plans to an executable query plan
 */
class QueryCompiler {
  public:
    QueryCompiler();
    ~QueryCompiler() = default;
    /**
     * Creates a new query compiler
     * @param codeGenerator
     * @return
     */
    static QueryCompilerPtr create();
    /**
     * @brief compiles a queryplan to a executable query plan
     * @param queryPlan
     * @return
     */
    void compile(GeneratedQueryExecutionPlanBuilder&, OperatorNodePtr queryPlan);

  private:
    void compilePipelineStages(
        GeneratedQueryExecutionPlanBuilder& queryExecutionPlanBuilder,
        CodeGeneratorPtr codeGenerator,
        PipelineContextPtr context);
};

QueryCompilerPtr createDefaultQueryCompiler();

}// namespace NES

#endif//INCLUDE_QUERYCOMPILER_QUERYCOMPILER_HPP_
