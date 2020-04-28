#ifndef INCLUDE_QUERYCOMPILER_QUERYCOMPILER_HPP_
#define INCLUDE_QUERYCOMPILER_QUERYCOMPILER_HPP_
#include <memory>
#include <vector>
namespace NES {
class Dispatcher;
typedef std::shared_ptr<Dispatcher> DispatcherPtr;

class BufferManager;
typedef std::shared_ptr<BufferManager> BufferManagerPtr;


class QueryExecutionPlan;
typedef std::shared_ptr<QueryExecutionPlan> QueryExecutionPlanPtr;

class Operator;
typedef std::shared_ptr<Operator> OperatorPtr;

class QueryCompiler;
typedef std::shared_ptr<QueryCompiler> QueryCompilerPtr;

class CodeGenerator;
typedef std::shared_ptr<CodeGenerator> CodeGeneratorPtr;

class PipelineStage;
typedef std::shared_ptr<PipelineStage> PipelineStagePtr;

class PipelineContext;
typedef std::shared_ptr<PipelineContext> PipelineContextPtr;

/**
 * @brief The query compiler compiles physical query plans to an executable query plan
 */
class QueryCompiler {
  public:
    QueryCompiler(QueryCompiler* queryCompiler);
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
    QueryExecutionPlanPtr compile(OperatorPtr queryPlan);

    void setDispatcher(DispatcherPtr dispatcher);
    void setBufferManager(BufferManagerPtr bufferManager);

  private:
    DispatcherPtr dispatcher;
    BufferManagerPtr bufferManager;

    void compilePipelineStages(QueryExecutionPlanPtr queryExecutionPlan,
                               CodeGeneratorPtr codeGenerator,
                               PipelineContextPtr context);
    QueryCompiler();
};

QueryCompilerPtr createDefaultQueryCompiler(DispatcherPtr dispatcher);

}

#endif //INCLUDE_QUERYCOMPILER_QUERYCOMPILER_HPP_
