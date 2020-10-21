#ifndef INCLUDE_PIPELINESTAGE_H_
#define INCLUDE_PIPELINESTAGE_H_
#include <QueryCompiler/CodeGenerator.hpp>
#include <NodeEngine/WorkerContext.hpp>
#include <Plans/Query/QuerySubPlanId.hpp>
#include <QueryCompiler/QueryExecutionPlan.hpp>
#include <Windowing/WindowingForwardRefs.hpp>
#include <memory>
#include <vector>

namespace NES {
class TupleBuffer;
class PipelineStage;
typedef std::shared_ptr<PipelineStage> PipelineStagePtr;

class ExecutablePipeline;
typedef std::shared_ptr<ExecutablePipeline> ExecutablePipelinePtr;

class QueryExecutionPlan;
typedef std::shared_ptr<QueryExecutionPlan> QueryExecutionPlanPtr;

class QueryManager;
typedef std::shared_ptr<QueryManager> QueryManagerPtr;

class PipelineExecutionContext;
typedef std::shared_ptr<PipelineExecutionContext> QueryExecutionContextPtr;

typedef WorkerContext& WorkerContextRef;

class PipelineStage {
  public:
    PipelineStage(
        uint32_t pipelineStageId,
        QuerySubPlanId qepId,
        ExecutablePipelinePtr executablePipeline,
        QueryExecutionContextPtr pipelineContext,
        PipelineStagePtr nextPipelineStage,
        Windowing::WindowHandlerPtr windowHandler = Windowing::WindowHandlerPtr());

    /**
     * @brief Execute a pipeline stage
     * @param inputBuffer
     * @param workerContext
     * @return true if no error occurred
     */
    bool execute(TupleBuffer& inputBuffer, WorkerContextRef workerContext);

    /**
   * @brief Initialises a pipeline stage
   * @return boolean if successful
   */
    bool setup(QueryManagerPtr queryManager, BufferManagerPtr bufferManager);

    /**
     * @brief Starts a pipeline stage
     * @return boolean if successful
     */
    bool start();

    /**
     * @brief Stops pipeline stage
     * @return
     */
    bool stop();

    /**
     * @brief Get next pipeline stage
     * @return
     */
    PipelineStagePtr getNextStage();

    /**
    * @brief Get id of pipeline stage
    * @return
    */
    uint32_t getPipeStageId();

    QuerySubPlanId getQepParentId() const;

    ~PipelineStage() = default;

    /**
    * @return returns true if the pipeline contains a function pointer for a reconfiguration task
    */
    bool isReconfiguration() const;

  public:
    static PipelineStagePtr create(uint32_t pipelineStageId,
                                   const QuerySubPlanId querySubPlanId,
                                   const ExecutablePipelinePtr compiledCode,
                                   QueryExecutionContextPtr pipelineContext,
                                   const PipelineStagePtr nextPipelineStage,
                                   const Windowing::WindowHandlerPtr& windowHandler = Windowing::WindowHandlerPtr());

  private:
    uint32_t pipelineStageId;
    QuerySubPlanId qepId;
    ExecutablePipelinePtr executablePipeline;
    Windowing::WindowHandlerPtr windowHandler;
    PipelineStagePtr nextStage;
    QueryExecutionContextPtr pipelineContext;

  public:
    bool hasWindowHandler();
};
typedef std::shared_ptr<PipelineStage> PipelineStagePtr;

class CompiledCode;
typedef std::shared_ptr<CompiledCode> CompiledCodePtr;

}// namespace NES

#endif /* INCLUDE_PIPELINESTAGE_H_ */
