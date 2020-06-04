#ifndef INCLUDE_PIPELINESTAGE_H_
#define INCLUDE_PIPELINESTAGE_H_
#include <memory>
#include <vector>

namespace NES {
class TupleBuffer;
class PipelineStage;
typedef std::shared_ptr<PipelineStage> PipelineStagePtr;

template<class PartialAggregateType>
class WindowSliceStore;

class WindowHandler;
typedef std::shared_ptr<WindowHandler> WindowHandlerPtr;

class ExecutablePipeline;
typedef std::shared_ptr<ExecutablePipeline> ExecutablePipelinePtr;

class QueryExecutionPlan;
typedef std::shared_ptr<QueryExecutionPlan> QueryExecutionPlanPtr;

class WindowManager;

class PipelineExecutionContext;
typedef std::unique_ptr<PipelineExecutionContext> QueryExecutionContextPtr;

class PipelineStage {
  public:
    PipelineStage(
        uint32_t pipelineStageId,
        QueryExecutionPlanPtr queryExecutionPlan,
        ExecutablePipelinePtr executablePipeline,
        WindowHandlerPtr windowHandler);
    explicit PipelineStage(uint32_t pipelineStageId,
                           QueryExecutionPlanPtr queryExecutionPlan, ExecutablePipelinePtr executablePipeline);
    bool execute(TupleBuffer& inputBuffer,
                 PipelineExecutionContext& context);

    /**
   * @brief Initialises a pipeline stage
   * @return boolean if successful
   */
    bool setup();

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

    ~PipelineStage();
    PipelineStagePtr getNextStage();
    void setNextStage(PipelineStagePtr ptr);
    int getPipeStageId();

  private:
    uint32_t pipelineStageId;
    QueryExecutionPlanPtr queryExecutionPlan;
    ExecutablePipelinePtr executablePipeline;
    WindowHandlerPtr windowHandler;
    PipelineStagePtr nextStage;
    bool hasWindowHandler();
};
typedef std::shared_ptr<PipelineStage> PipelineStagePtr;

class CompiledCode;
typedef std::shared_ptr<CompiledCode> CompiledCodePtr;

PipelineStagePtr createPipelineStage(uint32_t pipelineStageId,
                                     const QueryExecutionPlanPtr& queryExecutionPlanPtr,
                                     const ExecutablePipelinePtr& compiled_code,
                                     const WindowHandlerPtr& window_handler);
PipelineStagePtr createPipelineStage(uint32_t pipelineStageId,
                                     const QueryExecutionPlanPtr& queryExecutionPlanPtr,
                                     const ExecutablePipelinePtr& compiled_code);

}// namespace NES

#endif /* INCLUDE_PIPELINESTAGE_H_ */
