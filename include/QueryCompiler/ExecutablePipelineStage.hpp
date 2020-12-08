#ifndef NES_INCLUDE_QUERYCOMPILER_EXECUTABLEPIPELINESTAGE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_EXECUTABLEPIPELINESTAGE_HPP_
#include <NodeEngine/TupleBuffer.hpp>
#include <NodeEngine/WorkerContext.hpp>
#include <QueryCompiler/PipelineExecutionContext.hpp>
#include <State/StateVariable.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/Runtime/WindowManager.hpp>
#include <Windowing/Runtime/WindowSliceStore.hpp>
#include <Windowing/WindowHandler/AggregationWindowHandler.hpp>
#include <Windowing/WindowHandler/JoinHandler.hpp>

namespace NES {

class ExecutablePipelineStage {
  public:
    explicit ExecutablePipelineStage() = default;
    /**
    * @brief Must be called only once per executable pipeline and initializes the pipeline execution context.
    * e.g, creates the individual operator states -> window handler
    * @param pipelineExecutionContext
    * @return 0 if and error occurred.
    */
    virtual uint32_t setup(PipelineExecutionContext& pipelineExecutionContext);

    /**
    * @brief Must be called only once per executable pipeline and starts the executable pipeline.
    * e.g. starts the threads for the window handler.
    * @param pipelineExecutionContext
    * @return 0 if and error occurred.
    */
    virtual uint32_t start(PipelineExecutionContext& pipelineExecutionContext);

    /**
    * @brief Must be called exactly once per worker thread and initializes worker local state.
    * For instance a worker local aggregation state.
    * @param pipelineExecutionContext
    * @param workerContext
    * @return 0 if and error occurred.
    */
    virtual uint32_t open(PipelineExecutionContext& pipelineExecutionContext, WorkerContext& workerContext);

    /**
    * @brief Is called once per input buffer and performs the computation of each operator.
    * It can access the state in the PipelineExecutionContext and uns the WorkerContext to
    * identify the current worker thread.
    * @param inputTupleBuffer
    * @param pipelineExecutionContext
    * @param workerContext
    * @return 0 if and error occurred.
    */
    virtual uint32_t execute(TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext,
                             WorkerContext& workerContext) = 0;

    /**
     * @brief Must be called exactly once per worker thread to remove worker local state.
     * @param pipelineExecutionContext
     * @param workerContext
     * @return 0 if and error occurred.
     */
    virtual uint32_t close(PipelineExecutionContext& pipelineExecutionContext, WorkerContext& workerContext);

    /**
     * @brief Must be called exactly once per executable pipeline to remove operator state.
     * @param pipelineExecutionContext
     * @return 0 if and error occurred.
     */
    virtual uint32_t stop(PipelineExecutionContext& pipelineExecutionContext);
};

typedef std::shared_ptr<ExecutablePipelineStage> ExecutablePipelineStagePtr;

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_EXECUTABLEPIPELINESTAGE_HPP_
