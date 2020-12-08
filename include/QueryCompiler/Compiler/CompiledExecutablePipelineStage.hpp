#ifndef NES_INCLUDE_QUERYCOMPILER_COMPILEDEXECUTABLEPIPELINESTAGE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_COMPILEDEXECUTABLEPIPELINESTAGE_HPP_
#include <NodeEngine/Execution/ExecutablePipelineStage.hpp>

namespace NES {

/**
 * @brief The CompiledExecutablePipelineStage maintains a reference to an compiled ExecutablePipelineStage.
 * To this end, it ensures that the compiled code is correctly destructed.
 */
class CompiledExecutablePipelineStage : public ExecutablePipelineStage {

  public:
    explicit CompiledExecutablePipelineStage(CompiledCodePtr compiledCode);
    static ExecutablePipelineStagePtr create(CompiledCodePtr compiledCode);
    ~CompiledExecutablePipelineStage();

    uint32_t setup(PipelineExecutionContext& pipelineExecutionContext) override;
    uint32_t start(PipelineExecutionContext& pipelineExecutionContext) override;
    uint32_t open(PipelineExecutionContext& pipelineExecutionContext, WorkerContext& workerContext) override;
    uint32_t execute(TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext,
                     WorkerContext& workerContext) override;
    uint32_t close(PipelineExecutionContext& pipelineExecutionContext, WorkerContext& workerContext) override;
    uint32_t stop(PipelineExecutionContext& pipelineExecutionContext) override;

  private:
    enum ExecutionStage { NotInitialized, Initialized, Running, Stopped };
    ExecutablePipelineStagePtr executablePipelineStage;
    CompiledCodePtr compiledCode;
    std::mutex executionStageLock;
    std::atomic<ExecutionStage> currentExecutionStage;
};

typedef std::shared_ptr<ExecutablePipelineStage> ExecutablePipelineStagePtr;

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_COMPILEDEXECUTABLEPIPELINESTAGE_HPP_
