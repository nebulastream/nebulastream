#ifndef NES_INCLUDE_QUERYCOMPILER_COMPILEDEXECUTABLEPIPELINESTAGE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_COMPILEDEXECUTABLEPIPELINESTAGE_HPP_
#include <NodeEngine/Execution/ExecutablePipelineStage.hpp>

namespace NES {

/**
 * @brief The CompiledExecutablePipelineStage maintains a reference to an compiled ExecutablePipelineStage.
 * To this end, it ensures that the compiled code is correctly destructed.
 */
class CompiledExecutablePipelineStage : public NodeEngine::Execution::ExecutablePipelineStage {

  public:
    explicit CompiledExecutablePipelineStage(CompiledCodePtr compiledCode);
    static NodeEngine::Execution::ExecutablePipelineStagePtr create(CompiledCodePtr compiledCode);
    ~CompiledExecutablePipelineStage();

    uint32_t setup(NodeEngine::Execution::PipelineExecutionContext& pipelineExecutionContext) override;
    uint32_t start(NodeEngine::Execution::PipelineExecutionContext& pipelineExecutionContext) override;
    uint32_t open(NodeEngine::Execution::PipelineExecutionContext& pipelineExecutionContext, NodeEngine::WorkerContext& workerContext) override;
    uint32_t execute(TupleBuffer& inputTupleBuffer, NodeEngine::Execution::PipelineExecutionContext& pipelineExecutionContext,
                     NodeEngine::WorkerContext& workerContext) override;
    uint32_t close(NodeEngine::Execution::PipelineExecutionContext& pipelineExecutionContext, NodeEngine::WorkerContext& workerContext) override;
    uint32_t stop(NodeEngine::Execution::PipelineExecutionContext& pipelineExecutionContext) override;

  private:
    enum ExecutionStage { NotInitialized, Initialized, Running, Stopped };
    NodeEngine::Execution::ExecutablePipelineStagePtr executablePipelineStage;
    CompiledCodePtr compiledCode;
    std::mutex executionStageLock;
    std::atomic<ExecutionStage> currentExecutionStage;
};


}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_COMPILEDEXECUTABLEPIPELINESTAGE_HPP_
