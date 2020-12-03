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

namespace NES{

class ExecutablePipelineStage {
  public:
   explicit ExecutablePipelineStage() = default;
   virtual uint32_t execute(TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext, WorkerContext& workerContext) = 0;
};

typedef std::shared_ptr<ExecutablePipelineStage> ExecutablePipelineStagePtr;

}

#endif//NES_INCLUDE_QUERYCOMPILER_EXECUTABLEPIPELINESTAGE_HPP_
