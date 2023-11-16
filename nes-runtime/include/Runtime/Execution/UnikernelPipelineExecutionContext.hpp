//
// Created by ls on 09.09.23.
//

#ifndef NES_UNIKERNELPIPELINEEXECUTIONCONTEXT_HPP
#define NES_UNIKERNELPIPELINEEXECUTIONCONTEXT_HPP
#include <UnikernelStage.hpp>
#include "Runtime/TupleBuffer.hpp"
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>

namespace NES::Runtime::Execution {
class OperatorHandler;
}

extern NES::Runtime::BufferManagerPtr TheBufferManager;
extern NES::Runtime::WorkerContextPtr TheWorkerContext;
namespace NES::Unikernel {

class UnikernelPipelineExecutionContext {
    std::function<void(NES::Runtime::TupleBuffer&)> emitProxy;
    std::function<NES::Runtime::Execution::OperatorHandler*(int)> getOperatorHandlerProxy;
    size_t currentStageId;
    size_t nextStageId;
    UnikernelPipelineExecutionContext(std::function<void(NES::Runtime::TupleBuffer&)> emitProxy,
                                      std::function<NES::Runtime::Execution::OperatorHandler*(int)> getOperatorHandlerProxy,
                                      size_t currentStageId,
                                      size_t nextStageId)
        : emitProxy(std::move(emitProxy)), getOperatorHandlerProxy(std::move(getOperatorHandlerProxy)),
          currentStageId(currentStageId), nextStageId(nextStageId) {}

  public:
    template<typename T, typename Stage>
    static UnikernelPipelineExecutionContext create() {
        return UnikernelPipelineExecutionContext(&T::execute, &Stage::getOperatorHandler, Stage::StageId, T::StageId);
    }
    template<IsEmitablePipelineStage T>
    static UnikernelPipelineExecutionContext create() {
        return UnikernelPipelineExecutionContext(&T::execute, nullptr, 0, T::StageId);
    }
    [[nodiscard]] NES::Runtime::Execution::OperatorHandler* getOperatorHandler(int index) const {
        NES_INFO("getOperatorHandler for {}", currentStageId);
        auto opHandler = getOperatorHandlerProxy(index);
        NES_ASSERT2_FMT(opHandler, "Op Handler is Null");

        NES_INFO("OpHandler for Stage {} @ {}", currentStageId, static_cast<void*>(opHandler));
        return opHandler;
    }
    void dispatchBuffer(NES::Runtime::TupleBuffer& tb) const {
        NES_INFO("Dispatching to {}", nextStageId);
        emitProxy(tb);
    }
    void emit(NES::Runtime::TupleBuffer& tb) const {
        NES_INFO("Emitting to {}", nextStageId);
        emitProxy(tb);
    }
    NES::Runtime::BufferManagerPtr getBufferManager() { return TheBufferManager; }
    size_t getNumberOfWorkerThreads() { return 1; }
};
}// namespace NES::Unikernel
#endif//NES_UNIKERNELPIPELINEEXECUTIONCONTEXT_HPP
