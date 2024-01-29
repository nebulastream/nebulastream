//
// Created by ls on 09.09.23.
//

#ifndef NES_UNIKERNELPIPELINEEXECUTIONCONTEXT_HPP
#define NES_UNIKERNELPIPELINEEXECUTIONCONTEXT_HPP
#include "Runtime/TupleBuffer.hpp"
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/WorkerContext.hpp>
#include <UnikernelStage.hpp>
#include <Util/Logger/Logger.hpp>
#include <iomanip>
#include <utility>

namespace NES::Runtime::Execution {
class OperatorHandler;
}

extern NES::Runtime::BufferManagerPtr TheBufferManager;
extern NES::Runtime::WorkerContextPtr TheWorkerContext;
namespace NES::Unikernel {

class UnikernelPipelineExecutionContext {
    std::function<void(NES::Runtime::TupleBuffer&)> emitProxy = nullptr;
    std::function<void(size_t stageId, Runtime::QueryTerminationType type)> stopProxy = nullptr;
    std::function<NES::Runtime::Execution::OperatorHandler*(int)> getOperatorHandlerProxy = nullptr;
    size_t currentStageId;
    size_t nextStageId;
    size_t numberOfWorkerThreads;
    UnikernelPipelineExecutionContext(std::function<void(NES::Runtime::TupleBuffer&)> emitProxy,
                                      std::function<void(size_t stageId, Runtime::QueryTerminationType type)> stop,
                                      std::function<NES::Runtime::Execution::OperatorHandler*(int)> getOperatorHandlerProxy,
                                      size_t currentStageId,
                                      size_t nextStageId,
                                      size_t numberOfWorkerThreads)
        : emitProxy(std::move(emitProxy)), stopProxy(std::move(stop)),
          getOperatorHandlerProxy(std::move(getOperatorHandlerProxy)), currentStageId(currentStageId), nextStageId(nextStageId),
          numberOfWorkerThreads(numberOfWorkerThreads) {}

  public:
    template<typename T, typename Stage, size_t NumberOfWorkerThreads>
    static UnikernelPipelineExecutionContext create() {
        return UnikernelPipelineExecutionContext(&T::execute,
                                                 &T::stop,
                                                 &Stage::getOperatorHandler,
                                                 Stage::StageId,
                                                 T::StageId,
                                                 NumberOfWorkerThreads);
    }
    template<IsEmitablePipelineStage T, size_t NumberOfWorkerThreads>
    static UnikernelPipelineExecutionContext create() {
        return UnikernelPipelineExecutionContext(&T::execute, &T::stop, nullptr, 0, T::StageId, NumberOfWorkerThreads);
    }
    [[nodiscard]] NES::Runtime::Execution::OperatorHandler* getOperatorHandler(int index) const {
        NES_TRACE("getOperatorHandler for {}", currentStageId);
        auto opHandler = getOperatorHandlerProxy(index);
        NES_ASSERT2_FMT(opHandler, "Op Handler is Null");

        NES_TRACE("OpHandler for Stage {} @ {}", currentStageId, static_cast<void*>(opHandler));
        return opHandler;
    }

    static void printHexBuffer(const uint8_t* buffer, std::size_t bufferLength, std::stringstream& ss) {
        for (std::size_t i = 0; i < bufferLength; i += 16) {
            std::size_t chunkSize = std::min(static_cast<std::size_t>(16), bufferLength - i);
            const auto* chunk = buffer + i;
            ss << std::setw(8) << std::setfill('0') << std::hex << i << ": ";

            for (std::size_t j = 0; j < 16; ++j) {
                if (j < chunkSize) {
                    ss << std::setw(2) << std::setfill('0') << std::hex << static_cast<int>(static_cast<unsigned char>(chunk[j]))
                       << ' ';
                } else {
                    ss << "   ";// Three spaces for non-existent bytes
                }
            }

            ss << "  ";
            for (std::size_t j = 0; j < chunkSize; ++j) {
                char c = chunk[j];
                ss << (std::isprint(c) ? c : '.');
            }

            ss << std::endl;
        }
    }
    void dispatchBuffer(NES::Runtime::TupleBuffer& tb) const {
        NES_TRACE("Dispatching to {}", nextStageId);

        if constexpr (NES_COMPILE_TIME_LOG_LEVEL >= NES::getLogLevel(NES::LogLevel::LOG_TRACE)) {
            auto buffer = tb.getBuffer();
            std::stringstream ss;
            printHexBuffer(buffer, 32, ss);
            NES_TRACE("Buffer Content: \n{}", ss.str());
        }

        emitProxy(tb);
    }
    void emit(NES::Runtime::TupleBuffer& tb) const {
        NES_TRACE("Emitting to {}", nextStageId);

        if constexpr (NES_COMPILE_TIME_LOG_LEVEL >= NES::getLogLevel(NES::LogLevel::LOG_DEBUG)) {
            auto buffer = tb.getBuffer();
            std::stringstream ss;
            printHexBuffer(buffer, 32, ss);
            NES_TRACE("Buffer Content: \n{}", ss.str());
        }

        emitProxy(tb);
    }

    void stop(Runtime::QueryTerminationType type) const {
        NES_INFO("Stop was called from: {}", currentStageId);
        stopProxy(currentStageId, type);
    }

    NES::Runtime::BufferManagerPtr getBufferManager() { return TheBufferManager; }
    size_t getNumberOfWorkerThreads() { return numberOfWorkerThreads; }
};
}// namespace NES::Unikernel
#endif//NES_UNIKERNELPIPELINEEXECUTIONCONTEXT_HPP
