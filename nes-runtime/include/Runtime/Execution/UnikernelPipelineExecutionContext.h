//
// Created by ls on 09.09.23.
//

#ifndef NES_UNIKERNELPIPELINEEXECUTIONCONTEXT_H
#define NES_UNIKERNELPIPELINEEXECUTIONCONTEXT_H
#include "Runtime/TupleBuffer.hpp"
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger/Logger.hpp>
extern NES::Runtime::WorkerContextPtr the_workerContext;
namespace NES::Unikernel {

class UnikernelPipelineExecutionContextBase {
  public:
    virtual void execute(NES::Runtime::TupleBuffer& tupleBuffer) = 0;
    virtual void emit(NES::Runtime::TupleBuffer& tupleBuffer) = 0;
    virtual size_t getNumberOfWorkerThreads() { return 1; }
    virtual void setup() = 0;
    virtual void stop() = 0;

    virtual NES::Runtime::Execution::OperatorHandler& getOperatorHandler(uint8_t index) = 0;
};

template<typename Sink, typename Stage, typename... Stages>
class UnikernelPipelineExecutionContext : public UnikernelPipelineExecutionContextBase {
  public:
    UnikernelPipelineExecutionContext<Sink, Stages...> nextContext;

    void execute(Runtime::TupleBuffer& tupleBuffer) override {
        //      hexdump(tupleBuffer.getBuffer(), tupleBuffer.getBufferSize());
        NES_DEBUG("Executing Stage {}", typeid(Stage).name());
        Stage::execute(*static_cast<UnikernelPipelineExecutionContextBase*>(this), *the_workerContext, tupleBuffer);
    }

    void emit(NES::Runtime::TupleBuffer& tupleBuffer) override { nextContext.execute(tupleBuffer); }

    NES::Runtime::Execution::OperatorHandler& getOperatorHandler(uint8_t index) override {
        auto handlerPtr = Stage::getOperatorHandler(index);
        return *handlerPtr;
    }

    void stop() override {
        Stage::terminate(*static_cast<UnikernelPipelineExecutionContextBase*>(this), *the_workerContext);
        nextContext.stop();
    }

    void setup() override {
        assert(static_cast<UnikernelPipelineExecutionContextBase*>(this) == this);
        NES_DEBUG("This: {}, {}", (void*) this, (void*) static_cast<UnikernelPipelineExecutionContextBase*>(this));
        Stage::setup(*static_cast<UnikernelPipelineExecutionContextBase*>(this), *the_workerContext);
        nextContext.setup();
    }
};

template<typename Sink, typename Stage>
class UnikernelPipelineExecutionContext<Sink, Stage> : public UnikernelPipelineExecutionContextBase {
  public:
    Sink sink;

    void execute(Runtime::TupleBuffer& tupleBuffer) override {
        Stage::execute(*static_cast<UnikernelPipelineExecutionContextBase*>(this), *the_workerContext, tupleBuffer);
    }

    void emit(NES::Runtime::TupleBuffer& tupleBuffer) override { sink.writeBuffer(tupleBuffer); }

    NES::Runtime::Execution::OperatorHandler& getOperatorHandler(uint8_t index) override {
        return *Stage::getOperatorHandler(index);
    }

    void setup() override {
        Stage::setup(*static_cast<UnikernelPipelineExecutionContextBase*>(this), *the_workerContext);
        sink.setup();
    }

    void stop() override {
        Stage::terminate(*static_cast<UnikernelPipelineExecutionContextBase*>(this), *the_workerContext);
        sink.stop();
    }
};

}// namespace NES::Unikernel
#endif//NES_UNIKERNELPIPELINEEXECUTIONCONTEXT_H
