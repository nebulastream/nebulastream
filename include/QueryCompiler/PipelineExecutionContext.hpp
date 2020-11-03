#ifndef NES_INCLUDE_QUERYCOMPILER_PIPELINEEXECUTIONCONTEXT_HPP_
#define NES_INCLUDE_QUERYCOMPILER_PIPELINEEXECUTIONCONTEXT_HPP_
#include <Plans/Query/QuerySubPlanId.hpp>
#include <Windowing/WindowingForwardRefs.hpp>
#include <Windowing/Runtime/WindowHandlerImpl.hpp>
#include <functional>
#include <memory>

namespace NES {
class WorkerContext;
class BufferManager;
typedef std::shared_ptr<BufferManager> BufferManagerPtr;

class Schema;
typedef std::shared_ptr<Schema> SchemaPtr;

typedef WorkerContext& WorkerContextRef;

class Schema;
typedef std::shared_ptr<Schema> SchemaPtr;

class TupleBuffer;

// TODO Philipp, please clarify if we should introduce WindowManager, StateVars, etc... here
// TODO so that we have one central point that the compiled code uses to access runtime

/**
 * @brief The PipelineExecutionContext is passed to a compiled pipeline and offers basic functionality to interact with the runtime.
 * Via the context, the compile code is able to allocate new TupleBuffers and to emit tuple buffers to the runtime.
 */
class PipelineExecutionContext {

  public:
    /**
     * @brief The PipelineExecutionContext is passed to the compiled pipeline and enables interaction with the NES runtime.
     * @param bufferManager a reference to the buffer manager to enable allocation from within the pipeline
     * @param emitFunctionHandler an handler to receive the emitted buffers from the pipeline.
     */
    explicit PipelineExecutionContext(
        QuerySubPlanId queryId,
        BufferManagerPtr bufferManager,
        std::function<void(TupleBuffer&, WorkerContextRef)>&& emitFunctionHandler,
        Windowing::WindowHandlerPtr windowHandler = Windowing::WindowHandlerPtr());

    /**
     * @brief Allocates a new tuple buffer.
     * @return TupleBuffer
     */
    TupleBuffer allocateTupleBuffer();

    /**
     * @brief Emits a output tuple buffer to the runtime. Internally we call the emit function which is a callback to the correct handler.
     * @param outputBuffer the output tuple buffer that is passed to the runtime
     */
    void emitBuffer(TupleBuffer& outputBuffer, WorkerContext&);

    /**
     * @brief getter/setter window definition
     * @return
     */
    Windowing::LogicalWindowDefinitionPtr getWindowDef();
    void setWindowDef(Windowing::LogicalWindowDefinitionPtr windowDef);


    /**
     * @brief
     */
    Windowing::WindowHandlerPtr getWindowHandler() {
        return windowHandler;
    }

    template<class KeyType, class InputType, class PartialAggregateType, class FinalAggregateType>
    auto getWindowHandler() {
        return std::dynamic_pointer_cast<Windowing::WindowHandlerImpl<KeyType, InputType, PartialAggregateType, FinalAggregateType>>(windowHandler);
    }


    // TODO remove above

    /**
     * @brief getter/setter input schema
     * @return
     */
    SchemaPtr getInputSchema();
    void setInputSchema(SchemaPtr inputSchema);

  private:
    /**
     * @brief Id of the local qep that owns the pipeline
     */
    QuerySubPlanId queryId;

    /**
     * @brief Reference to the buffer manager to enable buffer allocation
     */
    BufferManagerPtr bufferManager;
    /**
     * @brief The emit function handler to react on an emitted tuple buffer.
     */
    std::function<void(TupleBuffer&, WorkerContext&)> emitFunctionHandler;

    Windowing::WindowHandlerPtr windowHandler;

    // TODO remove this stuff from here
    Windowing::LogicalWindowDefinitionPtr windowDef;
    SchemaPtr inputSchema;


};

}// namespace NES
#endif//NES_INCLUDE_QUERYCOMPILER_PIPELINEEXECUTIONCONTEXT_HPP_
