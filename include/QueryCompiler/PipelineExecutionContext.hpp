#ifndef NES_INCLUDE_QUERYCOMPILER_PIPELINEEXECUTIONCONTEXT_HPP_
#define NES_INCLUDE_QUERYCOMPILER_PIPELINEEXECUTIONCONTEXT_HPP_
#include <functional>
#include <memory>
namespace NES {
class BufferManager;
typedef std::shared_ptr<BufferManager> BufferManagerPtr;

class WindowDefinition;
typedef std::shared_ptr<WindowDefinition> WindowDefinitionPtr;

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
        BufferManagerPtr bufferManager,
        std::function<void(TupleBuffer&)>&& emitFunctionHandler);

    /**
     * @brief Allocates a new tuple buffer.
     * @return TupleBuffer
     */
    TupleBuffer allocateTupleBuffer();

    /**
     * @brief Emits a output tuple buffer to the runtime. Internally we call the emit function which is a callback to the correct handler.
     * @param outputBuffer the output tuple buffer that is passed to the runtime
     */
    void emitBuffer(TupleBuffer& outputBuffer);

    /**
     * @brief getter/setter window definition
     * @return
     */
    WindowDefinitionPtr getWindowDef();
    void setWindowDef(WindowDefinitionPtr windowDef);

    /**
     * @brief getter/setter input schema
     * @return
     */
    SchemaPtr getInputSchema();
    void setInputSchema(SchemaPtr inputSchema);

  private:
    /**
     * @brief Reference to the buffer manager to enable buffer allocation
     */
    BufferManagerPtr bufferManager;
    /**
     * @brief The emit function handler to react on an emitted tuple buffer.
     */
    std::function<void(TupleBuffer&)> emitFunctionHandler;

    WindowDefinitionPtr windowDef;
    SchemaPtr inputSchema;
};

}// namespace NES
#endif//NES_INCLUDE_QUERYCOMPILER_PIPELINEEXECUTIONCONTEXT_HPP_
