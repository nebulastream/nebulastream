#ifndef NES_INCLUDE_QUERYCOMPILER_PIPELINEEXECUTIONCONTEXT_HPP_
#define NES_INCLUDE_QUERYCOMPILER_PIPELINEEXECUTIONCONTEXT_HPP_
#include <memory>
#include <functional>
namespace NES {
class BufferManager;
typedef std::shared_ptr<BufferManager> BufferManagerPtr;

class TupleBuffer;
class PipelineExecutionContext {

  public:
    explicit PipelineExecutionContext(BufferManagerPtr bufferManager,
                                      std::function<void(TupleBuffer&)> emitFunction);

    TupleBuffer allocateTupleBuffer();

    void emitBuffer(TupleBuffer& outputBuffer);

  private:
    BufferManagerPtr bufferManager;
    std::function<void(TupleBuffer&)> emitFunction;
};

typedef std::unique_ptr<PipelineExecutionContext> QueryExecutionContextPtr;

}
#endif //NES_INCLUDE_QUERYCOMPILER_PIPELINEEXECUTIONCONTEXT_HPP_
