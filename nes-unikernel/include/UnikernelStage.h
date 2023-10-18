//
// Created by ls on 09.09.23.
//

#ifndef NES_UNIKERNELSTAGE_H
#define NES_UNIKERNELSTAGE_H
#include <cstdint>

namespace NES::Runtime {
namespace Execution {
class OperatorHandler;
}
class WorkerContext;
class TupleBuffer;
}// namespace NES::Runtime

namespace NES::Unikernel {
class UnikernelPipelineExecutionContextBase;
template<std::size_t stage_id>
class Stage {
  public:
    Stage() {}
    static NES::Runtime::Execution::OperatorHandler* getOperatorHandler(int index);
    static void execute(NES::Unikernel::UnikernelPipelineExecutionContextBase& context,
                        NES::Runtime::WorkerContext& workerContext,
                        NES::Runtime::TupleBuffer& buffer);
    static void setup(NES::Unikernel::UnikernelPipelineExecutionContextBase& context, NES::Runtime::WorkerContext& workerContext);
    static void terminate(NES::Unikernel::UnikernelPipelineExecutionContextBase& context,
                          NES::Runtime::WorkerContext& workerContext);
};
};    // namespace NES::Unikernel
#endif//NES_UNIKERNELSTAGE_H
