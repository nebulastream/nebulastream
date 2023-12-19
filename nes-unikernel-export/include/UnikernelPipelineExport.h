#ifndef NES_UNIKERNELPIPELINEEXPORT_H
#define NES_UNIKERNELPIPELINEEXPORT_H

#include <Execution/Pipelines/CompiledExecutablePipelineStage.hpp>
#include <Identifiers.hpp>
#include <OperatorHandlerTracer.hpp>

namespace llvm {
class LLVMContext;
class Module;
}// namespace llvm

namespace NES::Unikernel {

class UnikernelPipelineExport {
  public:
    UnikernelPipelineExport() = default;

    ~UnikernelPipelineExport() = default;

    void exportPipelineIntoModule(llvm::Module& module,
                                  PipelineId pipelineId,
                                  NES::Runtime::Execution::CompiledExecutablePipelineStage* stage);
};

}// namespace NES::Unikernel
#endif//NES_UNIKERNELPIPELINEEXPORT_H
