#ifndef NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_MLIR_MLIREXECUTABLE_HPP_
#define NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_MLIR_MLIREXECUTABLE_HPP_
#include <Nautilus/Backends/Executable.hpp>
#include <mlir/ExecutionEngine/ExecutionEngine.h>
namespace NES::Nautilus::Backends::MLIR {
class MLIRExecutable : public Executable {
  public:
    MLIRExecutable(std::unique_ptr<mlir::ExecutionEngine> engine);
  protected:
    void* getInvocableFunctionPtr(const std::string& member) override;
  private:
    std::unique_ptr<mlir::ExecutionEngine> engine;
};
}// namespace NES::Nautilus::Backends::MLIR
#endif//NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_MLIR_MLIREXECUTABLE_HPP_
