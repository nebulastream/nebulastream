#ifndef NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_MLIR_MLIRCOMPILATIONBACKEND_HPP_
#define NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_MLIR_MLIRCOMPILATIONBACKEND_HPP_
#include <Nautilus/Backends/CompilationBackend.hpp>
namespace NES::Nautilus::Backends::MLIR {

class MLIRCompilationBackend : CompilationBackend {
  public:
    std::unique_ptr<Executable>  compile(std::shared_ptr<IR::IRGraph> ir) override;
};

}// namespace NES::Nautilus::Backends::MLIR

#endif//NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_MLIR_MLIRCOMPILATIONBACKEND_HPP_
