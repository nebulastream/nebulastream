#include <Nautilus/Backends/MLIR/LLVMIROptimizer.hpp>
#include <Nautilus/Backends/MLIR/MLIRCompilationBackend.hpp>
#include <Nautilus/Backends/MLIR/MLIRExecutable.hpp>
#include <Nautilus/Backends/MLIR/MLIRLoweringProvider.hpp>
#include <Nautilus/Backends/MLIR/MLIRPassManager.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Timer.hpp>
#include <mlir/IR/MLIRContext.h>

namespace NES::Nautilus::Backends::MLIR {
MLIRExecutable::MLIRExecutable(std::unique_ptr<mlir::ExecutionEngine> engine) : engine(std::move(engine)) {}

void* MLIRExecutable::getInvocableFunctionPtr(const std::string& member) {
    return engine->lookup(member).get();
}
}// namespace NES::Nautilus::Backends::MLIR